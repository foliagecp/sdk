package cache

// A gate for tiering: with tiering off and on, Store must answer the same.
// The test goes only through public methods — that is, it checks exactly
// what CRUD and JPGQL see.

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"sync/atomic"
)

// storeOp is one operation on the cache that applies to any Store.
type storeOp func(cs *Store)

// storeQuery is one question to the cache. The answer is reduced to a string
// to compare two stores without knowing how they are built.
type storeQuery func(cs *Store) string

func randomOps(rng *rand.Rand, vertices, links, steps int) ([]storeOp, []storeQuery) {
	vid := func(i int) string { return fmt.Sprintf("dom/v-%03d", i) }
	lname := func(i int) string { return fmt.Sprintf("l%03d", i) }
	ltype := func(i int) string { return fmt.Sprintf("t%d", i%4) }
	tgt := func(i int) string { return fmt.Sprintf("dom/tgt-%03d", i%17) }

	var ops []storeOp
	now := int64(1_000_000)
	for step := 0; step < steps; step++ {
		now++
		t := now
		v := vid(rng.Intn(vertices))
		n := rng.Intn(links)
		name, lt, to := lname(n), ltype(n), tgt(n)

		switch rng.Intn(10) {
		case 0: // vertex body
			body := easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(rng.Intn(100)))
			ops = append(ops, func(cs *Store) { cs.SetValueJSON(v, &body, false, t) })
		case 1: // delete the body
			ops = append(ops, func(cs *Store) { cs.DeleteValue(v, false, t) })
		case 2, 3, 4: // create a whole link, the way CRUD does
			ops = append(ops, func(cs *Store) {
				cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte(lt+"."+to), false, t)
				cs.SetValue(fmt.Sprintf(kOutBody, v, name), []byte(`{"w":1}`), false, t)
				cs.SetValue(fmt.Sprintf(kLinkType, v, lt, to), []byte(name), false, t)
				cs.SetValue(fmt.Sprintf(kIndexType, v, name, lt), nil, false, t)
				cs.SetValue(fmt.Sprintf(kIndexTag, v, name, "tag0"), nil, false, t)
				cs.SetValue(fmt.Sprintf(kInLink, to, v, name), []byte(lt), false, t)
			})
		case 5: // delete a whole link
			ops = append(ops, func(cs *Store) {
				for _, k := range cs.GetKeysByPattern(fmt.Sprintf("%s.out.index.%s.>", v, name)) {
					cs.DeleteValue(k, false, t)
				}
				cs.DeleteValue(fmt.Sprintf(kLinkType, v, lt, to), false, t)
				cs.DeleteValue(fmt.Sprintf(kOutBody, v, name), false, t)
				cs.DeleteValue(fmt.Sprintf(kOutTo, v, name), false, t)
				cs.DeleteValue(fmt.Sprintf(kInLink, to, v, name), false, t)
			})
		case 6: // add a tag
			tag := fmt.Sprintf("tag%d", rng.Intn(3))
			ops = append(ops, func(cs *Store) {
				cs.SetValue(fmt.Sprintf(kIndexTag, v, name, tag), nil, false, t)
			})
		case 7: // remove a tag
			tag := fmt.Sprintf("tag%d", rng.Intn(3))
			ops = append(ops, func(cs *Store) {
				cs.DeleteValue(fmt.Sprintf(kIndexTag, v, name, tag), false, t)
			})
		case 8: // a late write — both must reject it the same way
			ops = append(ops, func(cs *Store) {
				cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte("stale.dom/stale"), false, t-1000)
			})
		case 9: // a runtime key — must stay in the tree in both modes
			ops = append(ops, func(cs *Store) {
				body := easyjson.NewJSONObjectWithKeyValue("rev", easyjson.NewJSON(t))
				cs.SetValueJSON(v+"-lock", &body, false, t)
				ctxKey := fmt.Sprintf("functions.cmdb.api.object.update.%s", v)
				cs.SetValueJSON(ctxKey, &body, false, t)
			})
		}
	}

	var qs []storeQuery
	for i := 0; i < vertices; i++ {
		v := vid(i)
		qs = append(qs,
			func(cs *Store) string { return fmt.Sprintf("exists(%s)=%v", v, cs.Exists(v)) },
			func(cs *Store) string { return fmt.Sprintf("existsJson(%s)=%v", v, cs.ExistsJson(v)) },
			func(cs *Store) string { return fmt.Sprintf("time(%s)=%d", v, cs.GetValueUpdateTime(v)) },
			func(cs *Store) string { return "body(" + v + ")=" + getStr(cs, v) },
			func(cs *Store) string { return "lock=" + getStr(cs, v+"-lock") },
			func(cs *Store) string {
				return "ctx=" + getStr(cs, "functions.cmdb.api.object.update."+v)
			},
		)
		for _, pat := range []string{".out.to.>", ".out.body.>", ".out.index.>", ".ltype.>", ".in.>", ".out.to.*"} {
			pattern := v + pat
			qs = append(qs, func(cs *Store) string {
				keys := cs.GetKeysByPattern(pattern)
				sort.Strings(keys)
				return pattern + "=" + fmt.Sprint(keys)
			})
		}
		for n := 0; n < links; n++ {
			name, lt, to := lname(n), ltype(n), tgt(n)
			for _, key := range []string{
				fmt.Sprintf(kOutTo, v, name),
				fmt.Sprintf(kOutBody, v, name),
				fmt.Sprintf(kLinkType, v, lt, to),
				fmt.Sprintf(kIndexType, v, name, lt),
				fmt.Sprintf(kIndexTag, v, name, "tag0"),
				fmt.Sprintf(kInLink, v, vid(0), name),
			} {
				k := key
				qs = append(qs, func(cs *Store) string {
					return fmt.Sprintf("%s|%v|%d|%s", k, cs.Exists(k), cs.GetValueUpdateTime(k), getStr(cs, k))
				})
			}
		}
	}
	return ops, qs
}

func getStr(cs *Store, key string) string {
	v, err := cs.GetValue(key)
	if err != nil {
		return "<absent>"
	}
	return string(v)
}

func runOps(t *testing.T, mode string, ops []storeOp, qs []storeQuery) []string {
	t.Helper()
	restore := SetCacheModeForTest(mode)
	defer restore()

	cs := NewStoreForTest("tiering")
	for _, op := range ops {
		op(cs)
	}
	out := make([]string, len(qs))
	for i, q := range qs {
		out[i] = q(cs)
	}
	return out
}

// Test_Tiering_SameAnswers — the main test: tiering changes nothing seen from
// outside.
func Test_Tiering_SameAnswers(t *testing.T) {
	for _, cfg := range []struct{ vertices, links, steps int }{
		{3, 4, 200},
		{5, 20, 2000},
		{2, 60, 4000},
	} {
		t.Run(fmt.Sprintf("v=%d/l=%d", cfg.vertices, cfg.links), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(cfg.steps)))
			ops, qs := randomOps(rng, cfg.vertices, cfg.links, cfg.steps)

			off := runOps(t, "tree", ops, qs)
			on := runOps(t, "records", ops, qs)

			require.Equal(t, len(off), len(on))
			diffs := 0
			for i := range off {
				if off[i] != on[i] {
					if diffs < 10 {
						t.Errorf("mismatch:\n  tree:   %s\n  record: %s", off[i], on[i])
					}
					diffs++
				}
			}
			require.Zero(t, diffs, "total mismatches: %d of %d queries", diffs, len(off))
		})
	}
}

// Test_Tiering_RuntimeKeysStayInTree — contexts and object mutexes do not
// become records: they are short-lived and hot, and the record format does
// not describe them.
func Test_Tiering_RuntimeKeysStayInTree(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("runtime")
	body := easyjson.NewJSONObjectWithKeyValue("a", easyjson.NewJSON(1))

	cs.SetValueJSON("dom/v", &body, false, 1)                      // vertex
	cs.SetValueJSON("dom/v-lock", &body, false, 1)                 // object mutex
	cs.SetValueJSON("functions.cmdb.api.x.dom/v", &body, false, 1) // context

	require.Equal(t, 1, cs.RecordCountForTest(), "only the vertex must become a record")
	require.True(t, cs.ExistsJson("dom/v-lock"))
	require.True(t, cs.ExistsJson("functions.cmdb.api.x.dom/v"))
	require.True(t, cs.ExistsJson("dom/v"))
}

// Test_Tiering_CompactionKeepsAnswers — compaction from the maintenance pass
// changes nothing in the answers.
func Test_Tiering_CompactionKeepsAnswers(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	rng := rand.New(rand.NewSource(77))
	ops, qs := randomOps(rng, 4, 30, 3000)

	cs := NewStoreForTest("compact")
	for _, op := range ops {
		op(cs)
	}
	before := make([]string, len(qs))
	for i, q := range qs {
		before[i] = q(cs)
	}

	n := cs.compactRecords()
	require.Greater(t, n, 0, "the writes must have left decoded buckets behind")

	for i, q := range qs {
		require.Equal(t, before[i], q(cs), "compaction changed an answer")
	}
	require.Zero(t, cs.compactRecords(), "a second compaction must find nothing")
}

// Test_Tiering_ConcurrentWrites — concurrent writes through the public API.
func Test_Tiering_ConcurrentWrites(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("conc")
	const goroutines, per = 32, 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := g*per + i
				name := fmt.Sprintf("l%05d", id)
				now := int64(1000 + id)
				cs.SetValue(fmt.Sprintf(kOutTo, "dom/hub", name), []byte("t."+fmt.Sprintf("dom/o%05d", id)), false, now)
				cs.SetValue(fmt.Sprintf(kIndexType, "dom/hub", name, "t"), nil, false, now)
			}
		}(g)
	}
	wg.Wait()

	for id := 0; id < goroutines*per; id++ {
		name := fmt.Sprintf("l%05d", id)
		key := fmt.Sprintf(kOutTo, "dom/hub", name)
		require.True(t, cs.Exists(key), "link %s is lost", name)
		v, err := cs.GetValue(key)
		require.NoError(t, err)
		require.Equal(t, "t."+fmt.Sprintf("dom/o%05d", id), string(v))
	}
	keys := cs.GetKeysByPattern("dom/hub.out.to.>")
	require.Len(t, keys, goroutines*per, "the listing does not match the number of writes")
}

// Test_Tiering_TreeStaysEmpty — a vertex that has moved into a record must
// not also grow a subtree: otherwise memory is not saved but spent twice.
func Test_Tiering_TreeStaysEmpty(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("empty")
	now := int64(1_000_000)
	for i := 0; i < 200; i++ {
		v := fmt.Sprintf("dom/v-%03d", i)
		body := easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(i))
		cs.SetValueJSON(v, &body, false, now)
		for k := 0; k < 5; k++ {
			name := fmt.Sprintf("l%02d", k)
			cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte("t.dom/o"), false, now)
			cs.SetValue(fmt.Sprintf(kIndexType, v, name, "t"), nil, false, now)
			cs.SetValue(fmt.Sprintf(kInLink, v, "dom/src", name), []byte("t"), false, now)
		}
	}

	require.Equal(t, 200, cs.RecordCountForTest())
	st := cs.StatsForTest()
	require.LessOrEqual(t, st.TotalNodes, 2,
		"the tree must stay empty: %d nodes for 200 vertices held in records", st.TotalNodes)

	// and service keys are still in the tree
	body := easyjson.NewJSONObjectWithKeyValue("rev", easyjson.NewJSON(1))
	cs.SetValueJSON("dom/v-000-lock", &body, false, now)
	st = cs.StatsForTest()
	require.Greater(t, st.TotalNodes, 1, "the object mutex must live in the tree")
}

// Test_Rehydrate_ClearsRecords — a promotion from passive to active replaces
// the whole world: what was deleted while the node was silent must not come
// back to life.
func Test_Rehydrate_ClearsRecords(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("rehydrate")
	cs.SetValue("dom/v.out.to.l1", []byte("t.dom/x"), false, 10)
	body, _ := easyjson.JSONFromString(`{"n":1}`)
	cs.SetValueJSON("dom/v", &body, false, 10)
	require.True(t, cs.Exists("dom/v.out.to.l1"))
	require.Equal(t, 1, cs.RecordCountForTest())

	cs.records.reset()

	require.Zero(t, cs.RecordCountForTest(), "the records must have been emptied")
	require.False(t, cs.Exists("dom/v.out.to.l1"), "the link survived the reset")
	require.False(t, cs.Exists("dom/v"), "the body survived the reset")

	// and after the reset the world is built again
	cs.SetValue("dom/v.out.to.l2", []byte("t.dom/y"), false, 20)
	require.True(t, cs.Exists("dom/v.out.to.l2"))
	require.False(t, cs.Exists("dom/v.out.to.l1"))
}

// Test_GetValueJSON_DistinguishesAbsentFromInvalid — "no key" and "present,
// but not JSON" are different answers, and a record must tell them apart the
// same way the tree does.
func Test_GetValueJSON_DistinguishesAbsentFromInvalid(t *testing.T) {
	for _, mode := range []string{"tree", "records"} {
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()

			cs := NewStoreForTest("jsonerr")
			cs.SetValue("dom/v", []byte("не json"), false, 10)

			_, err := cs.GetValueJSON("dom/v")
			require.ErrorContains(t, err, "not valid JSON")

			_, err = cs.GetValueJSON("dom/нет")
			require.ErrorContains(t, err, "does not exist")
		})
	}
}

// Test_RecordGauges_ReportWhatIsHeld — the metrics must show what is actually
// held in the cache, and change along with it. Without that, the switched
// default is invisible in production.
func Test_RecordGauges_ReportWhatIsHeld(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("gauges")
	require.Equal(t, recordStats{}, cs.recordStats(), "an empty cache must report zeros")

	body, _ := easyjson.JSONFromString(`{"name":"узел","cpu":8}`)
	const vertices = 40
	for i := 0; i < vertices; i++ {
		id := fmt.Sprintf("dom/v-%03d", i)
		require.True(t, cs.SetValueJSON(id, &body, false, 1))
		for l := 0; l < 3; l++ {
			cs.SetValue(fmt.Sprintf("%s.out.to.l%d", id, l), []byte("t.dom/x"), false, 1)
		}
	}

	st := cs.recordStats()
	require.Equal(t, vertices, st.vertices, "vertices")
	require.Positive(t, st.bytes, "record bytes")
	require.Positive(t, st.buckets, "buckets")
	require.Positive(t, st.decoded, "a write must leave its bucket decoded")
	require.Zero(t, st.compressed, "without compression there can be no compressed buckets")
	require.Zero(t, st.parsedBodies, "there must be no parsed bodies before any reads")

	// maintenance encodes what the write left behind
	cs.compactRecords()
	require.Zero(t, cs.recordStats().decoded, "there must be no decoded buckets after compaction")

	// two reads of a body leave it parsed, and the metric sees that
	for round := 0; round < 2; round++ {
		for i := 0; i < vertices; i++ {
			cs.GetValueJSON(fmt.Sprintf("dom/v-%03d", i)) //nolint:errcheck
		}
	}
	require.Equal(t, vertices, cs.recordStats().parsedBodies, "parsed bodies")
	cs.ageParsedBodies()
	cs.ageParsedBodies()
	require.Zero(t, cs.recordStats().parsedBodies, "cooled-down bodies must drop out of the metric")

	// splits show as a growing bucket count — no separate counter is needed
	before := cs.recordStats().buckets
	for l := 0; l < 200; l++ {
		cs.SetValue(fmt.Sprintf("dom/v-000.out.to.big%03d", l), []byte("t.dom/y"), false, 2)
	}
	require.Greater(t, cs.recordStats().buckets, before,
		"the bucket count must grow — otherwise the metric does not show splits")
}

// Test_RecordGauges_TreeReportsZero — in tree mode the record metrics must be
// zeros, not absent: a gap in a graph cannot be told from broken collection.
func Test_RecordGauges_TreeReportsZero(t *testing.T) {
	restore := SetCacheModeForTest("tree")
	defer restore()

	cs := NewStoreForTest("gauges_tree")
	body, _ := easyjson.JSONFromString(`{"n":1}`)
	cs.SetValueJSON("dom/v", &body, false, 1)
	require.True(t, cs.Exists("dom/v"))
	require.Equal(t, recordStats{}, cs.recordStats())
	require.Equal(t, "tree", CacheMode())
}

// Test_RecordGauges_Published — the metrics must actually reach Prometheus
// under the names the dashboard looks them up by. Otherwise a typo in a name
// is noticed only when the graph is needed.
func Test_RecordGauges_Published(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("published")
	body, _ := easyjson.JSONFromString(`{"n":1}`)
	for i := 0; i < 5; i++ {
		cs.SetValueJSON(fmt.Sprintf("dom/v-%d", i), &body, false, 1)
	}
	cs.publishRecordGauges()

	value := func(name string, labels prometheus.Labels) float64 {
		gv, err := system.GlobalPrometrics.EnsureGaugeVecSimple(name, "", labelNamesOf(labels))
		require.NoErrorf(t, err, "metric %s", name)
		var m dto.Metric
		require.NoErrorf(t, gv.With(labels).Write(&m), "metric %s", name)
		return m.GetGauge().GetValue()
	}
	id := prometheus.Labels{"id": cs.cacheConfig.id}

	require.Equal(t, float64(5), value("cache_record_vertices", id))
	require.Positive(t, value("cache_record_bytes", id))
	require.Positive(t, value("cache_record_buckets", id))
	require.Equal(t, float64(0), value("cache_record_parsed_bodies", id))
	require.Equal(t, float64(1),
		value("cache_mode", prometheus.Labels{"id": cs.cacheConfig.id, "mode": "records"}),
		"the mode must be visible in the metric")
	require.Equal(t, float64(0),
		value("cache_mode", prometheus.Labels{"id": cs.cacheConfig.id, "mode": "tree"}))
}

func labelNamesOf(l prometheus.Labels) []string {
	names := make([]string, 0, len(l))
	for k := range l {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// Test_SweepRecords_RemovesWhatIsGone — a vertex that no longer exists must
// not leave a record behind. Without this, a "create, delete" workload grows
// the index by one record per deleted vertex and never gives it back.
func Test_SweepRecords_RemovesWhatIsGone(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("sweep")
	body, _ := easyjson.JSONFromString(`{"n":1}`)
	const vertices = 20
	create := func(i int) {
		id := fmt.Sprintf("dom/v-%02d", i)
		cs.SetValueJSON(id, &body, false, 1)
		cs.SetValue(id+".out.to.l1", []byte("t.dom/x"), false, 1)
		cs.SetValue(id+".ltype.t.dom/x", []byte("l1"), false, 1)
		cs.SetValue(id+".out.index.l1.type.t", nil, false, 1)
		cs.SetValue("dom/x.in."+id+".l1", []byte("t"), false, 1)
	}
	erase := func(i int) {
		id := fmt.Sprintf("dom/v-%02d", i)
		cs.DeleteValue(id, false, 2)
		cs.DeleteValue(id+".out.to.l1", false, 2)
		cs.DeleteValue(id+".ltype.t.dom/x", false, 2)
		cs.DeleteValue(id+".out.index.l1.type.t", false, 2)
		cs.DeleteValue("dom/x.in."+id+".l1", false, 2)
	}

	for i := 0; i < vertices; i++ {
		create(i)
	}
	require.Equal(t, vertices+1, cs.RecordCountForTest(), "the vertices and the link target")
	require.Zero(t, cs.sweepRecords(), "live vertices must not be swept")

	for i := 0; i < vertices; i++ {
		erase(i)
	}
	require.Equal(t, vertices+1, cs.RecordCountForTest(),
		"before maintenance the records are still in place — they hold tombstones")

	require.Equal(t, vertices+1, cs.sweepRecords(), "every emptied record must go")
	require.Zero(t, cs.RecordCountForTest(), "the index must become empty")

	// and the graph builds again from scratch
	for i := 0; i < vertices; i++ {
		create(i)
	}
	require.Equal(t, vertices+1, cs.RecordCountForTest())
	for i := 0; i < vertices; i++ {
		require.True(t, cs.Exists(fmt.Sprintf("dom/v-%02d.out.to.l1", i)), "link %d", i)
	}
}

// Test_SweepRecords_CyclesReturnToBaseline — repeating the "create, delete,
// maintain" cycle must bring the index back to where it started, not grow it.
func Test_SweepRecords_CyclesReturnToBaseline(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("sweep_cycles")
	body, _ := easyjson.JSONFromString(`{"n":1}`)
	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < 30; i++ {
			id := fmt.Sprintf("dom/c%d-v%02d", cycle, i)
			cs.SetValueJSON(id, &body, false, int64(cycle*10+1))
			cs.SetValue(id+".out.to.l1", []byte("t.dom/tgt"), false, int64(cycle*10+1))
		}
		for i := 0; i < 30; i++ {
			id := fmt.Sprintf("dom/c%d-v%02d", cycle, i)
			cs.DeleteValue(id, false, int64(cycle*10+2))
			cs.DeleteValue(id+".out.to.l1", false, int64(cycle*10+2))
		}
		cs.RunMaintenanceForTest()
		// dom/tgt holds incoming links, so it survives the cycle; everything
		// else must go.
		require.LessOrEqualf(t, cs.RecordCountForTest(), 1,
			"after cycle %d the index still holds %d records", cycle, cs.RecordCountForTest())
	}
}

// Test_SweepRecords_WriteIntoASweptRecordIsRepeated — a write that lands
// exactly in the window between fetching a record and accessing it must not
// be lost.
//
// The window is nanoseconds wide, and no amount of concurrency reproduces it
// on demand: without the seam the test passed even with the repeat turned
// off, so it checked nothing. Here the sweep happens inside the window on
// purpose.
func Test_SweepRecords_WriteIntoASweptRecordIsRepeated(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("sweep_window")
	// an empty record: a link is created and deleted right away
	cs.SetValue("dom/v.out.to.l1", []byte("t.dom/x"), false, 1)
	cs.DeleteValue("dom/v.out.to.l1", false, 2)
	first, ok := cs.records.get("dom/v")
	require.True(t, ok)
	require.True(t, first.dead())

	once := false
	afterRecordFetchForTest = func(id string) {
		if once || id != "dom/v" {
			return
		}
		once = true
		require.Equal(t, 1, cs.sweepRecords(), "a sweep inside the window must remove the record")
	}
	defer func() { afterRecordFetchForTest = nil }()

	require.True(t, cs.SetValue("dom/v.out.to.l2", []byte("t.dom/y"), false, 3))
	require.True(t, once, "the seam did not fire — the window was not checked")
	require.True(t, first.retired.Load(), "the old record must be marked retired")

	v, err := cs.GetValue("dom/v.out.to.l2")
	require.NoError(t, err, "a write that landed in a swept record is lost")
	require.Equal(t, "t.dom/y", string(v))

	second, ok := cs.records.get("dom/v")
	require.True(t, ok)
	require.NotSame(t, first, second, "the write must have gone to the new record")
}

// Test_SweepRecords_SweepingAlongsideWritingLosesNothing — sweeping runs at
// the same time as writing and breaks nothing.
//
// This test does not reproduce the exact window — it is too narrow, and the
// test with the seam above covers it. This one checks something else: a sweep
// interleaved with writes and deletes leaves the graph whole.
func Test_SweepRecords_SweepingAlongsideWritingLosesNothing(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("sweep_race")

	const writers, rounds = 16, 300
	stop := make(chan struct{})
	var sweeps int64
	var writersWG, sweeperWG sync.WaitGroup

	sweeperWG.Add(1)
	go func() {
		defer sweeperWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n := cs.sweepRecords(); n > 0 {
				atomic.AddInt64(&sweeps, int64(n))
			}
		}
	}()

	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			id := fmt.Sprintf("dom/w-%02d", w)
			ts := int64(1)
			for r := 0; r < rounds; r++ {
				name := fmt.Sprintf("%s.out.to.l%04d", id, r)
				cs.SetValue(name, []byte(fmt.Sprintf("t.dom/tgt-%d-%d", w, r)), false, ts)
				ts++
				if r < rounds-1 { // the last one is left alive
					cs.DeleteValue(name, false, ts)
					ts++
				}
			}
		}(w)
	}

	// The sweeper lives exactly as long as the writers.
	writersWG.Wait()
	close(stop)
	sweeperWG.Wait()

	require.Positive(t, atomic.LoadInt64(&sweeps), "the sweep must have removed something — otherwise there was no race")
	for w := 0; w < writers; w++ {
		key := fmt.Sprintf("dom/w-%02d.out.to.l%04d", w, rounds-1)
		v, err := cs.GetValue(key)
		require.NoErrorf(t, err, "the last link of writer %d is lost", w)
		require.Equal(t, fmt.Sprintf("t.dom/tgt-%d-%d", w, rounds-1), string(v))
	}
}

// Test_Tiering_ExistsJsonNudgesLikeTheTree — a record stores everything but
// the body as bytes, yet ExistsJson must give the "use Exists" hint exactly
// where the tree gives it. A link body is JSON: no hint, or else every object
// read that checks its type link writes a WARN. A link target and a body
// written as bytes are not JSON: the hint is given. Existence is always
// reported.
func Test_Tiering_ExistsJsonNudgesLikeTheTree(t *testing.T) {
	for _, mode := range []string{"tree", "records"} {
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()

			cs := NewStoreForTest("exists-json-" + mode)
			body := easyjson.NewJSONObject()
			cs.SetValueJSON("dom/v", &body, false, 1)
			cs.SetValueJSON(fmt.Sprintf(kOutBody, "dom/v", "type"), &body, false, 1)
			cs.SetValue(fmt.Sprintf(kOutTo, "dom/v", "type"), []byte("__type.dom/t"), false, 1)
			cs.SetValue("dom/raw", []byte("raw"), false, 1)

			logs := captureWarnings(t)
			nudged := map[string]bool{
				"dom/v":               false,
				"dom/v.out.body.type": false,
				"dom/v.out.to.type":   true,
				"dom/raw":             true,
			}
			for key := range nudged {
				require.Truef(t, cs.ExistsJson(key), "%s exists", key)
			}
			for key, want := range nudged {
				got := strings.Contains(logs.String(), "key="+key+" is []byte, use Exists method")
				require.Equalf(t, want, got, "hint for %s", key)
			}
		})
	}
}

// warnBuffer is a buffer the logger writes to from any goroutine.
type warnBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *warnBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *warnBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// captureWarnings points the global logger at a buffer until the end of the
// test and returns it to the default settings afterwards.
func captureWarnings(t *testing.T) *warnBuffer {
	w := &warnBuffer{}
	lg.GetLogger().SetOptions(w, lg.WarnLevel, false, false)
	t.Cleanup(func() { lg.GetLogger().SetOptions(os.Stdout, lg.InfoLevel, true, false) })
	return w
}
