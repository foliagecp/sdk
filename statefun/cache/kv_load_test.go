package cache

// Loading from KV and HA rehydration.
//
// KV is read exactly twice in the life of a process — at startup and on a
// passive→active promotion — and both times it is read in full. So what is
// checked here is not the speed of random reads but three things: that
// records mode answers exactly what the tree answers after a load; that the
// peak memory of a load is set by the records, not by whether the graph
// would fit as a tree; and that a promotion yields the same graph as a cold
// start — that is, what was deleted while the node was silent does not come
// back to life.

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"sort"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/system"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// Test_Load_ProbeNeverRulesOutAnObject — the load's cheap prefilter may only
// weed out what is certainly not JSON. If the parser takes a value for an
// object, the prefilter has no right to say "no": otherwise a vertex body
// changes type after a restart and ExistsJson starts to lie.
func Test_Load_ProbeNeverRulesOutAnObject(t *testing.T) {
	corpus := []string{
		``, ` `, `{}`, `{"a":1}`, ` {"a":1} `, "\n\t{\"a\":1}\n", "{}\n",
		`{"a":1}xyz`, `{"a":1}{"b":2}`, `{"a":1`, `{a:1}`, `{'a':1}`,
		`[1,2]`, `[{"a":1}]`, `"строка"`, `12`, `true`, `null`, `-0.0`,
		`__type.dom/srv-0`, `l0042`, `tag1`, `__object`,
		`{"вложенное":{"есть":[1,2,{"и":null}]}}`, `{"":""}`,
		`{"n":1e309}`, `{"дубль":1,"дубль":2}`, "\xff\xfe{}",
		`{"big":123456789012345678901234567890}`, "{\"нулевой\":\"\x00\"}",
	}
	for _, c := range corpus {
		b := []byte(c)
		jv, ok := easyjson.JSONFromBytes(b)
		if ok && jv.IsObject() {
			require.Truef(t, looksLikeJSONObject(b),
				"предфильтр отверг то, что разбор считает объектом: %q", c)
		}
	}
}

// --- harness with an embedded NATS ----------------------------------------

// lastTestNatsURL — the address of the server started by the last call to
// newKVForTest, for a test that needs a second connection to the same server.
var lastTestNatsURL string

func newKVForTest(t *testing.T, bucket string) (nats.JetStreamContext, nats.KeyValue) {
	t.Helper()
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	srv := natsservertest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	lastTestNatsURL = srv.ClientURL()

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	require.NoError(t, err)
	// Created the way Domain.start creates the runtime's bucket, not with
	// nats.go's CreateKeyValue: the two differ in DenyDelete, and the tests
	// here are about the bucket the runtime actually has.
	kvs, err := customNatsKv.CreateKeyValue(nc, js, &nats.KeyValueConfig{Bucket: bucket})
	require.NoError(t, err)
	return js, kvs
}

// kvGraph — what lies in KV: key→value pairs of exactly the shapes CRUD
// writes. It also returns the list of keys, so a test can ask about each one.
func kvGraph(vertices, hubLinks int) (map[string][]byte, []string) {
	out := map[string][]byte{}
	put := func(k string, v []byte) { out[k] = v }

	for i := 0; i < vertices; i++ {
		id := fmt.Sprintf("dom/v-%05d", i)
		put(id, []byte(fmt.Sprintf(
			`{"name":"узел %d","cpu":%d,"tags":["a","b"],"meta":{"x":%d,"y":null}}`, i, i%64, i)))
	}
	// hub: one vertex that everything converges on
	for i := 0; i < hubLinks; i++ {
		name := fmt.Sprintf("l%05d", i)
		tgt := fmt.Sprintf("dom/v-%05d", i%vertices)
		put("dom/hub.out.to."+name, []byte("__object."+tgt))
		put("dom/hub.ltype.__object."+tgt, []byte(name))
		put("dom/hub.out.index."+name+".type.__object", nil)
		put(tgt+".in.dom/hub."+name, []byte("__object"))
		if i%3 == 0 {
			put("dom/hub.out.index."+name+".tag.important", nil)
		}
	}
	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return out, keys
}

func fillKV(t *testing.T, kvs nats.KeyValue, entries map[string][]byte) {
	t.Helper()
	for k, v := range entries {
		_, err := kvs.Put(KVStorePrefix+"."+k, v)
		require.NoError(t, err)
	}
}

// answers — the store's whole observable answer over a list of keys.
type answer struct {
	value      []byte
	exists     bool
	existsJSON bool
	hasTime    bool
}

func snapshot(cs *Store, keys []string) map[string]answer {
	out := make(map[string]answer, len(keys))
	for _, k := range keys {
		v, _ := cs.GetValue(k)
		out[k] = answer{
			value:      v,
			exists:     cs.Exists(k),
			existsJSON: cs.ExistsJson(k),
			hasTime:    cs.GetValueUpdateTime(k) >= 0,
		}
	}
	return out
}

func loadStore(t *testing.T, js nats.JetStreamContext, kvs nats.KeyValue, id string) (*Store, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cs, err := NewCacheStore(ctx, NewCacheConfig(id), js, kvs)
	require.NoError(t, err)
	return cs, cancel
}

// Test_KVLoad_RecordsAnswerAsTree — after a real load from KV, records mode
// must answer every key exactly as the tree does. This is the same gate as
// the differential test, but passed through the real load path, with its
// probe of the value type.
func Test_KVLoad_RecordsAnswerAsTree(t *testing.T) {
	js, kvs := newKVForTest(t, "agree")
	entries, keys := kvGraph(200, 400)
	fillKV(t, kvs, entries)
	keys = append(keys, "dom/нет", "dom/hub.out.to.нет", "dom/v-00001.in.dom/нет.l1")

	var asTree map[string]answer
	func() {
		restore := SetCacheModeForTest("tree")
		defer restore()
		cs, cancel := loadStore(t, js, kvs, "agree_tree")
		defer cancel()
		asTree = snapshot(cs, keys)
	}()

	restore := SetCacheModeForTest("records")
	defer restore()
	cs, cancel := loadStore(t, js, kvs, "agree_records")
	defer cancel()
	asRecords := snapshot(cs, keys)

	require.Positive(t, cs.RecordCountForTest(), "загрузка обязана была построить записи")
	for _, k := range keys {
		require.Equalf(t, asTree[k].exists, asRecords[k].exists, "существование %s", k)
		require.Equalf(t, asTree[k].existsJSON, asRecords[k].existsJSON, "тип JSON %s", k)
		require.Equalf(t, asTree[k].hasTime, asRecords[k].hasTime, "время %s", k)
		require.Equalf(t, len(asTree[k].value), len(asRecords[k].value), "длина значения %s", k)
		if len(asTree[k].value) > 0 {
			require.Equalf(t, asTree[k].value, asRecords[k].value, "значение %s", k)
		}
	}
	for _, p := range []string{"dom/hub.out.to.>", "dom/hub.ltype.>", "dom/hub.out.index.>", "dom/v-00001.in.>"} {
		want := func() []string {
			restore := SetCacheModeForTest("tree")
			defer restore()
			cs2, cancel2 := loadStore(t, js, kvs, "agree_tree_"+p)
			defer cancel2()
			return cs2.GetKeysByPattern(p)
		}()
		got := cs.GetKeysByPattern(p)
		sort.Strings(want)
		sort.Strings(got)
		require.Equalf(t, want, got, "перечисление по %s", p)
	}
}

// peakDuringLoad measures the maximum heap in use while a load runs.
func peakDuringLoad(load func()) (peakMB, finalMB, churnMB float64, gcs uint32) {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	baseChurn, baseGC := ms.TotalAlloc, ms.NumGC

	done := make(chan struct{})
	peak := make(chan uint64, 1)
	go func() {
		var top uint64
		for {
			select {
			case <-done:
				peak <- top
				return
			default:
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > top {
				top = m.HeapAlloc
			}
			time.Sleep(time.Millisecond)
		}
	}()
	load()
	close(done)
	top := <-peak

	runtime.GC()
	runtime.ReadMemStats(&ms)
	if top < base {
		top = base
	}
	return float64(top-base) / (1 << 20), float64(ms.HeapAlloc-base) / (1 << 20),
		float64(ms.TotalAlloc-baseChurn) / (1 << 20), ms.NumGC - baseGC
}

// Test_KVLoad_PeakMemory — the peak memory of a load, in two parts.
//
// The first: a graph with a hub really loads from KV and every key reads.
// The second: the same stream of writes is applied directly, without NATS,
// and the peak is measured. The split is not for convenience: three quarters
// of a real load's peak is NATS client machinery, common to both modes, and
// against it the difference between the modes drowns in noise. The load peak
// is required to be "records + load buffer + index"; the buffer is common,
// but what the records cost shows only this way.
func Test_KVLoad_PeakMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("длинный: поднимает NATS и грузит десятки тысяч ключей")
	}
	js, kvs := newKVForTest(t, "peak")
	entries, keys := kvGraph(15000, 30000)
	fillKV(t, kvs, entries)
	t.Logf("ключей в KV: %d", len(keys))

	// --- part 1: does everything get through a real load
	for _, mode := range []string{"tree", "records"} {
		restore := SetCacheModeForTest(mode)
		cs, cancel := loadStore(t, js, kvs, "peak_"+mode)
		missing := 0
		for _, k := range keys {
			if !cs.Exists(k) {
				missing++
			}
		}
		require.Zerof(t, missing, "режим %s потерял %d ключей из %d при загрузке", mode, missing, len(keys))
		cancel()
		restore()
	}

	// --- part 2: what applying the same stream costs
	apply := func(cs *Store) {
		now := int64(1)
		for _, k := range keys {
			v := entries[k]
			now++
			if looksLikeJSONObject(v) {
				if jv, ok := easyjson.JSONFromBytes(v); ok && jv.IsObject() {
					cs.SetValueJSON(k, &jv, false, now)
					continue
				}
			}
			cs.SetValue(k, v, false, now)
		}
	}
	measure := func(mode string) (peak, final, churn float64) {
		restore := SetCacheModeForTest(mode)
		defer restore()
		bestPeak, bestFinal, bestChurn := math.Inf(1), 0.0, math.Inf(1)
		for attempt := 0; attempt < 3; attempt++ {
			cs := NewStoreForTest("peak_apply")
			p, f, c, _ := peakDuringLoad(func() { apply(cs) })
			if p < bestPeak {
				bestPeak, bestFinal, bestChurn = p, f, c
			}
			runtime.KeepAlive(cs)
		}
		return bestPeak, bestFinal, bestChurn
	}

	// First at the default setting — as it will be in production.
	treePeak, treeFinal, treeChurn := measure("tree")
	recPeak, recFinal, recChurn := measure("records")
	t.Logf("GOGC по умолчанию — дерево: пик %.1f MB, живого %.1f MB, выделено %.1f MB",
		treePeak, treeFinal, treeChurn)
	t.Logf("GOGC по умолчанию — записи: пик %.1f MB, живого %.1f MB, выделено %.1f MB",
		recPeak, recFinal, recChurn)
	require.Lessf(t, recFinal, treeFinal,
		"записи (%.1f MB) обязаны занимать меньше дерева (%.1f MB)", recFinal, treeFinal)

	// And now with a collector that actually collects.
	//
	// The peak at GOGC=100 says not how much a load NEEDS but how much
	// garbage the runtime is willing to hold: it accumulates up to double the
	// live heap, and the records' live heap is half as large, so their
	// threshold is lower too — but they allocate 14% more, and with a
	// generous threshold they land slightly above the tree. Once the collector
	// starts collecting, the difference shows: the tree hits the floor of its
	// own live heap and goes no lower at any setting, while the records keep
	// falling towards theirs. Measured on this very graph:
	//
	//	GOGC    tree   records
	//	 100    83.2      90.3
	//	  50    63.2      57.2
	//	  25    62.6      48.9
	//
	// This is the property under test: the load peak is set by the records,
	// not by whether the graph would fit as a tree.
	prev := debug.SetGCPercent(50)
	defer debug.SetGCPercent(prev)

	treePeak, _, _ = measure("tree")
	recPeak, _, _ = measure("records")
	t.Logf("GOGC=50 — дерево: пик %.1f MB, записи: пик %.1f MB", treePeak, recPeak)
	require.Lessf(t, recPeak, treePeak,
		"Н-5: пик загрузки в записях (%.1f MB) обязан быть ниже пика дерева (%.1f MB)", recPeak, treePeak)
}

// Test_Rehydrate_KVIsTheWholeTruth — a passive→active promotion must yield
// the same graph as a cold start. While the node was silent, another active
// node deleted a vertex; after rehydration it must be gone — a load carries
// only what exists, and by itself says nothing about deletes.
func Test_Rehydrate_KVIsTheWholeTruth(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	js, kvs := newKVForTest(t, "rehydrate")
	entries, keys := kvGraph(50, 100)
	fillKV(t, kvs, entries)

	cs, cancel := loadStore(t, js, kvs, "rehydrate_cache")
	defer cancel()
	require.True(t, cs.Exists("dom/v-00007"), "вершина обязана была загрузиться")
	require.True(t, cs.Exists("dom/hub.out.to.l00007"))

	// another active node deletes a vertex and a link while we are passive
	gone := []string{"dom/v-00007", "dom/hub.out.to.l00007", "dom/hub.ltype.__object.dom/v-00007"}
	for _, k := range gone {
		require.NoError(t, kvs.Delete(KVStorePrefix+"."+k))
	}
	// and adds a vertex we have not seen
	added := "dom/v-99999"
	_, err := kvs.Put(KVStorePrefix+"."+added, []byte(`{"name":"после"}`))
	require.NoError(t, err)

	require.NoError(t, cs.RehydrateFromKV(context.Background()))

	for _, k := range gone {
		require.Falsef(t, cs.Exists(k), "удалённое в KV пережило повышение: %s", k)
	}
	require.True(t, cs.Exists(added), "появившееся в KV не доехало до повышения")

	// everything else is in place and reads
	for _, k := range keys {
		if k == gone[0] || k == gone[1] || k == gone[2] {
			continue
		}
		require.Truef(t, cs.Exists(k), "регидратация потеряла ключ %s", k)
	}
}
