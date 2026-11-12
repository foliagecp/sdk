package cache

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"time"
)

func mkOut(i int) outLink {
	t := int64(i + 1)
	lt := fmt.Sprintf("type-%d", i%7)
	tgt := fmt.Sprintf("dom/obj-%05d", i)
	return outLink{
		Name:     fmt.Sprintf("link-%05d", i),
		To:       subValue{Value: lt + "." + tgt, Time: t, Live: true},
		IdxTypes: []subValue{{Value: lt, Time: t, Live: true}},
	}
}

// putOutLinkForTest writes every key of a link, the way CRUD does.
func putOutLinkForTest(r *vertexRecord, l outLink) bool {
	ok := r.setOutTo(l.Name, l.To.Value, l.To.Time)
	for _, it := range l.IdxTypes {
		r.setOutIndexType(l.Name, it.Value, it.Time, it.Live)
	}
	if l.Body.Live {
		r.setOutBody(l.Name, []byte(l.Body.Value), l.Body.Time)
	}
	for _, tg := range l.Tags {
		r.setOutTag(l.Name, tg.Value, tg.Time, tg.Live)
	}
	lt, tgt := splitTypeTarget(l.To.Value)
	r.putPair(pairEntry{Type: lt, Target: tgt, Name: l.Name, UpdateTime: l.To.Time})
	return ok
}

// deleteOutLinkForTest removes every key of a link.
func deleteOutLinkForTest(r *vertexRecord, l outLink, t int64) bool {
	lt, tgt := splitTypeTarget(l.To.Value)
	r.deletePair(lt, tgt, t)
	for _, it := range l.IdxTypes {
		r.setOutIndexType(l.Name, it.Value, t, false)
	}
	r.deleteOutBody(l.Name, t)
	return r.deleteOutTo(l.Name, t)
}

func mkIn(i int) inLink {
	return inLink{
		From:       fmt.Sprintf("dom/src-%05d", i),
		Name:       fmt.Sprintf("link-%05d", i),
		Type:       fmt.Sprintf("type-%d", i%7),
		UpdateTime: int64(i + 1),
	}
}

func buildRecord(t testing.TB, n int) *vertexRecord {
	t.Helper()
	v := vertexData{Body: []byte(`{"name":"v","kind":"k"}`), BodyTime: 1}
	for i := 0; i < n; i++ {
		v.Out = append(v.Out, mkOut(i))
		v.In = append(v.In, mkIn(i))
	}
	return newVertexRecord(v, defaultBucketLinks)
}

// ---------------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------------

func Test_Record_RoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 8, 31, 32, 33, 500, 5000} {
		t.Run(fmt.Sprintf("links=%d", n), func(t *testing.T) {
			r := buildRecord(t, n)

			body, bt, ok := r.bodyBytes()
			require.True(t, ok)
			require.Equal(t, `{"name":"v","kind":"k"}`, body)
			require.Equal(t, int64(1), bt)

			for i := 0; i < n; i++ {
				want := mkOut(i)
				got, ok := r.lookupOutLink(want.Name)
				require.True(t, ok, "outgoing link %s", want.Name)
				require.Equal(t, want.To.Value, got.To.Value)
				require.Equal(t, want.To.Time, got.To.Time)

				lt, tgt, ok := r.lookupOutTarget(want.Name)
				require.True(t, ok)
				wlt, wtgt := splitTypeTarget(want.To.Value)
				require.Equal(t, wlt, lt)
				require.Equal(t, wtgt, tgt)

				wi := mkIn(i)
				gi, ok := r.lookupInLink(wi.From, wi.Name)
				require.True(t, ok, "incoming link %s", wi.From)
				require.Equal(t, wi.Type, gi.Type)
				require.Equal(t, wi.UpdateTime, gi.UpdateTime)
			}

			_, ok = r.lookupOutLink("нет такой")
			require.False(t, ok)
			_, ok = r.lookupInLink("нет", "такой")
			require.False(t, ok)

			// enumeration yields exactly what was put in, each once
			seen := map[string]int{}
			r.rangeOutLinks(func(l outLink) bool { seen[l.Name]++; return true })
			require.Len(t, seen, n)
			for _, c := range seen {
				require.Equal(t, 1, c, "a link was enumerated twice")
			}

			seenIn := map[string]int{}
			r.rangeInLinks(func(l inLink) bool { seenIn[l.From+"/"+l.Name]++; return true })
			require.Len(t, seenIn, n)
			for _, c := range seenIn {
				require.Equal(t, 1, c)
			}
		})
	}
}

func Test_Record_LookupByTypeTarget(t *testing.T) {
	r := buildRecord(t, 100)
	want := mkOut(42)
	wlt, wtgt := splitTypeTarget(want.To.Value)
	got, ok := r.lookupPair(wlt, wtgt)
	require.True(t, ok)
	require.Equal(t, want.Name, got.Name)

	_, ok = r.lookupPair(wlt, "dom/нет")
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// writing
// ---------------------------------------------------------------------------

func Test_Record_WriteThrough(t *testing.T) {
	r := buildRecord(t, 10)

	// a new link
	nl := outLink{Name: "link-new", To: subValue{Value: "t.dom/x", Time: 100, Live: true}}
	require.True(t, putOutLinkForTest(r, nl))
	got, ok := r.lookupOutLink("link-new")
	require.True(t, ok)
	require.Equal(t, "dom/x", got.target())

	// replacing an existing one
	upd := mkOut(3)
	upd.To = subValue{Value: "type-3.dom/moved", Time: 1000, Live: true}
	require.True(t, putOutLinkForTest(r, upd))
	got, _ = r.lookupOutLink(upd.Name)
	require.Equal(t, "dom/moved", got.target())

	// deletion
	require.True(t, deleteOutLinkForTest(r, upd, 2000))
	_, ok = r.lookupOutLink(upd.Name)
	require.False(t, ok, "a deleted link must not read")

	// the rest are still in place
	for i := 0; i < 10; i++ {
		if i == 3 {
			continue
		}
		_, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "link %d was lost", i)
	}
}

// Test_Record_LastWriterWins — the guard that tombstones exist for.
func Test_Record_LastWriterWins(t *testing.T) {
	r := buildRecord(t, 4)
	name := mkOut(1).Name

	// a write with a time older than the current one is ignored
	old := mkOut(1)
	old.To = subValue{Value: "type-1.dom/stale", Time: 0, Live: true}
	require.False(t, putOutLinkForTest(r, old), "an older write must not apply")
	got, _ := r.lookupOutLink(name)
	require.Equal(t, mkOut(1).target(), got.target())

	// after a delete, a late write does not resurrect the link
	require.True(t, deleteOutLinkForTest(r, mkOut(1), 500))
	late := mkOut(1)
	late.To.Time = 499
	require.False(t, putOutLinkForTest(r, late), "a late write resurrected the deleted link")
	_, ok := r.lookupOutLink(name)
	require.False(t, ok)

	// but a newer one does, and that is legitimate
	fresh := mkOut(1)
	fresh.To.Time = 501
	require.True(t, putOutLinkForTest(r, fresh))
	_, ok = r.lookupOutLink(name)
	require.True(t, ok)

	// the body: the same guard
	require.False(t, r.putBody([]byte(`{"a":1}`), 0, true))
	require.True(t, r.putBody([]byte(`{"a":1}`), 10, true))
	require.True(t, r.deleteBody(20))
	_, _, ok = r.bodyBytes()
	require.False(t, ok)
	require.False(t, r.putBody([]byte(`{"a":2}`), 19, true), "a late write resurrected the body")
	require.True(t, r.putBody([]byte(`{"a":2}`), 21, true))
	body, _, ok := r.bodyBytes()
	require.True(t, ok)
	require.Equal(t, `{"a":2}`, body)
}

// Test_Record_SplitKeepsEverything — a split loses no links.
func Test_Record_SplitKeepsEverything(t *testing.T) {
	r := newVertexRecord(vertexData{BodyTime: 1}, defaultBucketLinks)

	const n = 5000
	for i := 0; i < n; i++ {
		require.True(t, putOutLinkForTest(r, mkOut(i)), "outgoing link %d", i)
		require.True(t, r.putInLink(mkIn(i)), "incoming link %d", i)
	}
	require.Greater(t, int(r.out.Load().depth), 0, "the directory must have grown")

	for i := 0; i < n; i++ {
		got, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "outgoing link %d was lost after the split", i)
		require.Equal(t, mkOut(i).target(), got.target())

		wi := mkIn(i)
		_, ok = r.lookupInLink(wi.From, wi.Name)
		require.True(t, ok, "incoming link %d was lost after the split", i)
	}

	// enumeration gives exactly n, no repeats (buckets share directory slots)
	seen := map[string]int{}
	r.rangeOutLinks(func(l outLink) bool { seen[l.Name]++; return true })
	require.Len(t, seen, n)
	for name, c := range seen {
		require.Equal(t, 1, c, "%s enumerated %d times", name, c)
	}

	// no bucket is left overfull
	r.out.Load().each(func(b *bucket) bool {
		require.LessOrEqual(t, b.entryCount(), 2*defaultBucketLinks,
			"a bucket is still over twice the limit: the split did not happen")
		return true
	})
}

// Test_Record_ConcurrentWritesLoseNothing — a test for a lost update.
//
// It is checked by counting, not as a race: two writers that changed different
// links of one bucket both decode the old block, and the second overwrites the
// first. There is no data race in that, so -race does not see such a bug.
func Test_Record_ConcurrentWritesLoseNothing(t *testing.T) {
	r := newVertexRecord(vertexData{BodyTime: 1}, defaultBucketLinks)

	const goroutines = 64
	const perGoroutine = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := g*perGoroutine + i
				if !putOutLinkForTest(r, mkOut(id)) {
					t.Errorf("outgoing link %d rejected", id)
					return
				}
				if !r.putInLink(mkIn(id)) {
					t.Errorf("incoming link %d rejected", id)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	total := goroutines * perGoroutine
	for i := 0; i < total; i++ {
		got, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "outgoing link %d lost", i)
		require.Equal(t, mkOut(i).target(), got.target())

		wi := mkIn(i)
		_, ok = r.lookupInLink(wi.From, wi.Name)
		require.True(t, ok, "incoming link %d lost", i)
	}

	count := 0
	r.rangeOutLinks(func(outLink) bool { count++; return true })
	require.Equal(t, total, count, "the enumeration does not match the number of links written")
}

// Test_Record_ConcurrentReadWrite — readers work without locks while buckets
// are replaced under them.
func Test_Record_ConcurrentReadWrite(t *testing.T) {
	r := buildRecord(t, 200)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := rng.Intn(200)
				if _, _, ok := r.lookupOutTarget(mkOut(n).Name); !ok {
					t.Errorf("a reader did not find link %d", n)
					return
				}
			}
		}(i)
	}

	for i := 200; i < 2000; i++ {
		require.True(t, putOutLinkForTest(r, mkOut(i)))
	}
	close(stop)
	wg.Wait()

	for i := 0; i < 2000; i++ {
		_, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "link %d lost", i)
	}
}

// ---------------------------------------------------------------------------
// cost
// ---------------------------------------------------------------------------

// Benchmark_Record_Write: the cost of a write does not depend on the vertex's
// degree.
func Benchmark_Record_Write(b *testing.B) {
	for _, n := range []int{8, 1000, 10691} {
		b.Run(fmt.Sprintf("degree=%d", n), func(b *testing.B) {
			r := buildRecord(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l := mkOut(i % n)
				l.To.Time = int64(1_000_000 + i)
				if !putOutLinkForTest(r, l) {
					b.Fatal("rejected")
				}
			}
		})
	}
}

func Benchmark_Record_Read(b *testing.B) {
	for _, n := range []int{8, 1000, 10691} {
		b.Run(fmt.Sprintf("degree=%d", n), func(b *testing.B) {
			r := buildRecord(b, n)
			names := make([]string, n)
			for i := range names {
				names[i] = mkOut(i).Name
			}
			sort.Strings(names)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, ok := r.lookupOutTarget(names[i%n]); !ok {
					b.Fatal("not found")
				}
			}
		})
	}
}

// Test_Record_CompactBuckets — compaction returns memory and changes no answer.
func Test_Record_CompactBuckets(t *testing.T) {
	r := newVertexRecord(vertexData{BodyTime: 1}, defaultBucketLinks)
	const n = 2000
	for i := 0; i < n; i++ {
		require.True(t, putOutLinkForTest(r, mkOut(i)))
		require.True(t, r.putInLink(mkIn(i)))
	}
	require.Greater(t, r.dirtyBuckets(), 0, "the writes must have left buckets decoded")

	before := map[string]outLink{}
	r.rangeOutLinks(func(l outLink) bool { before[l.Name] = l; return true })

	encoded := r.compactBuckets()
	require.Greater(t, encoded, 0)
	require.Equal(t, 0, r.dirtyBuckets(), "no decoded bucket must remain after compaction")

	after := map[string]outLink{}
	r.rangeOutLinks(func(l outLink) bool { after[l.Name] = l; return true })
	require.Equal(t, len(before), len(after))
	for name, l := range before {
		got, ok := after[name]
		require.True(t, ok, "%s vanished during compaction", name)
		require.Equal(t, l.To.Value, got.To.Value)
		require.Equal(t, l.To.Time, got.To.Time)
	}
	for i := 0; i < n; i++ {
		_, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "%d does not read after compaction", i)
		wi := mkIn(i)
		_, ok = r.lookupInLink(wi.From, wi.Name)
		require.True(t, ok)
	}

	// a second compaction does nothing
	require.Equal(t, 0, r.compactBuckets())
}

// Test_Record_CompactionReturnsMemory — compaction returns memory.
//
// The assertion rests on a deterministic estimate, not on the heap size: a heap
// measurement inside one process depends on what the collector has managed to
// do, and such a test fails every other run. approxBytes counts what is
// actually stored — and it is the same estimate the budget regulator uses.
func Test_Record_CompactionReturnsMemory(t *testing.T) {
	for _, links := range []int{40, 400, 4000} {
		t.Run(fmt.Sprintf("links=%d", links), func(t *testing.T) {
			r := newVertexRecord(vertexData{Body: []byte(`{"n":1}`), BodyTime: 1}, defaultBucketLinks)
			for i := 0; i < links; i++ {
				require.True(t, putOutLinkForTest(r, mkOut(i)))
			}
			dirty := r.approxBytes()
			require.Greater(t, r.dirtyBuckets(), 0)

			r.compactBuckets()
			clean := r.approxBytes()

			t.Logf("%d links: decoded %d B, compacted %d B, %.2fx",
				links, dirty, clean, float64(dirty)/float64(clean))
			require.Less(t, clean, dirty, "compaction must return memory")
			require.Less(t, float64(clean), 0.6*float64(dirty),
				"compaction returned less than 40%: the record layout does not pay off")
		})
	}
}

func runtimeGC()             { runtime.GC(); runtime.GC() }
func runtimeKeepAlive(v any) { runtime.KeepAlive(v) }
func heapAllocNow() uint64   { var ms runtime.MemStats; runtime.ReadMemStats(&ms); return ms.HeapAlloc }

// Test_Record_PairKeyIsIndependent — the (type, target) → name key has a life
// of its own.
//
// That is why it is kept in a separate table: CRUD deletes it itself when a
// link's type or target changes, and that state cannot be derived from the
// link table — a derivation would find the live link and return its name.
func Test_Record_PairKeyIsIndependent(t *testing.T) {
	r := newVertexRecord(vertexData{BodyTime: 1}, defaultBucketLinks)

	l := outLink{Name: "l1", To: subValue{Value: "t1.dom/a", Time: 10, Live: true}}
	require.True(t, putOutLinkForTest(r, l))
	require.True(t, r.putPair(pairEntry{Type: "t1", Target: "dom/a", Name: "l1", UpdateTime: 10}))

	p, ok := r.lookupPair("t1", "dom/a")
	require.True(t, ok)
	require.Equal(t, "l1", p.Name)

	// delete the pair, leaving the link live
	require.True(t, r.deletePair("t1", "dom/a", 20))
	_, ok = r.lookupPair("t1", "dom/a")
	require.False(t, ok, "the pair key must be gone")
	_, ok = r.lookupOutLink("l1")
	require.True(t, ok, "nobody asked to delete the link")

	// the guard holds here too
	require.False(t, r.putPair(pairEntry{Type: "t1", Target: "dom/a", Name: "l1", UpdateTime: 19}),
		"a late write resurrected the pair key")
	require.True(t, r.putPair(pairEntry{Type: "t1", Target: "dom/a", Name: "l2", UpdateTime: 21}))
	p, ok = r.lookupPair("t1", "dom/a")
	require.True(t, ok)
	require.Equal(t, "l2", p.Name, "the last write must win")

	// two links on one pair: as in the tree, the pair keeps the last one's name
	require.True(t, r.setOutTo("a", "t2.dom/b", 30))
	require.True(t, r.putPair(pairEntry{Type: "t2", Target: "dom/b", Name: "a", UpdateTime: 30}))
	require.True(t, r.setOutTo("b", "t2.dom/b", 31))
	require.True(t, r.putPair(pairEntry{Type: "t2", Target: "dom/b", Name: "b", UpdateTime: 31}))
	p, _ = r.lookupPair("t2", "dom/b")
	require.Equal(t, "b", p.Name)
	// both links are still in place
	_, ok = r.lookupOutLink("a")
	require.True(t, ok)
	_, ok = r.lookupOutLink("b")
	require.True(t, ok)
}

// parseTailBySplit — the former implementation via strings.Split, kept as the
// reference: the new one must answer exactly the same for any tail.
func parseTailBySplit(tail string) (tailKind, string, string) {
	if tail == "" {
		return tailBody, "", ""
	}
	t := strings.Split(tail, ".")
	switch t[0] {
	case "out":
		if len(t) < 3 {
			return tailUnknown, "", ""
		}
		switch t[1] {
		case "to":
			if len(t) == 3 {
				return tailOutTo, t[2], ""
			}
		case "body":
			if len(t) == 3 {
				return tailOutBody, t[2], ""
			}
		case "index":
			if len(t) == 5 {
				switch t[3] {
				case "type":
					return tailIndexType, t[2], t[4]
				case "tag":
					return tailIndexTag, t[2], t[4]
				}
			}
		}
	case "ltype":
		if len(t) == 3 {
			return tailLinkType, t[1], t[2]
		}
	case "in":
		if len(t) == 3 {
			return tailIn, t[1], t[2]
		}
	}
	return tailUnknown, "", ""
}

func Test_ParseTail_MatchesSplit(t *testing.T) {
	tokens := []string{"", "out", "in", "to", "body", "index", "type", "tag", "ltype",
		"l001", "dom/v-1", "__object", "имя", "a.b", "."}
	var tails []string
	// every tail of three tokens, plus the real forms
	var build func(prefix string, depth int)
	build = func(prefix string, depth int) {
		if depth == 0 {
			tails = append(tails, prefix)
			return
		}
		for _, tk := range tokens[:9] {
			next := tk
			if prefix != "" {
				next = prefix + "." + tk
			}
			build(next, depth-1)
		}
	}
	build("", 3)
	for _, a := range tokens {
		for _, b := range tokens {
			tails = append(tails,
				"out.to."+a, "out.body."+a, "ltype."+a+"."+b, "in."+a+"."+b,
				"out.index."+a+".type."+b, "out.index."+a+".tag."+b,
				"out.index."+a+"."+b, "out."+a+"."+b, a+"."+b)
		}
	}
	for _, tail := range tails {
		wk, wa, wb := parseTailBySplit(tail)
		gk, ga, gb := parseTail(tail)
		require.Equalf(t, wk, gk, "tail kind %q", tail)
		require.Equalf(t, wa, ga, "first token %q", tail)
		require.Equalf(t, wb, gb, "second token %q", tail)
	}
	t.Logf("tails compared: %d", len(tails))
}

func Benchmark_ParseTail(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parseTail("out.index.l00042.type.__object")
	}
}
func Benchmark_ParseTailBySplit(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parseTailBySplit("out.index.l00042.type.__object")
	}
}

// Test_KeysByPattern_PinnedPrefixDoesNotScanTheVertex — a question about one
// link must not cost a scan of every link of the vertex.
//
// The query engine asks for the indexes of each link in turn, with the pattern
// `<vertex>.out.index.<name>.>`. While enumeration answered such a question by
// walking every link, walking a hub cost the square of their number: with a
// thousand links a JPGQL query took 205 ms against 2 ms for the tree. This
// checks the property itself: the cost of an answer does not grow with the
// size of the vertex.
func Test_KeysByPattern_PinnedPrefixDoesNotScanTheVertex(t *testing.T) {
	build := func(links int) *vertexRecord {
		r := newVertexRecord(vertexData{BodyTime: -1}, defaultBucketLinks)
		for i := 0; i < links; i++ {
			name := fmt.Sprintf("l%05d", i)
			require.True(t, r.setOutTo(name, "t.dom/tgt", int64(i+1)))
			require.True(t, r.setOutIndexType(name, "t", int64(i+1), true))
			require.True(t, r.setOutTag(name, "важный", int64(i+1), true))
		}
		return r
	}
	ask := func(r *vertexRecord, links, rounds int) time.Duration {
		best := time.Duration(1<<62 - 1)
		for attempt := 0; attempt < 3; attempt++ {
			started := time.Now()
			for i := 0; i < rounds; i++ {
				name := fmt.Sprintf("l%05d", i%links)
				got := r.keysByPattern("dom/v", "dom/v.out.index."+name+".>")
				if len(got) != 2 {
					t.Fatalf("link %s: expected a type index and a tag, got %v", name, got)
				}
			}
			if d := time.Since(started); d < best {
				best = d
			}
		}
		return best / time.Duration(rounds)
	}

	const small, large = 100, 3000
	perSmall := ask(build(small), small, 2000)
	perLarge := ask(build(large), large, 2000)
	t.Logf("vertex with %d links: %s per query", small, perSmall)
	t.Logf("vertex with %d links: %s per query", large, perLarge)

	// The vertex is 30 times larger. A fivefold margin keeps the test from
	// failing on machine noise, yet still catches a return of the scan, which
	// would give thirty.
	require.Lessf(t, perLarge, perSmall*5,
		"an answer about one link got costlier as the vertex grew: %s vs %s, so the scan is back",
		perLarge, perSmall)
}

// Test_SpareFor_NeverBelowWhatIsHeld — the spare capacity must never be less
// than what the bucket already holds: make() with such a capacity panics. And a
// bucket can outgrow its ceiling — a split comes after the write, and at the
// maximum directory depth it does not come at all.
func Test_SpareFor_NeverBelowWhatIsHeld(t *testing.T) {
	for n := 0; n < 4*defaultBucketLinks+10; n++ {
		require.GreaterOrEqualf(t, spareFor(n), n, "the spare capacity for %d entries is below their count", n)
	}
}
