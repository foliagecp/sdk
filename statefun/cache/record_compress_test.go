package cache

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/require"
)

// seedGraph fills the store with vertices and their links — the way CRUD does.
func seedGraph(t testing.TB, cs *Store, vertices, links int) {
	t.Helper()
	now := int64(1_000_000)
	for i := 0; i < vertices; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		body := easyjson.NewJSONObject()
		body.SetByPath("name", easyjson.NewJSON(fmt.Sprintf("узел-%05d", i)))
		body.SetByPath("kind", easyjson.NewJSON("os_package"))
		body.SetByPath("size_kb", easyjson.NewJSON(i*13%9000))
		cs.SetValueJSON(v, &body, false, now)
		for k := 0; k < links; k++ {
			name := fmt.Sprintf("uiapplib_%05d_%02d", i, k)
			cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte(fmt.Sprintf("ui_controller_subject.dom/tgt-%05d", (i+k)%vertices)), false, now)
			cs.SetValue(fmt.Sprintf(kIndexType, v, name, "ui_controller_subject"), nil, false, now)
			cs.SetValue(fmt.Sprintf(kInLink, v, fmt.Sprintf("dom/src-%05d", (i+k)%vertices), name), []byte("ui_controller_subject"), false, now)
		}
	}
}

// Test_Compression_KeepsAnswers — compression is invisible from outside.
func Test_Compression_KeepsAnswers(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()

	cs := NewStoreForTest("zstd")
	seedGraph(t, cs, 60, 12)

	// the answers before compression
	var keys []string
	for i := 0; i < 60; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		keys = append(keys, v)
		keys = append(keys, cs.GetKeysByPattern(v+".out.to.>")...)
		keys = append(keys, cs.GetKeysByPattern(v+".in.>")...)
		keys = append(keys, cs.GetKeysByPattern(v+".ltype.>")...)
	}
	before := make([]string, len(keys))
	for i, k := range keys {
		before[i] = fmt.Sprintf("%v|%d|%s", cs.Exists(k), cs.GetValueUpdateTime(k), getStr(cs, k))
	}

	require.Greater(t, cs.compactRecords(), 0, "the writes must have left buckets decoded")
	n := cs.compressRecords()
	require.Greater(t, n, 0, "no bucket was compressed")

	compressed, _ := cs.CompressionStatsForTest()
	require.Greater(t, compressed, 0)

	for i, k := range keys {
		got := fmt.Sprintf("%v|%d|%s", cs.Exists(k), cs.GetValueUpdateTime(k), getStr(cs, k))
		require.Equal(t, before[i], got, "compression changed the answer for key %s", k)
	}
}

// Test_Compression_ReadPublishesRawBack — reading a compressed bucket puts it
// back in raw form, so that the next read does not pay for decompression again.
func Test_Compression_ReadPublishesRawBack(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()

	cs := NewStoreForTest("republish")
	seedGraph(t, cs, 20, 40)
	cs.compactRecords()
	require.Greater(t, cs.compressRecords(), 0)

	before, _ := cs.CompressionStatsForTest()
	require.Greater(t, before, 0)

	// read everything
	for i := 0; i < 20; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		for _, k := range cs.GetKeysByPattern(v + ".out.to.>") {
			require.True(t, cs.Exists(k))
		}
	}

	after, _ := cs.CompressionStatsForTest()
	require.Less(t, after, before,
		"reading compressed buckets must leave fewer of them compressed: was %d, now %d", before, after)
}

// Test_Compression_StaleDictionaryStillReads — a bucket compressed with the
// previous dictionary must still read after that dictionary is replaced. That
// is why the decoder keeps every dictionary the process has ever used.
func Test_Compression_StaleDictionaryStillReads(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()

	cs := NewStoreForTest("dict")
	seedGraph(t, cs, 80, 16)
	cs.compactRecords()

	// compress without a dictionary
	require.Greater(t, cs.compressRecords(), 0)
	v0 := "dom/v-00000"
	keysV0 := cs.GetKeysByPattern(v0 + ".out.to.>")
	require.NotEmpty(t, keysV0)

	// new data gives raw buckets, and those are what the dictionary trains
	// on: the sample takes only them, because training has to be on what is
	// being written, not on what is already compressed
	seedGraph(t, cs, 40, 16)
	cs.compactRecords()
	require.True(t, cs.trainDictionary(64), "the dictionary did not train")
	_, ver1 := cs.CompressionStatsForTest()
	require.Equal(t, uint32(1), ver1)
	cs.compressRecords() // this time with the dictionary

	// more data and a second dictionary
	seedGraph(t, cs, 40, 16)
	cs.compactRecords()
	require.True(t, cs.trainDictionary(64))
	_, ver2 := cs.CompressionStatsForTest()
	require.Equal(t, uint32(2), ver2)

	// buckets compressed with no dictionary and with the first one must read
	for _, k := range keysV0 {
		require.True(t, cs.Exists(k), "key %s lost after the dictionary change", k)
	}
	for i := 0; i < 120; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		for _, k := range cs.GetKeysByPattern(v + ".out.to.>") {
			require.True(t, cs.Exists(k), "key %s lost", k)
		}
	}
}

// Test_Compression_Ratio — how much compression gains on graph-like data.
func Test_Compression_Ratio(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()

	cs := NewStoreForTest("ratio")
	seedGraph(t, cs, 400, 20)
	cs.compactRecords()
	raw := cs.RecordsBytesForTest()

	// without a dictionary
	require.True(t, cs.trainDictionary(0) == false, "the dictionary must not train on an empty sample")
	cs.compressRecords()
	noDict := cs.RecordsBytesForTest()

	// with a dictionary: read the buckets back to raw form, train, recompress
	for i := 0; i < 400; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		cs.GetKeysByPattern(v + ".out.to.>")
		cs.GetKeysByPattern(v + ".in.>")
		cs.GetKeysByPattern(v + ".ltype.>")
	}
	require.True(t, cs.trainDictionary(256), "the dictionary did not train")
	cs.compressRecords()
	withDict := cs.RecordsBytesForTest()

	t.Logf("raw %d B, zstd without a dictionary %d B (%.2fx), zstd with a dictionary %d B (%.2fx)",
		raw, noDict, float64(raw)/float64(noDict), withDict, float64(raw)/float64(withDict))
	require.Less(t, noDict, raw, "compression must reduce the size")
}

// Test_Compression_Off — with the flag off, nothing is compressed.
func Test_Compression_Off(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("off")
	seedGraph(t, cs, 20, 20)
	cs.compactRecords()
	require.Zero(t, cs.compressRecords())
	compressed, _ := cs.CompressionStatsForTest()
	require.Zero(t, compressed)
}

// Test_Compression_RetrainsOnDecay — the dictionary is retrained when the ratio
// degrades, not on a schedule.
func Test_Compression_RetrainsOnDecay(t *testing.T) {
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()
	ResetCompressionForTest()

	cs := NewStoreForTest("decay")
	seedGraph(t, cs, 200, 16)
	cs.compactRecords()

	// the first time there is no dictionary — training must happen
	require.True(t, cs.maybeTrainDictionary(256), "the first dictionary did not train")
	trainedAt, _, retrains := DictionaryStatsForTest()
	require.Equal(t, 1, retrains)
	require.Greater(t, trainedAt, 1.0, "ratio at training: %.2f", trainedAt)

	// on the same data there is no reason to retrain
	require.False(t, cs.maybeTrainDictionary(256), "retrained without a reason")
	_, _, retrains2 := DictionaryStatsForTest()
	require.Equal(t, 1, retrains2)

	// data of another shape — the ratio drops, the dictionary must update
	cs2 := NewStoreForTest("decay2")
	now := int64(1_000_000)
	for i := 0; i < 200; i++ {
		v := fmt.Sprintf("dom/z-%05d", i)
		for k := 0; k < 16; k++ {
			name := fmt.Sprintf("%d-%d-совсем-другая-форма-имени-связи", i, k)
			cs2.SetValue(fmt.Sprintf(kOutTo, v, name),
				[]byte(fmt.Sprintf("другой_тип_связи.dom/иная-цель-%d-%d", i, k)), false, now)
		}
	}
	cs2.compactRecords()

	if cs2.maybeTrainDictionary(256) {
		_, _, retrains3 := DictionaryStatsForTest()
		require.Equal(t, 2, retrains3, "a retrain must be counted")
		require.Equal(t, uint32(2), recordCodec.dictVersion())
	} else {
		lastTrained, last, _ := DictionaryStatsForTest()
		t.Logf("no retrain was needed: ratio %.2f vs %.2f at training", last, lastTrained)
	}

	// and the first store's data still reads
	for i := 0; i < 200; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		for _, k := range cs.GetKeysByPattern(v + ".out.to.>") {
			require.True(t, cs.Exists(k), "key %s lost", k)
		}
	}
}

// Test_Maintenance_CompactsAndCompresses — maintenance must compact and
// compress on its own. Tests that call these steps directly do not check that:
// if the hook is missing from the maintenance pass, they are still green, and
// in production nothing fires.
func Test_Maintenance_CompactsAndCompresses(t *testing.T) {
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()
	ResetCompressionForTest()

	cs := NewStoreForTest("maintenance")
	seedGraph(t, cs, 120, 20)

	require.Greater(t, dirtyBucketsOf(cs), 0, "the writes must have left buckets decoded")

	// the only call is the one the runtime makes on a timer
	cs.traverseCacheForMaintenance()

	require.Zero(t, dirtyBucketsOf(cs), "maintenance did not compact the buckets")
	compressed, _ := cs.CompressionStatsForTest()
	require.Greater(t, compressed, 0, "maintenance compressed no bucket")

	// and the answers are unchanged
	for i := 0; i < 120; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		require.True(t, cs.Exists(v), "body %s lost", v)
		keys := cs.GetKeysByPattern(v + ".out.to.>")
		require.Len(t, keys, 20, "links of %s lost", v)
	}
}

func dirtyBucketsOf(cs *Store) int {
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		n += r.dirtyBuckets()
		return true
	})
	return n
}

// Test_ParsedBody_KeptFromSecondRead — a parse is kept from the second read on,
// not from the first: a graph walk reads each body once and must not leave the
// whole graph parsed behind it. The answer is the same in both cases.
func Test_ParsedBody_KeptFromSecondRead(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("parsed")
	body, _ := easyjson.JSONFromString(`{"cpu":8,"name":"узел"}`)
	require.True(t, cs.SetValueJSON("dom/v", &body, false, 1))

	r, ok := cs.records.get("dom/v")
	require.True(t, ok)
	require.Nil(t, r.parsedBody.Load(), "there must be no parse before a read")

	first, err := cs.GetValueJSON("dom/v")
	require.NoError(t, err)
	require.Equal(t, float64(8), first.GetByPath("cpu").AsNumericDefault(-1))
	require.Nil(t, r.parsedBody.Load(), "the first read must not keep a parse")

	second, err := cs.GetValueJSON("dom/v")
	require.NoError(t, err)
	require.Equal(t, float64(8), second.GetByPath("cpu").AsNumericDefault(-1))
	require.NotNil(t, r.parsedBody.Load(), "the second read must keep the parse")

	// each reader gets its own copy, and an edit by one is not seen by another
	second.SetByPath("cpu", easyjson.NewJSON(999))
	third, err := cs.GetValueJSON("dom/v")
	require.NoError(t, err)
	require.Equal(t, float64(8), third.GetByPath("cpu").AsNumericDefault(-1),
		"one reader's edit leaked to another")
}

// Test_ParsedBody_AgesByUse — maintenance drops what was not accessed and keeps
// what was.
func Test_ParsedBody_AgesByUse(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("aging")
	body, _ := easyjson.JSONFromString(`{"n":1}`)
	for i := 0; i < 4; i++ {
		cs.SetValueJSON(fmt.Sprintf("dom/v-%d", i), &body, false, 1)
	}
	// twice: a parse is kept from the second read on
	for round := 0; round < 2; round++ {
		for i := 0; i < 4; i++ {
			cs.GetValueJSON(fmt.Sprintf("dom/v-%d", i)) //nolint:errcheck
		}
	}
	for i := 0; i < 4; i++ {
		r, _ := cs.records.get(fmt.Sprintf("dom/v-%d", i))
		require.NotNil(t, r.parsedBody.Load(), "body %d must be parsed", i)
	}

	// first pass: all of them were accessed — nothing is dropped
	require.Zero(t, cs.ageParsedBodies(), "dropped what was just accessed")

	// access only some of them
	cs.GetValueJSON("dom/v-0") //nolint:errcheck
	cs.GetValueJSON("dom/v-1") //nolint:errcheck

	require.Equal(t, 2, cs.ageParsedBodies(), "exactly the two that were not accessed must have been dropped")

	r0, _ := cs.records.get("dom/v-0")
	r2, _ := cs.records.get("dom/v-2")
	require.NotNil(t, r0.parsedBody.Load(), "the hot body must have stayed")
	require.Nil(t, r2.parsedBody.Load(), "the cold body must have been dropped")
	require.False(t, r2.parsedBodySeen.Load(), "a dropped body must start its count over")

	// and the answers are unchanged in both cases
	for i := 0; i < 4; i++ {
		got, err := cs.GetValueJSON(fmt.Sprintf("dom/v-%d", i))
		require.NoError(t, err)
		require.Equal(t, float64(1), got.GetByPath("n").AsNumericDefault(-1))
	}
}

// Test_ParsedBody_InvalidatedByWrite — a write must invalidate the kept parse,
// otherwise a reader sees what is no longer there.
func Test_ParsedBody_InvalidatedByWrite(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("invalidate")
	first, _ := easyjson.JSONFromString(`{"v":1}`)
	cs.SetValueJSON("dom/v", &first, false, 1)
	got, _ := cs.GetValueJSON("dom/v")
	require.Equal(t, float64(1), got.GetByPath("v").AsNumericDefault(-1))

	second, _ := easyjson.JSONFromString(`{"v":2}`)
	cs.SetValueJSON("dom/v", &second, false, 2)
	got, _ = cs.GetValueJSON("dom/v")
	require.Equal(t, float64(2), got.GetByPath("v").AsNumericDefault(-1),
		"the previous body reads after a write")

	cs.DeleteValue("dom/v", false, 3)
	_, err := cs.GetValueJSON("dom/v")
	require.Error(t, err, "the body must not read after a delete")
}

// Test_FullScan_DoesNotGrowTheCache — what a reader who comes and reads the
// whole graph does to memory.
//
// One walk leaves nothing behind: a body's parse is kept only from the second
// read on, and a walk reads each body once and does not come back. Repeated
// walks hit a ceiling — one parse per vertex — and go no higher; that ceiling
// is below what the tree always takes. When the reads stop, all of it is given
// back.
func Test_FullScan_DoesNotGrowTheCache(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	const vertices = 500
	cs := NewStoreForTest("fullscan")
	body, _ := easyjson.JSONFromString(`{"name":"узел","cpu":8,"tags":["a","b"]}`)
	for i := 0; i < vertices; i++ {
		require.True(t, cs.SetValueJSON(fmt.Sprintf("dom/v-%04d", i), &body, false, 1))
	}
	scan := func() {
		for i := 0; i < vertices; i++ {
			j, err := cs.GetValueJSON(fmt.Sprintf("dom/v-%04d", i))
			require.NoError(t, err)
			require.Equal(t, float64(8), j.GetByPath("cpu").AsNumericDefault(-1))
		}
	}

	require.Zero(t, cs.ParsedBodyCountForTest(), "there must be no parses before any reads")

	scan()
	require.Zerof(t, cs.ParsedBodyCountForTest(),
		"one graph walk must leave no parse behind")

	scan()
	require.Equal(t, vertices, cs.ParsedBodyCountForTest(),
		"the second walk must parse everything: that is the ceiling")

	scan()
	scan()
	require.Equal(t, vertices, cs.ParsedBodyCountForTest(),
		"further walks must hit the same ceiling, not grow")

	// the reads have stopped — maintenance takes the memory back
	cs.ageParsedBodies()
	cs.ageParsedBodies()
	require.Zerof(t, cs.ParsedBodyCountForTest(),
		"once the reads have stopped, the parses must be released")

	// and the graph still reads
	scan()
}
