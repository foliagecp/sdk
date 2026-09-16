package cache

// The maintenance pass over the records visits what changed and nothing else.
//
// A pass used to walk every record once a second whether anything had
// happened or not — on a graph nobody was writing to, that walk was most of a
// core. Now a record is visited only after it asked: a write left a bucket
// decoded or a body to compress, a read decompressed a bucket or kept a body
// parse, a delete may have emptied the vertex. A pass on a quiet graph visits
// nothing; and what the pass does for the records it visits is what it always
// did — compact, compress, age the parse, sweep the dead.

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/require"
)

func pass(cs *Store) recordPass {
	cs.RunMaintenanceForTest()
	v, c, z, a, r := cs.LastRecordPassForTest()
	return recordPass{visited: v, compacted: c, compressed: z, aged: a, removed: r}
}

// Test_Pass_VisitsOnlyWhatChanged — after the graph is written every record is
// visited once; a quiet graph is visited not at all; a write to one vertex
// brings back exactly that vertex.
func Test_Pass_VisitsOnlyWhatChanged(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()
	cs := NewStoreForTest("pass_changed")
	const vertices = 50
	seedGraph(t, cs, vertices, 8)
	require.Equal(t, vertices, cs.RecordCountForTest())

	p := pass(cs)
	require.Equal(t, vertices, p.visited, "every written record is visited by the pass after the writes")
	require.Greater(t, p.compacted, 0, "the writes left buckets decoded, and the pass encoded them")

	p = pass(cs)
	require.Zero(t, p.visited, "nothing was written or read: the pass has nothing to look at")
	require.Zero(t, cs.RecordsAwaitingPassForTest())

	cs.SetValue(fmt.Sprintf(kOutTo, "dom/v-00007", "late"), []byte("ui_controller_subject.dom/tgt-00001"), false, 2_000_000)
	require.Equal(t, 1, cs.RecordsAwaitingPassForTest(), "one write, one record asking")
	p = pass(cs)
	require.Equal(t, 1, p.visited, "one write, one record visited")
	require.Greater(t, p.compacted, 0)
	require.Zero(t, pass(cs).visited)
}

// Test_Pass_SweepsADeletedVertex — deleting every key of a vertex puts the
// record on the list, and the pass that visits it sweeps it out of the index.
func Test_Pass_SweepsADeletedVertex(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()
	cs := NewStoreForTest("pass_sweep")
	seedGraph(t, cs, 10, 4)
	require.Zero(t, pass(cs).removed)
	require.Equal(t, 10, cs.RecordCountForTest())

	v := "dom/v-00003"
	later := int64(3_000_000)
	require.True(t, cs.DeleteValue(v, false, later), "the body")
	for _, k := range cs.GetKeysByPattern(v + ".out.to.>") {
		require.True(t, cs.DeleteValue(k, false, later), k)
	}
	for _, k := range cs.GetKeysByPattern(v + ".out.index.>") {
		require.True(t, cs.DeleteValue(k, false, later), k)
	}
	for _, k := range cs.GetKeysByPattern(v + ".in.>") {
		require.True(t, cs.DeleteValue(k, false, later), k)
	}
	require.Equal(t, 1, cs.RecordsAwaitingPassForTest(), "only the deleted vertex is asking")

	p := pass(cs)
	require.Equal(t, 1, p.visited)
	require.Equal(t, 1, p.removed, "a vertex with nothing left is swept out of the index")
	require.Equal(t, 9, cs.RecordCountForTest())
	require.Zero(t, pass(cs).visited)
}

// Test_Pass_AgesAKeptParse — a parse kept for a reader is visited by the pass
// until it has gone a whole pass unused; then it is dropped, and the record
// stops asking.
func Test_Pass_AgesAKeptParse(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()
	cs := NewStoreForTest("pass_parse")
	seedGraph(t, cs, 5, 2)
	require.Equal(t, 5, pass(cs).visited)
	require.Zero(t, pass(cs).visited, "sanity: quiet after the writes are dealt with")
	require.Zero(t, cs.ParsedBodyCountForTest())

	// The first read parses and keeps nothing; the second keeps the parse.
	_, err := cs.GetValueJSON("dom/v-00002")
	require.NoError(t, err)
	require.Zero(t, cs.ParsedBodyCountForTest(), "a first read keeps no parse")
	_, err = cs.GetValueJSON("dom/v-00002")
	require.NoError(t, err)
	require.Equal(t, 1, cs.ParsedBodyCountForTest(), "a second read keeps the parse")
	require.Equal(t, 1, cs.RecordsAwaitingPassForTest(), "and the record asks for the pass that ages it")

	p := pass(cs)
	require.Equal(t, 1, p.visited)
	require.Zero(t, p.aged, "used since the last pass: the parse stays")
	require.Equal(t, 1, cs.ParsedBodyCountForTest())
	require.Equal(t, 1, cs.RecordsAwaitingPassForTest(), "a kept parse keeps the record on the list")

	p = pass(cs)
	require.Equal(t, 1, p.visited)
	require.Equal(t, 1, p.aged, "unused for a whole pass: the parse goes")
	require.Zero(t, cs.ParsedBodyCountForTest())
	require.Zero(t, cs.RecordsAwaitingPassForTest(), "with the parse gone the record has nothing to ask for")
	require.Zero(t, pass(cs).visited)
}

// Test_Pass_RecompressesWhatAReadDecompressed — a read that decompresses a
// bucket or a body leaves the raw form behind for the next read, and the pass
// compresses it again: the record asked.
func Test_Pass_RecompressesWhatAReadDecompressed(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd")
	defer restore()
	cs := NewStoreForTest("pass_recompress")
	// Bodies and buckets well past the compression threshold.
	now := int64(1_000_000)
	for i := 0; i < 8; i++ {
		v := fmt.Sprintf("dom/big-%02d", i)
		body := easyjson.NewJSONObject()
		for f := 0; f < 40; f++ {
			body.SetByPath(fmt.Sprintf("field_%02d", f), easyjson.NewJSON(fmt.Sprintf("value-%02d-%02d-padding-padding-padding", i, f)))
		}
		cs.SetValueJSON(v, &body, false, now)
		for k := 0; k < 40; k++ {
			name := fmt.Sprintf("link_%02d_%02d", i, k)
			cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte(fmt.Sprintf("__object.dom/tgt-%02d", k)), false, now)
		}
	}
	p := pass(cs)
	require.Equal(t, 8, p.visited)
	require.Greater(t, p.compressed, 0, "the pass compresses what the writes left")
	_, _, _, compressedBefore, _, _ := cs.RecordStatsForTest()
	require.Greater(t, compressedBefore, 0)
	require.Zero(t, pass(cs).visited, "sanity: quiet once compressed")

	// Reads: the body, and a link lookup that decompresses its bucket.
	_, err := cs.GetValueJSON("dom/big-03")
	require.NoError(t, err)
	require.True(t, cs.Exists(fmt.Sprintf(kOutTo, "dom/big-03", "link_03_07")))
	_, _, _, compressedAfterRead, _, _ := cs.RecordStatsForTest()
	require.Less(t, compressedAfterRead, compressedBefore, "sanity: the reads left raw forms behind")
	require.Equal(t, 1, cs.RecordsAwaitingPassForTest(), "the record the reads decompressed is asking")

	p = pass(cs)
	require.Equal(t, 1, p.visited)
	require.Greater(t, p.compressed, 0, "the pass compresses again what the reads decompressed")
	_, _, _, compressedAfterPass, _, _ := cs.RecordStatsForTest()
	require.Equal(t, compressedBefore, compressedAfterPass, "everything is compressed again")
	require.Zero(t, pass(cs).visited)
}

// Test_Pass_EveryReadThatDecompressesAsks — a record cannot list itself: a
// read that leaves a raw form behind raises the record's flag, and the store
// lists the record on the way out of the read. Every read the store answers
// from a record goes through that, whichever kind of key it asked about.
func Test_Pass_EveryReadThatDecompressesAsks(t *testing.T) {
	bigBody := func(i int) easyjson.JSON {
		body := easyjson.NewJSONObject()
		for f := 0; f < 40; f++ {
			body.SetByPath(fmt.Sprintf("field_%02d", f), easyjson.NewJSON(fmt.Sprintf("value-%02d-%02d-padding-padding-padding", i, f)))
		}
		return body
	}
	reads := []struct {
		name string
		read func(cs *Store, v string)
	}{
		{"GetValue of a link", func(cs *Store, v string) { _, _ = cs.GetValue(fmt.Sprintf(kOutTo, v, "link_07")) }},
		{"Exists of a link", func(cs *Store, v string) { _ = cs.Exists(fmt.Sprintf(kOutTo, v, "link_07")) }},
		{"GetValueUpdateTime of a link", func(cs *Store, v string) { _ = cs.GetValueUpdateTime(fmt.Sprintf(kOutTo, v, "link_07")) }},
		{"GetKeysByPattern over the links", func(cs *Store, v string) { _ = cs.GetKeysByPattern(v + ".out.to.>") }},
		{"GetValue of the body", func(cs *Store, v string) { _, _ = cs.GetValue(v) }},
		{"GetValueJSON of the body", func(cs *Store, v string) { _, _ = cs.GetValueJSON(v) }},
		{"ExistsJson of the body", func(cs *Store, v string) { _ = cs.ExistsJson(v) }},
		{"StoredValueEquals of the body", func(cs *Store, v string) { _, _ = cs.StoredValueEquals(v, []byte("{}")) }},
	}
	for i, rd := range reads {
		t.Run(rd.name, func(t *testing.T) {
			ResetCompressionForTest()
			restore := SetCacheModeForTest("zstd")
			defer restore()
			cs := NewStoreForTest(fmt.Sprintf("pass_reads_%02d", i))
			v := "dom/read-me"
			body := bigBody(i)
			cs.SetValueJSON(v, &body, false, 1_000_000)
			for k := 0; k < 40; k++ {
				cs.SetValue(fmt.Sprintf(kOutTo, v, fmt.Sprintf("link_%02d", k)), []byte(fmt.Sprintf("__object.dom/tgt-%02d", k)), false, 1_000_000)
			}
			require.Equal(t, 1, pass(cs).visited)
			require.Zero(t, pass(cs).visited, "sanity: quiet once compressed")
			_, _, _, compressed, _, _ := cs.RecordStatsForTest()
			require.Greater(t, compressed, 0, "sanity: there is something to decompress")
			// A raw form is bigger than its frame, body or bucket: the bytes
			// the record holds say whether one was left behind.
			cold := cs.RecordsBytesForTest()

			rd.read(cs, v)
			require.Greater(t, cs.RecordsBytesForTest(), cold, "sanity: the read left a raw form behind")
			require.Equal(t, 1, cs.RecordsAwaitingPassForTest(), "the store must have listed the record after the read")

			require.Equal(t, 1, pass(cs).visited)
			require.Equal(t, cold, cs.RecordsBytesForTest(), "the pass must have compressed again what the read decompressed")
		})
	}
}

// Test_Pass_AVertexWithoutABodyGoesQuiet — the pass asks every record it
// visits whether it is dead, and for a vertex without a body that means
// looking into its link buckets. Looking must not decompress them into the
// slot: that would leave the record asking for the pass that just compressed
// it, and the pass would visit it — compress, look, decompress — forever.
func Test_Pass_AVertexWithoutABodyGoesQuiet(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd")
	defer restore()
	cs := NewStoreForTest("pass_nobody")
	v := "dom/no-body"
	for k := 0; k < 40; k++ {
		cs.SetValue(fmt.Sprintf(kOutTo, v, fmt.Sprintf("link_%02d", k)), []byte(fmt.Sprintf("__object.dom/tgt-%02d", k)), false, 1_000_000)
	}
	require.False(t, cs.Exists(v), "sanity: no body")
	p := pass(cs)
	require.Equal(t, 1, p.visited)
	require.Greater(t, p.compressed, 0, "sanity: the links compressed")
	_, _, _, compressed, _, _ := cs.RecordStatsForTest()
	require.Greater(t, compressed, 0)

	for i := 0; i < 3; i++ {
		require.Zero(t, pass(cs).visited, "pass %d: a vertex nobody touches must not keep coming back", i+2)
		_, _, _, still, _, _ := cs.RecordStatsForTest()
		require.Equal(t, compressed, still, "pass %d: and its buckets must stay compressed", i+2)
	}
	require.Equal(t, 1, cs.RecordCountForTest(), "a vertex with links is not dead")
}

// Test_Pass_ListIsDroppedWithTheIndex — a rehydration empties the index, and
// the attention list with it: it pointed at records that no longer exist.
func Test_Pass_ListIsDroppedWithTheIndex(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()
	cs := NewStoreForTest("pass_reset")
	seedGraph(t, cs, 3, 1)
	require.Equal(t, 3, cs.RecordsAwaitingPassForTest())
	cs.records.reset()
	require.Zero(t, cs.RecordsAwaitingPassForTest())
	require.Zero(t, cs.RecordCountForTest())
	require.Zero(t, pass(cs).visited)
}
