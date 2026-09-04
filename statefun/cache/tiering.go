package cache

// Tiering: a graph vertex is kept as a record instead of a subtree.
//
// Off by default. With CACHE_TIERING=on every graph vertex the cache is asked
// to write becomes a record; reads and writes of its keys are answered from
// there, and the tree keeps only what is not a graph vertex.
//
// WHAT STAYS IN THE TREE. Not everything in this cache is a graph vertex. The
// function runtime writes contexts under the function type's own name
// (`functions.…`) and object mutexes as a top-level key with a `-lock` suffix.
// A mutex key is shaped exactly like a vertex with a body and no links, so it
// cannot be told apart by shape — it is told apart by the rule below, which
// belongs to the runtime rather than to the cache. Demoting such a key would be
// pure overhead: it is written and deleted around every operation that takes the
// mutex.
//
// WHAT DOES NOT CHANGE. The keys are the keys, the WAL gets the same operations
// with the same bytes, and every question has the same answer it had before —
// which is what the differential tests exist to hold in place.

import (
	"os"
	"strings"
	"sync"

	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/sdk/statefun/system"
)

// cacheMode is how the cache stores the graph. One setting, four values, in
// increasing order of compactness — each does what the previous one does and
// more, so a single ordered value says everything there is to say and no two
// switches can contradict each other.
type cacheMode uint8

const (
	// modeTree is the representation this cache has always had: every key its
	// own node in a tree. No longer the default, and unchanged in every
	// respect — CACHE_MODE=tree brings it back exactly as it was.
	modeTree cacheMode = iota

	// modeRecords keeps a graph vertex as one compact record instead of a
	// subtree. The default since it beat the tree on both counts on the
	// reference dump: 3.5x less memory, and reads 34% (a link) and 13% (a body
	// read again) faster.
	modeRecords

	// modeZstd additionally compresses records that have gone cold.
	modeZstd

	// modeZstdDict compresses them against a dictionary trained on the graph's
	// own data, which is worth substantially more than plain compression
	// because buckets repeat each other across the whole graph.
	modeZstdDict
)

// CACHE_MODE names the mode in one setting. It is a PRESET: it decides what the
// individual settings default to, and an individual setting that is actually
// present in the environment still wins. So a deployment already passing
// CACHE_TIERING keeps behaving exactly as it did, and one that wants to say the
// whole thing in a word can.
// defaultCacheMode is what the cache does when nothing says otherwise.
const defaultCacheMode = "records"

const (
	cacheModeEnv   = "CACHE_MODE"
	tieringEnv     = "CACHE_TIERING"
	compressionEnv = "CACHE_RECORD_COMPRESSION"
)

var (
	modeGiven = envNamed(cacheModeEnv)

	// Records are the default. The tree is one setting away — CACHE_MODE=tree,
	// or CACHE_TIERING=off — and is unchanged, so a deployment that hits
	// something can go back without a build.
	currentMode = parseCacheMode(system.GetEnvMustProceed[string](cacheModeEnv, defaultCacheMode))

	// Resolved once at start: the preset, then whatever the environment states
	// outright.
	tieringOn     = resolveBool(tieringEnv, "on", currentMode >= modeRecords)
	compressionOn = resolveBool(compressionEnv, "zstd", currentMode >= modeZstd)

	// The dictionary follows the preset when there is one. Without a preset it
	// stays on wherever compression is — which is what the individual settings
	// did before CACHE_MODE existed, and changing that would have made adding a
	// convenience alter the behaviour of deployments that never asked for it.
	dictionaryOn = !modeGiven || currentMode >= modeZstdDict
)

// envNamed reports whether the environment states a setting at all.
func envNamed(key string) bool {
	v, present := os.LookupEnv(key)
	return present && strings.TrimSpace(v) != ""
}

// resolveBool takes the preset unless the environment names the setting, in
// which case what it names is what happens.
func resolveBool(key, onValue string, preset bool) bool {
	v, present := os.LookupEnv(key)
	if !present || strings.TrimSpace(v) == "" {
		return preset
	}
	return strings.EqualFold(strings.TrimSpace(v), onValue)
}

func parseCacheMode(name string) cacheMode {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		// Set but empty is not a choice of the tree — it is nothing said, the
		// same as unset, which is how envNamed reads it too.
		return parseCacheMode(defaultCacheMode)
	case "tree":
		return modeTree
	case "records":
		return modeRecords
	case "zstd":
		return modeZstd
	case "zstd-dict", "zstd+dict":
		return modeZstdDict
	default:
		lg.Logf(lg.ErrorLevel,
			"cache: unknown %s=%q, keeping the tree; expected tree, records, zstd or zstd-dict",
			cacheModeEnv, name)
		return modeTree
	}
}

func (m cacheMode) String() string {
	switch m {
	case modeRecords:
		return "records"
	case modeZstd:
		return "zstd"
	case modeZstdDict:
		return "zstd-dict"
	default:
		return "tree"
	}
}

// tieringEnabled reports whether graph vertices are kept as records.
func tieringEnabled() bool { return tieringOn }

// compressionEnabled reports whether cold buckets are compressed.
func compressionEnabled() bool { return tieringOn && compressionOn }

// dictionaryEnabled reports whether compression uses a trained dictionary.
//
// A sample count of zero is also a way to say no: there is nothing to learn
// from an empty sample, so asking for it is asking for compression without a
// dictionary, and what the mode reports has to agree with that.
func dictionaryEnabled() bool {
	return compressionEnabled() && dictionaryOn && dictSampleLimit > 0
}

// CacheMode reports what the cache is actually doing, resolved from the preset
// and the individual settings together — which is what belongs in a log line,
// since the preset alone can be overridden.
func CacheMode() string {
	switch {
	case !tieringEnabled():
		return "tree"
	case !compressionEnabled():
		return "records"
	case !dictionaryEnabled():
		return "zstd"
	default:
		return "zstd-dict"
	}
}

// SetTieringForTest switches records on or off and returns a restore function.
func SetTieringForTest(on bool) func() {
	prev := tieringOn
	tieringOn = on
	return func() { tieringOn = prev }
}

// SetCompressionForTest switches compression on or off.
func SetCompressionForTest(on bool) func() {
	prev := compressionOn
	compressionOn = on
	return func() { compressionOn = prev }
}

// SetCacheModeForTest switches the mode and returns a function restoring it.
// Takes the same names as the setting.
func SetCacheModeForTest(name string) func() {
	pm, pt, pc, pd := currentMode, tieringOn, compressionOn, dictionaryOn
	m := parseCacheMode(name)
	currentMode = m
	tieringOn = m >= modeRecords
	compressionOn = m >= modeZstd
	dictionaryOn = m >= modeZstdDict
	return func() { currentMode, tieringOn, compressionOn, dictionaryOn = pm, pt, pc, pd }
}

// isRuntimeKey reports whether a top-level key belongs to the runtime rather
// than to the graph. The rule is the runtime's, listed exhaustively:
//
//	functions…     a function context, keyed by the function type's name
//	<id>-lock      an object mutex
//
// Anything else written at the top level is a graph vertex. A new writer of
// runtime keys has to be added here — and that is the point of keeping the list
// in one place rather than guessing from a key's shape.
func isRuntimeKey(top string) bool {
	return top == runtimeFunctionsToken || strings.HasSuffix(top, objectMutexSuffix)
}

const (
	runtimeFunctionsToken = "functions"
	objectMutexSuffix     = "-lock"
)

// vertexKey is a cache key resolved once: the vertex it belongs to and the
// shape of its tail. Deciding a key belongs to a record already costs a full
// parse of the tail, so the answer is carried to whoever acts on it instead of
// being parsed a second time — this is on every read of every traversal.
type vertexKey struct {
	id   string
	kind tailKind
	a, b string
}

// isBody says the key is the vertex body itself rather than one of its links.
func (vk vertexKey) isBody() bool { return vk.kind == tailBody }

// tieredVertex resolves a key to the vertex it belongs to, and says whether the
// key is a graph vertex key at all.
func tieredVertex(key string) (vertexKey, bool) {
	if !tieringEnabled() {
		return vertexKey{}, false
	}
	id, tail := splitVertexKey(key)
	if id == "" || isRuntimeKey(id) {
		return vertexKey{}, false
	}
	if tail == "" {
		return vertexKey{id: id, kind: tailBody}, true
	}
	// Only the shapes CRUD writes live in a record; anything else stays a tree
	// key, so an unrecognised shape can never be silently swallowed.
	k, a, b := parseTail(tail)
	if k == tailUnknown {
		return vertexKey{}, false
	}
	return vertexKey{id: id, kind: k, a: a, b: b}, true
}

// recordIndex holds the records of a store. The root index of vertices is
// always fully in memory (Ф-1): asking about a key of a vertex that does not
// exist touches no record at all.
type recordIndex struct {
	mu sync.RWMutex
	m  map[string]*vertexRecord
}

func newRecordIndex() *recordIndex {
	return &recordIndex{m: map[string]*vertexRecord{}}
}

func (ri *recordIndex) get(id string) (*vertexRecord, bool) {
	ri.mu.RLock()
	r, ok := ri.m[id]
	ri.mu.RUnlock()
	return r, ok
}

// getOrCreate returns the record of id, making an empty one if needed.
func (ri *recordIndex) getOrCreate(id string) *vertexRecord {
	if r, ok := ri.get(id); ok {
		return r
	}
	ri.mu.Lock()
	defer ri.mu.Unlock()
	if r, ok := ri.m[id]; ok {
		return r
	}
	r := newVertexRecord(vertexData{BodyTime: -1}, defaultBucketLinks)
	ri.m[id] = r
	return r
}

// reset empties the index. A rehydration replaces the whole world, so what the
// records held before it is not a starting point to merge onto but state that
// no longer exists: without this a vertex deleted while this node was passive
// would come back to life on promotion.
func (ri *recordIndex) reset() {
	ri.mu.Lock()
	ri.m = map[string]*vertexRecord{}
	ri.mu.Unlock()
}

func (ri *recordIndex) len() int {
	ri.mu.RLock()
	defer ri.mu.RUnlock()
	return len(ri.m)
}

// each visits every record. The lock is held for the walk, so callers do short
// work per record — compaction takes the buckets' own locks, not this one.
func (ri *recordIndex) each(fn func(id string, r *vertexRecord) bool) {
	ri.mu.RLock()
	ids := make([]string, 0, len(ri.m))
	for id := range ri.m {
		ids = append(ids, id)
	}
	ri.mu.RUnlock()

	for _, id := range ids {
		r, ok := ri.get(id)
		if !ok {
			continue
		}
		if !fn(id, r) {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------------

// tieredGet answers a read from a record. handled is false when the key is not
// a record's business, and the caller falls through to the tree.
func (cs *Store) tieredGet(key string) (value []byte, exists bool, handled bool) {
	vk, ok := tieredVertex(key)
	if !ok {
		return nil, false, false
	}
	r, found := cs.records.get(vk.id)
	if !found {
		return nil, false, false
	}
	v, e := r.getParsed(vk.kind, vk.a, vk.b)
	return v, e, true
}

// tieredExists asks about existence without building the value: a traversal
// checks far more keys than it reads.
func (cs *Store) tieredExists(key string) (exists bool, handled bool) {
	vk, ok := tieredVertex(key)
	if !ok {
		return false, false
	}
	r, found := cs.records.get(vk.id)
	if !found {
		return false, false
	}
	return r.existsParsed(vk.kind, vk.a, vk.b), true
}

func (cs *Store) tieredUpdateTime(key string) (int64, bool) {
	vk, ok := tieredVertex(key)
	if !ok {
		return -1, false
	}
	r, found := cs.records.get(vk.id)
	if !found {
		return -1, false
	}
	return r.updateTimeParsed(vk.kind, vk.a, vk.b), true
}

// tieredKeys answers a pattern whose vertex is a record.
func (cs *Store) tieredKeys(pattern string) ([]string, bool) {
	vk, ok := tieredVertex(strings.TrimSuffix(strings.TrimSuffix(pattern, ">"), "*"))
	id := vk.id
	if !ok {
		// A pattern ending right after the vertex id has an empty tail, which
		// tieredVertex accepts; anything it refuses is not a record's business.
		id2, _ := splitVertexKey(pattern)
		if !tieringEnabled() || isRuntimeKey(id2) {
			return nil, false
		}
		id = id2
	}
	r, found := cs.records.get(id)
	if !found {
		return nil, false
	}
	return r.keysByPattern(id, pattern), true
}

// ---------------------------------------------------------------------------
// writing
// ---------------------------------------------------------------------------

// tieredSet applies a write to a record. handled is false for keys the tree
// still owns.
func (cs *Store) tieredSet(key string, value []byte, asJSON bool, t int64) (handled bool) {
	vk, ok := tieredVertex(key)
	if !ok {
		return false
	}
	r := cs.records.getOrCreate(vk.id)

	switch k, a, b := vk.kind, vk.a, vk.b; k {
	case tailBody:
		r.putBody(value, t, asJSON)

	case tailOutTo:
		r.setOutTo(a, string(value), t)

	case tailOutBody:
		r.setOutBody(a, value, t)

	case tailIndexType:
		r.setOutIndexType(a, b, t, true)

	case tailIndexTag:
		r.setOutTag(a, b, t, true)

	case tailLinkType:
		r.putPair(pairEntry{Type: a, Target: b, Name: string(value), UpdateTime: t})

	case tailIn:
		r.putInLink(inLink{From: a, Name: b, Type: string(value), UpdateTime: t})

	default:
		return false
	}
	return true
}

// tieredDelete removes one key from a record.
func (cs *Store) tieredDelete(key string, t int64) (handled bool) {
	vk, ok := tieredVertex(key)
	if !ok {
		return false
	}
	r, found := cs.records.get(vk.id)
	if !found {
		return false
	}

	switch k, a, b := vk.kind, vk.a, vk.b; k {
	case tailBody:
		r.deleteBody(t)

	case tailOutTo:
		r.deleteOutTo(a, t)

	case tailOutBody:
		r.deleteOutBody(a, t)

	case tailIndexType:
		// CRUD drops the index key of the old type when a link changes type,
		// while the link itself lives on with its new one.
		r.setOutIndexType(a, b, t, false)

	case tailIndexTag:
		r.setOutTag(a, b, t, false)

	case tailLinkType:
		r.deletePair(a, b, t)

	case tailIn:
		r.deleteInLink(a, b, t)

	default:
		return false
	}
	return true
}

// splitTypeTarget cuts the value of `out.to.<name>`, which CRUD writes as
// "<type>.<target>". Neither part can contain a dot: both are cache key tokens.
func splitTypeTarget(v string) (linkType, target string) {
	if i := strings.IndexByte(v, '.'); i >= 0 {
		return v[:i], v[i+1:]
	}
	return v, ""
}

// ---------------------------------------------------------------------------
// maintenance
// ---------------------------------------------------------------------------

// compactRecords re-encodes buckets left decoded by writes, and returns how
// many it encoded. Called from the maintenance pass: writing deliberately does
// not encode, so this is where the memory comes back.
func (cs *Store) compactRecords() int {
	if !tieringEnabled() || cs.records == nil {
		return 0
	}
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		n += r.compactBuckets()
		return true
	})
	return n
}

// recordStats is what one walk over the records sees: the shape of the graph as
// it is actually held. Collected in the maintenance pass and nowhere else —
// what the cache looks like is a thing to measure once a second, not something
// to pay for on every read.
type recordStats struct {
	vertices     int // vertices kept as records
	bytes        int // what those records hold, counted deterministically
	buckets      int // buckets across all three directories
	compressed   int // of those, held as a compressed frame
	decoded      int // of those, left as Go objects by a write, awaiting compaction
	parsedBodies int // bodies held as a parsed tree — the part that grows with reading
}

func (cs *Store) recordStats() recordStats {
	var st recordStats
	if !tieringEnabled() || cs.records == nil {
		return st
	}
	cs.records.each(func(_ string, r *vertexRecord) bool {
		st.vertices++
		st.bytes += r.approxBytes()
		total, compressed, decoded := r.bucketCounts()
		st.buckets += total
		st.compressed += compressed
		st.decoded += decoded
		if r.parsedBody.Load() != nil {
			st.parsedBodies++
		}
		return true
	})
	return st
}

// RecordsBytesForTest is the deterministic size of everything held as records.
func (cs *Store) RecordsBytesForTest() int {
	if cs.records == nil {
		return 0
	}
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		n += r.approxBytes()
		return true
	})
	return n
}

// RecordCountForTest is how many vertices are kept as records.
func (cs *Store) RecordCountForTest() int {
	if cs.records == nil {
		return 0
	}
	return cs.records.len()
}

// ParsedBodyCountForTest is how many records are currently holding a body as a
// parsed tree — the only part of a record that grows with reading.
func (cs *Store) ParsedBodyCountForTest() int {
	if !tieringEnabled() || cs.records == nil {
		return 0
	}
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		if r.parsedBody.Load() != nil {
			n++
		}
		return true
	})
	return n
}

// StoresAsRecord reports whether the value at key is held as a record, and so
// whether StoredValueEquals can answer about it. Asked before serializing the
// candidate, so a caller that will not get an answer does not pay for one.
func (cs *Store) StoresAsRecord(key string) bool {
	vk, ok := tieredVertex(key)
	if !ok {
		return false
	}
	_, found := cs.records.get(vk.id)
	return found
}

// StoredValueEquals reports whether the value stored at key is byte-for-byte
// what the caller has in hand, answering from a record without building a tree.
// known is false when the key is not a record's business and the caller has to
// decide for itself.
//
// This exists because the commonest question asked of a vertex body is not
// "what is in it" but "did it change" — an inventory rebuild asks it for every
// vertex on every cycle. Answering it by parsing the stored body and
// serializing both sides costs 6.7 us on a real body; comparing the bytes the
// record already holds against the serialization the write needs anyway costs
// 2.1 us, which is less than the tree ever spent on the same question.
//
// Byte comparison is the right test, not a weaker one: bodies are serialized by
// the same function, json.Marshal sorts object keys, and a number renders the
// same whichever Go type carried it. So equal bytes mean equal values, and
// different bytes mean the write has something to do.
func (cs *Store) StoredValueEquals(key string, serialized []byte) (equal bool, known bool) {
	vk, ok := tieredVertex(key)
	if !ok {
		return false, false
	}
	r, found := cs.records.get(vk.id)
	if !found {
		return false, false
	}
	if vk.isBody() {
		body, _, exists := r.bodyBytes()
		if !exists {
			return false, true
		}
		return body == string(serialized), true
	}
	v, _, exists := r.lookupParsed(vk.kind, vk.a, vk.b)
	if !exists {
		return false, true
	}
	return v == string(serialized), true
}
