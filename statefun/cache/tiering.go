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
	"strings"
	"sync"

	"github.com/foliagecp/sdk/statefun/system"
)

// tieringEnabled is the CACHE_TIERING switch, read once at start.
var tieringEnabled = strings.EqualFold(
	system.GetEnvMustProceed[string]("CACHE_TIERING", "off"), "on")

// TieringEnabled reports whether vertices are kept as records.
func TieringEnabled() bool { return tieringEnabled }

// SetTieringForTest flips the switch and returns a function restoring it.
func SetTieringForTest(on bool) func() {
	prev := tieringEnabled
	tieringEnabled = on
	return func() { tieringEnabled = prev }
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

// tieredVertex returns the vertex id a key belongs to, and whether the key is a
// graph vertex key at all.
func tieredVertex(key string) (string, string, bool) {
	if !tieringEnabled {
		return "", "", false
	}
	id, tail := splitVertexKey(key)
	if id == "" || isRuntimeKey(id) {
		return "", "", false
	}
	if tail != "" {
		// Only the shapes CRUD writes live in a record; anything else stays a
		// tree key, so an unrecognised shape can never be silently swallowed.
		if k, _, _ := parseTail(tail); k == tailUnknown {
			return "", "", false
		}
	}
	return id, tail, true
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
	id, tail, ok := tieredVertex(key)
	if !ok {
		return nil, false, false
	}
	r, found := cs.records.get(id)
	if !found {
		return nil, false, false
	}
	v, e := r.get(tail)
	return v, e, true
}

func (cs *Store) tieredExists(key string) (exists bool, handled bool) {
	_, e, h := cs.tieredGet(key)
	return e, h
}

func (cs *Store) tieredUpdateTime(key string) (int64, bool) {
	id, tail, ok := tieredVertex(key)
	if !ok {
		return -1, false
	}
	r, found := cs.records.get(id)
	if !found {
		return -1, false
	}
	return r.updateTime(tail), true
}

// tieredKeys answers a pattern whose vertex is a record.
func (cs *Store) tieredKeys(pattern string) ([]string, bool) {
	id, _, ok := tieredVertex(strings.TrimSuffix(strings.TrimSuffix(pattern, ">"), "*"))
	if !ok {
		// A pattern ending right after the vertex id has an empty tail, which
		// tieredVertex accepts; anything it refuses is not a record's business.
		id2, _ := splitVertexKey(pattern)
		if !tieringEnabled || isRuntimeKey(id2) {
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
	id, tail, ok := tieredVertex(key)
	if !ok {
		return false
	}
	r := cs.records.getOrCreate(id)

	switch k, a, b := parseTail(tail); k {
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
	id, tail, ok := tieredVertex(key)
	if !ok {
		return false
	}
	r, found := cs.records.get(id)
	if !found {
		return false
	}

	switch k, a, b := parseTail(tail); k {
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
	if !tieringEnabled || cs.records == nil {
		return 0
	}
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		n += r.compactBuckets()
		return true
	})
	return n
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
