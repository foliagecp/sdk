package system

import (
	"errors"
	"sync"
)

// shard is an independent segment with its own map and RWMutex.
// Contention is limited to a single shard for operations on keys that hash there.
//
// The map is lazily allocated on the first write into this shard
// (see Set / LoadOrStore). Reads, Delete, Len, Range, Keys and Snapshot all
// rely on Go's well-defined nil-map behaviour ("read/iterate/delete on a nil
// map is a no-op; len(nil) == 0") and therefore do not need to allocate.
//
// Why lazy: in the cache tree under cache.StoreValue.store a fresh node with
// one child still ends up creating an 8-shard ShardedMap. With eager
// allocation that meant 8 empty hmap headers (~48 B each) per node, even
// when only one shard ever gets used — at ~600 k such nodes that is ~200 MB
// of hmap headers and ~3.5 M extra heap objects walked by every GC cycle.
// Holding off until the first write deletes that overhead almost entirely
// without changing the concurrency profile of nodes that DO use multiple
// shards.
type shard struct {
	mu sync.RWMutex
	m  map[string]interface{} // nil until first write
}

// ShardedMap is a hash-sharded, concurrency-safe map.
// Shard selection is based on FNV-1a 64-bit hash of the key.
type ShardedMap struct {
	shards   []shard
	pow2Mask uint64 // if shardCount is power of two, this is shardCount-1; otherwise 0
}

// NewHashed creates a sharded map with the given number of shards.
// shardCount must be > 0. If shardCount is a power of two, shard selection is a fast bitmask.
//
// Shards are created with nil maps — see the comment on type shard for why.
// The first write into a shard allocates its map under that shard's lock.
func SharedMapNewHashed(shardCount int) (*ShardedMap, error) {
	if shardCount <= 0 {
		return nil, errors.New("shardCount must be > 0")
	}
	sm := &ShardedMap{
		shards:   make([]shard, shardCount),
		pow2Mask: pow2Mask(uint64(shardCount)),
	}
	return sm, nil
}

// MustNewHashed creates a sharded map or panics if shardCount is invalid.
func SharedMapMustNewHashed(shardCount int) *ShardedMap {
	sm, err := SharedMapNewHashed(shardCount)
	if err != nil {
		panic(err)
	}
	return sm
}

// ---------------- Basic operations ----------------

func (sm *ShardedMap) Set(key string, value interface{}) {
	s := sm.shardFor(key)
	s.mu.Lock()
	if s.m == nil {
		s.m = make(map[string]interface{})
	}
	s.m[key] = value
	s.mu.Unlock()
}

func (sm *ShardedMap) Get(key string) (interface{}, bool) {
	s := sm.shardFor(key)
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	return v, ok
}

// LoadOrStore stores the value only if the key doesn't exist.
// It returns the actual value and whether the key was already present.
func (sm *ShardedMap) LoadOrStore(key string, val interface{}) (actual interface{}, loaded bool) {
	s := sm.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	// Read on a nil map is well-defined (returns zero, false), so this works
	// before the lazy allocation below.
	if v, ok := s.m[key]; ok {
		return v, true
	}
	if s.m == nil {
		s.m = make(map[string]interface{})
	}
	s.m[key] = val
	return val, false
}

func (sm *ShardedMap) Delete(key string) {
	s := sm.shardFor(key)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

func (sm *ShardedMap) Has(key string) bool {
	_, ok := sm.Get(key)
	return ok
}

// Len returns the total number of key/value pairs across all shards.
// It walks shards sequentially under shared locks.
func (sm *ShardedMap) Len() int {
	total := 0
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.RLock()
		total += len(s.m)
		s.mu.RUnlock()
	}
	return total
}

// Keys returns a snapshot of all keys (order unspecified).
func (sm *ShardedMap) Keys() []string {
	out := make([]string, 0)
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.RLock()
		for k := range s.m {
			out = append(out, k)
		}
		s.mu.RUnlock()
	}
	return out
}

// Range walks over all (key,value) pairs.
// It snapshots each shard under RLock, releases the lock,
// and only then calls f on the copied pairs to avoid lock inversion/starvation.
func (sm *ShardedMap) Range(f func(key string, value interface{}) bool) {
	for i := range sm.shards {
		s := &sm.shards[i]

		// Snapshot this shard under RLock
		s.mu.RLock()
		tmp := make([]struct {
			k string
			v interface{}
		}, 0, len(s.m))
		for k, v := range s.m {
			tmp = append(tmp, struct {
				k string
				v interface{}
			}{k: k, v: v})
		}
		s.mu.RUnlock()

		// Call f without holding the shard lock
		for _, it := range tmp {
			if !f(it.k, it.v) {
				return
			}
		}
	}
}

// Snapshot returns a shallow copy into a standalone map.
func (sm *ShardedMap) Snapshot() map[string]interface{} {
	cp := make(map[string]interface{}, sm.Len())
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.RLock()
		for k, v := range s.m {
			cp[k] = v
		}
		s.mu.RUnlock()
	}
	return cp
}

// Clear wipes all shards. Resets each shard's map to nil so the lazy-alloc
// invariant is preserved across Clear/Set cycles — a Clear'd shard that is
// never touched again contributes zero heap overhead.
func (sm *ShardedMap) Clear() {
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.Lock()
		s.m = nil
		s.mu.Unlock()
	}
}

// ShardCount returns the number of shards.
func (sm *ShardedMap) ShardCount() int { return len(sm.shards) }

// ---------------- Internals ----------------

func (sm *ShardedMap) shardFor(key string) *shard {
	// Empty keys are allowed—hashing still works.
	h := fnv1a64(key)
	var idx uint64
	if sm.pow2Mask != 0 {
		idx = h & sm.pow2Mask
	} else {
		idx = h % uint64(len(sm.shards))
	}
	return &sm.shards[idx]
}

// fnv1a64 is a non-cryptographic 64-bit FNV-1a hash.
// Fast, stable across processes, good enough for shard distribution.
func fnv1a64(s string) uint64 {
	const (
		offset = 1469598103934665603
		prime  = 1099511628211
	)
	var h uint64 = offset
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

// pow2Mask returns N-1 if N is a power of two; otherwise 0.
// The mask allows shard selection with a single AND instead of modulo.
func pow2Mask(n uint64) uint64 {
	if n != 0 && (n&(n-1)) == 0 {
		return n - 1
	}
	return 0
}
