package system

import (
	"errors"
	"sort"
	"sync"
	"unicode"
	"unicode/utf8"
)

// shard is a segment with its own map and RWMutex.
type shard struct {
	mu sync.RWMutex
	m  map[string]interface{}
}

// ShardedMap is a sharded, concurrency-safe map.
type ShardedMap struct {
	shards       []shard      // N shards for each accepted first rune + 1 default shard
	runeToIndex  map[rune]int // mapping: first rune -> shard index
	defaultIndex int          // index of the default shard
}

// New creates a sharded map.
// spec is the content of a regex-like character-class (without square brackets), e.g. "a-zA-Z0-9/=_$#@$%+-".
func New(spec string) (*ShardedMap, error) {
	chars, err := expandCharClass(spec)
	if err != nil {
		return nil, err
	}
	// Deduplicate and sort for stable shard layout.
	uniq := make(map[rune]struct{}, len(chars))
	for _, r := range chars {
		uniq[r] = struct{}{}
	}
	keys := make([]rune, 0, len(uniq))
	for r := range uniq {
		keys = append(keys, r)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// +1 for the default shard.
	totalShards := len(keys) + 1
	sm := &ShardedMap{
		shards:       make([]shard, totalShards),
		runeToIndex:  make(map[rune]int, len(keys)),
		defaultIndex: totalShards - 1,
	}
	for i := range sm.shards {
		sm.shards[i].m = make(map[string]interface{})
	}
	for i, r := range keys {
		sm.runeToIndex[r] = i
	}
	return sm, nil
}

// MustNew is New that panics on error.
func MustNew(spec string) *ShardedMap {
	sm, err := New(spec)
	if err != nil {
		panic(err)
	}
	return sm
}

// -------- basic operations --------

func (sm *ShardedMap) Set(key string, value interface{}) {
	idx := sm.chooseShard(key)
	s := &sm.shards[idx]
	s.mu.Lock()
	s.m[key] = value
	s.mu.Unlock()
}

func (sm *ShardedMap) Get(key string) (interface{}, bool) {
	idx := sm.chooseShard(key)
	s := &sm.shards[idx]
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	return v, ok
}

func (sm *ShardedMap) LoadOrStore(key string, val interface{}) (actual interface{}, loaded bool) {
	idx := sm.chooseShard(key)
	s := &sm.shards[idx]
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.m[key]; ok {
		return v, true
	}
	s.m[key] = val
	return val, false
}

func (sm *ShardedMap) Delete(key string) {
	idx := sm.chooseShard(key)
	s := &sm.shards[idx]
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

func (sm *ShardedMap) Has(key string) bool {
	_, ok := sm.Get(key)
	return ok
}

// Len returns the total number of key/value pairs across all shards.
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

// Keys returns a copy of all keys (unordered).
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

// Range walks over all (key,value) pairs. It locks shards one-by-one under RLock.
// The order is unspecified. If f returns false, iteration stops early.
func (sm *ShardedMap) Range(f func(key string, value interface{}) bool) {
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.RLock()
		for k, v := range s.m {
			if !f(k, v) {
				s.mu.RUnlock()
				return
			}
		}
		s.mu.RUnlock()
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

// Clear wipes all shards.
func (sm *ShardedMap) Clear() {
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.Lock()
		s.m = make(map[string]interface{})
		s.mu.Unlock()
	}
}

// ShardCount returns the number of shards (including the default shard).
func (sm *ShardedMap) ShardCount() int { return len(sm.shards) }

// -------- internals --------

func (sm *ShardedMap) chooseShard(key string) int {
	if key == "" {
		return sm.defaultIndex
	}
	r, _ := utf8.DecodeRuneInString(key)
	if r == utf8.RuneError {
		return sm.defaultIndex
	}
	if idx, ok := sm.runeToIndex[r]; ok {
		return idx
	}
	return sm.defaultIndex
}

// expandCharClass parses a regex-like character-class spec such as "a-zA-Z0-9/=_$#@$%+-" into a set of runes.
// Rules:
//   - X-Y is treated as a range only if X and Y are both digits, both lowercase Latin, or both uppercase Latin.
//   - Otherwise '-' is a literal character.
//   - No backslashes are required; the string is the content of [] without brackets.
//   - Unicode named classes/ranges are intentionally unsupported (ASCII ranges and literals only).
func expandCharClass(spec string) ([]rune, error) {
	if spec == "" {
		return nil, errors.New("char class spec is empty")
	}
	rs := []rune(spec)
	out := make([]rune, 0, len(rs))

	isRangeOK := func(a, b rune) bool {
		// Allowed ranges: 0-9, a-z, A-Z
		return (unicode.IsDigit(a) && unicode.IsDigit(b)) ||
			(unicode.IsLower(a) && unicode.IsLower(b)) ||
			(unicode.IsUpper(a) && unicode.IsUpper(b))
	}

	i := 0
	for i < len(rs) {
		// Try to consume an X-Y pattern if valid.
		if i+2 < len(rs) && rs[i+1] == '-' && isRangeOK(rs[i], rs[i+2]) {
			a, b := rs[i], rs[i+2]
			if b < a {
				return nil, errors.New("invalid range in char class: end < start")
			}
			for r := a; r <= b; r++ {
				out = append(out, r)
			}
			i += 3
			continue
		}
		// Otherwise: treat current rune as a literal.
		out = append(out, rs[i])
		i++
	}
	return out, nil
}
