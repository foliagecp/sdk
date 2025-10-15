package system

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// --- constructors ---

func TestNewHashed_InvalidShardCount(t *testing.T) {
	_, err := SharedMapNewHashed(0)
	if err == nil {
		t.Fatalf("expected error for shardCount=0")
	}
}

func TestMustNewHashed_PanicsOnInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for shardCount=0")
		}
	}()
	_ = SharedMapMustNewHashed(0)
}

// --- basic API ---

func TestSetGetHasDelete(t *testing.T) {
	sm := SharedMapMustNewHashed(16)

	// Set & Get
	sm.Set("a", 1)
	if v, ok := sm.Get("a"); !ok || v.(int) != 1 {
		t.Fatalf("Get('a') = (%v,%v), want (1,true)", v, ok)
	}

	// Has
	if !sm.Has("a") {
		t.Fatalf("Has('a') = false, want true")
	}
	if sm.Has("missing") {
		t.Fatalf("Has('missing') = true, want false")
	}

	// Delete
	sm.Delete("a")
	if _, ok := sm.Get("a"); ok {
		t.Fatalf("Get after Delete should be false")
	}
}

func TestLoadOrStore(t *testing.T) {
	sm := SharedMapMustNewHashed(8)

	// 1st store
	act, loaded := sm.LoadOrStore("k", 42)
	if loaded || act.(int) != 42 {
		t.Fatalf("LoadOrStore new = (%v,%v), want (42,false)", act, loaded)
	}

	// 2nd load (existing)
	act, loaded = sm.LoadOrStore("k", 7)
	if !loaded || act.(int) != 42 {
		t.Fatalf("LoadOrStore existing = (%v,%v), want (42,true)", act, loaded)
	}
}

func TestLenKeysSnapshotClear(t *testing.T) {
	sm := SharedMapMustNewHashed(32)

	// populate
	N := 1000
	for i := 0; i < N; i++ {
		sm.Set(fmt.Sprintf("k%04d", i), i)
	}

	// Len
	if l := sm.Len(); l != N {
		t.Fatalf("Len=%d, want=%d", l, N)
	}

	// Keys: same cardinality
	keys := sm.Keys()
	if len(keys) != N {
		t.Fatalf("Keys len=%d, want=%d", len(keys), N)
	}

	// Snapshot: same cardinality and values present
	snap := sm.Snapshot()
	if len(snap) != N {
		t.Fatalf("Snapshot len=%d, want=%d", len(snap), N)
	}
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k%04d", i)
		v, ok := snap[k]
		if !ok || v.(int) != i {
			t.Fatalf("Snapshot missing or wrong value for %s: (%v,%v)", k, v, ok)
		}
	}

	// Clear
	sm.Clear()
	if l := sm.Len(); l != 0 {
		t.Fatalf("Len after Clear=%d, want=0", l)
	}
}

func TestRangeEarlyExit(t *testing.T) {
	sm := SharedMapMustNewHashed(16)
	for i := 0; i < 1000; i++ {
		sm.Set(fmt.Sprintf("k%04d", i), i)
	}
	var count int
	sm.Range(func(key string, value interface{}) bool {
		count++
		return count < 10 // stop early
	})
	if count != 10 {
		t.Fatalf("Range iterated %d items, want 10 (early stop)", count)
	}
}

// --- concurrency ---

func TestConcurrentAccess(t *testing.T) {
	sm := SharedMapMustNewHashed(64)
	workers := runtime.GOMAXPROCS(0) * 8
	iters := 5000

	wg := sync.WaitGroup{}
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				// derive a few distinct keys per worker
				k1 := "w" + strconv.Itoa(id) + "/a/" + strconv.Itoa(i)
				k2 := "w" + strconv.Itoa(id) + "/b/" + strconv.Itoa(i%100)

				// mix operations
				sm.Set(k1, i)
				sm.LoadOrStore(k2, id)
				if v, ok := sm.Get(k2); ok && v.(int) == id && i%7 == 0 {
					sm.Delete(k2)
				}
				// small shuffle to trigger scheduler
				if i%251 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}(w)
	}
	wg.Wait()

	// basic consistency: no panic, and Len is non-negative with some keys likely left
	if sm.Len() < 0 {
		t.Fatalf("Len < 0 - impossible")
	}
}

// --- distribution & stability (package-level to access internals safely) ---

func TestShardDistribution_PowerOfTwo(t *testing.T) {
	// This test checks that the hash roughly spreads keys across shards.
	// We use a generous tolerance to avoid flakiness.
	shardCount := 64
	sm := SharedMapMustNewHashed(shardCount)

	totalKeys := 100_000
	for i := 0; i < totalKeys; i++ {
		sm.Set(fmt.Sprintf("k-%06d", i), i)
	}
	// Count keys per shard by peeking internals (same package).
	counts := make([]int, shardCount)
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.RLock()
		counts[i] = len(s.m)
		s.mu.RUnlock()
	}

	// Stats
	sum := 0
	min := math.MaxInt
	max := 0
	for _, c := range counts {
		sum += c
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	if sum != totalKeys {
		t.Fatalf("sum per-shard=%d, want %d", sum, totalKeys)
	}
	mean := float64(totalKeys) / float64(shardCount)

	// Allow [0.5x .. 1.5x] of the mean per shard.
	low := 0.5 * mean
	high := 1.5 * mean
	if float64(min) < low || float64(max) > high {
		// Attach a small summary to help debugging.
		sorted := append([]int(nil), counts...)
		sort.Ints(sorted)
		t.Fatalf("distribution too skewed: min=%d max=%d mean=%.1f allowed=[%.1f..%.1f]", min, max, mean, low, high)
	}
}

func TestShardStability_SameKeySameShard(t *testing.T) {
	sm := SharedMapMustNewHashed(128)

	keys := []string{
		"", "a", "A", "0", "-", "foo", "bar", "baz",
		"user:123", "user:124", "αβγ", "😀", // unicode is fine; we hash bytes
	}
	for _, k := range keys {
		idx1 := indexOfShard(sm, k)
		idx2 := indexOfShard(sm, k)
		if idx1 != idx2 {
			t.Fatalf("shard index not stable for key %q: %d vs %d", k, idx1, idx2)
		}
		// Changing value must not affect shard
		sm.Set(k, 1)
		idx3 := indexOfShard(sm, k)
		if idx3 != idx1 {
			t.Fatalf("shard index changed after Set for key %q: %d -> %d", k, idx1, idx3)
		}
	}
}

// indexOfShard peeks internals to compute the shard index for a key.
// (Only used in tests; same package so it's safe.)
func indexOfShard(sm *ShardedMap, key string) int {
	h := fnv1a64(key)
	var idx uint64
	if sm.pow2Mask != 0 {
		idx = h & sm.pow2Mask
	} else {
		idx = h % uint64(len(sm.shards))
	}
	return int(idx)
}
