package system

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var benchSink int64

// Benchmarks for the per-key lock, to quantify the FIFO-gate overhead vs the bare
// sync.RWMutex. Run the same set against HEAD's system.go (no gate) for an apples-
// to-apples delta:
//   go test ./statefun/system/ -bench BenchmarkKeyMutex -run '^$' -benchmem
//
// "Cold" benches lock/unlock a key whose ref-count drops to 0 each iteration, so
// the map entry (and its gate channel) is deleted and re-created every time —
// worst case for allocation. "HotEntry" benches pin one extra hold so the entry
// lives for the whole run (the realistic hot/contended key, where the entry is
// created once and the gate channel is reused) — this isolates the gate's
// steady-state per-op cost from entry churn.

const benchTimeout = 10 * time.Second // large: timeout benches stay on the fast path

func BenchmarkKeyMutex_WriteUncontendedCold(b *testing.B) {
	km := NewKeyMutex()
	const key = "k"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		km.Lock(key)
		km.Unlock(key)
	}
}

func BenchmarkKeyMutex_ReadUncontendedCold(b *testing.B) {
	km := NewKeyMutex()
	const key = "k"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		km.RLock(key)
		km.RUnlock(key)
	}
}

func BenchmarkKeyMutex_LockTimeoutUncontendedCold(b *testing.B) {
	km := NewKeyMutex()
	const key = "k"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		km.LockTimeout(key, benchTimeout)
		km.Unlock(key)
	}
}

func BenchmarkKeyMutex_RLockTimeoutUncontendedCold(b *testing.B) {
	km := NewKeyMutex()
	const key = "k"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		km.RLockTimeout(key, benchTimeout)
		km.RUnlock(key)
	}
}

// HotEntry: the map entry is pinned alive (extra hold), so no per-op create/delete
// of the refMutex+gate — the realistic hot-key steady state.
func BenchmarkKeyMutex_RLockTimeoutHotEntry(b *testing.B) {
	km := NewKeyMutex()
	const key = "k"
	km.RLock(key) // pin: refs stays >= 1, entry never deleted
	defer km.RUnlock(key)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		km.RLockTimeout(key, benchTimeout)
		km.RUnlock(key)
	}
}

func BenchmarkKeyMutex_LockTimeoutHotEntry(b *testing.B) {
	km := NewKeyMutex()
	const key = "k"
	km.RLock(key) // pin the entry without blocking the write TryLock
	defer km.RUnlock(key)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// NB: a read hold makes TryLock fail, so this exercises the slow poll path;
		// kept only to confirm it does not allocate per-op once the entry is hot.
		if km.LockTimeout(key, 1*time.Millisecond) {
			km.Unlock(key)
		}
	}
}

// Readers hammering ONE key in parallel — does the gate serialize the read entry?
func BenchmarkKeyMutex_RLockTimeoutParallelSameKey(b *testing.B) {
	km := NewKeyMutex()
	const key = "k"
	km.RLock(key) // pin the entry so we measure entry cost, not churn
	defer km.RUnlock(key)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			km.RLockTimeout(key, benchTimeout)
			km.RUnlock(key)
		}
	})
}

// Writers on DISTINCT keys in parallel — sharded throughput (precomputed keys, so
// no per-iter string allocation pollutes the alloc count).
func BenchmarkKeyMutex_LockTimeoutParallelDistinctKeys(b *testing.B) {
	km := NewKeyMutex()
	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var n int
		for pb.Next() {
			key := keys[n%len(keys)]
			n++
			km.LockTimeout(key, benchTimeout)
			km.Unlock(key)
		}
	})
}

// Realistic: distinct-key writers doing a little work while holding the lock — a
// stand-in for the cache/JSON work a real graph op does (a real op also does NATS
// round-trips, far more). Shows the gate overhead against a non-trivial critical
// section instead of an empty lock/unlock loop.
func BenchmarkKeyMutex_LockTimeoutParallelWithWork(b *testing.B) {
	km := NewKeyMutex()
	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var n int
		local := 0
		for pb.Next() {
			key := keys[n%len(keys)]
			n++
			km.LockTimeout(key, benchTimeout)
			for j := 0; j < 300; j++ { // ~hundreds of ns of busy work, not elided
				local = local*31 + j
			}
			km.Unlock(key)
		}
		atomic.AddInt64(&benchSink, int64(local))
	})
}
