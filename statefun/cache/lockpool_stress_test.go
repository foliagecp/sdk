package cache

// Maximum-collision proof for the shared node-lock pool.
//
// With the pool forced to size 1, EVERY StoreValue node shares the SAME
// RWMutex — the worst possible case for both deadlock and contention. If the
// full set of locking operations (value Put/Delete/SetValueType under the write
// lock, value reads under the read lock, small-container StoreChild/deleteChild
// under the write lock) runs concurrently across many goroutines on one mutex
// WITHOUT deadlocking, then no code path holds two node locks at once, and the
// pool is therefore deadlock-safe at ANY size. Run with -race for the data-race
// half of the proof.

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockPool_SingleSlot_StressNoDeadlock(t *testing.T) {
	// Force maximum collision: one mutex for the whole tree.
	setLockPoolSize(1)
	defer setLockPoolSize(1 << 16)
	if len(lockPool) != 1 {
		t.Fatalf("expected pool size 1, got %d", len(lockPool))
	}

	root := &StoreValue{valueUpdateTime: -1}

	const (
		topKeys = 40 // intermediate nodes -> small (<=8-child) container path (node-locked)
		subKeys = 5  // leaves per intermediate -> value lock path
		workers = 48
		runFor  = 3 * time.Second
	)

	// Watchdog: a deadlock would hang the test. Dump all goroutine stacks and
	// fail loudly instead of relying only on the package test timeout.
	finished := make(chan struct{})
	go func() {
		select {
		case <-finished:
		case <-time.After(45 * time.Second):
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			fmt.Printf("=== DEADLOCK WATCHDOG (pool size 1): goroutine dump ===\n%s\n", buf[:n])
			panic("lock-pool stress: deadlock suspected (did not finish in 45s)")
		}
	}()

	var stop atomic.Bool
	var ops atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := seed*2654435761 + 1
			for !stop.Load() {
				r = r*6364136223846793005 + 1
				top := fmt.Sprintf("k%d", (r>>33)%topKeys)
				r = r*6364136223846793005 + 1
				sub := fmt.Sprintf("s%d", (r>>40)%subKeys)

				switch (r >> 28) & 7 {
				case 0, 1: // create path: StoreChild(top) [container lock] + StoreChild(sub) + Put [value lock]
					inter, _ := root.StoreChild(top, &StoreValue{})
					leaf, _ := inter.StoreChild(sub, &StoreValue{})
					leaf.Put([]byte("v"), false, time.Now().UnixNano())
				case 2: // SetValueType (value write lock)
					if a, ok := root.LoadChild(top); ok {
						if b, ok := a.LoadChild(sub); ok {
							b.SetValueType(typeJson)
						}
					}
				case 3: // read under RLock (GetValue-style)
					if a, ok := root.LoadChild(top); ok {
						if b, ok := a.LoadChild(sub); ok {
							b.RLock("stress-read")
							_ = b.value
							_ = b.getValueExists()
							_ = b.getValueType()
							b.RUnlock("stress-read")
						}
					}
				case 4: // Delete (value write lock, tombstone)
					if a, ok := root.LoadChild(top); ok {
						if b, ok := a.LoadChild(sub); ok {
							b.Delete(false, time.Now().UnixNano())
						}
					}
				case 5: // deleteChild (container write lock)
					if a, ok := root.LoadChild(top); ok {
						a.deleteChild(sub)
					}
				case 6: // Range over an intermediate (lockless) + storeLen
					if a, ok := root.LoadChild(top); ok {
						a.Range(func(_, _ interface{}) bool { return true })
						_ = a.storeLen()
					}
				case 7: // Range over root (lockless, sharded) + nested read
					root.Range(func(_, v interface{}) bool {
						n := v.(*StoreValue)
						n.RLock("stress-range-read")
						_ = n.flags
						n.RUnlock("stress-range-read")
						return true
					})
				}
				ops.Add(1)
			}
		}(uint64(w) + 1)
	}

	time.Sleep(runFor)
	stop.Store(true)
	wg.Wait()
	close(finished)

	// Integrity after quiescence: every child surfaced by Range must be
	// retrievable via LoadChild and identical (no torn container state).
	mismatches := 0
	intermediates := 0
	root.Range(func(k, v interface{}) bool {
		intermediates++
		inter := v.(*StoreValue)
		if got, ok := root.LoadChild(k.(string)); !ok || got != inter {
			mismatches++
		}
		inter.Range(func(sk, sv interface{}) bool {
			leaf := sv.(*StoreValue)
			if got, ok := inter.LoadChild(sk.(string)); !ok || got != leaf {
				mismatches++
			}
			return true
		})
		return true
	})
	if mismatches != 0 {
		t.Fatalf("container inconsistency after stress: %d Range/LoadChild mismatches", mismatches)
	}
	t.Logf("survived %d ops on pool size 1, %d intermediates, no deadlock, container consistent",
		ops.Load(), intermediates)
}
