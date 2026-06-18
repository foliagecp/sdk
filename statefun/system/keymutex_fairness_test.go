package system

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestKeyMutex_FairReaderNotStarvedByWriterStream is the regression test for the
// reader-starvation convoy (GraphML export / front-end hanging while a flood of
// deletes keeps writing). Under an unbroken stream of writers on one key, a reader
// going through the RLockTimeout path the graph ops use must still acquire promptly.
//
// With Go's writer-preferring sync.RWMutex alone the reader is starved for the
// whole stream — TryRLock keeps failing while any writer is pending — and only
// returns at the timeout. The FIFO entry gate lets the reader in within a couple
// of writer cycles, regardless of how long the writes keep coming.
func TestKeyMutex_FairReaderNotStarvedByWriterStream(t *testing.T) {
	km := NewKeyMutex()
	const key = "hot"

	stop := make(chan struct{})
	var writes int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				km.Lock(key)
				atomic.AddInt64(&writes, 1)
				time.Sleep(200 * time.Microsecond) // brief held work
				km.Unlock(key)
			}
		}()
	}

	time.Sleep(50 * time.Millisecond) // let the write stream saturate the key

	start := time.Now()
	got := km.RLockTimeout(key, 5*time.Second)
	waited := time.Since(start)
	if got {
		km.RUnlock(key)
	}

	close(stop)
	wg.Wait()

	if !got {
		t.Fatalf("reader starved: RLockTimeout did not acquire within 5s under a continuous write stream")
	}
	if waited > 2*time.Second {
		t.Fatalf("reader waited %v under the write stream — fairness regression (want a prompt acquire)", waited)
	}
	t.Logf("reader acquired in %v while %d writes were churning the key", waited, atomic.LoadInt64(&writes))
}

// TestKeyMutex_MutualExclusionWrite verifies the gate did not weaken write
// exclusivity: a non-atomic counter guarded only by the per-key write lock must
// land exactly. Run with -race to also catch any torn access.
func TestKeyMutex_MutualExclusionWrite(t *testing.T) {
	km := NewKeyMutex()
	const key = "k"
	const goroutines = 20
	const iters = 500

	var counter int
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				km.Lock(key)
				counter++ // guarded solely by the exclusive lock
				km.Unlock(key)
			}
		}()
	}
	wg.Wait()

	if want := goroutines * iters; counter != want {
		t.Fatalf("mutual exclusion violated: counter=%d want=%d", counter, want)
	}
}

// TestKeyMutex_ReadersAreConcurrent verifies the gate does not serialize readers:
// it is held only for the brief entry, so multiple RLock holders overlap.
func TestKeyMutex_ReadersAreConcurrent(t *testing.T) {
	km := NewKeyMutex()
	const key = "k"
	const readers = 8

	var active, maxActive int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			km.RLock(key)
			n := atomic.AddInt32(&active, 1)
			for {
				m := atomic.LoadInt32(&maxActive)
				if n <= m || atomic.CompareAndSwapInt32(&maxActive, m, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond) // hold so peers can overlap
			atomic.AddInt32(&active, -1)
			km.RUnlock(key)
		}()
	}
	close(start)
	wg.Wait()

	if atomic.LoadInt32(&maxActive) < 2 {
		t.Fatalf("readers did not run concurrently: max concurrent = %d (want >= 2)", maxActive)
	}
}

// TestKeyMutex_LockTimeoutExpiresWhenHeld confirms the bounded write acquire still
// gives up (returns false ~timeout) when the key is held — the gate path must not
// break the timeout safety net.
func TestKeyMutex_LockTimeoutExpiresWhenHeld(t *testing.T) {
	km := NewKeyMutex()
	const key = "k"

	km.Lock(key)
	defer km.Unlock(key)

	start := time.Now()
	got := km.LockTimeout(key, 100*time.Millisecond)
	waited := time.Since(start)
	if got {
		km.Unlock(key)
		t.Fatalf("LockTimeout acquired a lock held exclusively by another holder")
	}
	if waited < 90*time.Millisecond || waited > 2*time.Second {
		t.Fatalf("LockTimeout returned after %v, want ~100ms", waited)
	}
}

// TestKeyMutex_RLockTimeoutExpiresWhenWriteHeld is the shared-lock counterpart.
func TestKeyMutex_RLockTimeoutExpiresWhenWriteHeld(t *testing.T) {
	km := NewKeyMutex()
	const key = "k"

	km.Lock(key)
	defer km.Unlock(key)

	if got := km.RLockTimeout(key, 100*time.Millisecond); got {
		km.RUnlock(key)
		t.Fatalf("RLockTimeout acquired a read lock while a writer held the key")
	}
}

// TestKeyMutex_MixedStressNoDeadlock hammers a few keys with interleaved readers
// and writers (both timeout paths) to shake out gate/refcount deadlocks or leaks.
// Meaningful under -race.
func TestKeyMutex_MixedStressNoDeadlock(t *testing.T) {
	km := NewKeyMutex()
	keys := []string{"a", "b", "c"}

	var wg sync.WaitGroup
	deadline := time.Now().Add(300 * time.Millisecond)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				k := keys[id%len(keys)]
				if id%2 == 0 {
					if km.LockTimeout(k, time.Second) {
						km.Unlock(k)
					}
				} else {
					if km.RLockTimeout(k, time.Second) {
						km.RUnlock(k)
					}
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("mixed stress deadlocked: workers did not finish within 15s")
	}
}
