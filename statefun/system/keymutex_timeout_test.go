package system

// Tests for the timeout-bounded KeyMutex acquisition (LockTimeout /
// RLockTimeout): a caller must never block longer than the timeout on a
// contended key, so a stuck lock holder cannot hang waiters indefinitely.

import (
	"testing"
	"time"
)

func Test_KeyMutex_LockTimeout_AcquiresWhenFree(t *testing.T) {
	km := NewKeyMutex()
	if !km.LockTimeout("k", time.Second) {
		t.Fatal("expected to acquire a free lock")
	}
	km.Unlock("k")
}

func Test_KeyMutex_LockTimeout_TimesOutWhenHeld(t *testing.T) {
	km := NewKeyMutex()
	km.Lock("k") // hold it
	defer km.Unlock("k")

	start := time.Now()
	if km.LockTimeout("k", 100*time.Millisecond) {
		t.Fatal("expected timeout on a held lock, but acquired")
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("timeout fired at an unreasonable time: %s", elapsed)
	}
}

func Test_KeyMutex_LockTimeout_AcquiresAfterRelease(t *testing.T) {
	km := NewKeyMutex()
	km.Lock("k")

	got := make(chan bool, 1)
	go func() { got <- km.LockTimeout("k", 2*time.Second) }()

	time.Sleep(30 * time.Millisecond)
	km.Unlock("k") // release; waiter should acquire well within its timeout

	select {
	case ok := <-got:
		if !ok {
			t.Fatal("expected to acquire after release")
		}
		km.Unlock("k")
	case <-time.After(2 * time.Second):
		t.Fatal("did not acquire after release")
	}
}

// Test_KeyMutex_LockTimeout_RefAccountingAfterTimeout guards against a refcount
// leak on the timeout path: after a timed-out waiter and the holder's release,
// the key must be fully free for a fresh acquisition.
func Test_KeyMutex_LockTimeout_RefAccountingAfterTimeout(t *testing.T) {
	km := NewKeyMutex()
	km.Lock("k")

	if km.LockTimeout("k", 50*time.Millisecond) {
		t.Fatal("expected timeout while held")
	}
	km.Unlock("k") // holder releases

	if !km.LockTimeout("k", time.Second) {
		t.Fatal("fresh acquire failed — refcount leak on timeout path")
	}
	km.Unlock("k")
}

func Test_KeyMutex_RLockTimeout_SharedAndTimeout(t *testing.T) {
	km := NewKeyMutex()

	// While write-locked, a reader must time out.
	km.Lock("k")
	if km.RLockTimeout("k", 50*time.Millisecond) {
		t.Fatal("RLock acquired while write-locked")
	}
	km.Unlock("k")

	// After release, multiple readers share concurrently.
	if !km.RLockTimeout("k", time.Second) {
		t.Fatal("reader1 should acquire")
	}
	if !km.RLockTimeout("k", time.Second) {
		t.Fatal("reader2 should share with reader1")
	}
	km.RUnlock("k")
	km.RUnlock("k")
}
