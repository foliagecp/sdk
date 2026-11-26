package cache

// The writer of a cancelled store that may publish nothing.
//
// The writer goroutine checked for cancellation only on the path that
// publishes. A store with no transaction generator, or with WAL writes off as
// on a passive instance, kept its writer asleep for the rest of the process,
// and the writer kept the store: every such store stayed in memory with its
// whole graph after its context was cancelled.

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/require"
)

// countingWAL counts the transactions it is asked to publish.
type countingWAL struct{ published atomic.Int32 }

func (w *countingWAL) GenerateTransactionID() string { return "unused" }
func (w *countingWAL) PublishTransaction(_ string, _ []WALOp) error {
	w.published.Add(1)
	return nil
}

// cancelledStore loads a store, lets prepare bring it to the state under
// test, and cancels it. It returns a channel closed when the collector takes
// the store back, and the store's Synced. The store is referenced from nowhere
// in the test after this returns, so only the store's own goroutines can keep
// it.
func cancelledStore(t *testing.T, bucket string, prepare func(*Store)) (released, synced <-chan struct{}) {
	t.Helper()
	js, kvs := newKVForTest(t, bucket)
	cs, cancel := loadStore(t, js, kvs, bucket)
	if prepare != nil {
		prepare(cs)
	}
	gone := make(chan struct{})
	runtime.SetFinalizer(cs, func(*Store) { close(gone) })
	cancel()
	return gone, cs.Synced
}

func requireReleased(t *testing.T, released <-chan struct{}) {
	t.Helper()
	require.Eventually(t, func() bool {
		runtime.GC()
		select {
		case <-released:
			return true
		default:
			return false
		}
	}, 10*time.Second, 50*time.Millisecond, "the cancelled store is still held")
}

func requireOpen(t *testing.T, synced <-chan struct{}) {
	t.Helper()
	select {
	case <-synced:
		t.Fatal("Synced was closed by a writer that flushed nothing")
	default:
	}
}

// Test_Cancel_ReleasesAStoreThatMayNotPublish — a cancelled store is released
// whether or not it was ever allowed to publish, it publishes nothing on the
// way out, and it does not claim a flush.
func Test_Cancel_ReleasesAStoreThatMayNotPublish(t *testing.T) {
	t.Run("no transaction generator", func(t *testing.T) {
		released, synced := cancelledStore(t, "stop_no_generator", nil)
		requireReleased(t, released)
		requireOpen(t, synced)
	})

	t.Run("WAL writes off", func(t *testing.T) {
		wal := &countingWAL{}
		released, synced := cancelledStore(t, "stop_writes_off", func(cs *Store) {
			cs.SetTransactionGenerator(wal)
			cs.SetWALWriteEnabled(false)
		})
		requireReleased(t, released)
		requireOpen(t, synced)
		require.Zero(t, wal.published.Load())
	})

	// An instance demoted with operations it had not published yet: they are
	// not published by the passive instance on its way out, and their loss is
	// not reported as a flush.
	t.Run("WAL writes off with unpublished operations", func(t *testing.T) {
		wal := &countingWAL{}
		released, synced := cancelledStore(t, "stop_unpublished", func(cs *Store) {
			cs.SetTransactionGenerator(wal)
			cs.SetWALWriteEnabled(true)
			// An operation still in flight holds the writer back from
			// publishing anything written at or after its time.
			opTime := system.GetCurrentTimeNs()
			cs.MarkOperationActive(opTime)
			require.True(t, cs.SetValue("dom/unpublished", []byte("kept back"), true, opTime))
			require.True(t, cs.HasPendingWrites())
			cs.SetWALWriteEnabled(false)
		})
		requireReleased(t, released)
		requireOpen(t, synced)
		require.Zero(t, wal.published.Load(), "a store with WAL writes off must not publish")
	})
}
