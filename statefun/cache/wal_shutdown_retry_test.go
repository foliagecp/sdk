package cache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type shutdownRetryWAL struct {
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	allow    atomic.Bool
	failures atomic.Int32
}

func (w *shutdownRetryWAL) GenerateTransactionID() string { return "unused" }
func (w *shutdownRetryWAL) PublishTransaction(_ string, _ []WALOp) error {
	w.once.Do(func() { close(w.entered); <-w.release })
	if !w.allow.Load() {
		w.failures.Add(1)
		return errors.New("injected temporary publish failure")
	}
	return nil
}

func Test_Shutdown_RetainsFailedPublishesUntilRetrySucceeds(t *testing.T) {
	js, kv := newKVForTest(t, "shutdown_retry")
	cs, cancel := loadStore(t, js, kv, "shutdown_retry")
	defer cancel()
	wal := &shutdownRetryWAL{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	defer func() { wal.allow.Store(true); releaseOnce.Do(func() { close(wal.release) }) }()
	cs.SetTransactionGenerator(wal)
	cs.SetWALWriteEnabled(true)
	require.True(t, cs.SetValue("dom/unflushed", []byte("keep"), true, -1))
	select {
	case <-wal.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publish was not attempted")
	}
	cs.Destroy()
	releaseOnce.Do(func() { close(wal.release) })
	require.Eventually(t, func() bool { return wal.failures.Load() >= 2 }, 5*time.Second, time.Millisecond)
	require.True(t, cs.HasPendingWrites(), "an unsuccessful publish must retain its pending operations")
	select {
	case <-cs.Synced:
		t.Fatal("shutdown claimed a successful flush after failed publication")
	default:
	}
	wal.allow.Store(true)
	select {
	case <-cs.Synced:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish after publishing recovered")
	}
	require.False(t, cs.HasPendingWrites())
}
