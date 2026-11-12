package statefun

// Startup re-checks every proven delete still in the receipt window. After a
// large rebuild that is hundreds of thousands of leader reads, so the phase
// must finish however long it takes while it keeps progressing, and fail only
// when it stops. As in the replay tests, the watchdog runs on a controlled
// clock: host scheduling is not part of what is checked.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

type walClock struct {
	base    time.Time
	elapsed atomic.Int64
}

func newWALClock() *walClock                { return &walClock{base: time.Now()} }
func (c *walClock) now() time.Time          { return c.base.Add(time.Duration(c.elapsed.Load())) }
func (c *walClock) advance(d time.Duration) { c.elapsed.Add(int64(d)) }

// walPacedReads moves the watchdog clock on every leader read: a restore that
// takes long in total while answering each read promptly.
type walPacedReads struct {
	nats.JetStreamContext
	clock *walClock
	step  time.Duration
	reads *atomic.Int64
}

func (j walPacedReads) GetLastMsg(name, subject string, opts ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	j.clock.advance(j.step)
	j.reads.Add(1)
	return j.JetStreamContext.GetLastMsg(name, subject, opts...)
}

// walStuckReads never answers a leader read before its context ends.
type walStuckReads struct {
	nats.JetStreamContext
	blocked chan struct{}
	once    *sync.Once
}

func (j walStuckReads) GetLastMsg(_, _ string, opts ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	ctx := context.Background()
	for _, opt := range opts {
		if c, ok := opt.(nats.ContextOpt); ok {
			ctx = c.Context
		}
	}
	j.once.Do(func() { close(j.blocked) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func walTestKey(name string) string { return "store.recovery/" + name }

func walTestKVPut(t *testing.T, dm *Domain, name, value string) uint64 {
	t.Helper()
	rev, err := dm.kv.Put(walTestKey(name), []byte(value))
	require.NoError(t, err)
	return rev
}

// walTestReceipt stores the applied receipt the committer writes for a
// transaction with these deletes.
func walTestReceipt(t *testing.T, dm *Domain, seq uint64, deletes ...walAppliedDelete) {
	t.Helper()
	consumer := CommitterDurableName + "-" + dm.kv.Bucket()
	receipt := walReceipt{
		Version: 1, StreamCreated: dm.walCreated, KVCreated: dm.walKVCreated, Bucket: dm.kv.Bucket(),
		Consumer: consumer, Sequence: seq, TxID: fmt.Sprint(time.Now().UnixNano()),
		Digest: walDigest([]byte(fmt.Sprint(seq))), State: "applied", Deletes: deletes,
		AckSubject: fmt.Sprintf("$JS.ACK.%s.%s.1.%d.%d.%d.0", WALCommitsStreamName, consumer, seq, seq, time.Now().UnixNano()),
	}
	require.NoError(t, dm.validateWALReceipt(&receipt, seq))
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	_, err = dm.kv.Create(dm.walReceiptKey(seq), data)
	require.NoError(t, err)
}

func requireWALKeyDeleted(t *testing.T, dm *Domain, name string) {
	t.Helper()
	_, err := dm.kv.Get(walTestKey(name))
	require.ErrorIsf(t, err, nats.ErrKeyNotFound, "%s must be deleted", name)
}

func TestWALRecovery_RestoredDeletesFollowTheNewestProof(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	// A value that resurfaced under a reclaimed delete marker: only the marker's
	// revision, above the value, is left in the receipt.
	walTestKVPut(t, dm, "resurrected", "old")
	reclaimed := walTestKVPut(t, dm, "filler-1", "x")
	// A legitimate write after the proven delete.
	proven := walTestKVPut(t, dm, "filler-2", "x")
	walTestKVPut(t, dm, "rewritten", "new")
	// Deleted, written again, deleted again, and the second marker reclaimed:
	// the value between the two deletes resurfaced.
	first := walTestKVPut(t, dm, "filler-3", "x")
	walTestKVPut(t, dm, "twice", "between")
	second := walTestKVPut(t, dm, "filler-4", "x")
	// A delete whose marker is still in place.
	walTestKVPut(t, dm, "gone", "old")
	require.NoError(t, dm.kv.Delete(walTestKey("gone")))
	marker := walTestKVPut(t, dm, "filler-5", "x") - 1

	// The newer delete of "twice" sits in the earlier receipt on purpose.
	walTestReceipt(t, dm, 101,
		walAppliedDelete{walTestKey("twice"), second},
		walAppliedDelete{walTestKey("resurrected"), reclaimed})
	walTestReceipt(t, dm, 102,
		walAppliedDelete{walTestKey("twice"), first},
		walAppliedDelete{walTestKey("rewritten"), proven},
		walAppliedDelete{walTestKey("gone"), marker},
		walAppliedDelete{walTestKey("never-written"), proven})

	require.NoError(t, dm.restoreWALDeletes(ctx, time.Minute, time.Now))

	requireWALKeyDeleted(t, dm, "resurrected")
	requireWALKeyDeleted(t, dm, "twice")
	requireWALKeyDeleted(t, dm, "gone")
	requireWALKeyDeleted(t, dm, "never-written")
	entry, err := dm.kv.Get(walTestKey("rewritten"))
	require.NoError(t, err)
	require.Equal(t, "new", string(entry.Value()), "a write after the proven delete is kept")
	for i := 1; i <= 5; i++ {
		_, err := dm.kv.Get(walTestKey(fmt.Sprintf("filler-%d", i)))
		require.NoError(t, err)
	}
}

func TestWALRecovery_LongDeleteRestoreWithProgressDoesNotTimeOut(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	const receipts, perReceipt = 40, 15
	for i := 0; i < receipts*perReceipt; i++ {
		walTestKVPut(t, dm, fmt.Sprintf("bulk-%04d", i), "old")
	}
	reclaimed := walTestKVPut(t, dm, "filler", "x")
	for r := 0; r < receipts; r++ {
		deletes := make([]walAppliedDelete, perReceipt)
		for j := range deletes {
			deletes[j] = walAppliedDelete{walTestKey(fmt.Sprintf("bulk-%04d", r*perReceipt+j)), reclaimed}
		}
		walTestReceipt(t, dm, uint64(1000+r), deletes...)
	}

	// Each read moves the clock 1 ms; with walDeleteVerifyWorkers reads in
	// flight no gap between settled keys reaches the stall interval, while the
	// restore as a whole runs six times longer than it.
	const stallTimeout = 100 * time.Millisecond
	clock := newWALClock()
	var reads atomic.Int64
	dm.js = walPacedReads{dm.js, clock, time.Millisecond, &reads}
	require.NoError(t, dm.restoreWALDeletes(ctx, stallTimeout, clock.now))

	require.Greater(t, time.Duration(clock.elapsed.Load()), 3*stallTimeout, "the restore must outlast the stall interval")
	require.EqualValues(t, receipts*perReceipt, reads.Load(), "one leader read per deleted key")
	for i := 0; i < receipts*perReceipt; i++ {
		requireWALKeyDeleted(t, dm, fmt.Sprintf("bulk-%04d", i))
	}
}

func TestWALRecovery_StalledDeleteRestoreFailsClosed(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	walTestKVPut(t, dm, "stuck", "old")
	walTestReceipt(t, dm, 7, walAppliedDelete{walTestKey("stuck"), walTestKVPut(t, dm, "filler", "x")})

	const stallTimeout = 100 * time.Millisecond
	clock := newWALClock()
	blocked := make(chan struct{})
	dm.js = walStuckReads{dm.js, blocked, &sync.Once{}}
	done := make(chan error, 1)
	go func() { done <- dm.restoreWALDeletes(ctx, stallTimeout, clock.now) }()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("restore never read the deleted key")
	}
	clock.advance(stallTimeout)
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrWALRecovery)
		require.ErrorContains(t, err, "no progress for 100ms restoring applied deletes")
		require.ErrorContains(t, err, "1 receipts read, 0 of 1 deleted keys verified")
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled restore was not stopped")
	}
	entry, err := dm.kv.Get(walTestKey("stuck"))
	require.NoError(t, err)
	require.Equal(t, "old", string(entry.Value()), "an unverified delete is not guessed")
}
