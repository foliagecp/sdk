package statefun

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// walAckGate delays real KV PubAcks on the wire, before nats.go can release
// its pending slots. Delaying reads of PubAckFuture alone cannot reproduce
// publisher saturation. All writes still go to the real JetStream KV store.
type walAckGate struct {
	net.Conn
	reader    *bufio.Reader
	buffered  []byte
	stream    string
	held      chan struct{}
	release   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	publisher *nats.Conn
}

func (g *walAckGate) Close() error {
	var err error
	g.closeOnce.Do(func() { close(g.closed); err = g.Conn.Close() })
	return err
}

func (g *walAckGate) Read(p []byte) (int, error) {
	if len(g.buffered) == 0 {
		line, err := g.reader.ReadString('\n')
		if err != nil {
			return 0, err
		}
		frame := []byte(line)
		fields := strings.Fields(line)
		if len(fields) > 0 && (fields[0] == "MSG" || fields[0] == "HMSG") {
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil || size < 0 {
				return 0, fmt.Errorf("invalid NATS frame size: %q", line)
			}
			body := make([]byte, size+2)
			if _, err := io.ReadFull(g.reader, body); err != nil {
				return 0, err
			}
			frame = append(frame, body...)
			var ack nats.PubAck
			if fields[0] == "MSG" && json.Unmarshal(body[:size], &ack) == nil && ack.Stream == g.stream && ack.Sequence > 0 {
				select {
				case g.held <- struct{}{}:
				default:
				}
				select {
				case <-g.release:
				case <-g.closed:
					return 0, net.ErrClosed
				}
			}
		}
		g.buffered = frame
	}
	n := copy(p, g.buffered)
	g.buffered = g.buffered[n:]
	return n, nil
}

type walAckDialer struct {
	gate *walAckGate
	used atomic.Bool
}

func (d *walAckDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, address, time.Second)
	if err != nil {
		return nil, err
	}
	// Reconnected sockets are not gated: disconnect tests exercise recovery
	// through the same nats.go connection and JetStream context.
	if !d.used.CompareAndSwap(false, true) {
		return conn, nil
	}
	d.gate.Conn = conn
	d.gate.reader = bufio.NewReader(conn)
	return d.gate, nil
}

func walGatedPublisher(t *testing.T, dm *Domain) *walAckGate {
	t.Helper()
	gate := &walAckGate{
		stream: customNatsKv.KVStreamName(dm.kv),
		held:   make(chan struct{}, 1), release: make(chan struct{}), closed: make(chan struct{}),
	}
	nc, err := nats.Connect(dm.nc.ConnectedUrl(), nats.ReconnectWait(10*time.Millisecond), nats.SetCustomDialer(&walAckDialer{gate: gate}))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	gate.publisher = nc
	// This is the runtime's actual limit, not nats.go's default of 4000.
	dm.js, err = nc.JetStream(nats.PublishAsyncMaxPending(runtimePublishAsyncMaxPending))
	require.NoError(t, err)
	return gate
}

func walWaitHeld(t *testing.T, gate *walAckGate) {
	t.Helper()
	select {
	case <-gate.held:
	case <-time.After(3 * time.Second):
		t.Fatal("no KV acknowledgement reached the gate")
	}
}

func walMustStillWait(t *testing.T, done <-chan error) {
	t.Helper()
	// Longer than nats.go's default 200 ms async-publish stall timeout.
	select {
	case err := <-done:
		t.Fatalf("apply stopped while acknowledgements were held: %v", err)
	case <-time.After(350 * time.Millisecond):
	}
}

func walAwaitCommit(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("WAL commit did not finish")
	}
}

func TestWALBackpressure_LargeTransactionWaitsForAcks(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	control := dm.js
	sub := walTestSubscription(t, dm)
	// More than two of the old 1024-operation chunks, including deletes and
	// multiple writes to one key. Publish order must preserve the final value.
	ops := make([]cache.WALOp, 0, 2300)
	for i := 0; i < 1100; i++ {
		ops = append(ops, walTestPut(fmt.Sprint(i), "value"))
	}
	for i := 0; i < 1100; i++ {
		if i%2 == 0 {
			ops = append(ops, cache.WALOp{OpType: cache.OpTypeDelete, Key: ops[i].Key})
		} else {
			ops = append(ops, walTestPut(fmt.Sprint(i), "new"))
		}
	}
	walTestMessage(t, dm, ops)
	msg := walTestNext(t, sub)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	gate := walGatedPublisher(t, dm)
	done := make(chan error, 1)
	go func() { done <- dm.commitWALMessage(ctx, msg) }()
	walWaitHeld(t, gate)
	walMustStillWait(t, done)
	require.Less(t, dm.js.PublishAsyncPending(), 256)
	require.Greater(t, dm.js.PublishAsyncPending(), 0)
	// A partial apply must neither ACK the WAL nor create a completion receipt.
	_, err = control.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.NoError(t, err)
	_, err = dm.kv.Get(dm.walReceiptKey(meta.Sequence.Stream))
	require.ErrorIs(t, err, nats.ErrKeyNotFound)
	close(gate.release)
	walAwaitCommit(t, done)
	require.Zero(t, dm.js.PublishAsyncPending())
	_, err = control.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.ErrorIs(t, err, nats.ErrMsgNotFound)
	receipt, err := dm.readWALReceipt(meta.Sequence.Stream)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.Len(t, receipt.Deletes, 550)
	for i := 0; i < 1100; i++ {
		entry, err := dm.kv.Get(ops[i].Key)
		if i%2 == 0 {
			require.ErrorIs(t, err, nats.ErrKeyNotFound)
		} else {
			require.NoError(t, err)
			require.Equal(t, "new", string(entry.Value()))
		}
	}
}

func TestWALBackpressure_CancellationRetainsWALAndRetryWaits(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%v", deadline), func(t *testing.T) {
			dm, _, _, _ := walTestDomain(t)
			control := dm.js
			sub := walTestSubscription(t, dm)
			ops := make([]cache.WALOp, 1025)
			for i := range ops {
				ops[i] = walTestPut(fmt.Sprint(i), "value")
			}
			walTestMessage(t, dm, ops)
			msg := walTestNext(t, sub)
			meta, err := msg.Metadata()
			require.NoError(t, err)
			gate := walGatedPublisher(t, dm)
			ctx, cancel := context.WithCancel(context.Background())
			expected := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 400*time.Millisecond)
				expected = context.DeadlineExceeded
			}
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- dm.commitWALMessage(ctx, msg) }()
			walWaitHeld(t, gate)
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, expected)
			case <-time.After(2 * time.Second):
				t.Fatal("apply ignored cancellation")
			}
			pending := dm.js.PublishAsyncPending()
			require.Greater(t, pending, 0)
			_, err = control.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
			require.NoError(t, err)
			_, err = dm.kv.Get(dm.walReceiptKey(meta.Sequence.Stream))
			require.ErrorIs(t, err, nats.ErrKeyNotFound)
			go func() { done <- dm.commitWALMessage(context.Background(), msg) }()
			walMustStillWait(t, done)
			require.Equal(t, pending, dm.js.PublishAsyncPending(), "retry must drain the previous attempt before publishing")
			close(gate.release)
			walAwaitCommit(t, done)
			require.Zero(t, dm.js.PublishAsyncPending())
			for _, op := range ops {
				entry, err := dm.kv.Get(op.Key)
				require.NoError(t, err)
				require.Equal(t, op.Value, entry.Value())
			}
		})
	}
}

func TestWALBackpressure_FullSharedPoolWaitsBeforePublishing(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	gate := walGatedPublisher(t, dm)
	for i := 0; i < 255; i++ {
		_, err := customNatsKv.KVPutAsync(dm.js, dm.kv, fmt.Sprintf("store.recovery/earlier-%d", i), []byte("earlier"))
		require.NoError(t, err)
	}
	walWaitHeld(t, gate)
	ops := []cache.WALOp{walTestPut("next", "next")}
	done := make(chan error, 1)
	go func() { _, err := dm.applyTransactionOps(ctx, ops, "next", 1); done <- err }()
	walMustStillWait(t, done)
	require.Equal(t, 255, dm.js.PublishAsyncPending())
	_, err := dm.kv.Get(ops[0].Key)
	require.ErrorIs(t, err, nats.ErrKeyNotFound)
	close(gate.release)
	walAwaitCommit(t, done)
	require.Zero(t, dm.js.PublishAsyncPending())
	entry, err := dm.kv.Get(ops[0].Key)
	require.NoError(t, err)
	require.Equal(t, "next", string(entry.Value()))
}

func TestWALBackpressure_DisconnectRetainsWAL(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	control := dm.js
	sub := walTestSubscription(t, dm)
	ops := make([]cache.WALOp, 1025)
	for i := range ops {
		ops[i] = walTestPut(fmt.Sprint(i), "value")
	}
	walTestMessage(t, dm, ops)
	msg := walTestNext(t, sub)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	gate := walGatedPublisher(t, dm)
	done := make(chan error, 1)
	go func() { done <- dm.commitWALMessage(ctx, msg) }()
	walWaitHeld(t, gate)
	require.NoError(t, gate.Close())
	select {
	case err := <-done:
		require.Error(t, err)
		require.False(t, errors.Is(err, ErrWALRecovery))
	case <-time.After(3 * time.Second):
		t.Fatal("apply did not notice the disconnected publisher")
	}
	_, err = control.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.NoError(t, err)
	_, err = dm.kv.Get(dm.walReceiptKey(meta.Sequence.Stream))
	require.ErrorIs(t, err, nats.ErrKeyNotFound)
	require.Eventually(t, gate.publisher.IsConnected, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, dm.commitWALMessage(ctx, msg))
	_, err = control.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.ErrorIs(t, err, nats.ErrMsgNotFound)
	for _, op := range ops {
		entry, err := dm.kv.Get(op.Key)
		require.NoError(t, err)
		require.Equal(t, op.Value, entry.Value())
	}
}

// Fill the actual nats.go pool after the WAL's capacity check, so the race
// between a shared publisher and the committer exercises the stall retry path.
type walCompetingPublisher struct {
	nats.JetStreamContext
	once     sync.Once
	compete  func() error
	competed chan error
	stalled  chan struct{}
}

func (j *walCompetingPublisher) PublishAsync(subject string, data []byte, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	j.once.Do(func() { j.competed <- j.compete() })
	future, err := j.JetStreamContext.PublishAsync(subject, data, opts...)
	if err != nil && err.Error() == "nats: stalled with too many outstanding async published messages" {
		select {
		case j.stalled <- struct{}{}:
		default:
		}
	}
	return future, err
}

func TestWALBackpressure_ConcurrentPublisherSaturationIsRetried(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	gate := walGatedPublisher(t, dm)
	original := dm.js
	competitor := &walCompetingPublisher{JetStreamContext: original, competed: make(chan error, 1), stalled: make(chan struct{}, 1)}
	competitor.compete = func() error {
		for i := 0; i < 255; i++ {
			if _, err := customNatsKv.KVPutAsync(original, dm.kv, fmt.Sprintf("store.recovery/competing-%d", i), []byte("earlier")); err != nil {
				return err
			}
		}
		return nil
	}
	dm.js = competitor
	done := make(chan error, 1)
	go func() {
		_, err := dm.applyTransactionOps(ctx, []cache.WALOp{walTestPut("after-competition", "new")}, "competing", 1)
		done <- err
	}()
	walWaitHeld(t, gate)
	select {
	case err := <-competitor.competed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("competing publisher did not fill the pending pool")
	}
	select {
	case <-competitor.stalled:
	case <-time.After(3 * time.Second):
		t.Fatal("test did not exercise nats.go's full-pool error")
	}
	walMustStillWait(t, done)
	require.Equal(t, 255, original.PublishAsyncPending())
	close(gate.release)
	walAwaitCommit(t, done)
	entry, err := dm.kv.Get("store.recovery/after-competition")
	require.NoError(t, err)
	require.Equal(t, "new", string(entry.Value()))
	for i := 0; i < 255; i++ {
		entry, err := dm.kv.Get(fmt.Sprintf("store.recovery/competing-%d", i))
		require.NoError(t, err)
		require.Equal(t, "earlier", string(entry.Value()))
	}
	require.Zero(t, original.PublishAsyncPending())
}

func TestWALBackpressure_AckFailureRetainsWALAndRetryCompletes(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	ops := []cache.WALOp{walTestPut("before-error", "old"), walTestPut("too-large", strings.Repeat("x", 256)), walTestPut("after-error", "new")}
	walTestMessage(t, dm, ops)
	msg := walTestNext(t, sub)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	info, err := dm.js.StreamInfo(customNatsKv.KVStreamName(dm.kv))
	require.NoError(t, err)
	info.Config.MaxMsgSize = 128
	_, err = dm.js.UpdateStream(&info.Config)
	require.NoError(t, err)
	require.ErrorContains(t, dm.commitWALMessage(ctx, msg), "ack failed")
	_, err = dm.js.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.NoError(t, err)
	receipt, err := dm.readWALReceipt(meta.Sequence.Stream)
	require.NoError(t, err)
	require.Nil(t, receipt)
	info.Config.MaxMsgSize = -1
	_, err = dm.js.UpdateStream(&info.Config)
	require.NoError(t, err)
	require.NoError(t, dm.commitWALMessage(ctx, msg))
	require.Zero(t, dm.js.PublishAsyncPending())
	for _, op := range ops {
		entry, err := dm.kv.Get(op.Key)
		require.NoError(t, err)
		require.Equal(t, op.Value, entry.Value())
	}
	_, err = dm.js.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.ErrorIs(t, err, nats.ErrMsgNotFound)
}
