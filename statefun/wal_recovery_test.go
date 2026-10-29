package statefun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func walTestDomain(t *testing.T) (*Domain, context.Context, context.CancelFunc, *server.Server) {
	t.Helper()
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	nc, js, srv := domainTestNode(t, "recovery")
	kv, err := customNatsKv.CreateKeyValue(nc, js, &nats.KeyValueConfig{Bucket: "recovery_recovery_cache_bucket"})
	require.NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: WALCommitsStreamName, Subjects: []string{WALCommitsSubject}, Retention: nats.WorkQueuePolicy, Storage: nats.FileStorage})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store, err := cache.NewCacheStore(ctx, cache.NewCacheConfig("recovery"), js, kv)
	require.NoError(t, err)
	dm := &Domain{name: "recovery", hubDomainName: "recovery", nc: nc, js: js, kv: kv, cache: store}
	require.NoError(t, dm.prepareWALConsumer(ctx))
	return dm, ctx, cancel, srv
}
func walTestMessage(t *testing.T, dm *Domain, ops []cache.WALOp) *nats.Msg {
	t.Helper()
	data, err := json.Marshal(ops)
	require.NoError(t, err)
	msg := nats.NewMsg("wal.commits." + dm.kv.Bucket())
	msg.Header.Set("tx_id", fmt.Sprint(time.Now().UnixNano()))
	msg.Data = data
	_, err = dm.js.PublishMsg(msg)
	require.NoError(t, err)
	return msg
}
func walTestSubscription(t *testing.T, dm *Domain) *nats.Subscription {
	t.Helper()
	consumer := CommitterDurableName + "-" + dm.kv.Bucket()
	sub, err := dm.js.QueueSubscribeSync("wal.commits."+dm.kv.Bucket(), consumer+"-group", nats.Bind(WALCommitsStreamName, consumer), nats.ManualAck())
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return sub
}
func walTestNext(t *testing.T, s *nats.Subscription) *nats.Msg {
	t.Helper()
	m, err := s.NextMsg(3 * time.Second)
	require.NoError(t, err)
	return m
}
func walTestPut(key, value string) cache.WALOp {
	return cache.WALOp{OpType: cache.OpTypePUT, Key: "store.recovery/" + key, Value: []byte(value)}
}
func walTestPersistReceipt(t *testing.T, dm *Domain, msg *nats.Msg) {
	t.Helper()
	var ops []cache.WALOp
	require.NoError(t, json.Unmarshal(msg.Data, &ops))
	meta, err := msg.Metadata()
	require.NoError(t, err)
	deletes, err := dm.applyTransactionOps(context.Background(), ops, msg.Header.Get("tx_id"), meta.Sequence.Stream)
	require.NoError(t, err)
	require.NoError(t, dm.saveWALReceipt(msg, meta, msg.Header.Get("tx_id"), deletes))
}

type walMissingRead struct {
	nats.JetStreamContext
	err error
}

func (j walMissingRead) GetMsg(string, uint64, ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	return nil, j.err
}

type walUnreadableConsumer struct{ nats.JetStreamContext }

func (j walUnreadableConsumer) ConsumerInfo(string, string, ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return nil, nats.ErrTimeout
}

func TestWALRecovery_ReceiptPrecedesAckAndIsOutsideGraph(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "10"), walTestPut("b", "20")})
	msg := walTestNext(t, sub)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	require.NoError(t, dm.commitWALMessage(ctx, msg))
	_, err = dm.js.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.ErrorIs(t, err, nats.ErrMsgNotFound)
	receipt, err := dm.readWALReceipt(meta.Sequence.Stream)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.NoError(t, dm.validateWALReceipt(receipt, meta.Sequence.Stream))
	require.Equal(t, walDigest(msg.Data), receipt.Digest)
	for key, value := range map[string]string{"a": "10", "b": "20"} {
		entry, err := dm.kv.Get("store.recovery/" + key)
		require.NoError(t, err)
		require.Equal(t, value, string(entry.Value()))
	}
	require.NoError(t, dm.cache.RehydrateFromKV(ctx))
	_, err = dm.cache.GetValue(dm.walReceiptKey(meta.Sequence.Stream))
	require.Error(t, err)
}

func TestWALRecovery_RedeliveryDoesNotOverwriteNewerData(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "old"), {OpType: cache.OpTypeDelete, Key: "store.recovery/b"}})
	msg := walTestNext(t, sub)
	walTestPersistReceipt(t, dm, msg)
	_, err := dm.kv.Put("store.recovery/a", []byte("new"))
	require.NoError(t, err)
	_, err = dm.kv.Put("store.recovery/b", []byte("recreated"))
	require.NoError(t, err)
	require.NoError(t, dm.commitWALMessage(ctx, msg))
	for key, value := range map[string]string{"a": "new", "b": "recreated"} {
		entry, err := dm.kv.Get("store.recovery/" + key)
		require.NoError(t, err)
		require.Equal(t, value, string(entry.Value()))
	}
}

func TestWALRecovery_ReceiptFailureRetainsTransaction(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "small")})
	msg := walTestNext(t, sub)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	info, err := dm.js.StreamInfo(customNatsKv.KVStreamName(dm.kv))
	require.NoError(t, err)
	info.Config.MaxMsgSize = 64
	_, err = dm.js.UpdateStream(&info.Config)
	require.NoError(t, err)
	require.ErrorContains(t, dm.commitWALMessage(ctx, msg), "persist applied receipt")
	_, err = dm.js.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.NoError(t, err)
	receipt, err := dm.readWALReceipt(meta.Sequence.Stream)
	require.NoError(t, err)
	require.Nil(t, receipt)
	info.Config.MaxMsgSize = -1
	_, err = dm.js.UpdateStream(&info.Config)
	require.NoError(t, err)
	require.NoError(t, dm.commitWALMessage(ctx, msg))
}

func TestWALRecovery_ApplyFailureDoesNotCreateReceipt(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", strings.Repeat("x", 100))})
	msg := walTestNext(t, sub)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	info, err := dm.js.StreamInfo(customNatsKv.KVStreamName(dm.kv))
	require.NoError(t, err)
	info.Config.MaxMsgSize = 32
	_, err = dm.js.UpdateStream(&info.Config)
	require.NoError(t, err)
	require.Error(t, dm.commitWALMessage(ctx, msg))
	receipt, err := dm.readWALReceipt(meta.Sequence.Stream)
	require.NoError(t, err)
	require.Nil(t, receipt)
	_, err = dm.js.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.NoError(t, err)
}

func TestWALRecovery_ProvenMissingRecordRepeatsOriginalAck(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "10")})
	msg := walTestNext(t, sub)
	walTestPersistReceipt(t, dm, msg)
	original := dm.js
	consumer := CommitterDurableName + "-" + dm.kv.Bucket()
	before, err := original.ConsumerInfo(WALCommitsStreamName, consumer)
	require.NoError(t, err)
	// Model the broker's missing-message response while keeping its real ACK
	// subscription and durable consumer. The Docker regression creates the actual
	// missing record through SIGKILL, without a mocked broker response.
	dm.js = walMissingRead{original, nats.ErrMsgNotFound}
	recovered, err := dm.recoverMissingWALAck(ctx)
	require.NoError(t, err)
	require.True(t, recovered)
	after, err := original.ConsumerInfo(WALCommitsStreamName, consumer)
	require.NoError(t, err)
	require.Equal(t, before.Created, after.Created, "recovery must not recreate the consumer")
	require.Zero(t, after.NumAckPending)
}

func TestWALRecovery_UnprovenMissingRecordIsNeverAcknowledged(t *testing.T) {
	for _, kind := range []string{"absent", "malformed", "generation", "bucket", "sequence", "ack-address", "digest", "version"} {
		t.Run(kind, func(t *testing.T) {
			dm, ctx, _, _ := walTestDomain(t)
			sub := walTestSubscription(t, dm)
			walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "10")})
			msg := walTestNext(t, sub)
			meta, err := msg.Metadata()
			require.NoError(t, err)
			if kind != "absent" {
				walTestPersistReceipt(t, dm, msg)
				r, err := dm.readWALReceipt(meta.Sequence.Stream)
				require.NoError(t, err)
				switch kind {
				case "generation":
					r.StreamCreated = r.StreamCreated.Add(time.Second)
				case "bucket":
					r.Bucket = "other"
				case "sequence":
					r.Sequence++
				case "ack-address":
					r.AckSubject = "unrelated.service"
				case "digest":
					r.Digest = "invalid"
				case "version":
					r.Version = 2
				}
				data, err := json.Marshal(r)
				require.NoError(t, err)
				if kind == "malformed" {
					data = []byte("not-json")
				}
				_, err = dm.kv.Put(dm.walReceiptKey(meta.Sequence.Stream), data)
				require.NoError(t, err)
			}
			original := dm.js
			dm.js = walMissingRead{original, nats.ErrMsgNotFound}
			repaired, err := dm.recoverMissingWALAck(ctx)
			require.ErrorIs(t, err, ErrWALRecovery)
			require.False(t, repaired)
			info, err := original.ConsumerInfo(WALCommitsStreamName, CommitterDurableName+"-"+dm.kv.Bucket())
			require.NoError(t, err)
			require.Equal(t, 1, info.NumAckPending)
			_, err = original.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
			require.NoError(t, err)
		})
	}
}

func TestWALRecovery_ReadFailureIsNotMissingEvidence(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "10")})
	walTestNext(t, sub)
	dm.js = walMissingRead{dm.js, nats.ErrTimeout}
	repaired, err := dm.recoverMissingWALAck(ctx)
	require.ErrorIs(t, err, nats.ErrTimeout)
	require.False(t, repaired)
}

func TestWALRecovery_BoundedWaitRequiresRealProgress(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "10")})
	walTestNext(t, sub)
	start := time.Now()
	err := dm.waitForWALRecovery(ctx, 80*time.Millisecond, 5*time.Millisecond)
	require.ErrorIs(t, err, ErrWALRecovery)
	require.ErrorContains(t, err, "no durable apply or ACK progress")
	require.Less(t, time.Since(start), time.Second)
}

func TestWALRecovery_LongReplayWithProgressDoesNotTimeOut(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	for i := 0; i < 5; i++ {
		walTestMessage(t, dm, []cache.WALOp{walTestPut(fmt.Sprint(i), "value")})
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 5; i++ {
			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				done <- err
				return
			}
			time.Sleep(35 * time.Millisecond)
			if err = dm.commitWALMessage(ctx, msg); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	require.NoError(t, dm.waitForWALRecovery(ctx, 120*time.Millisecond, 5*time.Millisecond))
	require.NoError(t, <-done)
}

func TestWALRecovery_ConsumerReadErrorNeverReportsDurability(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	dm.js = walUnreadableConsumer{dm.js}
	_, err := dm.countPendingCommits(CommitterDurableName + "-" + dm.kv.Bucket())
	require.ErrorIs(t, err, nats.ErrTimeout)
	require.ErrorIs(t, dm.WaitForKVCaughtUp(ctx, time.Second), nats.ErrTimeout)
	require.ErrorIs(t, dm.waitForWALRecovery(ctx, 30*time.Millisecond, 5*time.Millisecond), ErrWALRecovery)
}

func TestWALRecovery_InvalidTransactionsAreRetained(t *testing.T) {
	for _, kind := range []string{"no-tx-id", "json", "null", "unknown-op", "empty-key", "reserved-key"} {
		t.Run(kind, func(t *testing.T) {
			dm, ctx, _, _ := walTestDomain(t)
			sub := walTestSubscription(t, dm)
			msg := nats.NewMsg("wal.commits." + dm.kv.Bucket())
			msg.Header.Set("tx_id", "1")
			msg.Data = []byte("[]")
			switch kind {
			case "no-tx-id":
				msg.Header.Del("tx_id")
			case "json":
				msg.Data = []byte("broken")
			case "null":
				msg.Data = []byte("null")
			case "unknown-op":
				msg.Data = []byte(`[{"op":"invalid","key":"store.recovery/a"}]`)
			case "empty-key":
				msg.Data, _ = json.Marshal([]cache.WALOp{{OpType: cache.OpTypePUT}})
			case "reserved-key":
				msg.Data, _ = json.Marshal([]cache.WALOp{{OpType: cache.OpTypePUT, Key: walReceiptPrefix + "fake"}})
			}
			ack, err := dm.js.PublishMsg(msg)
			require.NoError(t, err)
			delivery := walTestNext(t, sub)
			require.ErrorIs(t, dm.commitWALMessage(ctx, delivery), ErrWALRecovery)
			_, err = dm.js.GetMsg(WALCommitsStreamName, ack.Sequence)
			require.NoError(t, err)
			r, err := dm.readWALReceipt(ack.Sequence)
			require.NoError(t, err)
			require.Nil(t, r)
		})
	}
}

func TestWALRecovery_PrunesOnlyOldAcknowledgedReceipts(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	var sequences []uint64
	for i := 0; i < 5; i++ {
		walTestMessage(t, dm, []cache.WALOp{walTestPut(fmt.Sprint(i), "x")})
		msg := walTestNext(t, sub)
		meta, err := msg.Metadata()
		require.NoError(t, err)
		sequences = append(sequences, meta.Sequence.Stream)
		require.NoError(t, dm.commitWALMessage(ctx, msg))
	}
	walTestMessage(t, dm, []cache.WALOp{walTestPut("pending", "x")})
	pending := walTestNext(t, sub)
	walTestPersistReceipt(t, dm, pending)
	require.NoError(t, dm.pruneWALReceipts(ctx, 2))
	for i, seq := range sequences {
		r, err := dm.readWALReceipt(seq)
		require.NoError(t, err)
		if i < 4 {
			require.Nil(t, r)
		} else {
			require.NotNil(t, r)
		}
	}
	meta, err := pending.Metadata()
	require.NoError(t, err)
	r, err := dm.readWALReceipt(meta.Sequence.Stream)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.ErrorIs(t, dm.checkWALReceiptWindow(sequences[0]), ErrWALRecovery)
	require.NoError(t, dm.commitWALMessage(ctx, pending))
	entry, err := dm.kv.Get("store.recovery/0")
	require.NoError(t, err)
	require.Equal(t, "x", string(entry.Value()))
}

func TestWALRecovery_PrunedReplayCannotOverwriteNewerValues(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "old")})
	msg := walTestNext(t, sub)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	require.NoError(t, dm.recordPrunedWALReceipts(meta.Sequence.Stream))
	_, err = dm.kv.Put("store.recovery/a", []byte("new"))
	require.NoError(t, err)
	require.ErrorIs(t, dm.commitWALMessage(ctx, msg), ErrWALRecovery)
	entry, err := dm.kv.Get("store.recovery/a")
	require.NoError(t, err)
	require.Equal(t, "new", string(entry.Value()))
	_, err = dm.js.GetMsg(WALCommitsStreamName, meta.Sequence.Stream)
	require.NoError(t, err)
}

func TestWALRecovery_StartupReloadsCacheAfterPutAndDelete(t *testing.T) {
	dm, _, cancel, srv := walTestDomain(t)
	cancel()
	_, err := dm.kv.Put("store.recovery/a", []byte("old"))
	require.NoError(t, err)
	_, err = dm.kv.Put("store.recovery/deleted", []byte("old"))
	require.NoError(t, err)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "new"), walTestPut("created", "present"), {OpType: cache.OpTypeDelete, Key: "store.recovery/deleted"}})
	rt, err := NewRuntime(*NewRuntimeConfigSimple(srv.ClientURL(), "recovery-runtime").UseJSDomainAsHubDomainName().SetActivePassiveMode(false))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- rt.Start(context.Background(), cache.NewCacheConfig("recovery")) }()
	t.Cleanup(func() {
		rt.Shutdown(true)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("runtime failed to stop")
		}
	})
	require.Eventually(t, rt.IsReady, 10*time.Second, 5*time.Millisecond)
	for key, value := range map[string]string{"a": "new", "created": "present"} {
		v, err := rt.Domain.Cache().GetValue("recovery/" + key)
		require.NoError(t, err)
		require.Equal(t, value, string(v))
	}
	_, err = rt.Domain.Cache().GetValue("recovery/deleted")
	require.Error(t, err)
}

func TestWALRecovery_StartupReturnsMalformedTransactionError(t *testing.T) {
	dm, _, cancel, srv := walTestDomain(t)
	cancel()
	msg := nats.NewMsg("wal.commits." + dm.kv.Bucket())
	msg.Header.Set("tx_id", "1")
	msg.Data = []byte("broken")
	ack, err := dm.js.PublishMsg(msg)
	require.NoError(t, err)
	rt, err := NewRuntime(*NewRuntimeConfigSimple(srv.ClientURL(), "recovery-runtime").UseJSDomainAsHubDomainName().SetActivePassiveMode(false))
	require.NoError(t, err)
	ctx, cancelStart := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelStart()
	err = rt.Start(ctx, cache.NewCacheConfig("recovery"))
	require.ErrorIs(t, err, ErrWALRecovery)
	require.False(t, rt.IsReady())
	_, err = dm.js.GetMsg(WALCommitsStreamName, ack.Sequence)
	require.NoError(t, err)
}

func TestWALRecovery_StartupFailureAndCancellationAreReturned(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	dm.js = walUnreadableConsumer{dm.js}
	child, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.Error(t, dm.TransactionCommitter(child))
	require.Less(t, time.Since(start), time.Second)
	cancelled, stop := context.WithCancel(ctx)
	stop()
	require.True(t, errors.Is(dm.TransactionCommitter(cancelled), context.Canceled))
}

func TestWALRecovery_DeleteEvidencePreservesNewerPut(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	_, err := dm.kv.Put("store.recovery/a", []byte("old"))
	require.NoError(t, err)
	walTestMessage(t, dm, []cache.WALOp{{OpType: cache.OpTypeDelete, Key: "store.recovery/a"}})
	msg := walTestNext(t, sub)
	walTestPersistReceipt(t, dm, msg)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	receipt, err := dm.readWALReceipt(meta.Sequence.Stream)
	require.NoError(t, err)
	require.Len(t, receipt.Deletes, 1)
	_, err = dm.kv.Put("store.recovery/a", []byte("legitimate-new-value"))
	require.NoError(t, err)
	require.NoError(t, dm.repairWALDeletes(ctx, receipt))
	entry, err := dm.kv.Get("store.recovery/a")
	require.NoError(t, err)
	require.Equal(t, "legitimate-new-value", string(entry.Value()))
	receipt.Deletes[0].Revision = receipt.revision
	require.ErrorIs(t, dm.validateWALReceipt(receipt, meta.Sequence.Stream), ErrWALRecovery)
}

func TestWALRecovery_ConsumerUpgradePreservesPendingPosition(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "10")})
	walTestNext(t, sub)
	name := CommitterDurableName + "-" + dm.kv.Bucket()
	before, err := dm.js.ConsumerInfo(WALCommitsStreamName, name)
	require.NoError(t, err)
	config := before.Config
	config.MaxDeliver = 5
	_, err = dm.js.UpdateConsumer(WALCommitsStreamName, &config)
	require.NoError(t, err)
	require.NoError(t, dm.prepareWALConsumer(ctx))
	after, err := dm.js.ConsumerInfo(WALCommitsStreamName, name)
	require.NoError(t, err)
	require.Equal(t, before.Created, after.Created)
	require.Equal(t, before.Delivered.Stream, after.Delivered.Stream)
	require.Equal(t, before.AckFloor.Stream, after.AckFloor.Stream)
	require.Equal(t, 1, after.NumAckPending)
	require.Equal(t, -1, after.Config.MaxDeliver)
}

func TestWALRecovery_RecreatedKVRejectsOldReceipt(t *testing.T) {
	dm, _, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "10")})
	msg := walTestNext(t, sub)
	walTestPersistReceipt(t, dm, msg)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	receipt, err := dm.readWALReceipt(meta.Sequence.Stream)
	require.NoError(t, err)
	receipt.KVCreated = receipt.KVCreated.Add(time.Second)
	require.ErrorIs(t, dm.validateWALReceipt(receipt, meta.Sequence.Stream), ErrWALRecovery)
}

func TestWALRecovery_RepeatedPartialApplyDoesNotExtendDeadline(t *testing.T) {
	dm, _, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	ops := make([]cache.WALOp, 0, applyTransactionOpsChunkSize+1)
	for i := 0; i < applyTransactionOpsChunkSize; i++ {
		ops = append(ops, walTestPut(fmt.Sprintf("key-%d", i), "v"))
	}
	ops = append(ops, walTestPut("too-large", strings.Repeat("x", 256)))
	walTestMessage(t, dm, ops)
	delivery := walTestNext(t, sub)
	meta, err := delivery.Metadata()
	require.NoError(t, err)
	info, err := dm.js.StreamInfo(customNatsKv.KVStreamName(dm.kv))
	require.NoError(t, err)
	info.Config.MaxMsgSize = 128
	_, err = dm.js.UpdateStream(&info.Config)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			_, _ = dm.applyTransactionOps(ctx, ops, "same-partial-transaction", meta.Sequence.Stream)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	err = dm.waitForWALRecovery(ctx, 250*time.Millisecond, 5*time.Millisecond)
	cancel()
	<-done
	require.ErrorIs(t, err, ErrWALRecovery, "retrying the same prefix is not new recovery progress")
}

// A replica can lag behind a successful KV publish. Recovery must consult the
// authoritative stream API even when a direct KV read reports stale absence.
type walStaleDirectKV struct{ nats.KeyValue }

func (kv walStaleDirectKV) Get(string) (nats.KeyValueEntry, error) { return nil, nats.ErrKeyNotFound }

func TestWALRecovery_StaleReplicaCannotHideAppliedReceipt(t *testing.T) {
	dm, ctx, _, _ := walTestDomain(t)
	sub := walTestSubscription(t, dm)
	walTestMessage(t, dm, []cache.WALOp{walTestPut("a", "10")})
	msg := walTestNext(t, sub)
	walTestPersistReceipt(t, dm, msg)
	dm.kv = walStaleDirectKV{dm.kv}
	dm.js = walMissingRead{dm.js, nats.ErrMsgNotFound}
	recovered, err := dm.recoverMissingWALAck(ctx)
	require.NoError(t, err)
	require.True(t, recovered)
}

func TestWALRecovery_StaleReplicaCannotHidePruningBoundary(t *testing.T) {
	dm, _, _, _ := walTestDomain(t)
	require.NoError(t, dm.recordPrunedWALReceipts(10))
	dm.kv = walStaleDirectKV{dm.kv}
	require.ErrorIs(t, dm.checkWALReceiptWindow(3), ErrWALRecovery)
}
