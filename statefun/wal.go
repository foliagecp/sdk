package statefun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

const (
	WALCommitsStreamName = "wal_commits"
	WALCommitsSubject    = "wal.commits.*"
	CommitterDurableName = "TRANSACTION_COMMITTER_CONSUMER"

	// Export-dedicated stream — parallel pipeline for export committer.
	WALExportCommitsStreamName = "wal_export_commits"
	WALExportCommitsSubject    = "wal.export.commits.*"
	ExportCommitterDurableName = "EXPORT_COMMITTER_CONSUMER"

	// --- Transaction consumer ---
	commitAckWait    = 5 * time.Second
	commitMaxDeliver = 5

	// --- Shutdown drain ---
	shutdownPollInterval = 200 * time.Millisecond
	shutdownLogInterval  = 5 * time.Second
	shutdownMaxWait      = 30 * time.Second

	// Keep the WAL window below the runtime's async publisher limit. nats.go
	// v1.37 starts stalling when the pending count reaches (not exceeds) it.
	runtimePublishAsyncMaxPending = 256
	applyTransactionOpsChunkSize  = runtimePublishAsyncMaxPending / 2

	// Bound publication backpressure and ACK collection together per window.
	// Cancellation also interrupts waits for an earlier attempt's ACKs.
	applyTransactionOpsAckTimeout = 30 * time.Second
)

type walStartResult struct{ err error }

func (dm *Domain) TransactionCommitter(ctx context.Context) error {
	ready := make(chan walStartResult, 1)
	go func() {
		err := dm.runTransactionCommitter(ctx, ready)
		if err != nil && !errors.Is(err, context.Canceled) {
			lg.Logf(lg.ErrorLevel, "TransactionCommitter error: %s", err)
		}
		// Startup errors must reach the caller, not leave it waiting forever.
		select {
		case ready <- walStartResult{err}:
		default:
		}
	}()
	select {
	case result := <-ready:
		return result.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (dm *Domain) runTransactionCommitter(ctx context.Context, ready chan walStartResult) error {
	dm.shutdown = make(chan struct{})
	defer close(dm.shutdown)
	consumerName := CommitterDurableName + "-" + dm.kv.Bucket()
	commitSubject := "wal.commits." + dm.kv.Bucket()
	if err := dm.prepareWALConsumer(ctx); err != nil {
		return err
	}
	if err := dm.setKVConsistent(false); err != nil {
		return err
	}
	if _, err := dm.recoverMissingWALAck(ctx); err != nil {
		return err
	}

	sub, err := dm.js.QueueSubscribe(commitSubject, consumerName+"-group", func(msg *nats.Msg) {
		if err := dm.walTerminalError(); err != nil {
			return
		}
		// Keep the broker from handing the same transaction to another replica
		// while a large apply or a backup barrier is still in progress.
		done := make(chan struct{})
		go func() {
			ticker := time.NewTicker(commitAckWait / 3)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ctx.Done():
					return
				case <-ticker.C:
					_ = msg.InProgress()
				}
			}
		}()
		err := dm.commitWALMessage(ctx, msg)
		close(done)
		if err != nil {
			lg.Logf(lg.ErrorLevel, "TransactionCommitter: %s", err)
			if errors.Is(err, ErrWALRecovery) {
				dm.walFailure.CompareAndSwap(nil, &walFailure{err})
			} else if ctx.Err() == nil {
				_ = msg.NakWithDelay(time.Second)
			}
		}
	}, nats.Bind(WALCommitsStreamName, consumerName), nats.ManualAck())
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	ready <- walStartResult{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-dm.cache.Synced:
	}
	if err := dm.setKVConsistent(false); err != nil {
		return err
	}
	deadline := time.NewTimer(shutdownMaxWait)
	defer deadline.Stop()
	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()
	for {
		if err := dm.walTerminalError(); err != nil {
			return err
		}
		pending, err := dm.countPendingCommits(consumerName)
		if err != nil {
			return err
		}
		if pending == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("WAL shutdown drain timed out with %d transactions outstanding", pending)
		case <-ticker.C:
		}
	}
}

func (dm *Domain) commitWALMessage(ctx context.Context, msg *nats.Msg) error {
	meta, err := msg.Metadata()
	if err != nil {
		return fmt.Errorf("%w: invalid commit metadata: %v", ErrWALRecovery, err)
	}
	txID := msg.Header.Get("tx_id")
	if txID == "" {
		return fmt.Errorf("%w: WAL sequence %d has no tx_id; transaction retained", ErrWALRecovery, meta.Sequence.Stream)
	}
	var ops []cache.WALOp
	if err := json.Unmarshal(msg.Data, &ops); err != nil || ops == nil {
		return fmt.Errorf("%w: malformed transaction at WAL sequence %d; transaction retained", ErrWALRecovery, meta.Sequence.Stream)
	}
	for _, op := range ops {
		if op.Key == "" || strings.HasPrefix(op.Key, walReceiptPrefix) || op.Key == cache.ConsistencyKey || (op.OpType != cache.OpTypePUT && op.OpType != cache.OpTypeDelete) {
			return fmt.Errorf("%w: invalid operation at WAL sequence %d; transaction retained", ErrWALRecovery, meta.Sequence.Stream)
		}
	}
	receipt, err := dm.readWALReceipt(meta.Sequence.Stream)
	if err != nil {
		return err
	}
	var deletes []walAppliedDelete
	if receipt != nil {
		deletes = receipt.Deletes
		if err := dm.validateWALReceipt(receipt, meta.Sequence.Stream); err != nil {
			return err
		}
		if receipt.TxID != txID || receipt.Digest != walDigest(msg.Data) {
			return fmt.Errorf("%w: applied receipt differs from WAL sequence %d; transaction retained", ErrWALRecovery, meta.Sequence.Stream)
		}
	} else {
		if err := dm.checkWALReceiptWindow(meta.Sequence.Stream); err != nil {
			return err
		}
		for dm.isBackupBarrierActive() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
		deletes, err = dm.applyTransactionOps(ctx, ops, txID, meta.Sequence.Stream)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := dm.saveWALReceipt(msg, meta, txID, deletes); err != nil {
			return err
		}
	}
	// Reclaim delete markers only after their revisions are covered by a receipt.
	for _, deletion := range deletes {
		dm.cache.NoteKVTombstone(deletion.Key, deletion.Revision)
	}
	// An existing receipt means this is a redelivery of a completed apply.
	// Do not reapply an older PUT or DELETE over newer values in KV.
	if txTime, err := strconv.ParseInt(txID, 10, 64); err == nil {
		dm.cache.RecordCommittedTx(txTime)
	}
	if err := msg.AckSync(nats.Context(ctx)); err != nil {
		return fmt.Errorf("WAL sequence %d acknowledgement: %w", meta.Sequence.Stream, err)
	}
	return nil
}

// pendingKVOp is one in-flight async KV write whose PubAck is pending.
// We carry the original op for diagnostics; the WAL message will be
// redelivered (and reapplied idempotently) on any error.
type pendingKVOp struct {
	key    string
	opType string
	future nats.PubAckFuture
}

// applyTransactionOps pipelines bounded windows of KV writes in publish order.
// Transaction size is independent of the in-flight limit. A failed attempt
// leaves the WAL unacknowledged; its outstanding publishes must settle before
// a retry sends any more writes. Only a fully acknowledged apply may produce
// the completion receipt and ACK the WAL message.
func (dm *Domain) applyTransactionOps(ctx context.Context, ops []cache.WALOp, txID string, sequence uint64) ([]walAppliedDelete, error) {
	var deletes []walAppliedDelete
	for start := 0; start < len(ops); start += applyTransactionOpsChunkSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+applyTransactionOpsChunkSize, len(ops))
		windowCtx, cancel := context.WithTimeout(ctx, applyTransactionOpsAckTimeout)
		appliedDeletes, err := dm.applyWALWindow(windowCtx, ops[start:end], txID, start)
		cancel()
		if err != nil {
			return nil, err
		}
		deletes = append(deletes, appliedDeletes...)
		dm.recordWALApplyProgress(sequence, end)
	}
	return deletes, nil
}

func (dm *Domain) applyWALWindow(ctx context.Context, ops []cache.WALOp, txID string, start int) ([]walAppliedDelete, error) {
	// The context owns a shared pending pool. In particular, cancellation or
	// an early error in a previous attempt does not remove its sent messages.
	// Do not clean up the publisher: that would discard their acknowledgements
	// and fail other users of the same JetStream context.
	if err := dm.waitForWALPublisher(ctx); err != nil {
		return nil, fmt.Errorf("KV apply waiting for outstanding publishes in tx=%s: %w", txID, err)
	}
	pending := make([]pendingKVOp, 0, len(ops))
	for _, op := range ops {
		if op.Key == "" || (op.OpType != cache.OpTypePUT && op.OpType != cache.OpTypeDelete) {
			continue
		}
		future, err := dm.publishWALOp(ctx, op)
		if err != nil {
			return nil, fmt.Errorf("KV apply (publish %s) failed for key=%s in tx=%s: %w", op.OpType, op.Key, txID, err)
		}
		pending = append(pending, pendingKVOp{key: op.Key, opType: op.OpType, future: future})
	}
	var deletes []walAppliedDelete
	for i, p := range pending {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("KV apply ack wait in tx=%s at op %d (key=%s): %w", txID, start+i, p.key, ctx.Err())
		case pa := <-p.future.Ok():
			if pa == nil || pa.Sequence == 0 {
				return nil, fmt.Errorf("KV apply returned no durable revision in tx=%s", txID)
			}
			// Preserve the exact durable DEL revision for receipt-backed purging.
			if p.opType == cache.OpTypeDelete {
				deletes = append(deletes, walAppliedDelete{Key: p.key, Revision: pa.Sequence})
			}
		case err := <-p.future.Err():
			return nil, fmt.Errorf("KV apply ack failed in tx=%s at op %d (key=%s, type=%s): %w", txID, start+i, p.key, p.opType, err)
		}
	}
	return deletes, ctx.Err()
}

func (dm *Domain) waitForWALPublisher(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-dm.js.PublishAsyncComplete():
		return ctx.Err()
	}
}

func (dm *Domain) publishWALOp(ctx context.Context, op cache.WALOp) (nats.PubAckFuture, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if dm.js.PublishAsyncPending() >= applyTransactionOpsChunkSize {
			if err := dm.waitForWALPublisher(ctx); err != nil {
				return nil, err
			}
		}
		var future nats.PubAckFuture
		var err error
		if op.OpType == cache.OpTypePUT {
			future, err = customNatsKv.KVPutAsync(dm.js, dm.kv, op.Key, op.Value)
		} else {
			future, err = customNatsKv.KVDeleteAsync(dm.js, dm.kv, op.Key)
		}
		// Another publisher can consume the shared capacity between the check
		// and the publish. The legacy nats.go API exposes this one error only
		// as text. It removes the unsent future, so retry just this operation
		// after draining, within the same window deadline. Other errors still
		// return to the WAL consumer for its normal retry handling.
		if err == nil || err.Error() != "nats: stalled with too many outstanding async published messages" {
			return future, err
		}
		if err := dm.waitForWALPublisher(ctx); err != nil {
			return nil, err
		}
	}
}

func (dm *Domain) setKVConsistent(consistent bool) error {
	value := []byte("false")
	if consistent {
		value = []byte("true")
	}

	_, err := customNatsKv.KVPut(dm.js, dm.kv, cache.ConsistencyKey, value)
	return err
}

func (dm *Domain) isKVConsistent() (bool, error) {
	entry, err := dm.kv.Get(cache.ConsistencyKey)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	return string(entry.Value()) == "true", nil
}

// WaitForKVCaughtUp blocks until everything written through the cache has
// landed in JetStream KV — both the cache has nothing left to publish to WAL
// and the committer has no pending WAL messages left to apply. Returns nil on
// success, ctx.Err() if cancelled, or an error if the timeout elapses with
// pending work still present.
//
// In the cache-as-source-of-truth model, cache writes (SetValue / Delete)
// return as soon as the in-memory state is updated; the WAL→committer→KV
// chain is asynchronous. Callers that need a hard "writes are durable in KV"
// barrier — backup tooling, HA-aware tests that hand off to a second runtime
// joining the same KV, graceful-shutdown drains — use this.
func (dm *Domain) WaitForKVCaughtUp(ctx context.Context, timeout time.Duration) error {
	if dm.cache == nil {
		return fmt.Errorf("cache is not initialised")
	}
	deadline := time.Now().Add(timeout)
	consumerName := CommitterDurableName + "-" + dm.kv.Bucket()
	for {
		cachePending := dm.cache.HasPendingWrites()
		if err := dm.walTerminalError(); err != nil {
			return err
		}
		committerPending, err := dm.countPendingCommits(consumerName)
		if err != nil {
			return err
		}
		if !cachePending && committerPending == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("KV not caught up within %s (cache_pending=%v committer_pending=%d)",
				timeout, cachePending, committerPending)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// countPendingCommits reports how many WAL transactions have not reached KV
// yet. That is both the ones the committer has not been handed (NumPending)
// and the one it is holding right now (NumAckPending): the KV writes a
// transaction carries happen between delivery and Ack, so a transaction that
// is being applied is no longer "pending" to the server while being very much
// not in KV. Counting only the undelivered ones let WaitForKVCaughtUp report
// that everything was durable while the last transaction was still being
// written — whoever reloaded the cache from KV next was short its whole tail.
func (dm *Domain) countPendingCommits(consumerName string) (int, error) {
	info, err := dm.js.ConsumerInfo(WALCommitsStreamName, consumerName)
	if err != nil {
		return 0, fmt.Errorf("read WAL consumer %s: %w", consumerName, err)
	}

	pending := int(info.NumPending) + int(info.NumAckPending)
	lg.Logf(lg.TraceLevel, "Consumer %s has %d transactions not in KV yet (%d undelivered, %d being applied)",
		consumerName, pending, info.NumPending, info.NumAckPending)
	return pending, nil
}

func generateTransactionID() string {
	return fmt.Sprintf("%d", system.GetCurrentTimeNs())
}

// publishWALTransaction publishes a complete WAL transaction as a single message.
// The message body contains JSON-serialized ops. Headers carry tx_id and ops_count.
func (dm *Domain) publishWALTransaction(txID string, ops []cache.WALOp) error {
	subject := fmt.Sprintf("wal.commits.%s", dm.kv.Bucket())

	opsData, err := json.Marshal(ops)
	if err != nil {
		return fmt.Errorf("marshal WAL ops: %w", err)
	}

	msg := nats.NewMsg(subject)
	msg.Header.Set("tx_id", txID)
	msg.Header.Set("ops_count", strconv.Itoa(len(ops)))
	msg.Header.Set("commit_time", strconv.FormatInt(time.Now().UnixNano(), 10))
	msg.Data = opsData

	lg.Logf(lg.TraceLevel, "Publishing WAL tx=%s, ops=%d, subject=%s", txID, len(ops), subject)

	if _, err := dm.js.PublishMsg(msg); err != nil {
		lg.Logf(lg.ErrorLevel, "Failed to publish WAL transaction: %s", err)
		return err
	}

	// Duplicate to export commits stream for parallel export pipeline
	if dm.exportEnabled {
		exportSubject := fmt.Sprintf("wal.export.commits.%s", dm.kv.Bucket())
		exportMsg := nats.NewMsg(exportSubject)
		exportMsg.Header.Set("tx_id", txID)
		exportMsg.Header.Set("ops_count", strconv.Itoa(len(ops)))
		exportMsg.Header.Set("commit_time", msg.Header.Get("commit_time"))
		exportMsg.Data = opsData
		if _, err := dm.js.PublishMsg(exportMsg); err != nil {
			lg.Logf(lg.WarnLevel, "Failed to publish export WAL transaction (non-fatal): %s", err)
		}
	}

	return nil
}

// startExportCommitter runs a parallel pipeline that reads WAL transactions
// from the export-dedicated streams and publishes them to the export committer.
// It is independent of the KV apply pipeline (TransactionCommitter) and is not
// blocked by sequential KV Put latency.
func (dm *Domain) startExportCommitter(ctx context.Context) error {
	if dm.exportCommitter == nil {
		return nil
	}

	commitSubject := fmt.Sprintf("wal.export.commits.%s", dm.kv.Bucket())
	consumerName := ExportCommitterDurableName + "-" + dm.kv.Bucket()

	lg.Logf(lg.DebugLevel, "ExportCommitter pipeline starting for bucket=%s", dm.kv.Bucket())

	_, err := dm.js.AddConsumer(WALExportCommitsStreamName, &nats.ConsumerConfig{
		Name:           consumerName,
		Durable:        consumerName,
		DeliverSubject: consumerName,
		DeliverGroup:   consumerName + "-group",
		FilterSubject:  commitSubject,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        commitAckWait,
		MaxDeliver:     commitMaxDeliver,
		MaxAckPending:  1,
	})
	if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		return fmt.Errorf("ExportCommitter: failed to create consumer: %w", err)
	}

	_, err = dm.js.QueueSubscribe(
		commitSubject,
		consumerName+"-group",
		func(msg *nats.Msg) {
			txID := msg.Header.Get("tx_id")
			if txID == "" {
				system.MsgOnErrorReturn(msg.Ack())
				return
			}

			var ops []cache.WALOp
			if err := json.Unmarshal(msg.Data, &ops); err != nil {
				lg.Logf(lg.ErrorLevel, "ExportCommitter: failed to unmarshal ops for tx=%s: %s", txID, err)
				system.MsgOnErrorReturn(msg.Ack())
				return
			}

			lg.Logf(lg.TraceLevel, "ExportCommitter: processing tx_id=%s, ops=%d", txID, len(ops))

			if len(ops) > 0 {
				storePrefix := cache.KVStorePrefix
				if dm.cache != nil {
					storePrefix = dm.cache.GetStorePrefix()
				}
				// Convert cache.WALOp to statefun.WALOp for export committer
				exportOps := make([]WALOp, len(ops))
				for i, op := range ops {
					exportOps[i] = WALOp{OpType: op.OpType, Key: op.Key, Value: op.Value}
				}
				if err := dm.exportCommitter.ProcessTransaction(txID, exportOps, storePrefix); err != nil {
					lg.Logf(lg.WarnLevel, "ExportCommitter: ProcessTransaction failed for tx=%s (non-fatal): %s", txID, err)
				}
			}

			system.MsgOnErrorReturn(msg.Ack())
		},
		nats.Bind(WALExportCommitsStreamName, consumerName),
		nats.ManualAck(),
	)
	if err != nil {
		return fmt.Errorf("ExportCommitter: failed to subscribe: %w", err)
	}

	lg.Logf(lg.DebugLevel, "ExportCommitter pipeline started")
	return nil
}
