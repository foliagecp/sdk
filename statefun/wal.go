package statefun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

const (
	WALCommitsStreamName    = "wal_commits"
	WALCommitsSubject       = "wal.commits.*"
	CommitterDurableName    = "TRANSACTION_COMMITTER_CONSUMER"

	// Export-dedicated stream — parallel pipeline for export committer.
	WALExportCommitsStreamName = "wal_export_commits"
	WALExportCommitsSubject   = "wal.export.commits.*"
	ExportCommitterDurableName = "EXPORT_COMMITTER_CONSUMER"

	// --- Transaction consumer ---
	commitAckWait    = 5 * time.Second
	commitMaxDeliver = 5

	// --- Shutdown drain ---
	shutdownPollInterval = 200 * time.Millisecond
	shutdownLogInterval  = 5 * time.Second
	shutdownMaxWait      = 30 * time.Second

	// --- applyTransactionOps pipelining ---
	//
	// applyTransactionOpsChunkSize bounds how many WAL ops we
	// pipeline through PublishAsync simultaneously before we wait
	// for their PubAcks. The NATS Go client default for
	// PublishAsyncMaxPending is 4000; we deliberately stay under
	// that with safety room for other publishers on the same
	// JetStream context (publishWALTransaction, ExportCommitter,
	// callers' own KVPut). Picking too low penalises latency by
	// adding extra wait barriers per transaction; picking too high
	// risks "stalled" errors from nats.go when the pool fills.
	//
	// 1024 is a measured sweet spot: well below the global cap,
	// large enough that for typical WAL transactions (10–500 ops)
	// the whole transaction goes through in a single chunk.
	applyTransactionOpsChunkSize = 1024

	// applyTransactionOpsAckTimeout caps the wait for each chunk's
	// PubAcks. Hitting this means NATS is unhealthy and the WAL
	// message will be redelivered — applyTransactionOps is
	// idempotent, so the retry will reapply the chunk cleanly.
	applyTransactionOpsAckTimeout = 30 * time.Second
)

func (dm *Domain) TransactionCommitter(ctx context.Context) error {
	ready := make(chan struct{})
	go func() {
		if err := dm.runTransactionCommitter(ctx, ready); err != nil {
			lg.Logf(lg.ErrorLevel, "TransactionCommitter error: %s", err)
		}
	}()
	<-ready
	return nil
}

func (dm *Domain) runTransactionCommitter(ctx context.Context, ready chan struct{}) error {
	dm.shutdown = make(chan struct{})
	defer close(dm.shutdown)

	commitSubject := fmt.Sprintf("wal.commits.%s", dm.kv.Bucket())
	consumerName := CommitterDurableName + "-" + dm.kv.Bucket()

	lg.Logf(lg.TraceLevel, "TransactionCommitter starting for bucket=%s, subject=%s", dm.kv.Bucket(), commitSubject)

	if err := dm.setKVConsistent(false); err != nil {
		return err
	}
	lg.Logln(lg.TraceLevel, "KV marked as inconsistent, will process pending transactions")

	// Retry on transient cluster-formation errors; an already-existing consumer
	// is success.
	err := retryStartupJS(context.Background(), "WAL consumer "+consumerName, func() error {
		_, e := dm.js.AddConsumer(WALCommitsStreamName, &nats.ConsumerConfig{
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
		if e != nil && !errors.Is(e, nats.ErrConsumerNameAlreadyInUse) {
			return e
		}
		return nil
	})
	if err != nil {
		lg.Logf(lg.ErrorLevel, "TransactionCommitter failed to create consumer: %s", err)
		return err
	}

	lg.Logf(lg.TraceLevel, "TransactionCommitter consumer created/exists: %s", consumerName)

	pendingCount := dm.countPendingCommits(consumerName)
	lg.Logf(lg.TraceLevel, "Found %d pending transactions to process", pendingCount)

	if pendingCount == 0 {
		lg.Logln(lg.TraceLevel, "No pending transactions, marking KV as consistent immediately")
		if err = dm.setKVConsistent(true); err != nil {
			lg.Logf(lg.ErrorLevel, "Failed to set KV as consistent: %s", err)
			return err
		}
	}

	// Safe without mutex: MaxAckPending=1 guarantees sequential processing
	processedCount := 0

	_, err = dm.js.QueueSubscribe(
		commitSubject,
		consumerName+"-group",
		func(msg *nats.Msg) {
			txID := msg.Header.Get("tx_id")
			if txID == "" {
				lg.Logln(lg.ErrorLevel, "TransactionCommitter: received commit without tx_id")
				system.MsgOnErrorReturn(msg.Ack())
				return
			}

			// Parse ops from message body
			var ops []cache.WALOp
			if err := json.Unmarshal(msg.Data, &ops); err != nil {
				lg.Logf(lg.ErrorLevel, "TransactionCommitter: failed to unmarshal ops for tx=%s: %s", txID, err)
				system.MsgOnErrorReturn(msg.Ack())
				return
			}

			lg.Logf(lg.TraceLevel, "TransactionCommitter: processing tx_id=%s, ops=%d", txID, len(ops))

			// Backup barrier pause
			for dm.isBackupBarrierActive() {
				lg.Logln(lg.WarnLevel, "Backup barrier active, pausing commit apply")
				time.Sleep(200 * time.Millisecond)
			}

			if err := dm.applyTransactionOps(ops, txID); err != nil {
				lg.Logf(lg.ErrorLevel, "TransactionCommitter: failed to apply transaction %s: %s", txID, err)
				system.MsgOnErrorReturn(msg.Nak())
				return
			}

			// Advance the cache's "last committed tx" watermark. The
			// backup-write-barrier readiness check uses this watermark
			// instead of the old per-node syncedWithKV scan: a barrier set
			// at time B is ready once the watermark ≥ B (i.e. every WAL
			// transaction up to B has landed in KV). txID is the int64 tx
			// timestamp formatted by cache.kvLazyWriter.
			if txTime, perr := strconv.ParseInt(txID, 10, 64); perr == nil {
				dm.cache.RecordCommittedTx(txTime)
			}

			lg.Logf(lg.TraceLevel, "TransactionCommitter: transaction %s completed, Ack()", txID)
			system.MsgOnErrorReturn(msg.Ack())

			processedCount++
			if pendingCount > 0 && processedCount >= pendingCount {
				lg.Logf(lg.DebugLevel, "TransactionCommitter: all %d pending transactions processed, marking KV as consistent", pendingCount)
				if err = dm.setKVConsistent(true); err != nil {
					lg.Logf(lg.ErrorLevel, "TransactionCommitter: failed to set KV as consistent: %s", err)
				}
				pendingCount = 0
			}
		},
		nats.Bind(WALCommitsStreamName, consumerName),
		nats.ManualAck(),
	)

	if err != nil {
		return err
	}

	close(ready)

	<-dm.cache.Synced

	if err = dm.setKVConsistent(false); err != nil {
		lg.Logf(lg.ErrorLevel, "Failed to set KV as inconsistent: %s", err)
	}

	lg.Logf(lg.TraceLevel, "Cache synced, recounting pending transactions for shutdown")

	finalPendingCount := dm.countPendingCommits(consumerName)
	lg.Logf(lg.TraceLevel, "Final pending transactions count: %d", finalPendingCount)

	if finalPendingCount == 0 {
		lg.Logf(lg.TraceLevel, "No final pending transactions, shutdown complete")
		return nil
	}

	startWait := time.Now()
	lastLogTime := startWait

	for {
		currentPending := dm.countPendingCommits(consumerName)

		if time.Since(lastLogTime) >= shutdownLogInterval {
			lg.Logf(lg.TraceLevel, "Shutdown: waiting for transactions, pending=%d, elapsed=%v",
				currentPending, time.Since(startWait).Round(time.Second))
			lastLogTime = time.Now()
		}

		if currentPending == 0 {
			lg.Logf(lg.TraceLevel, "All transactions processed in %v, shutdown complete",
				time.Since(startWait).Round(time.Millisecond))
			return nil
		}

		if time.Since(startWait) > shutdownMaxWait {
			lg.Logf(lg.WarnLevel, "Shutdown timeout reached after %v with %d pending transactions",
				shutdownMaxWait, currentPending)
			return nil
		}

		time.Sleep(shutdownPollInterval)
	}
}

// pendingKVOp is one in-flight async KV write whose PubAck is pending.
// We carry the original op for diagnostics; the WAL message will be
// redelivered (and reapplied idempotently) on any error.
type pendingKVOp struct {
	key    string
	opType string
	future nats.PubAckFuture
}

// applyTransactionOps applies a batch of WAL ops to the underlying
// JetStream KV store, pipelining the publishes through PublishAsync
// instead of paying one synchronous round-trip per op.
//
// Atomicity model — unchanged from the pre-pipeline implementation:
//
//   - JetStream provides no multi-key atomic write API. Each op is a
//     standalone publish (PUT) or DEL tombstone publish.
//
//   - Atomicity is achieved at the WAL layer: we Ack the WAL
//     transaction message only after every op in the batch has been
//     acknowledged by the server. If any op fails or times out, we
//     return error → WAL message is NOT Acked → JetStream redelivers
//     → applyTransactionOps reruns the whole batch. Per-op reapply
//     is idempotent (PUT overwrites the same value; DEL is a no-op
//     against an already-deleted key).
//
//   - Per-op ordering within a single key inside a single
//     transaction: typical WAL ops touch each key at most once per
//     transaction (cache.SetValueJSON ↔ publishDirtyOp), so reorder
//     between two writes to the same key from the same transaction is
//     not a concern. If callers begin to write the same key multiple
//     times within one transaction, that needs separate consideration
//     (sequential per-key publish, or merge before publish).
//
// Pipelining mechanics:
//
//   - We issue up to applyTransactionOpsChunkSize PublishAsync calls
//     before waiting for any acks. This stays well under the
//     PublishAsyncMaxPending cap (default 4000) shared by every
//     publisher on the JetStream context — so we don't starve
//     concurrent subsystems (WAL publisher, ExportCommitter).
//
//   - After issuing a chunk we drain it via per-future select on
//     Ok()/Err()/timeout. We deliberately do NOT use
//     PublishAsyncComplete here because it waits for ALL outstanding
//     futures on the JS context, including ones owned by other
//     subsystems.
//
//   - A "stalled" error from PublishAsync (pool exhausted by
//     concurrent publishers) is propagated as a normal error → WAL
//     redelivery → retry. Self-healing.
//
// Expected throughput improvement vs the sequential KVPut loop:
// roughly 5–10× on a local NATS server (RT ≈ 200µs, chunk of 1024
// publishes shares one pipeline), and 15–25× on a remote NATS
// (RT ≈ 5ms+).
func (dm *Domain) applyTransactionOps(ops []cache.WALOp, txID string) error {
	for start := 0; start < len(ops); start += applyTransactionOpsChunkSize {
		end := start + applyTransactionOpsChunkSize
		if end > len(ops) {
			end = len(ops)
		}

		pending := make([]pendingKVOp, 0, end-start)
		for _, op := range ops[start:end] {
			if op.Key == "" {
				continue
			}
			switch op.OpType {
			case cache.OpTypePUT:
				f, err := customNatsKv.KVPutAsync(dm.js, dm.kv, op.Key, op.Value)
				if err != nil {
					return fmt.Errorf("KV apply (publish PUT) failed for key=%s in tx=%s: %w", op.Key, txID, err)
				}
				pending = append(pending, pendingKVOp{key: op.Key, opType: op.OpType, future: f})

			case cache.OpTypeDelete:
				f, err := customNatsKv.KVDeleteAsync(dm.js, dm.kv, op.Key)
				if err != nil {
					return fmt.Errorf("KV apply (publish DEL) failed for key=%s in tx=%s: %w", op.Key, txID, err)
				}
				pending = append(pending, pendingKVOp{key: op.Key, opType: op.OpType, future: f})

			default:
				continue
			}
		}

		// Drain the chunk's futures. Cap the wait globally per chunk
		// so a stuck NATS does not block the consumer forever — WAL
		// redelivery kicks in and reapplies on the next tick.
		deadline := time.After(applyTransactionOpsAckTimeout)
		for i, p := range pending {
			select {
			case <-p.future.Ok():
				// happy path

			case err := <-p.future.Err():
				// DEL tombstone publish never reports "not found":
				// the server accepts the message regardless of prior
				// state. So unlike the old SecureDeleteMsg path, we
				// do not swallow "key not found" / "message not
				// found" here — any error is a real publish failure
				// and must trigger redelivery.
				return fmt.Errorf("KV apply ack failed in tx=%s at op %d (key=%s, type=%s): %w", txID, start+i, p.key, p.opType, err)

			case <-deadline:
				return fmt.Errorf("KV apply ack timed out (%s) in tx=%s at op %d (key=%s, type=%s, pool may be saturated)",
					applyTransactionOpsAckTimeout, txID, start+i, p.key, p.opType)
			}
		}
	}

	return nil
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
		committerPending := dm.countPendingCommits(consumerName)
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

func (dm *Domain) countPendingCommits(consumerName string) int {
	info, err := dm.js.ConsumerInfo(WALCommitsStreamName, consumerName)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Failed to get consumer info: %s", err)
		return 0
	}

	pending := int(info.NumPending)
	lg.Logf(lg.TraceLevel, "Consumer %s has %d pending messages", consumerName, pending)
	return pending
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

