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

var errTransactionAlreadyApplied = fmt.Errorf("transaction already applied by another runtime")

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

	_, err := dm.js.AddConsumer(WALCommitsStreamName, &nats.ConsumerConfig{
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

// applyTransactionOps applies WAL operations from the transaction message body to KV store.
func (dm *Domain) applyTransactionOps(ops []cache.WALOp, txID string) error {
	for _, op := range ops {
		if op.Key == "" {
			continue
		}
		var kvErr error
		switch op.OpType {
		case cache.OpTypePUT:
			_, kvErr = customNatsKv.KVPut(dm.js, dm.kv, op.Key, op.Value)
		case cache.OpTypeDelete:
			kvErr = customNatsKv.KVDelete(dm.js, dm.kv, op.Key)
			if kvErr != nil {
				errMsg := kvErr.Error()
				if errors.Is(kvErr, nats.ErrKeyNotFound) ||
					strings.Contains(errMsg, "message not found") ||
					strings.Contains(errMsg, "key not found") {
					kvErr = nil
				}
			}
		default:
			continue
		}
		if kvErr != nil {
			return fmt.Errorf("KV apply failed for key=%s: %w", op.Key, kvErr)
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

