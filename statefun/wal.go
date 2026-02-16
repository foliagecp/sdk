package statefun

import (
	"context"
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
	WALOperationsStreamName = "wal_operations"
	WALCommitsStreamName    = "wal_commits"
	WALOperationsSubject    = "wal.ops.*.*"
	WALCommitsSubject       = "wal.commits.*"
	CommitterDurableName    = "TRANSACTION_COMMITTER_CONSUMER"

	// --- Commit consumer (TransactionCommitter) ---

	// How long NATS waits for Ack before redelivering a commit message.
	commitAckWait = 5 * time.Second

	// How many times NATS retries an unacked commit before sending to DLQ.
	commitMaxDeliver = 5

	// --- Operations consumer (per-transaction) ---

	// How long NATS waits for Ack on each operation batch.
	opsAckWait = 10 * time.Second

	// Fewer retries than commits: if a single op fails 3 times, the KV is likely
	// unavailable and the whole transaction will be retried via commit redelivery.
	opsMaxDeliver = 3

	// Number of operation messages to fetch per pull batch.
	// 100 balances throughput (fewer round-trips) vs memory (buffered messages).
	opsFetchBatchSize = 100

	// How long to wait for a batch before returning what's available.
	opsFetchTimeout = 1 * time.Second

	// --- Shutdown drain ---

	// How often to poll for remaining pending commits during shutdown.
	shutdownPollInterval = 200 * time.Millisecond

	// How often to log "still waiting" during shutdown drain.
	shutdownLogInterval = 5 * time.Second

	// Max time to wait for pending transactions during shutdown.
	// After this, shutdown proceeds with data loss warning.
	shutdownMaxWait = 30 * time.Second
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

			lg.Logf(lg.TraceLevel, "TransactionCommitter: processing commit for tx_id=%s", txID)

			// Backup barrier pause
			for dm.isBackupBarrierActive() {
				lg.Logln(lg.WarnLevel, "Backup barrier active, pausing commit apply")
				time.Sleep(200 * time.Millisecond)
			}

			if err := dm.applyTransactionOperations(ctx, txID); err != nil {
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

func (dm *Domain) applyTransactionOperations(ctx context.Context, txID string) error {
	opsSubject := fmt.Sprintf("wal.ops.%s.%s", dm.kv.Bucket(), txID)
	consumerName := "TX_OPS_" + dm.kv.Bucket() + "_" + txID

	lg.Logf(lg.TraceLevel, "applyTransactionOperations: processing tx_id=%s, subject=%s", txID, opsSubject)

	info, err := dm.js.ConsumerInfo(WALOperationsStreamName, consumerName)
	if err != nil || info == nil {
		lg.Logf(lg.TraceLevel, "Consumer %s not found, creating new one", consumerName)

		consumerConfig := &nats.ConsumerConfig{
			Name:          consumerName,
			Durable:       consumerName,
			FilterSubject: opsSubject,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       opsAckWait,
			MaxDeliver:    opsMaxDeliver,
		}

		if _, err = dm.js.AddConsumer(WALOperationsStreamName, consumerConfig); err != nil {
			lg.Logf(lg.ErrorLevel, "Failed to create consumer: %s", err)
			return fmt.Errorf("failed to create operations consumer: %w", err)
		}
		lg.Logf(lg.TraceLevel, "Created durable consumer %s", consumerName)
	} else {
		lg.Logf(lg.TraceLevel, "Reusing existing consumer %s", consumerName)
	}

	sub, err := dm.js.PullSubscribe(opsSubject, consumerName)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Failed to create subscription: %s", err)
		return fmt.Errorf("failed to subscribe to operations: %w", err)
	}

	if !sub.IsValid() {
		return fmt.Errorf("subscription invalid after creation")
	}

	defer func() {
		system.MsgOnErrorReturn(sub.Unsubscribe())
	}()

	totalOps := 0

	for {
		msgs, err := sub.Fetch(opsFetchBatchSize, nats.MaxWait(opsFetchTimeout))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				lg.Logf(lg.TraceLevel, "applyTransactionOperations: finished, processed %d operations for tx_id=%s", totalOps, txID)
				break
			}
			return fmt.Errorf("failed to fetch operations: %w", err)
		}

		if len(msgs) == 0 {
			lg.Logf(lg.TraceLevel, "applyTransactionOperations: no more messages, processed %d operations for tx_id=%s", totalOps, txID)
			break
		}

		//lg.GetLogger().Tracef(ctx, "applyTransactionOperations: fetched %d operations for tx_id=%s", len(msgs), txID)

		for _, msg := range msgs {
			opType := msg.Header.Get("op_type")
			key := msg.Header.Get("key")

			if key == "" {
				lg.Logf(lg.WarnLevel, "Operation without key in transaction %s", txID)
				system.MsgOnErrorReturn(msg.Ack())
				continue
			}

			var kvErr error
			switch opType {
			case cache.OpTypePUT:
				_, kvErr = customNatsKv.KVPut(dm.js, dm.kv, key, msg.Data)
			case cache.OpTypeDelete:
				kvErr = customNatsKv.KVDelete(dm.js, dm.kv, key)
				if kvErr != nil {
					errMsg := kvErr.Error()
					if errors.Is(kvErr, nats.ErrKeyNotFound) ||
						strings.Contains(errMsg, "message not found") ||
						strings.Contains(errMsg, "key not found") {
						kvErr = nil
					}
				}
			default:
				lg.Logf(lg.TraceLevel, "Unknown operation type %s in transaction %s", opType, txID)
				system.MsgOnErrorReturn(msg.Ack())
				continue
			}

			if kvErr != nil {
				lg.Logf(lg.ErrorLevel, "Failed to apply operation to KV: %s", kvErr)
				system.MsgOnErrorReturn(msg.Nak())
				return fmt.Errorf("failed to apply operation: %w", kvErr)
			}

			totalOps++
			system.MsgOnErrorReturn(msg.Ack())
		}
	}

	lg.Logf(lg.TraceLevel, "applyTransactionOperations: all operations processed, deleting consumer %s", consumerName)
	system.MsgOnErrorReturn(dm.js.DeleteConsumer(WALOperationsStreamName, consumerName))

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

func (dm *Domain) publishWALOperation(txID string, opTime int64, opType cache.OpType, key string, value []byte) error {
	subject := fmt.Sprintf("wal.ops.%s.%s", dm.kv.Bucket(), txID)

	msg := nats.NewMsg(subject)
	msg.Header.Set("tx_id", txID)
	msg.Header.Set("op_time", strconv.FormatInt(opTime, 10))
	msg.Header.Set("op_type", opType)
	msg.Header.Set("key", key)
	msg.Data = value

	if _, err := dm.js.PublishMsg(msg); err != nil {
		lg.Logf(lg.ErrorLevel, "Failed to publish WAL operation: %s", err)
		return err
	}
	return nil
}

func (dm *Domain) publishWALCommit(txID string) error {
	subject := fmt.Sprintf("wal.commits.%s", dm.kv.Bucket())

	msg := nats.NewMsg(subject)
	msg.Header.Set("tx_id", txID)
	msg.Header.Set("commit_time", strconv.FormatInt(time.Now().UnixNano(), 10))

	lg.Logf(lg.TraceLevel, "Publishing WAL commit: tx=%s, subject=%s", txID, subject)

	if _, err := dm.js.PublishMsg(msg); err != nil {
		lg.Logf(lg.ErrorLevel, "Failed to publish WAL commit: %s", err)
		return err
	}
	return nil
}
