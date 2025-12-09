package statefun

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	WALOperationsSubject    = "wal.ops.%s.%s"
	WALCommitsSubject       = "wal.commits.%s"
	CommitterDurableName    = "TRANSACTION_COMMITTER_CONSUMER"
)

func (dm *Domain) TransactionCommitter(ctx context.Context) error {
	go func() {
		if err := dm.runTransactionCommitter(ctx); err != nil {
			lg.GetLogger().Debugf(ctx, "TransactionCommitter error: %s", err)
		}
	}()
	return nil
}

func (dm *Domain) runTransactionCommitter(ctx context.Context) error {
	commitSubject := fmt.Sprintf(WALCommitsSubject, dm.kv.Bucket())
	consumerName := CommitterDurableName + "-" + dm.kv.Bucket()

	lg.GetLogger().Debugf(ctx, "TransactionCommitter starting for bucket=%s, subject=%s", dm.kv.Bucket(), commitSubject)

	if err := dm.setKVConsistent(false); err != nil {
		lg.GetLogger().Debugf(ctx, "Failed to set KV as inconsistent: %s", err)
		return err
	}
	lg.GetLogger().Debugf(ctx, "KV marked as inconsistent, will process pending transactions")

	processedTxs := sync.Map{}

	_, err := dm.js.AddConsumer(WALCommitsStreamName, &nats.ConsumerConfig{
		Name:           consumerName,
		Durable:        consumerName,
		DeliverSubject: consumerName,
		DeliverGroup:   consumerName + "-group",
		FilterSubject:  commitSubject,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxDeliver:     5,
	})
	if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		lg.GetLogger().Debugf(ctx, "TransactionCommitter failed to create consumer: %s", err)
		return err
	}

	lg.GetLogger().Debugf(ctx, "TransactionCommitter consumer created/exists: %s", consumerName)

	pendingCount := dm.countPendingCommits(consumerName)
	lg.GetLogger().Debugf(ctx, "Found %d pending transactions to process", pendingCount)

	if pendingCount == 0 {
		lg.GetLogger().Debugf(ctx, "No pending transactions, marking KV as consistent immediately")
		if err = dm.setKVConsistent(true); err != nil {
			lg.GetLogger().Debugf(ctx, "Failed to set KV as consistent: %s", err)
			return err
		}
	}

	processedCount := 0
	processingTxs := sync.Map{}

	_, err = dm.js.QueueSubscribe(
		commitSubject,
		consumerName+"-group",
		func(msg *nats.Msg) {
			txID := msg.Header.Get("tx_id")
			if txID == "" {
				lg.GetLogger().Debugf(ctx, "TransactionCommitter: received commit without tx_id")
				system.MsgOnErrorReturn(msg.Ack())
				return
			}

			if _, processing := processingTxs.LoadOrStore(txID, true); processing {
				lg.GetLogger().Debugf(ctx, "TransactionCommitter: transaction %s is already being processed, ignoring duplicate", txID)
				return
			}
			defer processingTxs.Delete(txID)

			if _, processed := processedTxs.Load(txID); processed {
				lg.GetLogger().Debugf(ctx, "TransactionCommitter: transaction %s already processed, Ack() final", txID)
				system.MsgOnErrorReturn(msg.Ack())
				processedTxs.Delete(txID)

				processedCount++
				if pendingCount > 0 && processedCount >= pendingCount {
					lg.GetLogger().Debugf(ctx, "All %d pending transactions processed, marking KV as consistent", pendingCount)
					if err := dm.setKVConsistent(true); err != nil {
						lg.GetLogger().Debugf(ctx, "Failed to set KV as consistent: %s", err)
					}
					pendingCount = 0
				}

				return
			}

			lg.GetLogger().Debugf(ctx, "TransactionCommitter: first time processing commit for tx_id=%s, Nak()", txID)
			system.MsgOnErrorReturn(msg.Nak())

			if err = dm.applyTransactionOperations(ctx, txID); err != nil {
				lg.GetLogger().Debugf(ctx, "TransactionCommitter: failed to apply transaction %s: %s", txID, err)
				return
			}

			lg.GetLogger().Debugf(ctx, "TransactionCommitter: successfully applied transaction %s, marked as processed", txID)
			processedTxs.Store(txID, true)
		},
		nats.Bind(WALCommitsStreamName, consumerName),
		nats.ManualAck(),
	)

	if err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

func (dm *Domain) applyTransactionOperations(ctx context.Context, txID string) error {
	opsSubject := fmt.Sprintf(WALOperationsSubject, dm.kv.Bucket(), txID)
	consumerName := "TX_OPS_" + dm.kv.Bucket() + "_" + txID

	lg.GetLogger().Debugf(ctx, "applyTransactionOperations: processing tx_id=%s, subject=%s", txID, opsSubject)

	info, err := dm.js.ConsumerInfo(WALOperationsStreamName, consumerName)
	if err != nil || info == nil {
		lg.GetLogger().Debugf(ctx, "Consumer %s not found, creating new one", consumerName)

		consumerConfig := &nats.ConsumerConfig{
			Name:          consumerName,
			Durable:       consumerName,
			FilterSubject: opsSubject,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       10 * time.Second,
			MaxDeliver:    3,
		}

		if _, err = dm.js.AddConsumer(WALOperationsStreamName, consumerConfig); err != nil {
			lg.GetLogger().Debugf(ctx, "Failed to create consumer: %s", err)
			return fmt.Errorf("failed to create operations consumer: %w", err)
		}
		lg.GetLogger().Debugf(ctx, "Created durable consumer %s", consumerName)
	} else {
		lg.GetLogger().Debugf(ctx, "Reusing existing consumer %s", consumerName)
	}

	sub, err := dm.js.PullSubscribe(opsSubject, consumerName)
	if err != nil {
		lg.GetLogger().Debugf(ctx, "Failed to create subscription: %s", err)
		return fmt.Errorf("failed to subscribe to operations: %w", err)
	}
	lg.GetLogger().Debugf(ctx, "Created subscription for consumer %s, checking if valid...", consumerName)

	if !sub.IsValid() {
		lg.GetLogger().Debugf(ctx, "Subscription is INVALID immediately after creation!")
		return fmt.Errorf("subscription invalid after creation")
	}
	lg.GetLogger().Debugf(ctx, "Subscription is valid, proceeding with Fetch")

	defer func() {
		lg.GetLogger().Debugf(ctx, "Unsubscribing from consumer %s", consumerName)
		system.MsgOnErrorReturn(sub.Unsubscribe())
	}()

	totalOps := 0

	for {
		lg.GetLogger().Debugf(ctx, "Attempting to Fetch up to 100 messages, subscription valid=%v", sub.IsValid())
		msgs, err := sub.Fetch(100, nats.MaxWait(1*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				lg.GetLogger().Debugf(ctx, "applyTransactionOperations: finished, processed %d operations for tx_id=%s", totalOps, txID)
				break
			}
			return fmt.Errorf("failed to fetch operations: %w", err)
		}

		if len(msgs) == 0 {
			lg.GetLogger().Debugf(ctx, "applyTransactionOperations: no more messages, processed %d operations for tx_id=%s", totalOps, txID)
			break
		}

		lg.GetLogger().Debugf(ctx, "applyTransactionOperations: fetched %d operations for tx_id=%s", len(msgs), txID)

		for _, msg := range msgs {
			opType := msg.Header.Get("op_type")
			key := msg.Header.Get("key")

			if key == "" {
				lg.GetLogger().Debugf(ctx, "Operation without key in transaction %s", txID)
				system.MsgOnErrorReturn(msg.Ack())
				continue
			}

			var kvErr error
			switch opType {
			case cache.OpTypePUT:
				lg.GetLogger().Debugf(ctx, "Applying PUT for key=%s in tx=%s", key, txID)
				_, kvErr = customNatsKv.KVPut(dm.js, dm.kv, key, msg.Data)
			case cache.OpTypeDelete:
				lg.GetLogger().Debugf(ctx, "Applying DELETE for key=%s in tx=%s", key, txID)
				kvErr = customNatsKv.KVDelete(dm.js, dm.kv, key)
				if kvErr != nil {
					errMsg := kvErr.Error()
					if errors.Is(kvErr, nats.ErrKeyNotFound) ||
						strings.Contains(errMsg, "message not found") ||
						strings.Contains(errMsg, "key not found") {
						lg.GetLogger().Debugf(ctx, "Key %s already deleted, ignoring error: %s", key, errMsg)
						kvErr = nil
					}
				}
			default:
				lg.GetLogger().Debugf(ctx, "Unknown operation type %s in transaction %s", opType, txID)
				system.MsgOnErrorReturn(msg.Ack())
				continue
			}

			if kvErr != nil {
				lg.GetLogger().Debugf(ctx, "Failed to apply operation to KV: %s", kvErr)
				system.MsgOnErrorReturn(msg.Nak())
				return fmt.Errorf("failed to apply operation: %w", kvErr)
			}

			totalOps++
			system.MsgOnErrorReturn(msg.Ack())
		}
	}

	lg.GetLogger().Debugf(ctx, "applyTransactionOperations: all operations processed, deleting consumer %s", consumerName)
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
		lg.GetLogger().Debugf(context.TODO(), "Failed to get consumer info: %s", err)
		return 0
	}

	pending := int(info.NumPending)
	lg.GetLogger().Debugf(context.TODO(), "Consumer %s has %d pending messages", consumerName, pending)
	return pending
}

func GenerateTransactionID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (dm *Domain) publishWALOperation(txID string, opTime int64, opType cache.OpType, key string, value []byte) error {
	subject := fmt.Sprintf(WALOperationsSubject, dm.kv.Bucket(), txID)

	msg := nats.NewMsg(subject)
	msg.Header.Set("tx_id", txID)
	msg.Header.Set("op_time", strconv.FormatInt(opTime, 10))
	msg.Header.Set("op_type", opType)
	msg.Header.Set("key", key)
	msg.Data = value

	lg.GetLogger().Debugf(context.TODO(), "::::::::::Publishing WAL operation: tx=%s, type=%s, key=%s, subject=%s", txID, opType, key, subject)

	if _, err := dm.js.PublishMsg(msg); err != nil {
		lg.GetLogger().Errorf(context.TODO(), "Failed to publish WAL operation: %s", err)
		return err
	}
	return nil
}

func (dm *Domain) publishWALCommit(txID string) error {
	subject := fmt.Sprintf(WALCommitsSubject, dm.kv.Bucket())

	msg := nats.NewMsg(subject)
	msg.Header.Set("tx_id", txID)
	msg.Header.Set("commit_time", strconv.FormatInt(time.Now().UnixNano(), 10))

	lg.GetLogger().Debugf(context.TODO(), "::::::::::Publishing WAL commit: tx=%s, subject=%s", txID, subject)

	if _, err := dm.js.PublishMsg(msg); err != nil {
		lg.GetLogger().Errorf(context.TODO(), "::::::::::Failed to publish WAL commit: %s", err)
		return err
	}
	return nil
}
