package statefun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/nats-io/nats.go"
)

// ErrWALRecovery means that replay cannot establish a consistent KV state.
// It never authorizes skipping a transaction, purging data, or resetting a domain.
var ErrWALRecovery = errors.New("WAL recovery cannot establish consistency")

const (
	walReceiptPrefix = "__wal_applied__."
	// Retain a bounded recovery window, not an ever-growing transaction history.
	// A consumer rolled back beyond that window fails closed rather than guessing.
	walReceiptRetain        = 4096
	walReceiptSweepInterval = time.Minute
)

type walApplyCursor struct {
	sequence  uint64
	completed int
}

// Only a new completed prefix counts as progress. Reapplying the same prefix
// after a later operation keeps failing must not extend recovery indefinitely.
func (dm *Domain) recordWALApplyProgress(sequence uint64, completed int) {
	for {
		previous := dm.walApply.Load()
		if previous != nil && (previous.sequence > sequence || (previous.sequence == sequence && previous.completed >= completed)) {
			return
		}
		next := &walApplyCursor{sequence: sequence, completed: completed}
		if dm.walApply.CompareAndSwap(previous, next) {
			dm.walProgress.Add(1)
			return
		}
	}
}

type walFailure struct{ err error }

type walAppliedDelete struct {
	Key      string `json:"key"`
	Revision uint64 `json:"revision"`
}

type walReceipt struct {
	Version       int                `json:"version"`
	StreamCreated time.Time          `json:"stream_created"`
	KVCreated     time.Time          `json:"kv_created"`
	Bucket        string             `json:"bucket"`
	Consumer      string             `json:"consumer"`
	Sequence      uint64             `json:"stream_seq"`
	TxID          string             `json:"tx_id"`
	Digest        string             `json:"sha256"`
	State         string             `json:"state"`
	AckSubject    string             `json:"ack_subject"`
	Deletes       []walAppliedDelete `json:"deletes,omitempty"`
	revision      uint64
}

func walDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func (dm *Domain) walReceiptGeneration() string {
	return walDigest([]byte(dm.walCreated.UTC().Format(time.RFC3339Nano)))[:16]
}
func (dm *Domain) walReceiptKey(seq uint64) string {
	return walReceiptPrefix + dm.walReceiptGeneration() + "." + strconv.FormatUint(seq, 10)
}
func (dm *Domain) walTerminalError() error {
	if p := dm.walFailure.Load(); p != nil {
		return p.err
	}
	return nil
}

func (dm *Domain) prepareWALConsumer(ctx context.Context) error {
	stream, err := dm.js.StreamInfo(WALCommitsStreamName, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("read WAL stream: %w", err)
	}
	if stream.Config.Retention != nats.WorkQueuePolicy {
		return fmt.Errorf("%w: WAL stream requires work-queue retention", ErrWALRecovery)
	}
	dm.walCreated = stream.Created
	dm.walKVStream = customNatsKv.KVStreamName(dm.kv)
	dm.walKVSubjectPrefix = customNatsKv.KVStoredSubject(dm.kv, "")
	kvInfo, err := dm.js.StreamInfo(dm.walKVStream, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("read KV stream generation: %w", err)
	}
	dm.walKVCreated = kvInfo.Created
	consumer := CommitterDurableName + "-" + dm.kv.Bucket()
	subject := "wal.commits." + dm.kv.Bucket()
	return retryStartupJS(ctx, "WAL consumer "+consumer, func() error {
		info, err := dm.js.ConsumerInfo(WALCommitsStreamName, consumer, nats.Context(ctx))
		if errors.Is(err, nats.ErrConsumerNotFound) {
			_, err = dm.js.AddConsumer(WALCommitsStreamName, &nats.ConsumerConfig{
				Name: consumer, Durable: consumer, DeliverSubject: consumer, DeliverGroup: consumer + "-group",
				FilterSubject: subject, AckPolicy: nats.AckExplicitPolicy, AckWait: commitAckWait,
				MaxDeliver: -1, MaxAckPending: 1,
			}, nats.Context(ctx))
			return err
		}
		if err != nil {
			return err
		}
		c := info.Config
		if c.AckPolicy != nats.AckExplicitPolicy || c.MaxAckPending != 1 || c.FilterSubject != subject || len(c.FilterSubjects) > 0 || c.DeliverSubject != consumer || c.DeliverGroup != consumer+"-group" {
			return fmt.Errorf("%w: incompatible WAL consumer %q; expected one explicit-ack transaction for bucket %q", ErrWALRecovery, consumer, dm.kv.Bucket())
		}
		// Exhausting delivery attempts can strand a still-retained WAL record outside
		// both pending counters. Persisted consumer position must never be reset here.
		if c.MaxDeliver != -1 {
			c.MaxDeliver = -1
			_, err = dm.js.UpdateConsumer(WALCommitsStreamName, &c, nats.Context(ctx))
		}
		return err
	})
}

// Proof and retention-boundary reads must go through the stream leader. A
// normal KV Get can use a replica's direct-get path, whose stale absence could
// discard valid proof or miss a pruning boundary and replay an old write.
func (dm *Domain) readWALKV(ctx context.Context, key string) (*nats.RawStreamMsg, error) {
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	entry, err := dm.js.GetLastMsg(dm.walKVStream, dm.walKVSubjectPrefix+key, nats.Context(readCtx))
	if errors.Is(err, nats.ErrMsgNotFound) {
		return nil, nats.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	switch entry.Header.Get("KV-Operation") {
	case "DEL", "PURGE":
		return nil, nats.ErrKeyDeleted
	}
	return entry, nil
}

func (dm *Domain) readWALReceipt(seq uint64) (*walReceipt, error) {
	entry, err := dm.readWALKV(context.Background(), dm.walReceiptKey(seq))
	if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read applied receipt for WAL sequence %d: %w", seq, err)
	}
	var receipt walReceipt
	if err := json.Unmarshal(entry.Data, &receipt); err != nil {
		return nil, fmt.Errorf("%w: malformed applied receipt for WAL sequence %d", ErrWALRecovery, seq)
	}
	receipt.revision = entry.Sequence
	return &receipt, nil
}

func (dm *Domain) validateWALReceipt(r *walReceipt, seq uint64) error {
	if r.Version != 1 || r.State != "applied" || !r.StreamCreated.Equal(dm.walCreated) || !r.KVCreated.Equal(dm.walKVCreated) || r.Bucket != dm.kv.Bucket() || r.Consumer != CommitterDurableName+"-"+dm.kv.Bucket() || r.Sequence != seq || r.TxID == "" || len(r.Digest) != 64 {
		return fmt.Errorf("%w: applied receipt does not match WAL generation, bucket or sequence %d", ErrWALRecovery, seq)
	}
	if _, err := hex.DecodeString(r.Digest); err != nil {
		return fmt.Errorf("%w: invalid digest in applied receipt for WAL sequence %d", ErrWALRecovery, seq)
	}
	if !strings.HasPrefix(r.AckSubject, "$JS.ACK.") {
		return fmt.Errorf("%w: invalid ACK address in applied receipt for WAL sequence %d", ErrWALRecovery, seq)
	}
	meta, err := (&nats.Msg{Reply: r.AckSubject, Sub: &nats.Subscription{}}).Metadata()
	if err != nil || meta.Stream != WALCommitsStreamName || meta.Consumer != r.Consumer || meta.Sequence.Stream != seq || meta.Sequence.Consumer == 0 {
		return fmt.Errorf("%w: ACK address does not match applied receipt for WAL sequence %d", ErrWALRecovery, seq)
	}
	for _, deletion := range r.Deletes {
		if !customNatsKv.KeyValid(deletion.Key) || strings.HasPrefix(deletion.Key, walReceiptPrefix) || deletion.Key == "__kv_consistent__" || deletion.Revision == 0 || (r.revision != 0 && deletion.Revision >= r.revision) {
			return fmt.Errorf("%w: invalid delete revision in receipt for WAL sequence %d", ErrWALRecovery, seq)
		}
	}
	return nil
}

func (dm *Domain) saveWALReceipt(msg *nats.Msg, meta *nats.MsgMetadata, txID string, deletes []walAppliedDelete) error {
	receipt := walReceipt{Version: 1, StreamCreated: dm.walCreated, KVCreated: dm.walKVCreated, Deletes: deletes, Bucket: dm.kv.Bucket(), Consumer: meta.Consumer, Sequence: meta.Sequence.Stream, TxID: txID, Digest: walDigest(msg.Data), State: "applied", AckSubject: msg.Reply}
	if err := dm.validateWALReceipt(&receipt, meta.Sequence.Stream); err != nil {
		return err
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	_, err = dm.kv.Create(dm.walReceiptKey(meta.Sequence.Stream), data)
	if errors.Is(err, nats.ErrKeyExists) {
		// A second replica may have finished the same redelivery. Only an identical
		// apply is acceptable; never overwrite evidence with a different payload.
		existing, readErr := dm.readWALReceipt(meta.Sequence.Stream)
		if readErr != nil {
			return readErr
		}
		if existing == nil {
			return fmt.Errorf("receipt disappeared while recording WAL sequence %d", meta.Sequence.Stream)
		}
		if err := dm.validateWALReceipt(existing, meta.Sequence.Stream); err != nil {
			return err
		}
		if existing.TxID != txID || existing.Digest != receipt.Digest {
			return fmt.Errorf("%w: conflicting applied receipt for WAL sequence %d", ErrWALRecovery, meta.Sequence.Stream)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("persist applied receipt for WAL sequence %d: %w", meta.Sequence.Stream, err)
	}
	dm.walProgress.Add(1)
	return nil
}

// recoverMissingWALAck resends the original ACK only when immutable evidence in
// the same KV bucket proves that the entire transaction was applied beforehand.
// It neither invents an ACK address nor deletes/recreates a consumer.
func (dm *Domain) recoverMissingWALAck(ctx context.Context) (bool, error) {
	consumer := CommitterDurableName + "-" + dm.kv.Bucket()
	info, err := dm.js.ConsumerInfo(WALCommitsStreamName, consumer, nats.Context(ctx))
	if err != nil {
		return false, err
	}
	if info.NumAckPending == 0 {
		return false, nil
	}
	if info.NumAckPending != 1 || info.Config.MaxAckPending != 1 {
		return false, fmt.Errorf("%w: consumer %q has unsupported pending ACK state", ErrWALRecovery, consumer)
	}
	seq := info.Delivered.Stream
	if seq == 0 {
		return false, fmt.Errorf("%w: consumer %q has an ACK pending without a WAL sequence", ErrWALRecovery, consumer)
	}
	_, err = dm.js.GetMsg(WALCommitsStreamName, seq, nats.Context(ctx))
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, nats.ErrMsgNotFound) {
		return false, fmt.Errorf("read pending WAL sequence %d: %w", seq, err)
	}
	receipt, err := dm.readWALReceipt(seq)
	if err != nil {
		return false, err
	}
	// A normal ACK in another replica can race the two read-only observations.
	// Confirm that the same missing record is still outstanding before deciding.
	current, err := dm.js.ConsumerInfo(WALCommitsStreamName, consumer, nats.Context(ctx))
	if err != nil {
		return false, err
	}
	if current.NumAckPending != 1 || current.Delivered.Stream != seq || current.AckFloor.Stream >= seq {
		return false, nil
	}
	if receipt == nil {
		return false, fmt.Errorf("%w: stream=%s consumer=%s bucket=%s sequence=%d is missing while its ACK is pending; no applied receipt proves the KV writes completed (legacy or expired receipt, or incomplete backup); transaction not skipped; preserve WAL/KV and verify or restore a consistent backup", ErrWALRecovery, WALCommitsStreamName, consumer, dm.kv.Bucket(), seq)
	}
	if err := dm.validateWALReceipt(receipt, seq); err != nil {
		return false, err
	}
	if err := dm.repairWALDeletes(ctx, receipt); err != nil {
		return false, err
	}
	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := dm.nc.RequestWithContext(ackCtx, receipt.AckSubject, []byte("+ACK")); err != nil {
		return false, fmt.Errorf("repeat proven ACK for WAL sequence %d: %w", seq, err)
	}
	after, err := dm.js.ConsumerInfo(WALCommitsStreamName, consumer, nats.Context(ctx))
	if err != nil {
		return false, err
	}
	if after.AckFloor.Stream < seq && after.NumAckPending != 0 {
		// A reply is not progress if the broker still holds the same orphan.
		// In particular, Delivered.Stream need not identify every corrupt state.
		return false, nil
	}
	dm.walProgress.Add(1)
	lg.Logf(lg.WarnLevel, "Recovered WAL acknowledgement from applied receipt: consumer=%s sequence=%d", consumer, seq)
	return true, nil
}

func (dm *Domain) remainingWALRecords(ctx context.Context) (uint64, error) {
	subject := "wal.commits." + dm.kv.Bucket()
	info, err := dm.js.StreamInfo(WALCommitsStreamName, &nats.StreamInfoRequest{SubjectsFilter: subject}, nats.Context(ctx))
	if err != nil {
		return 0, err
	}
	if !info.Created.Equal(dm.walCreated) {
		return 0, fmt.Errorf("%w: WAL stream was recreated during recovery", ErrWALRecovery)
	}
	return info.State.Subjects[subject], nil
}

func (dm *Domain) waitForWALRecovery(ctx context.Context, stallTimeout, pollInterval time.Duration) error {
	consumer := CommitterDurableName + "-" + dm.kv.Bucket()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	lastProgress := time.Now()
	lastApplied := dm.walProgress.Load()
	var lastFloor uint64
	var observed string
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := dm.walTerminalError(); err != nil {
			return err
		}
		if _, err := dm.recoverMissingWALAck(ctx); err != nil {
			if errors.Is(err, ErrWALRecovery) {
				return err
			}
			lastErr = err
		} else {
			info, err := dm.js.ConsumerInfo(WALCommitsStreamName, consumer, nats.Context(ctx))
			if err != nil {
				lastErr = err
			} else {
				applied := dm.walProgress.Load()
				if info.AckFloor.Stream > lastFloor || applied != lastApplied {
					lastProgress = time.Now()
					lastFloor = info.AckFloor.Stream
					lastApplied = applied
				}
				observed = fmt.Sprintf("pending=%d ack_pending=%d delivered=%d ack_floor=%d", info.NumPending, info.NumAckPending, info.Delivered.Stream, info.AckFloor.Stream)
				if info.NumPending == 0 && info.NumAckPending == 0 {
					remaining, err := dm.remainingWALRecords(ctx)
					if err != nil {
						if errors.Is(err, ErrWALRecovery) {
							return err
						}
						lastErr = err
					} else if remaining == 0 {
						if err := dm.walTerminalError(); err != nil {
							return err
						}
						if err := dm.restoreWALDeletes(ctx); err != nil {
							return err
						}
						return dm.setKVConsistent(true)
					} else {
						observed += fmt.Sprintf(" retained_records=%d", remaining)
					}
				}
			}
		}
		if time.Since(lastProgress) >= stallTimeout {
			return fmt.Errorf("%w: no durable apply or ACK progress for %s; stream=%s consumer=%s %s; last_read_error=%v; preserve WAL/KV and check broker health and transaction errors", ErrWALRecovery, stallTimeout, WALCommitsStreamName, consumer, observed, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (dm *Domain) maintainWALReceipts(ctx context.Context) {
	ticker := time.NewTicker(walReceiptSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			err := dm.pruneWALReceipts(sweepCtx, walReceiptRetain)
			cancel()
			if err != nil && ctx.Err() == nil {
				lg.Logf(lg.WarnLevel, "WAL receipt maintenance deferred: %v", err)
			}
		}
	}
}

// pruneWALReceipts removes exact KV revisions, never a moving key or a whole
// subject. Keep all records above the currently acknowledged floor as well as
// the newest recovery window. Missing older evidence later means a safe error.
func (dm *Domain) pruneWALReceipts(ctx context.Context, keep int) error {
	if keep < 1 {
		return fmt.Errorf("WAL receipt retention must be positive")
	}
	info, err := dm.js.ConsumerInfo(WALCommitsStreamName, CommitterDurableName+"-"+dm.kv.Bucket(), nats.Context(ctx))
	if err != nil {
		return err
	}
	prefix := walReceiptPrefix + dm.walReceiptGeneration() + "."
	w, err := dm.kv.Watch(prefix+">", nats.IgnoreDeletes(), nats.MetaOnly(), nats.Context(ctx))
	if err != nil {
		return err
	}
	defer w.Stop()
	type candidate struct {
		key           string
		seq, revision uint64
	}
	var entries []candidate
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-w.Updates():
			if !ok {
				return fmt.Errorf("WAL receipt inventory ended before its snapshot completed")
			}
			if entry == nil {
				goto complete
			}
			seq, err := strconv.ParseUint(strings.TrimPrefix(entry.Key(), prefix), 10, 64)
			if err == nil {
				entries = append(entries, candidate{entry.Key(), seq, entry.Revision()})
			}
		}
	}
complete:
	sort.Slice(entries, func(i, j int) bool { return entries[i].seq > entries[j].seq })
	var pruneThrough uint64
	for i := keep; i < len(entries); i++ {
		if entries[i].seq <= info.AckFloor.Stream && entries[i].seq > pruneThrough {
			pruneThrough = entries[i].seq
		}
	}
	if pruneThrough > 0 {
		if err := dm.recordPrunedWALReceipts(pruneThrough); err != nil {
			return err
		}
	}
	for i := keep; i < len(entries); i++ {
		entry := entries[i]
		if entry.seq > info.AckFloor.Stream {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := customNatsKv.KVPurgeTombstone(dm.js, dm.kv, entry.key, entry.revision); err != nil {
			return err
		}
	}
	return nil
}

// Persist the loss-of-evidence boundary BEFORE deleting any receipt. A stale
// retained WAL body below this boundary must not overwrite newer KV values.
func (dm *Domain) walPrunedKey() string {
	return walReceiptPrefix + dm.walReceiptGeneration() + ".pruned_through"
}

func (dm *Domain) checkWALReceiptWindow(seq uint64) error {
	entry, err := dm.readWALKV(context.Background(), dm.walPrunedKey())
	if errors.Is(err, nats.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read WAL receipt retention boundary: %w", err)
	}
	floor, err := strconv.ParseUint(string(entry.Data), 10, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid receipt retention boundary", ErrWALRecovery)
	}
	if seq <= floor {
		return fmt.Errorf("%w: WAL sequence %d is outside the retained applied-receipt window (pruned through %d); completion is unverified; transaction not replayed", ErrWALRecovery, seq, floor)
	}
	return nil
}

func (dm *Domain) recordPrunedWALReceipts(seq uint64) error {
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := dm.readWALKV(context.Background(), dm.walPrunedKey())
		if errors.Is(err, nats.ErrKeyNotFound) {
			_, err = dm.kv.Create(dm.walPrunedKey(), []byte(strconv.FormatUint(seq, 10)))
			if errors.Is(err, nats.ErrKeyExists) {
				continue
			}
			return err
		}
		if err != nil {
			return err
		}
		floor, err := strconv.ParseUint(string(entry.Data), 10, 64)
		if err != nil {
			return fmt.Errorf("%w: invalid receipt retention boundary", ErrWALRecovery)
		}
		if floor >= seq {
			return nil
		}
		_, err = dm.kv.Update(dm.walPrunedKey(), []byte(strconv.FormatUint(seq, 10)), entry.Sequence)
		if errors.Is(err, nats.ErrKeyExists) {
			continue
		}
		return err
	}
	return fmt.Errorf("WAL receipt retention boundary changed concurrently; cleanup deferred")
}

// A delete marker can be reclaimed while an older value still exists in the
// broker's recovery files. Record the delete's KV revision before reclaiming
// it, so recovery can remove a resurrected older value without deleting a
// legitimate later PUT. The conditional delete closes the read/write race.
func (dm *Domain) repairWALDeletes(ctx context.Context, receipt *walReceipt) error {
	for _, deletion := range receipt.Deletes {
		for attempt := 0; ; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry, err := dm.readWALKV(ctx, deletion.Key)
			if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) {
				break
			}
			if err != nil {
				return fmt.Errorf("verify delete in WAL sequence %d: %w", receipt.Sequence, err)
			}
			if entry.Sequence > deletion.Revision {
				break
			}
			if attempt >= 5 {
				return fmt.Errorf("delete revision changed repeatedly during WAL recovery")
			}
			err = dm.kv.Delete(deletion.Key, nats.LastRevision(entry.Sequence))
			if errors.Is(err, nats.ErrKeyExists) {
				continue
			}
			if err != nil {
				return fmt.Errorf("restore proven delete in WAL sequence %d: %w", receipt.Sequence, err)
			}
			dm.walProgress.Add(1)
			break
		}
	}
	return nil
}

func (dm *Domain) restoreWALDeletes(ctx context.Context) error {
	scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	prefix := walReceiptPrefix + dm.walReceiptGeneration() + "."
	watcher, err := dm.kv.Watch(prefix+">", nats.IgnoreDeletes(), nats.Context(scanCtx))
	if err != nil {
		return err
	}
	defer watcher.Stop()
	// Finish the snapshot before writes: newly repaired delete markers are not
	// receipt entries and must never change the inventory being validated.
	var receipts []*walReceipt
	for {
		select {
		case <-scanCtx.Done():
			return scanCtx.Err()
		case entry, ok := <-watcher.Updates():
			if !ok {
				return fmt.Errorf("WAL receipt replay ended before its snapshot completed")
			}
			if entry == nil {
				goto complete
			}
			if entry.Key() == dm.walPrunedKey() {
				continue
			}
			seq, err := strconv.ParseUint(strings.TrimPrefix(entry.Key(), prefix), 10, 64)
			if err != nil {
				return fmt.Errorf("%w: invalid applied-receipt key", ErrWALRecovery)
			}
			var receipt walReceipt
			if err = json.Unmarshal(entry.Value(), &receipt); err != nil {
				return fmt.Errorf("%w: malformed applied receipt for WAL sequence %d", ErrWALRecovery, seq)
			}
			receipt.revision = entry.Revision()
			if err = dm.validateWALReceipt(&receipt, seq); err != nil {
				return err
			}
			if txTime, err := strconv.ParseInt(receipt.TxID, 10, 64); err == nil {
				dm.cache.RecordCommittedTx(txTime)
			}
			if len(receipt.Deletes) > 0 {
				receipts = append(receipts, &receipt)
			}
		}
	}
complete:
	for _, receipt := range receipts {
		if err := dm.repairWALDeletes(scanCtx, receipt); err != nil {
			return err
		}
	}
	return nil
}
