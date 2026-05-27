// Foliage statefun cache package.
// Provides cache system that lives between stateful functions and NATS key/value
package cache

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

// Wire-format flags (FlagBytesAppend / FlagJSONAppend / FlagDeleted, plus the
// two deprecated 0/1 variants) and the timestamp prefix on KV values were
// removed in the cache-is-source-of-truth refactor. Cache writes go cache →
// WAL → KV one-way; KV is a downstream sink (like the postgres exporter), so
// the value bytes in KV are now the raw user payload with no header. If
// multi-instance coherence is ever needed it must be solved by a separate
// protocol, NOT by piggy-backing on KV-watch echoes.

const (
	//gracefully shutdown
	shutdownStatusNone = iota
	shutdownStatusWaiting
	shutdownStatusReady
)

var (
	keyValidationRegexp *regexp.Regexp = regexp.MustCompile(`^[a-zA-Z0-9/=_$#@$%+-][a-zA-Z0-9/=._$#@%+-]+[a-zA-Z0-9/=_$#@%+-]$|^[a-zA-Z0-9/=_$#@%+-]*$`)
)

// value types
const (
	typeByteArray = iota
	typeJson
)

type StoreValue struct {
	parent      *StoreValue
	keyInParent string
	value       interface{}
	valueExists bool
	valueType   uint8
	// store is the container of child nodes, allocated lazily on the first
	// child insertion. A freshly created node — and every leaf, which is
	// the overwhelming majority of nodes — keeps store == nil and carries
	// no ShardedMap, no shard maps and no shard mutexes. This is the main
	// memory win: the previous code eagerly allocated an 8-shard map (8
	// empty maps + 8 RWMutex) on EVERY node including leaves that never
	// get children. atomic.Pointer makes the one-time nil→ptr transition
	// safe against concurrent readers (LoadChild/Range read it locklessly).
	store           atomic.Pointer[system.ShardedMap]
	valueUpdateTime int64
	storeMutex      sync.RWMutex
	// Note: the old `syncedWithKV bool` and `purgeState int` fields are gone.
	// They existed for the two-phase delete protocol used to coordinate
	// multiple cache instances over a shared KV via kv.Watch echoes. With KV
	// now a one-way downstream sink and no continuous KV-watch, the cache
	// owns its own state and can delete from the tree immediately.
	//
	// The old `notifyUpdates sync.Map` field is also gone — its sole users
	// (SubscribeLevelCallback / UnsubscribeLevelCallback) had no callers
	// anywhere in the codebase; a sync.Map header on every one of millions
	// of cache nodes was pure dead weight.
}

func (csv *StoreValue) Lock(caller string) {
	//lg.Logf("------- Locking '%s' by '%s'", csv.keyInParent, caller)
	csv.storeMutex.Lock()
	//lg.Logf(">>>>>>> Locked '%s' by '%s'", csv.keyInParent, caller)
}

func (csv *StoreValue) Unlock(caller string) {
	//lg.Logf(">>>>>>> Unlocking '%s' by '%s'", csv.keyInParent, caller)
	csv.storeMutex.Unlock()
	//lg.Logf("------- Unlocked '%s' by '%s'", csv.keyInParent, caller)
}

func (csv *StoreValue) RLock(caller string) {
	csv.storeMutex.RLock()
}

func (csv *StoreValue) RUnlock(caller string) {
	csv.storeMutex.RUnlock()
}

func (csv *StoreValue) ValueExists() bool {
	return csv.valueExists
}

// shardCountForNewStore is the shard count used when a node lazily
// allocates its children container on the first child insertion. Kept at
// the historical value (8) so the concurrency profile of nodes that DO
// have children is identical to before — this change only removes the
// container from nodes that have no children, it does not alter sharding
// for those that do (that tuning is a separate change).
const shardCountForNewStore = 8

// loadStore returns the node's children container, or nil if it has none
// yet. Lockless atomic read — safe to call concurrently with ensureStore.
func (csv *StoreValue) loadStore() *system.ShardedMap {
	return csv.store.Load()
}

// ensureStore returns the node's children container, allocating it on the
// first call. Concurrent-safe: callers race via CompareAndSwap and the
// losers discard their candidate and adopt the winner's.
func (csv *StoreValue) ensureStore() *system.ShardedMap {
	if sm := csv.store.Load(); sm != nil {
		return sm
	}
	candidate := system.SharedMapMustNewHashed(shardCountForNewStore)
	if csv.store.CompareAndSwap(nil, candidate) {
		return candidate
	}
	return csv.store.Load()
}

// storeLen reports the number of children, treating a nil (never-allocated)
// container as empty.
func (csv *StoreValue) storeLen() int {
	if sm := csv.loadStore(); sm != nil {
		return sm.Len()
	}
	return 0
}

func (csv *StoreValue) LoadChild(key string) (*StoreValue, bool) {
	sm := csv.loadStore()
	if sm == nil {
		return nil, false
	}
	if v, ok := sm.Get(key); ok {
		return v.(*StoreValue), true
	}
	return nil, false
}

func (csv *StoreValue) StoreChild(key string, child *StoreValue) (actual *StoreValue, loaded bool) {
	child.parent = csv
	child.keyInParent = key

	a, l := csv.ensureStore().LoadOrStore(key, child)
	if l {
		return a.(*StoreValue), true
	}
	return child, false
}

func (csv *StoreValue) Put(value interface{}, updateInKV bool, customPutTime int64) {
	csv.Lock("Put")
	key := csv.keyInParent

	if customPutTime < 0 {
		customPutTime = system.GetCurrentTimeNs()
	}
	// Last-writer-wins by op time, kept as defence-in-depth for in-process
	// concurrent writes that supply explicit older timestamps (the previous
	// reason for this guard — stale PUTs re-arriving via KV-watch echo — is
	// gone with the cache-is-source-of-truth refactor). Delete keeps a
	// tombstoned node with its delete time so a subsequent SetValue with an
	// older customSetTime cannot resurrect it; equal/newer writes apply.
	if customPutTime < csv.valueUpdateTime {
		csv.Unlock("Put")
		return
	}

	csv.value = value
	csv.valueExists = true
	csv.valueUpdateTime = customPutTime
	_ = updateInKV // kept in signature for callers; no longer affects local state
	_ = key        // formerly used for parent-subscriber notifications; gone with notifyUpdates
	csv.Unlock("Put")
}

func (csv *StoreValue) collectGarbage() {
	system.GlobalPrometrics.GetRoutinesCounter().Started("cache.csv.collectGarbage")
	defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("cache.csv.collectGarbage")

	var canBeDeletedFromParent bool

	csv.Lock("collectGarbage")
	// A tombstoned leaf with no children can be dropped from the tree right
	// away. The old purge state machine (`purgeState` 0→1→2 gated on KV-watch
	// echo confirmation) and the noNotifySubscribers guard (gated on the now-
	// gone subscriber map) are both gone: the cache no longer waits for KV
	// before reclaiming memory.
	canBeDeletedFromParent = !csv.valueExists && csv.storeLen() == 0
	csv.Unlock("collectGarbage")

	if csv.parent != nil && canBeDeletedFromParent {
		// The parent necessarily has a container (this node lives in it),
		// but guard defensively against a nil store regardless.
		if parentStore := csv.parent.loadStore(); parentStore != nil {
			parentStore.Delete(csv.keyInParent)
		}

		go csv.parent.collectGarbage()
	}
}

func (csv *StoreValue) Delete(updateInKV bool, customDeleteTime int64) {
	csv.Lock("Delete")
	key := csv.keyInParent
	// We keep the node as a tombstone (valueExists=false, valueUpdateTime set)
	// so an in-process Put with an explicitly older customSetTime cannot
	// resurrect it through the Put LWW guard. collectGarbage removes the node
	// from the parent's map on a later maintenance pass once there are no
	// subscribers and no children.
	csv.value = nil
	csv.valueExists = false
	if customDeleteTime < 0 {
		customDeleteTime = system.GetCurrentTimeNs()
	}
	csv.valueUpdateTime = customDeleteTime
	_ = updateInKV // kept in signature for callers; no longer affects local state
	_ = key        // formerly used for parent-subscriber notifications; gone with notifyUpdates
	csv.Unlock("Delete")
}

func (csv *StoreValue) Range(f func(key, value interface{}) bool) {
	sm := csv.loadStore()
	if sm == nil {
		return
	}
	sm.Range(func(k string, v interface{}) bool {
		return f(k, v)
	})
}

func (csv *StoreValue) SetValueType(valueType uint8) {
	// valueType is read under csv.RLock in the GetValue/GetValueJSON paths, and
	// written from both in-process ops and the async KV-watch re-apply, so the
	// write must take the lock too — otherwise it's a data race (and a torn
	// type read could mis-decode a value).
	csv.Lock("SetValueType")
	csv.valueType = valueType
	csv.Unlock("SetValueType")
}

type Store struct {
	cacheConfig *Config
	js          nats.JetStreamContext
	kv          nats.KeyValue
	ctx         context.Context
	cancel      context.CancelFunc

	rootValue     *StoreValue
	valuesInCache int

	// walWriteEnabled - true for active instance
	// only active instances can write to WAL streams
	walWriteEnabled      atomic.Bool
	transactionGenerator atomic.Pointer[TransactionGenerator]

	// committedTxTime is the high-watermark of WAL transactions that have
	// been durably applied to KV. Updated atomically by RecordCommittedTx
	// from the transaction committer. Consumed by the backup-write-barrier
	// readiness check (replaces the old per-node syncedWithKV scan).
	committedTxTime atomic.Int64

	// activeOps tracks in-flight write operations by opTime.
	// key: int64 opTime, value: *atomic.Int32 (reference count).
	// kvLazyWriter waits until all operations with opTime ≤ barrierTime
	// have finished before forming a WAL transaction.
	activeOps sync.Map

	// pendingTxs accumulates WAL ops per transaction. Each write appends
	// directly via sync.Map.Store. kvLazyWriter publishes the full tx as
	// a single message when all in-flight operations are done.
	// key: int64 (timestamp/txID), value: *pendingTx
	pendingTxs sync.Map

	//write barrier state
	backupBarrierTimestamp   int64
	backupBarrierStatus      int32 // 0=unlocked, 1=locking, 2=locked
	backupBarrierLastChecked int64
	Synced                   chan struct{}
}

type maintenanceResult struct {
	valueCount int
	// Backup-barrier readiness is now determined by Store.committedTxTime,
	// not by walking the cache. The old `allBeforeBackupBarrierSynced` flag
	// was set by scanning every node for syncedWithKV; that machinery is
	// gone with the cache-as-source-of-truth refactor.
}

// pendingTx accumulates ops for a single WAL transaction.
// Mutex protects the slice to guarantee strict append order.
type pendingTx struct {
	mu  sync.Mutex
	ops []WALOp
}

func (ptx *pendingTx) Add(op WALOp) {
	ptx.mu.Lock()
	ptx.ops = append(ptx.ops, op)
	ptx.mu.Unlock()
}

func (ptx *pendingTx) Collect() []WALOp {
	ptx.mu.Lock()
	result := make([]WALOp, len(ptx.ops))
	copy(result, ptx.ops)
	ptx.mu.Unlock()
	return result
}

// WALOp represents a single operation within a WAL transaction.
// Exported for use by TransactionCommitter and ExportCommitter.
type WALOp struct {
	OpType string
	Key    string
	Value  []byte
}

// publishDirtyOp appends a WAL op to the pending transaction for writeTime.
// Lock-free — uses sync.Map internally.
func (cs *Store) publishDirtyOp(writeTime int64, key string, opType OpType, finalBytes []byte) {
	if cs.getTransactionGenerator() == nil || !cs.walWriteEnabled.Load() {
		return
	}
	if writeTime <= 0 {
		return
	}

	v, _ := cs.pendingTxs.LoadOrStore(writeTime, &pendingTx{})
	v.(*pendingTx).Add(WALOp{
		OpType: string(opType),
		Key:    cs.toStoreKey(key),
		Value:  finalBytes,
	})
}

// RecordCommittedTx is invoked by the WAL→KV transaction committer
// (statefun.Domain.applyTransactionOps) after a transaction has been
// durably written to KV. It advances the committedTxTime watermark
// monotonically. The backup-write-barrier readiness check uses this
// watermark instead of the old per-node syncedWithKV bit: a barrier set
// at timestamp B is "ready to lock" once committedTxTime ≥ B (i.e. all
// transactions issued before the barrier have landed in KV).
func (cs *Store) RecordCommittedTx(txTime int64) {
	for {
		cur := cs.committedTxTime.Load()
		if txTime <= cur {
			return
		}
		if cs.committedTxTime.CompareAndSwap(cur, txTime) {
			return
		}
	}
}

// CommittedTxTime returns the watermark — the largest tx timestamp known to
// have been durably applied to KV by the committer.
func (cs *Store) CommittedTxTime() int64 {
	return cs.committedTxTime.Load()
}

// HasPendingWrites returns true if there are transactions buffered in the
// cache that have not been published to the WAL stream yet. Used by
// callers that need a "cache → WAL → KV is drained" barrier.
func (cs *Store) HasPendingWrites() bool {
	has := false
	cs.pendingTxs.Range(func(_, _ any) bool {
		has = true
		return false
	})
	return has
}

// loadFromKV opens a KV watcher, replays the historical state into the cache
// as either typed JSON or raw bytes (probed per-entry), then stops. Called
// once at startup by NewCacheStore's initialLoader, and again from
// RehydrateFromKV on every passive→active promotion (HA failover). Wire
// format on KV is the raw user payload — no per-record header. The Put LWW
// guard is a non-issue here because callers MUST guarantee no concurrent
// in-process writes during a load (initial load runs before isReady;
// rehydration runs while walWriteEnabled is false and no function handlers
// are subscribed).
//
// Why we probe JSON-vs-bytes per entry: CMDB callers Set values either as
// JSON (SetValueJSON, vertex bodies) or as bytes (SetValue, link targets and
// index keys). That distinction is what later GetValueJSON/Exists/JPGQL rely
// on to know whether a value is a JSON-typed body. Without it, e.g.
// `ExistsJson(types/<T>)` returns false after restart and JPGQL enumerations
// like [l:type('__object')] silently produce empty results. The probe is one
// `easyjson.JSONFromBytes` per entry — cheap on a one-shot load.
func (cs *Store) loadFromKV(ctx context.Context) error {
	w, err := cs.kv.Watch(cs.cacheConfig.kvStorePrefix+".>", nats.IgnoreDeletes())
	if err != nil {
		return fmt.Errorf("kv.Watch: %w", err)
	}
	defer func() { system.MsgOnErrorReturn(w.Stop()) }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-w.Updates():
			if !ok {
				return fmt.Errorf("kv watcher channel closed unexpectedly during load")
			}
			if entry == nil {
				// End of historical replay — load complete.
				return nil
			}
			key := cs.fromStoreKey(entry.Key())
			valueBytes := entry.Value()
			now := system.GetCurrentTimeNs()
			// Empty value is a LEGITIMATE write, not a delete: CMDB writes
			// index keys (e.g. <v>.out.index.<linkName>.type.<linkType>) with
			// nil value because the information is encoded in the key shape
			// alone. With IgnoreDeletes() the watcher already filters real
			// tombstones, so any entry that arrives is a real key. Skipping
			// empty values here previously dropped every link-type/tag index
			// key on reload, which broke JPGQL enumeration after a restart
			// (consistency check would return 0 members).
			if len(valueBytes) == 0 {
				cs.SetValue(key, valueBytes, false, now)
				continue
			}
			// Probe: if the bytes are a valid JSON object, store as JSON so
			// callers that distinguish typeJson vs typeByteArray (ExistsJson,
			// JPGQL filters, GetValueJSON*) see the right type. Otherwise
			// store as raw bytes. This restores the type semantics that CMDB
			// originally wrote (SetValueJSON for vertex bodies, SetValue for
			// link targets and index keys).
			if jv, ok := easyjson.JSONFromBytes(valueBytes); ok && jv.IsObject() {
				cs.SetValueJSON(key, &jv, false, now)
			} else {
				cs.SetValue(key, valueBytes, false, now)
			}
		}
	}
}

// RehydrateFromKV drops the in-memory cache tree and reloads it from KV.
// The runtime calls this on every passive→active promotion in HA mode: a
// passive instance's cache is, by design, a stale snapshot frozen at the
// moment its initial load completed (there is no continuous kv.Watch
// reflecting subsequent active-side writes back). On promotion we must
// reconcile the local cache with KV before serving any request, because
// the just-departed active was writing to KV without our knowledge.
//
// Caller contract — MUST be guaranteed at the call site:
//   - walWriteEnabled is false (this is what makes kvLazyWriter idle and
//     unable to walk the tree concurrently);
//   - isReady is false on the runtime, so function handlers are not
//     serving requests and there are no in-process writes;
//   - the runtime promotion path waits for committer to drain pending WAL
//     transactions into KV BEFORE calling this — otherwise we would
//     re-load a KV state that is missing transactions still buffered in
//     the wal_commits stream.
//
// On error the cache is left empty; the caller should abort the promotion
// (the runtime stays passive and will retry on the next tick).
func (cs *Store) RehydrateFromKV(ctx context.Context) error {
	// Atomic swap of the root's children-container — readers/writers that
	// somehow still hold a child reference see a consistent old subtree
	// (released to GC after they drop it). The root identity is kept so
	// notifyUpdates subscribers stay valid.
	cs.rootValue.store.Store(system.SharedMapMustNewHashed(shardCountForNewStore))
	// Reset committed-tx watermark too — the new world starts from the
	// state we are about to load, and any old in-process backup-barrier
	// state is meaningless across a role transition.
	cs.committedTxTime.Store(0)
	return cs.loadFromKV(ctx)
}

// traverseCacheForMaintenance performs a post-order DFS that counts nodes
// (for the cache_values metric) AND collapses tombstone cascades.
//
// History — the previous version did an iterative DFS that called
// collectGarbage() only on nodes with no children ("noChildren" branch).
// That had a structural problem: a tombstone chain A→B(†)→C(†)→D(†) where
// only D is a leaf needed K maintenance passes to disappear (K = depth of
// the chain). Under churn — every ll_crud.go ObjectDelete fans into ~5
// DeleteValue calls, each leaving a tombstone whose subtree may contain
// other tombstones — the chain accumulates faster than leaf-only GC can
// shrink it, and the in-memory tree grows without bound while the KV
// itself stays constant. Stand 116 (2026-05-27) saw heap_objects climb
// 8M → 130M over 4h with KV stable at ~258k entries — that ratio (~500
// Go-objects per KV entry) is the signature of this cascade leaving
// orphaned StoreValue branches the runtime never reclaims.
//
// New implementation — post-order recursion. For each node we first
// sweep all its children, get back the keys of those that are themselves
// fully removable, then drop them from this node's store. After children
// are processed we report our own removability back to the caller. A
// single pass therefore collapses any cascade depth: D is reported dead
// to C, which becomes a leaf-tombstone and reports dead to B, etc.
//
// Race against concurrent writes — between a child returning canRemove=
// true and the parent actually deleting it from its store, a concurrent
// Put on the same path could resurrect the child. We protect against
// that by re-checking the child's state under its own lock just before
// the delete; if the recheck shows the child became valid again, we
// leave it alone. This is the protocol Skala-backend's report flagged
// the old asynchronous collectGarbage cascade for racing on, and it is
// fixed here.
//
// Root is never removed (it has no parent and is the cache entry point).
//
// Backup-barrier readiness is no longer decided here — it is a single
// watermark comparison (Store.CommittedTxTime() ≥ barrierTimestamp),
// done by kvLazyWriter at the same maintenance tick. That removal
// pre-dates this change.
func (cs *Store) traverseCacheForMaintenance() *maintenanceResult {
	result := &maintenanceResult{}
	cs.sweepSubtree(cs.rootValue, result)
	return result
}

// sweepSubtree post-order DFS sweep. Returns true if csv is now itself
// removable from its parent (tombstoned AND no surviving children).
//
// MUST NOT be called on the cache root from a context that would honour
// the returned bool — traverseCacheForMaintenance is the sole caller and
// discards the root's return value (the root has no parent so the value
// is meaningless there).
func (cs *Store) sweepSubtree(csv *StoreValue, result *maintenanceResult) bool {
	result.valueCount++

	// Step 1 — recurse into each child, collecting keys of children that
	// reported themselves as fully removable. We snapshot the children via
	// ShardedMap.Range (which itself snapshots each shard under RLock to
	// avoid lock inversion, see system/shardedmap.go); concurrent inserts
	// during the snapshot are fine, they just won't be visited this pass
	// and will be picked up the next time around.
	var deadKeys []string
	csv.Range(func(key, value interface{}) bool {
		child, ok := value.(*StoreValue)
		if !ok {
			return true
		}
		if cs.sweepSubtree(child, result) {
			deadKeys = append(deadKeys, key.(string))
		}
		return true
	})

	// Step 2 — drop removable children. Recheck each under the child's
	// own lock to close the race window: a concurrent Put could have
	// landed between the recursive sweepSubtree returning true and now,
	// which would mean the child is no longer dead. Skip those.
	if len(deadKeys) > 0 {
		if sm := csv.loadStore(); sm != nil {
			for _, k := range deadKeys {
				raw, ok := sm.Get(k)
				if !ok {
					continue
				}
				child, ok := raw.(*StoreValue)
				if !ok {
					continue
				}
				child.Lock("sweepSubtree-confirm")
				stillDead := !child.valueExists && child.storeLen() == 0
				child.Unlock("sweepSubtree-confirm")
				if stillDead {
					sm.Delete(k)
				}
			}
		}
	}

	// Step 3 — am I removable? Root never is.
	if csv.parent == nil {
		return false
	}
	csv.Lock("sweepSubtree-self")
	canRemove := !csv.valueExists && csv.storeLen() == 0
	csv.Unlock("sweepSubtree-self")
	return canRemove
}

func NewCacheStore(ctx context.Context, cacheConfig *Config, js nats.JetStreamContext, kv nats.KeyValue) *Store {
	le := lg.GetLogger()
	var inited atomic.Bool
	initChan := make(chan bool)
	cs := Store{
		cacheConfig: cacheConfig,
		js:          js,
		kv:          kv,
		rootValue: &StoreValue{
			parent:          nil,
			value:           nil,
			valueExists:     false,
			valueUpdateTime: -1,
		},
		valuesInCache: 0,

		//barrier init
		backupBarrierTimestamp:   0,
		backupBarrierStatus:      BackupBarrierStatusUnlocked,
		backupBarrierLastChecked: 0,
		Synced:                   make(chan struct{}),
	}

	cs.ctx, cs.cancel = context.WithCancel(ctx)

	// The root always holds children (every top-level key hangs off it),
	// so initialise its container eagerly with the historical shard count
	// rather than paying a lazy-alloc race on the very first insertion.
	cs.rootValue.store.Store(system.SharedMapMustNewHashed(shardCountForNewStore))

	// default - can not publish to WAL
	cs.walWriteEnabled.Store(false)

	initialLoader := func(cs *Store) {
		system.GlobalPrometrics.GetRoutinesCounter().Started("cache.initialLoader")
		defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("cache.initialLoader")
		if err := cs.loadFromKV(cs.ctx); err != nil {
			le.Errorf(ctx, "initial load from KV failed: %s", err)
			return // initChan stays open → NewCacheStore blocks → caller must abort
		}
		if inited.CompareAndSwap(false, true) {
			close(initChan)
		}
	}
	kvLazyWriterWithWAL := func(cs *Store) {
		system.GlobalPrometrics.GetRoutinesCounter().Started("cache.kvLazyWriter")
		defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("cache.kvLazyWriter")

		const maintenanceInterval = 10 // run maintenance DFS every N iterations

		shutdownStatus := shutdownStatusNone
		maintenanceCounter := 0
		lastWALNotReadyLogAt := time.Time{}
		lastWALDisabledLogAt := time.Time{}
		skipLogInterval := 5 * time.Second
		for {
			if shutdownStatus == shutdownStatusReady {
				le.Debugf(ctx, "cache synced, ready for shutdown")
				close(cs.Synced)
				return
			}

			tg := cs.getTransactionGenerator()
			if tg == nil {
				if time.Since(lastWALNotReadyLogAt) >= skipLogInterval {
					le.Tracef(ctx, "WAL is not ready, skip this iteration")
					lastWALNotReadyLogAt = time.Now()
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if !cs.walWriteEnabled.Load() {
				if time.Since(lastWALDisabledLogAt) >= skipLogInterval {
					le.Tracef(ctx, "WAL writes are disabled, skip this iteration")
					lastWALDisabledLogAt = time.Now()
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if shutdownStatus == shutdownStatusNone {
				// Check for shutdown signal
				select {
				case <-cs.ctx.Done():
					le.Debugf(ctx, "cache got shutdown signal")
					shutdownStatus = shutdownStatusWaiting
					continue
				default:
				}

				backupBarrierTimestamp, backupBarrierStatus := cs.getBackupBarrierState()
				if backupBarrierStatus == BackupBarrierStatusLocking {
					if backupBarrierTimestamp == 0 {
						backupBarrierTimestamp = system.GetCurrentTimeNs()
						system.MsgOnErrorReturn(cs.updateBackupBarrierWithTimestamp(backupBarrierTimestamp))
						le.Infof(ctx, "set barrier timestamp: %d", backupBarrierTimestamp)
					}
				}

				// FAST PATH: batch and publish pending transactions in time order.
				// Accumulate ops from multiple small txs until we reach walBatchMinOps,
				// then publish as a single merged transaction (txID = latest time in batch).
				var pendingTimes []int64
				cs.pendingTxs.Range(func(k, _ any) bool {
					pendingTimes = append(pendingTimes, k.(int64))
					return true
				})
				if len(pendingTimes) > 0 {
					sort.Slice(pendingTimes, func(i, j int) bool { return pendingTimes[i] < pendingTimes[j] })

					var batchOps []WALOp
					var batchTimes []int64

					for _, txTime := range pendingTimes {
						if cs.hasActiveOperationsUpTo(txTime) {
							break // earlier ops still in-flight — stop to preserve order
						}
						if err := cs.checkBackupBarrierInfoBeforeWrite(txTime); err != nil {
							break // backup barrier — stop
						}
						v, ok := cs.pendingTxs.Load(txTime)
						if !ok {
							continue
						}
						ops := v.(*pendingTx).Collect()
						batchOps = append(batchOps, ops...)
						batchTimes = append(batchTimes, txTime)

						if len(batchOps) >= cacheConfig.walBatchMinOps {
							// Publish batch — txID is the latest time in the batch
							txID := strconv.FormatInt(txTime, 10)
							le.Tracef(ctx, "Publishing batched tx=%s with %d ops (%d txs merged)", txID, len(batchOps), len(batchTimes))
							if err := cs.getTransactionGenerator().PublishTransaction(txID, batchOps); err != nil {
								le.Errorf(ctx, "kvLazyWriter: cannot publish tx=%s: %s", txID, err)
								break // retry next iteration
							}
							for _, t := range batchTimes {
								cs.pendingTxs.Delete(t)
							}
							batchOps = nil
							batchTimes = nil
						}
					}

					// Publish remaining batch if any ops accumulated
					if len(batchOps) > 0 {
						txID := strconv.FormatInt(batchTimes[len(batchTimes)-1], 10)
						le.Tracef(ctx, "Publishing batched tx=%s with %d ops (%d txs merged)", txID, len(batchOps), len(batchTimes))
						if err := cs.getTransactionGenerator().PublishTransaction(txID, batchOps); err != nil {
							le.Errorf(ctx, "kvLazyWriter: cannot publish tx=%s: %s", txID, err)
						} else {
							for _, t := range batchTimes {
								cs.pendingTxs.Delete(t)
							}
						}
					}
				}

				// SLOW PATH: maintenance — node count + GC (every N iterations).
				// Backup-barrier readiness is now a single watermark comparison
				// instead of a full-tree DFS that touched every node's lock.
				// (reuses backupBarrierTimestamp / backupBarrierStatus read at
				// the top of the iteration.)
				if backupBarrierStatus == BackupBarrierStatusLocking &&
					backupBarrierTimestamp > 0 &&
					cs.CommittedTxTime() >= backupBarrierTimestamp {
					cs.markCacheReadyForBackup()
				}

				maintenanceCounter++
				if maintenanceCounter >= maintenanceInterval {
					maintenanceCounter = 0
					maintResult := cs.traverseCacheForMaintenance()
					cs.valuesInCache = maintResult.valueCount

					if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_values", "", []string{"id"}); err == nil {
						gaugeVec.With(prometheus.Labels{"id": cs.cacheConfig.id}).Set(float64(cs.valuesInCache))
					}
				}

				time.Sleep(time.Duration(cacheConfig.lazyWriterRepeatDelayMkS) * time.Microsecond)
			}

			if shutdownStatus == shutdownStatusWaiting {
				// Publish all remaining pending transactions.
				var pendingTimes []int64
				cs.pendingTxs.Range(func(k, _ any) bool {
					pendingTimes = append(pendingTimes, k.(int64))
					return true
				})
				if len(pendingTimes) > 0 {
					sort.Slice(pendingTimes, func(i, j int) bool { return pendingTimes[i] < pendingTimes[j] })
					for _, txTime := range pendingTimes {
						v, ok := cs.pendingTxs.Load(txTime)
						if !ok {
							continue
						}
						ops := v.(*pendingTx).Collect()

						txID := strconv.FormatInt(txTime, 10)
						le.Debugf(ctx, "Shutdown: publishing tx=%s with %d ops", txID, len(ops))
						if err := cs.getTransactionGenerator().PublishTransaction(txID, ops); err != nil {
							le.Errorf(ctx, "Shutdown: cannot publish tx=%s: %s", txID, err)
						}
						cs.pendingTxs.Delete(txTime)
					}
				} else {
					shutdownStatus = shutdownStatusReady
				}
			}
		}
	}
	go initialLoader(&cs)
	go kvLazyWriterWithWAL(&cs)
	<-initChan
	return &cs
}

func (cs *Store) GetValueUpdateTime(key string) int64 {
	var result int64 = -1

	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
			csv.RLock("GetValueUpdateTime")
			if csv.valueExists {
				result = csv.valueUpdateTime
			}
			csv.RUnlock("GetValueUpdateTime")
		}
	}
	return result
}

func (cs *Store) GetValue(key string) ([]byte, error) {
	var result []byte = nil
	var resultError error = nil

	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
			csv.RLock("GetValue")
			if !csv.ValueExists() { // Value was intentionally deleted and was marked so, no cache miss policy can be applied here
				resultError = fmt.Errorf("value for key=%s does not exist", key)
			} else {
				switch csv.valueType {
				case typeByteArray:
					result = csv.value.([]byte)
				case typeJson:
					lg.Logf(lg.WarnLevel, "Value for key=%s is JSON, use GetValueJSON method", key)
					result = csv.value.(*easyjson.JSON).ToBytes()
				default:
					resultError = fmt.Errorf("unsupported value type: %d", csv.valueType)
				}
			}
			csv.RUnlock("GetValue")

			return result, resultError
		}
	}

	// Cache miss -----------------------------------------
	resultError = fmt.Errorf("value for for key=%s does not exist", key)

	return result, resultError
}

// Exists reports whether a byte-typed value is currently stored under key.
// Mirrors GetValue: if the underlying value is JSON-typed, a WarnLevel
// log entry directs the caller to ExistsJson, but existence is still
// reported truthfully (the entry does exist, the caller is just probing
// it via the wrong type-affinity API).
//
// Compared to "_, err := cache.GetValue(key); err == nil" the saving is
// the avoided []byte allocation / JSON serialization that GetValue would
// otherwise perform on the value just to throw it away.
func (cs *Store) Exists(key string) bool {
	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
			csv.RLock("Exists")
			defer csv.RUnlock("Exists")
			if !csv.ValueExists() {
				return false
			}
			if csv.valueType == typeJson {
				lg.Logf(lg.WarnLevel, "Value for key=%s is JSON, use ExistsJson method", key)
			}
			return true
		}
	}
	return false
}

// ExistsJson reports whether a JSON-typed value is currently stored under
// key. Mirrors GetValueJSON: if the underlying value is byte-typed, a
// WarnLevel log entry directs the caller to Exists, but existence is
// still reported truthfully.
//
// Compared to "_, err := cache.GetValueJSON(key); err == nil" the saving
// is the avoided JSON Clone that GetValueJSON would otherwise perform on
// the value just to throw it away. For large vertex bodies this saves
// O(N nodes) allocations on every existence probe (vertex.create /
// vertex.update / vertex.delete / orphan probes call this exact pattern
// in hl_crud.go and ll_crud.go).
func (cs *Store) ExistsJson(key string) bool {
	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
			csv.RLock("ExistsJson")
			defer csv.RUnlock("ExistsJson")
			if !csv.ValueExists() {
				return false
			}
			if csv.valueType == typeByteArray {
				lg.Logf(lg.WarnLevel, "Value for key=%s is []byte, use Exists method", key)
			}
			return true
		}
	}
	return false
}

func (cs *Store) GetValueJSON(key string) (*easyjson.JSON, error) {
	var result *easyjson.JSON
	var resultError error

	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
			csv.RLock("GetValueJSON")
			if !csv.ValueExists() {
				resultError = fmt.Errorf("value for key=%s does not exist", key)
			} else {
				switch csv.valueType {
				case typeJson:
					result = csv.value.(*easyjson.JSON).Clone().GetPtr()
				case typeByteArray:
					// Cache entries loaded from KV at startup / rehydration are
					// stored as raw bytes (typeByteArray); transparently parsing
					// them to JSON on demand is the normal path now, not a bug
					// signal — keep the conversion silent.
					if json, ok := easyjson.JSONFromBytes(csv.value.([]byte)); ok {
						result = &json
					} else {
						resultError = fmt.Errorf("value for key=%s is not valid JSON", key)
					}
				default:
					resultError = fmt.Errorf("unsupported value type: %d", csv.valueType)
				}
			}
			csv.RUnlock("GetValueJSON")

			return result, resultError
		}
	}

	// ---------------------Cache miss--------------------------
	resultError = fmt.Errorf("value for for key=%s does not exist", key)

	return result, resultError
}

// GetValueJSONByPath reads a single sub-path of the JSON value stored at key
// WITHOUT cloning the whole value. GetValueJSON clones the entire tree, which
// is wasteful when the caller only needs one field (e.g. JPGQL body-value
// filters reading body.<field> on a traversal candidate). Here only the
// resolved sub-value is cloned — a scalar/array field is tiny compared to a
// full object body. Returns a JSON{nil} (with no error) when the path is
// absent, so callers can treat "field missing" via the usual Is*/As* checks.
func (cs *Store) GetValueJSONByPath(key string, path string) (*easyjson.JSON, error) {
	var result *easyjson.JSON
	var resultError error

	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
			csv.RLock("GetValueJSONByPath")
			if !csv.ValueExists() {
				resultError = fmt.Errorf("value for key=%s does not exist", key)
			} else {
				switch csv.valueType {
				case typeJson:
					sub := csv.value.(*easyjson.JSON).GetByPath(path).Clone()
					result = &sub
				case typeByteArray:
					if json, ok := easyjson.JSONFromBytes(csv.value.([]byte)); ok {
						sub := json.GetByPath(path).Clone()
						result = &sub
					} else {
						resultError = fmt.Errorf("value for key=%s is not valid JSON", key)
					}
				default:
					resultError = fmt.Errorf("unsupported value type: %d", csv.valueType)
				}
			}
			csv.RUnlock("GetValueJSONByPath")

			return result, resultError
		}
	}

	// ---------------------Cache miss--------------------------
	resultError = fmt.Errorf("value for key=%s does not exist", key)

	return result, resultError
}

func (cs *Store) SetValueIfDoesNotExist(key string, newValue []byte, updateInKV bool, customSetTime int64) bool {
	if keyLastToken, parent := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true); len(keyLastToken) > 0 && parent != nil {
		candidate := &StoreValue{
			value:           newValue,
			valueExists:     true,
			valueUpdateTime: customSetTime,
		}
		actual, loaded := parent.StoreChild(keyLastToken, candidate)
		if !loaded {
			if updateInKV {
				cs.publishDirtyOp(customSetTime, key, OpTypePUT, newValue)
			}
			return true // created for the first time
		}

		// Already exists — set only if "empty"
		actual.Lock("SetValueIfDoesNotExist")
		if !actual.valueExists && actual.value == nil {
			actual.value = newValue
			actual.valueExists = true
			actual.valueUpdateTime = customSetTime
			actual.Unlock("SetValueIfDoesNotExist")
			if updateInKV {
				cs.publishDirtyOp(customSetTime, key, OpTypePUT, newValue)
			}
			return true
		}
		actual.Unlock("SetValueIfDoesNotExist")
	}
	return false
}

func (cs *Store) SetValue(key string, value []byte, updateInKV bool, customSetTime int64) bool {
	if !keyValidationRegexp.MatchString(key) {
		return false
	}
	if customSetTime < 0 {
		customSetTime = system.GetCurrentTimeNs()
	}
	keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true)
	if len(keyLastToken) == 0 || parentCacheStoreValue == nil {
		return true
	}
	if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
		csv.SetValueType(typeByteArray)
		csv.Put(value, updateInKV, customSetTime)
	} else {
		csvUpdate := &StoreValue{
			value:           value,
			valueExists:     true,
			valueType:       typeByteArray,
			valueUpdateTime: customSetTime,
		}
		actual, loaded := parentCacheStoreValue.StoreChild(keyLastToken, csvUpdate)
		if loaded && customSetTime >= actual.valueUpdateTime {
			actual.SetValueType(typeByteArray)
			actual.Put(value, updateInKV, customSetTime)
		}
	}
	if updateInKV {
		cs.publishDirtyOp(customSetTime, key, OpTypePUT, value)
	}
	return true
}

func (cs *Store) SetValueJSON(key string, originValue *easyjson.JSON, updateInKV bool, customSetTime int64) bool {
	if !keyValidationRegexp.MatchString(key) {
		return false
	}
	if customSetTime < 0 {
		customSetTime = system.GetCurrentTimeNs()
	}
	keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true)
	if len(keyLastToken) == 0 || parentCacheStoreValue == nil {
		return true
	}
	value := originValue.Clone().GetPtr()
	// Pre-serialize before any locks — reuse for WAL publish. KV stores
	// the raw JSON payload (no header) now.
	var walBytes []byte
	if updateInKV {
		walBytes = originValue.ToBytes()
	}
	if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
		csv.SetValueType(typeJson)
		csv.Put(value, updateInKV, customSetTime)
	} else {
		csvUpdate := &StoreValue{
			value:           value,
			valueExists:     true,
			valueType:       typeJson,
			valueUpdateTime: customSetTime,
		}
		actual, loaded := parentCacheStoreValue.StoreChild(keyLastToken, csvUpdate)
		if loaded && customSetTime >= actual.valueUpdateTime {
			actual.SetValueType(typeJson)
			actual.Put(value, updateInKV, customSetTime)
		}
	}
	if updateInKV {
		cs.publishDirtyOp(customSetTime, key, OpTypePUT, walBytes)
	}
	return true
}

func (cs *Store) Destroy() {
	cs.cancel()
}

func (cs *Store) DeleteValue(key string, updateInKV bool, customDeleteTime int64) {
	if customDeleteTime < 0 {
		customDeleteTime = system.GetCurrentTimeNs()
	}
	keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false)
	if len(keyLastToken) == 0 || parentCacheStoreValue == nil {
		return
	}
	csv, ok := parentCacheStoreValue.LoadChild(keyLastToken)
	if !ok {
		return
	}
	csv.RLock("DeleteValue probe")
	exists := csv.valueExists
	csv.RUnlock("DeleteValue probe")
	if !exists {
		return
	}
	csv.Delete(updateInKV, customDeleteTime)
	if updateInKV {
		// WAL DELETE op needs no value — the OpType tells the committer
		// to issue a KV delete.
		cs.publishDirtyOp(customDeleteTime, key, OpTypeDelete, nil)
	}
}

func (cs *Store) GetKeysByPattern(pattern string) []string {
	start := time.Now()

	keys := map[string]bool{}

	// The in-memory store is the complete authoritative mirror of KV: it is
	// fully loaded at startup and nothing is ever evicted (the LRU machinery
	// was removed). Therefore every key that exists in KV also exists here,
	// and pattern resolution is a pure traversal of the in-memory tree — no
	// KV read-back is needed.
	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(pattern, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		keyWithoutLastToken := pattern[:len(pattern)-1]
		if keyLastToken == "*" {
			parentCacheStoreValue.Range(func(key, value interface{}) bool {
				childCSV := value.(*StoreValue)
				childCSV.RLock("GetKeysByPattern *")
				if childCSV.valueExists {
					keys[keyWithoutLastToken+key.(string)] = true
				}
				childCSV.RUnlock("GetKeysByPattern *")
				return true
			})
		} else if keyLastToken == ">" {
			cacheStoreValueStack := []*StoreValue{parentCacheStoreValue}
			suffixPathsStack := []string{keyWithoutLastToken}
			depthsStack := []int{0}
			for len(cacheStoreValueStack) > 0 {
				lastID := len(cacheStoreValueStack) - 1

				currentStoreValue := cacheStoreValueStack[lastID]
				currentSuffix := suffixPathsStack[lastID]
				currentDepth := depthsStack[lastID]

				cacheStoreValueStack = cacheStoreValueStack[:lastID]
				suffixPathsStack = suffixPathsStack[:lastID]
				depthsStack = depthsStack[:lastID]

				currentStoreValue.Range(func(key, value interface{}) bool {
					var newSuffix string
					if currentDepth == 0 {
						newSuffix = currentSuffix + key.(string)
					} else {
						newSuffix = currentSuffix + "." + key.(string)
					}
					ch := value.(*StoreValue)
					ch.RLock("GetKeysByPattern >")
					if ch.valueExists {
						keys[newSuffix] = true
					}
					ch.RUnlock("GetKeysByPattern >")
					cacheStoreValueStack = append(cacheStoreValueStack, value.(*StoreValue))
					suffixPathsStack = append(suffixPathsStack, newSuffix)
					depthsStack = append(depthsStack, currentDepth+1)
					return true
				})
			}
		} else {
			if c, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
				c.RLock("GetKeysByPattern one")
				exists := c.valueExists
				c.RUnlock("GetKeysByPattern one")
				if exists {
					keys[pattern] = true
				}
			}
		}
	}

	keysSlice := make([]string, len(keys))
	i := 0
	for k := range keys {
		keysSlice[i] = k
		i++
	}

	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_get_keys_by_pattern", "", []string{"id"}); err == nil {
		gaugeVec.With(prometheus.Labels{"id": cs.cacheConfig.id}).Set(float64(time.Since(start).Microseconds()))
	}

	return keysSlice
}

// createIfNotexistsOption - 0 // Do not create, 1 // Create non parent StoreValue thread safe, 2 // Create parent StoreValue thread safe
func (cs *Store) getLastKeyTokenAndItsParentCacheStoreValue(key string, createIfNotexists bool) (string, *StoreValue) {
	tokens := strings.Split(key, ".")
	currentTokenID := 0
	currentStoreLevel := cs.rootValue
	for currentTokenID < len(tokens)-1 {
		if csv, ok := currentStoreLevel.LoadChild(tokens[currentTokenID]); ok {
			currentStoreLevel = csv
		} else {
			if createIfNotexists {
				csv := StoreValue{
					value:           nil,
					valueExists:     false,
					valueUpdateTime: system.GetCurrentTimeNs(),
				}
				actual, _ := currentStoreLevel.StoreChild(tokens[currentTokenID], &csv)
				currentStoreLevel = actual
			} else {
				return "", nil
			}
		}
		currentTokenID++
	}
	return tokens[currentTokenID], currentStoreLevel
}

func (cs *Store) toStoreKey(key string) string {
	return cs.cacheConfig.kvStorePrefix + "." + key
}

func (cs *Store) fromStoreKey(key string) string {
	return strings.Replace(key, cs.cacheConfig.kvStorePrefix+".", "", 1)
}

// -------- WAL transactions ---------

type OpType = string

const (
	OpTypePUT    = "PUT"
	OpTypeDelete = "DELETE"
)

const ConsistencyKey = "__kv_consistent__"

type TransactionGenerator interface {
	// PublishTransaction publishes a complete WAL transaction as a single message.
	// ops contains all operations for this transaction.
	PublishTransaction(txID string, ops []WALOp) error
	GenerateTransactionID() string
}

func (cs *Store) SetTransactionGenerator(tg TransactionGenerator) {
	cs.transactionGenerator.Store(&tg)
}

// getTransactionGenerator atomically loads the current TransactionGenerator.
// Returns nil if not set yet.
func (cs *Store) getTransactionGenerator() TransactionGenerator {
	if p := cs.transactionGenerator.Load(); p != nil {
		return *p
	}
	return nil
}

func (cs *Store) GetStorePrefix() string {
	return cs.cacheConfig.kvStorePrefix
}

func (cs *Store) SetWALWriteEnabled(enabled bool) {
	cs.walWriteEnabled.Store(enabled)
}

// MarkOperationActive increments the in-flight counter for the given opTime.
func (cs *Store) MarkOperationActive(opTime int64) {
	v, _ := cs.activeOps.LoadOrStore(opTime, new(atomic.Int32))
	v.(*atomic.Int32).Add(1)
}

// MarkOperationDone decrements the in-flight counter for the given opTime.
// When the counter reaches zero the entry is removed from the map.
func (cs *Store) MarkOperationDone(opTime int64) {
	if v, ok := cs.activeOps.Load(opTime); ok {
		if v.(*atomic.Int32).Add(-1) == 0 {
			cs.activeOps.Delete(opTime)
		}
	}
}

// hasActiveOperationsUpTo returns true if there is at least one in-flight
// write operation whose opTime is ≤ barrierTime.
func (cs *Store) hasActiveOperationsUpTo(barrierTime int64) bool {
	found := false
	cs.activeOps.Range(func(k, _ any) bool {
		if k.(int64) <= barrierTime {
			found = true
			return false
		}
		return true
	})
	return found
}

// -----------------------------------
