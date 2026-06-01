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

// StoreValue.flags bit layout. Packs the former `valueExists bool` and
// `valueType uint8` into a single byte.
const (
	flagValueExists   uint8 = 1 << 0 // the node carries a real value (possibly a nil/empty one, e.g. an index marker key) as opposed to being a structural-only or tombstoned node
	flagValueTypeJSON uint8 = 1 << 1 // value type: set => typeJson, clear => typeByteArray
)

// --- shared node-lock pool -------------------------------------------------
//
// Instead of a 24-byte sync.RWMutex inlined in every StoreValue (millions of
// nodes), node locks come from a fixed-size shared pool indexed by the node's
// lockIdx. This is safe because NO cache code path ever holds two node locks
// simultaneously — tree navigation (LoadChild/Range) is lockless via atomic
// pointers, and every Lock/RLock is acquired and released on a single node —
// so a pool-slot collision between two unrelated nodes can only cause extra
// serialization, never deadlock. The size=1 stress test (every node sharing
// ONE mutex) is the maximum-collision proof of this.
//
// Pool size is a power of two, configurable via CACHE_LOCK_POOL_SIZE (default
// 65536 -> 1.5 MB, negligible). It must only be (re)sized while the cache is
// quiescent (process init, or a test before it builds anything).
var (
	lockPool     []sync.RWMutex
	lockPoolMask uint32
	lockIdxNext  uint32 // round-robin assignment counter
)

func init() {
	setLockPoolSize(system.GetEnvMustProceed[int]("CACHE_LOCK_POOL_SIZE", 1<<16))
}

// setLockPoolSize rounds n down to a power of two (min 1) and (re)allocates the
// pool. NOT safe to call concurrently with cache use — init time or a quiescent
// test only.
func setLockPoolSize(n int) {
	size := 1
	for size*2 <= n {
		size *= 2
	}
	lockPool = make([]sync.RWMutex, size)
	lockPoolMask = uint32(size - 1)
}

func (csv *StoreValue) nodeMutex() *sync.RWMutex {
	return &lockPool[csv.lockIdx&lockPoolMask]
}

type StoreValue struct {
	parent      *StoreValue
	keyInParent string
	value       interface{}
	// flags packs the former `valueExists bool` and `valueType uint8` into a
	// single byte. All mutations happen under the node lock (Put / Delete /
	// SetValueType / SetValueIfDoesNotExist all hold it), so the read-modify-
	// write on the shared byte never loses an update.
	flags uint8
	// lockIdx selects this node's RWMutex from the shared lockPool (instead of
	// an inline 24-byte sync.RWMutex per node). It lands in what used to be
	// struct padding after `flags`, so it costs ZERO extra bytes while removing
	// the 24-byte inline mutex (96 -> 80 byte size class). Assigned once when
	// the node joins the tree (StoreChild); unattached/default nodes use slot 0
	// (always valid). A pool slot collision can only cause extra serialization,
	// never deadlock: no cache path ever holds two node locks at once (tree
	// navigation is lockless; every Lock/RLock is taken and released on a single
	// node). See lockpool_stress_test.go for the size=1 (max-collision) proof.
	lockIdx uint32
	// Children are held in an ADAPTIVE container sized to the node's fanout.
	// The fanout analysis of a real graph dump showed 88% of non-leaf nodes
	// have exactly one child and 98.6% have <=8, so an 8-shard ShardedMap on
	// every node (8 maps + 8 RWMutex) was hugely over-provisioned.
	//
	//   - c1: the single-child fast path (88% of non-leaf nodes). The child's
	//     own keyInParent IS the key, so no separate key field is needed —
	//     one atomic pointer, ZERO extra allocation, lockless reads.
	//   - more: overflow for 2+ children. A COW-immutable parallel-slice set
	//     for small fanout, upgrading to an 8-shard ShardedMap past
	//     overflowToShardedThreshold so HOT high-fanout nodes (e.g. the
	//     "objects" enumeration node written concurrently during a rebuild)
	//     keep their lock-striping. Both pointers are atomic; readers
	//     (LoadChild/Range/storeLen) never take a lock.
	//
	// Invariant: a single-child node has c1!=nil, more==nil; a multi-child
	// node has c1==nil, more!=nil; an empty node has both nil. During the
	// 1->2 migration the writer publishes `more` (containing both children)
	// BEFORE clearing c1, so a concurrent reader may transiently see both —
	// readers tolerate that (LoadChild never early-returns on a c1 key
	// mismatch; Range/storeLen over-approximate, which is safe for their
	// ==0 / dedup-by-map callers).
	c1              atomic.Pointer[StoreValue]
	more            atomic.Pointer[childOverflow]
	valueUpdateTime int64
	// Note: the per-node `storeMutex sync.RWMutex` (24 bytes) is gone — the node
	// lock now comes from the shared lockPool, indexed by lockIdx above. See
	// lockPool below.
	//
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
	csv.nodeMutex().Lock()
}

func (csv *StoreValue) Unlock(caller string) {
	csv.nodeMutex().Unlock()
}

func (csv *StoreValue) RLock(caller string) {
	csv.nodeMutex().RLock()
}

func (csv *StoreValue) RUnlock(caller string) {
	csv.nodeMutex().RUnlock()
}

func (csv *StoreValue) ValueExists() bool {
	return csv.getValueExists()
}

// flags accessors. Reads happen under the caller's RLock/Lock; writes under
// Lock (so the read-modify-write on the shared flags byte is race-free).
func (csv *StoreValue) getValueExists() bool { return csv.flags&flagValueExists != 0 }

func (csv *StoreValue) setValueExists(v bool) {
	if v {
		csv.flags |= flagValueExists
	} else {
		csv.flags &^= flagValueExists
	}
}

func (csv *StoreValue) getValueType() uint8 {
	if csv.flags&flagValueTypeJSON != 0 {
		return typeJson
	}
	return typeByteArray
}

func (csv *StoreValue) setValueType(t uint8) {
	if t == typeJson {
		csv.flags |= flagValueTypeJSON
	} else {
		csv.flags &^= flagValueTypeJSON
	}
}

// shardCountForNewStore is the shard count used when a NON-root node's overflow
// grows past overflowToShardedThreshold and upgrades to a ShardedMap. Non-root
// high-fanout nodes (a vertex's out.to / enumeration containers) are written by
// a SINGLE writer at a time — every link.create/vertex op serializes on the
// owning vertex's graph key-mutex (operationKeysMutexLock locks selfID), so
// only concurrent READERS race the one writer. 8 shards is therefore already
// generous here; it exists mainly to spread the map, not to absorb write
// contention.
const shardCountForNewStore = 8

// rootShardCount is the shard count for the ROOT node only. The root is the one
// place that takes genuinely CONCURRENT writes to DISTINCT keys: creating
// vertex X (locks X) and vertex Y (locks Y) both insert their UID into the
// root's container, and X's and Y's locks are different — so there is no single
// mutex serializing root writes the way the per-vertex lock serializes
// everything below a vertex UID. More shards => less write contention on this
// single hottest node. The extra memory is paid on exactly ONE node, so it is
// negligible.
const rootShardCount = 64

// overflowToShardedThreshold — small overflow (COW parallel slices) is used up
// to this many children; beyond it the node upgrades to an 8-shard ShardedMap.
// 98.6% of non-leaf nodes in the real dump stay at or below 8 children, so the
// vast majority never allocate a ShardedMap at all.
const overflowToShardedThreshold = 8

// childOverflow holds a node's children when it has 2 or more. Exactly one
// representation is active:
//   - {keys, vals}: COW-immutable parallel slices, used for small fanout.
//     Writers never mutate a published slice; they build a new childOverflow
//     and atomically swap it in, so concurrent readers iterate a stable
//     snapshot without locking.
//   - sharded: an 8-shard map for hot high-fanout nodes, internally
//     synchronized (its own per-shard locks).
type childOverflow struct {
	keys    []string
	vals    []*StoreValue
	sharded *system.ShardedMap
}

// initRootSharded (re)initialises the root node's children container as a fresh
// 8-shard map and clears any single-child fast-path pointer. Used at cache
// creation and on KV rehydration (role transition). Safe to call on the root
// only — it does not preserve existing children (the caller intends a reset or
// a fresh start).
func (csv *StoreValue) initRootSharded() {
	csv.more.Store(&childOverflow{sharded: system.SharedMapMustNewHashed(rootShardCount)})
	csv.c1.Store(nil)
}

// storeLen reports the number of children. Lockless. Callers only ever compare
// the result against 0; during the transient 1->2 migration window this may
// under-report (return 1 while the node briefly holds 2), which keeps the
// ==0 emptiness check correct (it never falsely returns 0 for a non-empty node).
func (csv *StoreValue) storeLen() int {
	if csv.c1.Load() != nil {
		return 1
	}
	if m := csv.more.Load(); m != nil {
		if m.sharded != nil {
			return m.sharded.Len()
		}
		return len(m.keys)
	}
	return 0
}

func (csv *StoreValue) LoadChild(key string) (*StoreValue, bool) {
	// Single-child fast path. NOTE: on a key MISMATCH we must NOT early-return —
	// during a 1->2 migration c1 may hold the old child while the one we want is
	// already published in `more`.
	if c := csv.c1.Load(); c != nil && c.keyInParent == key {
		return c, true
	}
	if m := csv.more.Load(); m != nil {
		if m.sharded != nil {
			if v, ok := m.sharded.Get(key); ok {
				return v.(*StoreValue), true
			}
			return nil, false
		}
		for i, k := range m.keys {
			if k == key {
				return m.vals[i], true
			}
		}
	}
	return nil, false
}

func (csv *StoreValue) StoreChild(key string, child *StoreValue) (actual *StoreValue, loaded bool) {
	child.parent = csv
	child.keyInParent = key
	// Spread the node across the lock pool round-robin as it joins the tree.
	// (Correctness does not depend on this — an unassigned node uses slot 0,
	// which is always valid — it only balances pool load.)
	if child.lockIdx == 0 {
		child.lockIdx = atomic.AddUint32(&lockIdxNext, 1)
	}

	// Hot-node fast path: already sharded — use the shard locks directly, no
	// node lock (this is what preserves concurrent-write scaling on high-fanout
	// nodes; see BenchmarkContainerConcurrentInsert_*).
	if m := csv.more.Load(); m != nil && m.sharded != nil {
		a, l := m.sharded.LoadOrStore(key, child)
		return a.(*StoreValue), l
	}

	csv.nodeMutex().Lock()
	defer csv.nodeMutex().Unlock()

	// Existing single child?
	if c := csv.c1.Load(); c != nil {
		if c.keyInParent == key {
			return c, true
		}
		// 1 -> 2: publish overflow holding BOTH children, then clear c1.
		csv.more.Store(&childOverflow{
			keys: []string{c.keyInParent, key},
			vals: []*StoreValue{c, child},
		})
		csv.c1.Store(nil)
		return child, false
	}

	// Existing overflow (re-load under lock; another goroutine may have upgraded).
	if m := csv.more.Load(); m != nil {
		if m.sharded != nil {
			a, l := m.sharded.LoadOrStore(key, child)
			return a.(*StoreValue), l
		}
		for i, k := range m.keys {
			if k == key {
				return m.vals[i], true
			}
		}
		if len(m.keys)+1 > overflowToShardedThreshold {
			sm := system.SharedMapMustNewHashed(shardCountForNewStore)
			for i := range m.keys {
				sm.LoadOrStore(m.keys[i], m.vals[i])
			}
			sm.LoadOrStore(key, child)
			csv.more.Store(&childOverflow{sharded: sm})
			return child, false
		}
		nk := make([]string, len(m.keys)+1)
		nv := make([]*StoreValue, len(m.vals)+1)
		copy(nk, m.keys)
		copy(nv, m.vals)
		nk[len(m.keys)] = key
		nv[len(m.vals)] = child
		csv.more.Store(&childOverflow{keys: nk, vals: nv})
		return child, false
	}

	// Empty -> first child.
	csv.c1.Store(child)
	return child, false
}

// deleteChild removes a child by key from this node's container. Mirrors the
// locking of StoreChild: sharded deletes go straight through the shard locks;
// the small (c1/COW-slice) cases serialize on the node lock and publish a new
// immutable snapshot.
func (csv *StoreValue) deleteChild(key string) {
	if m := csv.more.Load(); m != nil && m.sharded != nil {
		m.sharded.Delete(key)
		return
	}
	csv.nodeMutex().Lock()
	defer csv.nodeMutex().Unlock()
	if c := csv.c1.Load(); c != nil && c.keyInParent == key {
		csv.c1.Store(nil)
		return
	}
	m := csv.more.Load()
	if m == nil {
		return
	}
	if m.sharded != nil {
		m.sharded.Delete(key)
		return
	}
	idx := -1
	for i, k := range m.keys {
		if k == key {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	if len(m.keys) == 1 {
		csv.more.Store(nil)
		return
	}
	nk := make([]string, 0, len(m.keys)-1)
	nv := make([]*StoreValue, 0, len(m.vals)-1)
	for i := range m.keys {
		if i == idx {
			continue
		}
		nk = append(nk, m.keys[i])
		nv = append(nv, m.vals[i])
	}
	csv.more.Store(&childOverflow{keys: nk, vals: nv})
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
	csv.setValueExists(true)
	csv.valueUpdateTime = customPutTime
	_ = updateInKV // kept in signature for callers; no longer affects local state
	_ = key        // formerly used for parent-subscriber notifications; gone with notifyUpdates
	csv.Unlock("Put")
}

func (csv *StoreValue) Delete(updateInKV bool, customDeleteTime int64) {
	csv.Lock("Delete")
	key := csv.keyInParent
	// We keep the node as a tombstone (valueExists=false, valueUpdateTime set)
	// so an in-process Put with an explicitly older customSetTime cannot
	// resurrect it through the Put LWW guard. The maintenance pass
	// (traverseCacheForMaintenance -> sweepSubtree) removes the tombstoned node
	// from the parent's container once it has no children, collapsing whole
	// tombstone cascades in a single post-order sweep.
	csv.value = nil
	csv.setValueExists(false)
	if customDeleteTime < 0 {
		customDeleteTime = system.GetCurrentTimeNs()
	}
	csv.valueUpdateTime = customDeleteTime
	_ = updateInKV // kept in signature for callers; no longer affects local state
	_ = key        // formerly used for parent-subscriber notifications; gone with notifyUpdates
	csv.Unlock("Delete")
}

// Range iterates the node's children. Lockless reads of the adaptive container.
// During the transient 1->2 migration window a child may be visited twice (once
// via c1, once via the freshly published overflow); all callers tolerate this
// (sweepSubtree re-confirms under lock before deleting; GetKeysByPattern dedups
// into a map).
func (csv *StoreValue) Range(f func(key, value interface{}) bool) {
	if c := csv.c1.Load(); c != nil {
		if !f(c.keyInParent, c) {
			return
		}
	}
	m := csv.more.Load()
	if m == nil {
		return
	}
	if m.sharded != nil {
		m.sharded.Range(func(k string, v interface{}) bool {
			return f(k, v)
		})
		return
	}
	for i := range m.keys {
		if !f(m.keys[i], m.vals[i]) {
			return
		}
	}
}

func (csv *StoreValue) SetValueType(valueType uint8) {
	// valueType is read under csv.RLock in the GetValue/GetValueJSON paths, and
	// written from both in-process ops and the async KV-watch re-apply, so the
	// write must take the lock too — otherwise it's a data race (and a torn
	// type read could mis-decode a value).
	csv.Lock("SetValueType")
	csv.setValueType(valueType)
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

	// Sweep diagnostics, updated by kvLazyWriter after each maintenance
	// pass. Exported via the cache_sweep_runs_total / cache_sweep_removed_total
	// gauges. Atomic because the kvLazyWriter goroutine is the sole writer
	// but the Prometheus snapshot is happening on another goroutine.
	totalSweepRuns    int64
	totalSweepRemoved int64
}

type maintenanceResult struct {
	valueCount   int
	removedCount int // dead children dropped from parent.store during this sweep
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
	// (released to GC after they drop it). The root identity is kept.
	cs.rootValue.initRootSharded()
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
		for _, k := range deadKeys {
			child, ok := csv.LoadChild(k)
			if !ok {
				continue
			}
			child.Lock("sweepSubtree-confirm")
			stillDead := !child.getValueExists() && child.storeLen() == 0
			child.Unlock("sweepSubtree-confirm")
			if stillDead {
				csv.deleteChild(k)
				result.removedCount++
			}
		}
	}

	// Step 3 — am I removable? Root never is.
	if csv.parent == nil {
		return false
	}
	csv.Lock("sweepSubtree-self")
	canRemove := !csv.getValueExists() && csv.storeLen() == 0
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

	// The root holds every top-level key (thousands of <domain>/<id> entries),
	// so it is the single highest-fanout node — give it the sharded overflow
	// directly instead of letting it grow through the small-overflow stages.
	cs.rootValue.initRootSharded()

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

					// Diagnostic counters — required for the leak-hunt
					// soak to distinguish "sweep never runs" from "sweep
					// runs but doesn't collect anything" from "sweep
					// runs and collects but cannot keep up". Without
					// these the only signal is the heap_objects ramp,
					// which conflates all three failure modes.
					atomic.AddInt64(&cs.totalSweepRuns, 1)
					atomic.AddInt64(&cs.totalSweepRemoved, int64(maintResult.removedCount))
					if gv, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_sweep_runs_total", "", []string{"id"}); err == nil {
						gv.With(prometheus.Labels{"id": cs.cacheConfig.id}).Set(float64(atomic.LoadInt64(&cs.totalSweepRuns)))
					}
					if gv, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_sweep_removed_total", "", []string{"id"}); err == nil {
						gv.With(prometheus.Labels{"id": cs.cacheConfig.id}).Set(float64(atomic.LoadInt64(&cs.totalSweepRemoved)))
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
			if csv.getValueExists() {
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
				switch csv.getValueType() {
				case typeByteArray:
					result = csv.value.([]byte)
				case typeJson:
					lg.Logf(lg.WarnLevel, "Value for key=%s is JSON, use GetValueJSON method", key)
					result = csv.value.(*easyjson.JSON).ToBytes()
				default:
					resultError = fmt.Errorf("unsupported value type: %d", csv.getValueType())
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
			if csv.getValueType() == typeJson {
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
			if csv.getValueType() == typeByteArray {
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
				switch csv.getValueType() {
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
					resultError = fmt.Errorf("unsupported value type: %d", csv.getValueType())
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
				switch csv.getValueType() {
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
					resultError = fmt.Errorf("unsupported value type: %d", csv.getValueType())
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
			flags:           flagValueExists,
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
		if !actual.getValueExists() && actual.value == nil {
			actual.value = newValue
			actual.setValueExists(true)
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
			flags:           flagValueExists, // typeByteArray == flagValueTypeJSON clear
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
			flags:           flagValueExists | flagValueTypeJSON,
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
	exists := csv.getValueExists()
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
				if childCSV.getValueExists() {
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
					if ch.getValueExists() {
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
				exists := c.getValueExists()
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
