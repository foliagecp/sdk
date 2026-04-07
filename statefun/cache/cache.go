// Foliage statefun cache package.
// Provides cache system that lives between stateful functions and NATS key/value
package cache

import (
	"context"
	"encoding/binary"
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

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

const (
	// Flags 0 (delete) and 1 (append) are deprecated
	//Deprecated
	FlagDeletedOld uint8 = 0x00
	//Deprecated
	FlagAppendOld   uint8 = 0x01
	FlagDeleted     uint8 = 0x02
	FlagBytesAppend uint8 = 0x03
	FlagJSONAppend  uint8 = 0x04
)

const (
	//gracefully shutdown
	shutdownStatusNone = iota
	shutdownStatusWaiting
	shutdownStatusReady
)

var (
	keyValidationRegexp *regexp.Regexp = regexp.MustCompile(`^[a-zA-Z0-9/=_$#@$%+-][a-zA-Z0-9/=._$#@%+-]+[a-zA-Z0-9/=_$#@%+-]$|^[a-zA-Z0-9/=_$#@%+-]*$`)
)

type KeyValue struct {
	Key   interface{}
	Value interface{}
}

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
	// 0 - do not purge, 1 - wait for KV update confirmation and go to state 2, 2 - purge
	valueType  uint8
	purgeState int
	store      *system.ShardedMap
	// "0" if store contains all keys and all subkeys (no lru purged ones at any next level)
	storeConsistencyWithKVLossTime int64
	valueUpdateTime                int64
	storeMutex                     sync.RWMutex
	notifyUpdates                  sync.Map
	syncedWithKV                   bool
}

func notifySubscriber(c chan KeyValue, key interface{}, value interface{}) {
	select {
	case c <- KeyValue{Key: key, Value: value}:
	default:
		// drop if the buffer is full to avoid deadlock
	}
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

func (csv *StoreValue) GetFullKeyString() string {
	if csv.parent != nil {
		prefix := csv.parent.GetFullKeyString()
		if len(prefix) > 0 {
			return prefix + "." + csv.keyInParent
		}
	}
	return csv.keyInParent
}

func (csv *StoreValue) ConsistencyLoss(lossTime int64) {
	if lossTime > atomic.LoadInt64(&csv.storeConsistencyWithKVLossTime) {
		atomic.StoreInt64(&csv.storeConsistencyWithKVLossTime, lossTime)
	}
	if csv.parent != nil {
		csv.parent.ConsistencyLoss(lossTime)
	}
}

func (csv *StoreValue) ValueExists() bool {
	return csv.valueExists
}

func (csv *StoreValue) LoadChild(key string) (*StoreValue, bool) {
	if v, ok := csv.store.Get(key); ok {
		return v.(*StoreValue), true
	}
	return nil, false
}

func (csv *StoreValue) StoreChild(key string, child *StoreValue) (actual *StoreValue, loaded bool) {
	child.parent = csv
	child.keyInParent = key

	a, l := csv.store.LoadOrStore(key, child)
	if l {
		return a.(*StoreValue), true
	}

	// New child — notify subscribers (outside the lock)
	csv.notifyUpdates.Range(func(_, v interface{}) bool {
		notifySubscriber(v.(chan KeyValue), key, child.value)
		return true
	})
	return child, false
}

func (csv *StoreValue) Put(value interface{}, updateInKV bool, customPutTime int64) {
	csv.Lock("Put")
	key := csv.keyInParent

	csv.value = value
	csv.valueExists = true
	csv.purgeState = 0
	if customPutTime < 0 {
		customPutTime = system.GetCurrentTimeNs()
	}
	csv.valueUpdateTime = customPutTime
	csv.syncedWithKV = !updateInKV

	if csv.parent != nil {
		csv.parent.notifyUpdates.Range(func(_, v interface{}) bool {
			notifySubscriber(v.(chan KeyValue), key, value)
			return true
		})
	}

	csv.Unlock("Put")
}

func (csv *StoreValue) collectGarbage() {
	system.GlobalPrometrics.GetRoutinesCounter().Started("cache.csv.collectGarbage")
	defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("cache.csv.collectGarbage")

	var canBeDeletedFromParent bool

	csv.Lock("collectGarbage")

	if !csv.valueExists && csv.store.Len() == 0 && csv.syncedWithKV {
		csv.TryPurgeReady()
		csv.TryPurgeConfirm()
	}

	noNotifySubscribers := true
	csv.notifyUpdates.Range(func(_, _ interface{}) bool {
		noNotifySubscribers = false
		return false
	})
	canBeDeletedFromParent = csv.purgeState == 2 && csv.store.Len() == 0 && csv.syncedWithKV && noNotifySubscribers
	csv.Unlock("collectGarbage")

	if csv.parent != nil && canBeDeletedFromParent {
		csv.parent.store.Delete(csv.keyInParent)

		go csv.parent.collectGarbage()
	}
}

func (csv *StoreValue) TryPurgeReady() bool {
	if csv.purgeState == 0 {
		csv.purgeState = 1
		return true
	}
	return false
}

func (csv *StoreValue) TryPurgeConfirm() bool {
	if csv.syncedWithKV && csv.purgeState == 1 {
		csv.purgeState = 2
		return true
	}
	return false
}

func (csv *StoreValue) Delete(updateInKV bool, customDeleteTime int64) {
	csv.Lock("Delete")
	key := csv.keyInParent
	// Cannot really remove this value from the parent's store map beacause of the time comparison when updates come from NATS KV
	csv.value = nil
	csv.valueExists = false
	if customDeleteTime < 0 {
		customDeleteTime = system.GetCurrentTimeNs()
	}
	csv.valueUpdateTime = customDeleteTime
	if updateInKV {
		csv.purgeState = 1
		csv.syncedWithKV = false
	} else {
		csv.purgeState = 2
		csv.syncedWithKV = true
	}
	csv.Unlock("Delete")

	if csv.parent != nil {
		csv.parent.notifyUpdates.Range(func(_, v interface{}) bool {
			notifySubscriber(v.(chan KeyValue), key, nil)
			return true
		})
	}
}

func (csv *StoreValue) Range(f func(key, value interface{}) bool) {
	csv.store.Range(func(k string, v interface{}) bool {
		return f(k, v)
	})
}

func (csv *StoreValue) SetValueType(valueType uint8) {
	csv.valueType = valueType
}

func (csv *StoreValue) syncState() (bool, int64, bool) {
	csv.RLock("syncState")
	ve := csv.valueExists
	vut := csv.valueUpdateTime
	sw := csv.syncedWithKV
	csv.RUnlock("syncState")
	return ve, vut, sw
}

type TransactionOperator struct {
	operatorType int // 0 - set, 1 - delete
	key          string
	value        []byte
	updateInKV   bool
	customTime   int64
}

type Transaction struct {
	operators    []*TransactionOperator
	beginCounter int
	mutex        *sync.Mutex
}

type Store struct {
	cacheConfig *Config
	js          nats.JetStreamContext
	kv          nats.KeyValue
	ctx         context.Context
	cancel      context.CancelFunc

	rootValue       *StoreValue
	lruTresholdTime int64
	valuesInCache   int

	transactions                sync.Map
	transactionsMutex           *sync.Mutex
	getKeysByPatternFromKVMutex *sync.Mutex

	// walWriteEnabled - true for active instance
	// only active instances can write to WAL streams
	walWriteEnabled      atomic.Bool
	transactionGenerator TransactionGenerator

	// activeOps tracks in-flight write operations by opTime.
	// key: int64 opTime, value: *atomic.Int32 (reference count).
	// kvLazyWriter waits until all operations with opTime ≤ barrierTime
	// have finished before forming a WAL transaction.
	activeOps sync.Map

	// pendingTxs tracks WAL transactions that have ops published but no commit yet.
	// key: int64 (timestamp used as txID), value: *atomic.Int32 (ops count).
	// kvLazyWriter iterates this map, checks if all operations ≤ txTime are done,
	// and publishes commits — O(pending_count) instead of O(all_nodes) DFS.
	pendingTxs sync.Map

	// walPublishCh is the queue for the single WAL publisher goroutine.
	// publishDirtyOp enqueues ops here; the publisher goroutine drains it
	// and calls PublishOperation sequentially, preserving write order.
	walPublishCh chan walPublishRequest

	// pendingPublishes tracks how many ops are queued but not yet published
	// per txTime. Incremented when enqueuing, decremented after publish.
	// kvLazyWriter won't commit a TX until its count reaches 0.
	pendingPublishes sync.Map

	//write barrier state
	backupBarrierTimestamp   int64
	backupBarrierStatus      int32 // 0=unlocked, 1=locking, 2=locked
	backupBarrierLastChecked int64
	Synced                   chan struct{}
}

type maintenanceResult struct {
	lruTimes                     []int64
	allBeforeBackupBarrierSynced bool
}

type walPublishRequest struct {
	writeTime int64
	key       string
	opType    OpType
	data      []byte
}

// publishDirtyOp enqueues a WAL publish request to the publisher goroutine.
// Increments pendingPublishes BEFORE enqueue to prevent premature commit.
func (cs *Store) publishDirtyOp(writeTime int64, key string, opType OpType, finalBytes []byte) {
	if cs.transactionGenerator == nil || !cs.walWriteEnabled.Load() {
		return
	}
	if writeTime <= 0 {
		return
	}

	// Mark pending publish BEFORE enqueue — prevents premature commit
	pp, _ := cs.pendingPublishes.LoadOrStore(writeTime, new(atomic.Int32))
	pp.(*atomic.Int32).Add(1)

	cs.walPublishCh <- walPublishRequest{
		writeTime: writeTime,
		key:       key,
		opType:    opType,
		data:      finalBytes,
	}
}

// walPublisher is the single goroutine that drains walPublishCh and publishes
// WAL operations sequentially. Preserves write order and avoids goroutine sprawl.
func (cs *Store) walPublisher() {
	for req := range cs.walPublishCh {
		txID := strconv.FormatInt(req.writeTime, 10)
		storeKey := cs.toStoreKey(req.key)

		if err := cs.transactionGenerator.PublishOperation(txID, req.writeTime, req.opType, storeKey, req.data); err != nil {
			lg.Logf(lg.ErrorLevel, "walPublisher: failed for key=%s: %s", req.key, err)
		} else {
			// Register pending transaction (increment ops count)
			v, _ := cs.pendingTxs.LoadOrStore(req.writeTime, new(atomic.Int32))
			v.(*atomic.Int32).Add(1)
		}

		// Unmark pending publish
		if v, ok := cs.pendingPublishes.Load(req.writeTime); ok {
			if v.(*atomic.Int32).Add(-1) == 0 {
				cs.pendingPublishes.Delete(req.writeTime)
			}
		}
	}
}

// serializeForWAL builds the WAL entry bytes: [8-byte timestamp][1-byte flag][payload]
func serializeForWAL(writeTime int64, flag uint8, data []byte) []byte {
	timeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timeBytes, uint64(writeTime))
	header := append(timeBytes, flag)
	return append(header, data...)
}

// traverseCacheForMaintenance performs a DFS without collecting/publishing WAL
// ops. It only gathers LRU times, runs GC on empty leaves, and checks the
// backup barrier. Called infrequently (every N iterations) by kvLazyWriter.
func (cs *Store) traverseCacheForMaintenance() *maintenanceResult {
	result := &maintenanceResult{
		lruTimes:                     []int64{},
		allBeforeBackupBarrierSynced: true,
	}

	backupBarrierTimestamp, backupBarrierStatus := cs.getBackupBarrierState()

	cacheStoreValueStack := []*StoreValue{cs.rootValue}
	for len(cacheStoreValueStack) > 0 {
		lastID := len(cacheStoreValueStack) - 1
		currentStoreValue := cacheStoreValueStack[lastID]
		cacheStoreValueStack = cacheStoreValueStack[:lastID]

		currentStoreValue.RLock("maintenance")
		result.lruTimes = append(result.lruTimes, currentStoreValue.valueUpdateTime)
		currentStoreValue.RUnlock("maintenance")

		noChildren := true
		currentStoreValue.Range(func(key, value interface{}) bool {
			noChildren = false
			csvChild := value.(*StoreValue)

			if backupBarrierStatus == BackupBarrierStatusLocking {
				ve, vut, sw := csvChild.syncState()
				if ve && vut > 0 && vut <= backupBarrierTimestamp {
					if !sw {
						result.allBeforeBackupBarrierSynced = false
					}
				}
			}

			// LRU purge check
			csvChild.Lock("maintenance")
			if csvChild.valueUpdateTime > 0 && csvChild.valueUpdateTime <= cs.lruTresholdTime && csvChild.purgeState == 0 {
				csvChild.parent.ConsistencyLoss(system.GetCurrentTimeNs())
				csvChild.TryPurgeReady()
				csvChild.TryPurgeConfirm()
			}
			csvChild.Unlock("maintenance")

			cacheStoreValueStack = append(cacheStoreValueStack, csvChild)
			return true
		})

		if noChildren {
			currentStoreValue.collectGarbage()
		}
	}

	return result
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
			parent:                         nil,
			value:                          nil,
			store:                          system.SharedMapMustNewHashed(8),
			storeConsistencyWithKVLossTime: 0,
			valueExists:                    false,
			purgeState:                     0,
						syncedWithKV:                   true,
			valueUpdateTime:                -1,
		},
		lruTresholdTime:             0,
		valuesInCache:               0,
		transactionsMutex:           &sync.Mutex{},
		getKeysByPatternFromKVMutex: &sync.Mutex{},

		walPublishCh: make(chan walPublishRequest, 10000),

		//barrier init
		backupBarrierTimestamp:   0,
		backupBarrierStatus:      BackupBarrierStatusUnlocked,
		backupBarrierLastChecked: 0,
		Synced:                   make(chan struct{}),
	}

	cs.ctx, cs.cancel = context.WithCancel(ctx)

	// default - can not publish to WAL
	cs.walWriteEnabled.Store(false)

	storeUpdatesHandler := func(cs *Store) {
		system.GlobalPrometrics.GetRoutinesCounter().Started("cache.storeUpdatesHandler")
		defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("cache.storeUpdatesHandler")
		if w, err := kv.Watch(cacheConfig.kvStorePrefix+".>", nats.IgnoreDeletes()); err == nil {
			defer func() {
				system.MsgOnErrorReturn(w.Stop())
			}()
			activeKVSync := true
			for activeKVSync {
				select {
				case <-cs.ctx.Done():
					activeKVSync = false
				case entry, ok := <-w.Updates():
					if !ok {
						le.Warnf(ctx, "storeUpdatesHandler: KV watcher channel closed unexpectedly")
						activeKVSync = false
						break
					}
					if entry != nil {
						key := cs.fromStoreKey(entry.Key())
						valueBytes := entry.Value()
						if len(valueBytes) >= 9 { // Update or delete signal from KV store
							appendFlag := valueBytes[8]
							kvRecordTime := int64(binary.BigEndian.Uint64(valueBytes[:8]))

							cacheRecordTime := cs.GetValueUpdateTime(key)
							if kvRecordTime > cacheRecordTime {
								switch appendFlag {
								case FlagAppendOld, FlagBytesAppend:
									//lg.Logf("---CACHE_KV TF UPDATE: %s, %d, %d", key, kvRecordTime, appendFlag)
									cs.SetValue(key, valueBytes[9:], false, kvRecordTime, "")
								case FlagJSONAppend:
									if json, ok := easyjson.JSONFromBytes(valueBytes[9:]); ok {
										cs.SetValueJSON(key, &json, false, kvRecordTime, "")
									} else {
										le.Errorf(ctx, "Failed to parse JSON for key=%s", key)
									}
								default:
									// Someone else (other module) deleted a key from the cache
									//lg.Logf("---CACHE_KV TF DELETE: %s, %d, %d", key, kvRecordTime, appendFlag)

									//system.MsgOnErrorReturn(kv.Delete(entry.Key()))
									system.MsgOnErrorReturn(customNatsKv.KVDelete(cs.js, cs.kv, entry.Key()))

									//cs.rootValue.purgeReady
									//if csv := cs.getLastKeyCacheStoreValue(key); csv != nil {
									//	csv.Purge(true)
									//}
								}
							} else if kvRecordTime == cacheRecordTime { // KV confirms update
								if appendFlag == FlagDeletedOld || appendFlag == FlagDeleted {
									//system.MsgOnErrorReturn(kv.Delete(entry.Key()))
									system.MsgOnErrorReturn(customNatsKv.KVDelete(cs.js, cs.kv, entry.Key()))
								}
								if csv := cs.getLastKeyCacheStoreValue(key); csv != nil {
									csv.Lock("storeUpdatesHandler")
									csv.syncedWithKV = true
									csv.TryPurgeConfirm()
									csv.Unlock("storeUpdatesHandler")
								}
								//lg.Logf("---CACHE_KV TF TOO OLD: %s, %d, %d", key, kvRecordTime, appendFlag)
							}
						} else if len(valueBytes) == 0 { // Complete delete signal from KV store
							if csv := cs.getLastKeyCacheStoreValue(key); csv != nil {
								csv.Lock("storeUpdatesHandler complete_delete")
								csv.syncedWithKV = true
								csv.TryPurgeReady()
								csv.TryPurgeConfirm()
								csv.Unlock("storeUpdatesHandler complete_delete")
							}
							//lg.Logf("---CACHE_KV EMPTY: %s", key)
							// Deletion notify - omitting cause value must already be deleted from the cache
						} else {
							//lg.Logf("---CACHE_KV !T!F: %s", key)
							le.Error(ctx, "storeUpdatesHandler: received value without time and append flag!")
						}
					} else {
						if inited.CompareAndSwap(false, true) {
							close(initChan)
						}
					}
				}
			}
		} else {
			le.Errorf(ctx, "storeUpdatesHandler kv.Watch error %s", err)
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

			if cs.transactionGenerator == nil {
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

				// FAST PATH: commit pending transactions in time order.
				// Ops were already published at write time by publishDirtyOp.
				// Collect pending txTimes, sort ascending, commit sequentially.
				// If a TX can't be committed (active ops), stop — preserve ordering.
				var pendingTimes []int64
				cs.pendingTxs.Range(func(k, _ any) bool {
					pendingTimes = append(pendingTimes, k.(int64))
					return true
				})
				if len(pendingTimes) > 0 {
					sort.Slice(pendingTimes, func(i, j int) bool { return pendingTimes[i] < pendingTimes[j] })
					for _, txTime := range pendingTimes {
						if cs.HasActiveOperationsUpTo(txTime) {
							break // earlier ops still in-flight — stop to preserve order
						}
						if cs.hasPendingPublishes(txTime) {
							break // ops still being published async — stop to preserve order
						}
						// Backup barrier: don't commit transactions newer than barrier timestamp.
						// Ops are already in WAL stream but without commit they won't be applied to KV.
						if err := cs.checkBackupBarrierInfoBeforeWrite(txTime); err != nil {
							break // all subsequent txs are newer — stop
						}
						v, ok := cs.pendingTxs.Load(txTime)
						if !ok {
							continue
						}
						opsCount := int(v.(*atomic.Int32).Load())
						txID := strconv.FormatInt(txTime, 10)
						le.Tracef(ctx, "Committing pending tx=%s with %d ops", txID, opsCount)
						if err := cs.transactionGenerator.PublishCommit(txID, opsCount); err != nil {
							le.Errorf(ctx, "kvLazyWriter: cannot publish WAL commit for tx=%s: %s", txID, err)
							break // retry next iteration, preserve order
						}
						cs.pendingTxs.Delete(txTime)
					}
				}

				// SLOW PATH: maintenance DFS — LRU, GC, backup barrier (every N iterations)
				maintenanceCounter++
				if maintenanceCounter >= maintenanceInterval || backupBarrierStatus == BackupBarrierStatusLocking {
					maintenanceCounter = 0
					maintResult := cs.traverseCacheForMaintenance()

					if backupBarrierStatus == BackupBarrierStatusLocking {
						if maintResult.allBeforeBackupBarrierSynced {
							cs.markCacheReadyForBackup()
						}
					}

					sort.Slice(maintResult.lruTimes, func(i, j int) bool { return maintResult.lruTimes[i] > maintResult.lruTimes[j] })
					if len(maintResult.lruTimes) > cacheConfig.lruSize {
						cs.lruTresholdTime = maintResult.lruTimes[cacheConfig.lruSize-1]
					} else if len(maintResult.lruTimes) > 0 {
						cs.lruTresholdTime = maintResult.lruTimes[len(maintResult.lruTimes)-1]
					}

					cs.valuesInCache = len(maintResult.lruTimes)

					if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_values", "", []string{"id"}); err == nil {
						gaugeVec.With(prometheus.Labels{"id": cs.cacheConfig.id}).Set(float64(cs.valuesInCache))
					}
				}

				time.Sleep(time.Duration(cacheConfig.lazyWriterRepeatDelayMkS) * time.Microsecond)
			}

			if shutdownStatus == shutdownStatusWaiting {
				// Wait for all in-flight publishes to complete
				allPublished := true
				cs.pendingPublishes.Range(func(_, _ any) bool {
					allPublished = false
					return false
				})
				if !allPublished {
					time.Sleep(1 * time.Millisecond)
					continue
				}

				// Commit all remaining pending transactions — ops are already in WAL.
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
						opsCount := int(v.(*atomic.Int32).Load())
						txID := strconv.FormatInt(txTime, 10)
						le.Debugf(ctx, "Shutdown: committing pending tx=%s with %d ops", txID, opsCount)
						if err := cs.transactionGenerator.PublishCommit(txID, opsCount); err != nil {
							le.Errorf(ctx, "Shutdown: cannot publish WAL commit for tx=%s: %s", txID, err)
						}
						cs.pendingTxs.Delete(txTime)
					}
				} else {
					shutdownStatus = shutdownStatusReady
				}
			}
		}
	}
	go storeUpdatesHandler(&cs)
	go cs.walPublisher()
	go kvLazyWriterWithWAL(&cs)
	<-initChan
	return &cs
}

// key - level callback key, for e.g. "a.b.c.*"
// callbackID - unique id for this subscription
func (cs *Store) SubscribeLevelCallback(key string, callbackID string) chan KeyValue {
	if _, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true); parentCacheStoreValue != nil {
		onBufferOverflow := func() {
			lg.Logf(lg.WarnLevel, "SubscribeLevelCallback SubscriptionNotificationsBuffer overflow for key=%s!", key)
		}
		callbackChannelIn, callbackChannelOut := system.CreateDimSizeChannel[KeyValue](cs.cacheConfig.levelSubscriptionNotificationsBufferMaxSize, onBufferOverflow)
		parentCacheStoreValue.notifyUpdates.Store(callbackID, callbackChannelIn)

		return callbackChannelOut
	}
	return nil
}

func (cs *Store) UnsubscribeLevelCallback(key string, callbackID string) {
	if _, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); parentCacheStoreValue != nil {
		if v, ok := parentCacheStoreValue.notifyUpdates.Load(callbackID); ok {
			if callbackChannelIn, ok := v.(chan KeyValue); ok {
				close(callbackChannelIn)
			}
		}
		parentCacheStoreValue.notifyUpdates.Delete(callbackID)
	}
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
					lg.Logf(lg.WarnLevel, "Value for key=%s is []byte, converting to JSON", key)
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

func (cs *Store) TransactionBegin(transactionID string) {
	if v, ok := cs.transactions.Load(transactionID); ok {
		transaction := v.(*Transaction)
		transaction.mutex.Lock()
		transaction.beginCounter++
		transaction.mutex.Unlock()
	} else {
		cs.transactions.Store(transactionID, &Transaction{operators: []*TransactionOperator{}, beginCounter: 1, mutex: &sync.Mutex{}})
	}
}

func (cs *Store) TransactionEnd(transactionID string) {
	if v, ok := cs.transactions.Load(transactionID); ok {
		transaction := v.(*Transaction)
		transaction.mutex.Lock()
		transaction.beginCounter--
		if transaction.beginCounter == 0 {
			cs.transactionsMutex.Lock()
			for _, op := range transaction.operators {
				switch op.operatorType {
				case 0:
					cs.SetValue(op.key, op.value, op.updateInKV, op.customTime, "")
				case 1:
					cs.DeleteValue(op.key, op.updateInKV, op.customTime, "")
				}
			}
			cs.transactionsMutex.Unlock()
			cs.transactions.Delete(transactionID)
		}
		transaction.mutex.Unlock()
	}
}

func (cs *Store) SetValueIfDoesNotExist(key string, newValue []byte, updateInKV bool, customSetTime int64) bool {
	if keyLastToken, parent := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true); len(keyLastToken) > 0 && parent != nil {
		candidate := &StoreValue{
			value: newValue, store: system.SharedMapMustNewHashed(8),
			valueExists: true, purgeState: 0,
			syncedWithKV: !updateInKV,
			valueUpdateTime: customSetTime,
		}
		actual, loaded := parent.StoreChild(keyLastToken, candidate)
		if !loaded {
			if updateInKV {
				cs.publishDirtyOp(customSetTime, key, OpTypePUT, serializeForWAL(customSetTime, FlagBytesAppend, newValue))
			}
			return true // created for the first time
		}

		// Already exists — set only if "empty"
		actual.Lock("SetValueIfDoesNotExist")
		if !actual.valueExists && actual.value == nil {
			actual.value = newValue
			actual.valueExists = true
			actual.purgeState = 0
			actual.valueUpdateTime = customSetTime
						actual.syncedWithKV = !updateInKV

			if actual.parent != nil {
				actual.parent.notifyUpdates.Range(func(_, v interface{}) bool {
					notifySubscriber(v.(chan KeyValue), keyLastToken, actual.value)
					return true
				})
			}
			actual.Unlock("SetValueIfDoesNotExist")
			if updateInKV {
				cs.publishDirtyOp(customSetTime, key, OpTypePUT, serializeForWAL(customSetTime, FlagBytesAppend, newValue))
			}
			return true
		}
		actual.Unlock("SetValueIfDoesNotExist")
	}
	return false
}

func (cs *Store) SetValue(key string, value []byte, updateInKV bool, customSetTime int64, transactionID string) bool {
	if !keyValidationRegexp.MatchString(key) {
		return false
	}

	if customSetTime < 0 {
		customSetTime = system.GetCurrentTimeNs()
	}
	if len(transactionID) == 0 {
		//lg.Logln(">>1 " + key)
		if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
			//lg.Logln(">>2 " + key)
			var csvUpdate *StoreValue
			if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
				//lg.Logln(">>3 " + key)
				csv.SetValueType(typeByteArray)
				csv.Put(value, updateInKV, customSetTime)
				if updateInKV {
					cs.publishDirtyOp(customSetTime, key, OpTypePUT, serializeForWAL(customSetTime, FlagBytesAppend, value))
				}
			} else {
				csvUpdate = &StoreValue{
					value: value, store: system.SharedMapMustNewHashed(8),
					valueExists: true, purgeState: 0, valueType: typeByteArray,
					syncedWithKV: !updateInKV,
					valueUpdateTime: customSetTime,
				}
				actual, loaded := parentCacheStoreValue.StoreChild(keyLastToken, csvUpdate)
				if loaded && customSetTime >= actual.valueUpdateTime {
					actual.SetValueType(typeByteArray)
					actual.Put(value, updateInKV, customSetTime)
				}
				if updateInKV {
					cs.publishDirtyOp(customSetTime, key, OpTypePUT, serializeForWAL(customSetTime, FlagBytesAppend, value))
				}
			}
		}
	} else {
		if v, ok := cs.transactions.Load(transactionID); ok {
			transaction := v.(*Transaction)
			transaction.mutex.Lock()
			transaction.operators = append(transaction.operators, &TransactionOperator{operatorType: 0, key: key, value: value, updateInKV: updateInKV, customTime: customSetTime})
			transaction.mutex.Unlock()
		} else {
			lg.Logf(lg.ErrorLevel, "SetValue: transaction with id=%s doesn't exist", transactionID)
		}
	}
	return true
}

func (cs *Store) SetValueJSON(key string, originValue *easyjson.JSON, updateInKV bool, customSetTime int64, transactionID string) bool {
	if !keyValidationRegexp.MatchString(key) {
		return false
	}

	if customSetTime < 0 {
		customSetTime = system.GetCurrentTimeNs()
	}

	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		value := originValue.Clone().GetPtr()
		// Pre-serialize before any locks — reuse for WAL publish
		var walBytes []byte
		if updateInKV {
			walBytes = serializeForWAL(customSetTime, FlagJSONAppend, originValue.ToBytes())
		}
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
			csv.SetValueType(typeJson)
			csv.Put(value, updateInKV, customSetTime)
			if updateInKV {
				cs.publishDirtyOp(customSetTime, key, OpTypePUT, walBytes)
			}
		} else {
			csvUpdate := &StoreValue{
				value:           value,
				store:           system.SharedMapMustNewHashed(8),
				valueExists:     true,
				valueType:       typeJson,
				purgeState:      0,
								syncedWithKV:    !updateInKV,
				valueUpdateTime: customSetTime,
			}
			actual, loaded := parentCacheStoreValue.StoreChild(keyLastToken, csvUpdate)
			if loaded && customSetTime >= actual.valueUpdateTime {
				actual.SetValueType(typeJson)
				actual.Put(value, updateInKV, customSetTime)
			}
			if updateInKV {
				cs.publishDirtyOp(customSetTime, key, OpTypePUT, walBytes)
			}
		}
	}

	return true
}

func (cs *Store) Destroy() {
	cs.cancel()
}

func (cs *Store) DeleteValue(key string, updateInKV bool, customDeleteTime int64, transactionID string) {
	if customDeleteTime < 0 {
		customDeleteTime = system.GetCurrentTimeNs()
	}
	if len(transactionID) == 0 {
		if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
			if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
				csv.RLock("DeleteValue probe")
				exists := csv.valueExists
				csv.RUnlock("DeleteValue probe")
				if exists {
					csv.Delete(updateInKV, customDeleteTime)
					if updateInKV {
						cs.publishDirtyOp(customDeleteTime, key, OpTypeDelete, serializeForWAL(customDeleteTime, FlagDeleted, nil))
					}
				}
			}
		}
	} else {
		if v, ok := cs.transactions.Load(transactionID); ok {
			transaction := v.(*Transaction)
			transaction.mutex.Lock()
			transaction.operators = append(transaction.operators, &TransactionOperator{operatorType: 1, key: key, value: nil, updateInKV: updateInKV, customTime: customDeleteTime})
			transaction.mutex.Unlock()
		} else {
			lg.Logf(lg.ErrorLevel, "DeleteValue: transaction with id=%s doesn't exist", transactionID)
		}
	}
}

func (cs *Store) GetKeysByPattern(pattern string) []string {
	start := time.Now()

	keys := map[string]bool{}

	appendKeysFromKV := func() {
		cs.getKeysByPatternFromKVMutex.Lock()
		//lg.Logln("!!! GetKeysByPattern started appendKeysFromKV")
		if w, err := cs.kv.Watch(cs.toStoreKey(pattern), nats.IgnoreDeletes()); err == nil {
			defer func() { _ = w.Stop() }()
			for entry := range w.Updates() {
				if entry != nil && len(entry.Value()) >= 9 {
					keys[cs.fromStoreKey(entry.Key())] = true
				} else {
					break
				}
			}
		} else {
			lg.Logf(lg.ErrorLevel, "GetKeysByPattern kv.Watch error %s", err)
		}
		//lg.Logln("!!! GetKeysByPattern ended appendKeysFromKV")
		cs.getKeysByPatternFromKVMutex.Unlock()
	}

	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(pattern, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		keyWithoutLastToken := pattern[:len(pattern)-1]
		if keyLastToken == "*" {
			// Getting time of when CSV became inconsistent with KV
			consistencyWithKVLossTime := atomic.LoadInt64(&parentCacheStoreValue.storeConsistencyWithKVLossTime)
			// ------------------------------------------------------

			childrenStoresAreConsistentWithKV := true
			parentCacheStoreValue.Range(func(key, value interface{}) bool {
				childCSV := value.(*StoreValue)
				if atomic.LoadInt64(&childCSV.storeConsistencyWithKVLossTime) > 0 { // Child store not consistent with KV
					childrenStoresAreConsistentWithKV = false
				}
				childCSV.RLock("GetKeysByPattern *")
				if childCSV.valueExists {
					keys[keyWithoutLastToken+key.(string)] = true
				}
				childCSV.RUnlock("GetKeysByPattern *")
				return true
			})

			// If CSV is inconsistent with KV -----------------------
			if consistencyWithKVLossTime > 0 {
				keysCountBefore := len(keys)
				appendKeysFromKV()
				keysCountAfter := len(keys)

				if keysCountBefore == keysCountAfter && childrenStoresAreConsistentWithKV {
					// Restore consistency if relevant
					//if atomic.CompareAndSwapInt64(&parentCacheStoreValue.storeConsistencyWithKVLossTime, consistencyWithKVLossTime, 0) {
					//lg.Logf("Consistency restored for key=\"%s\" store", parentCacheStoreValue.GetFullKeyString())
					//}
					atomic.CompareAndSwapInt64(&parentCacheStoreValue.storeConsistencyWithKVLossTime, consistencyWithKVLossTime, 0)
				}
			}
			// ------------------------------------------------------
		} else if keyLastToken == ">" {
			// Remembering all CSVs on all sub levels which are inconsistent with KV ----
			allSubCSVsToConsistencyWithKVLossTime := map[*StoreValue]int64{}
			inconsistencyWithKVExistsOnSubLevel := false
			// --------------------------------------------------------------------------

			cacheStoreValueStack := []*StoreValue{parentCacheStoreValue}
			suffixPathsStack := []string{keyWithoutLastToken}
			depthsStack := []int{0}
			for len(cacheStoreValueStack) > 0 {
				lastID := len(cacheStoreValueStack) - 1

				currentStoreValue := cacheStoreValueStack[lastID]
				currentSuffix := suffixPathsStack[lastID]
				currentDepth := depthsStack[lastID]

				storeConsistencyWithKVLossTime := atomic.LoadInt64(&currentStoreValue.storeConsistencyWithKVLossTime)
				if storeConsistencyWithKVLossTime > 0 {
					allSubCSVsToConsistencyWithKVLossTime[currentStoreValue] = storeConsistencyWithKVLossTime
					inconsistencyWithKVExistsOnSubLevel = true
				}

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

			// If CSV is inconsistent with KV -----------------------
			if inconsistencyWithKVExistsOnSubLevel {
				keysCountBefore := len(keys)
				appendKeysFromKV()
				keysCountAfter := len(keys)

				if keysCountBefore == keysCountAfter {
					for subCSV, subCSVConsistencyWithKVLossTime := range allSubCSVsToConsistencyWithKVLossTime {
						// Restore consistency if relevant
						//if atomic.CompareAndSwapInt64(&subCSV.storeConsistencyWithKVLossTime, subCSVConsistencyWithKVLossTime, 0) {
						//lg.Logf("Consistency restored for key=\"%s\" store", subCSV.GetFullKeyString())
						//}
						atomic.CompareAndSwapInt64(&subCSV.storeConsistencyWithKVLossTime, subCSVConsistencyWithKVLossTime, 0)
					}
				}
			}
			// ------------------------------------------------------
		} else {
			// Getting time of when CSV became inconsistent with KV
			consistencyWithKVLossTime := atomic.LoadInt64(&parentCacheStoreValue.storeConsistencyWithKVLossTime)
			// ------------------------------------------------------

			if c, ok := parentCacheStoreValue.LoadChild(keyLastToken); ok {
				c.RLock("GetKeysByPattern one")
				exists := c.valueExists
				c.RUnlock("GetKeysByPattern one")
				if exists {
					keys[pattern] = true
				}
			}

			// If CSV is inconsistent with KV -----------------------
			if consistencyWithKVLossTime > 0 {
				appendKeysFromKV()
				// Cannot restore consistency here cause not checking all keys from KV for this CSV level
			}
			// ------------------------------------------------------
		}
	} else { // No CSV at the level corresponding to the pattern at all
		if ancestorCacheStoreValue := cs.getLastExistingCacheStoreValueByKey(pattern); ancestorCacheStoreValue != nil {
			if atomic.LoadInt64(&ancestorCacheStoreValue.storeConsistencyWithKVLossTime) > 0 {
				appendKeysFromKV()
				// Cannot restore consistency here
			}
		} else {
			lg.Logf(lg.ErrorLevel, "GetKeysByPattern: getLastExistingCacheStoreValueByKey returns nil")
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
					value:                          nil,
					store:                          system.SharedMapMustNewHashed(8),
					storeConsistencyWithKVLossTime: 0,
					valueExists:                    false,
					purgeState:                     0,
										syncedWithKV:                   true,
					valueUpdateTime:                system.GetCurrentTimeNs(),
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

func (cs *Store) getLastExistingCacheStoreValueByKey(key string) *StoreValue {
	tokens := strings.Split(key, ".")
	currentTokenID := 0
	currentStoreLevel := cs.rootValue

	for currentTokenID < len(tokens)-1 {
		if csv, ok := currentStoreLevel.LoadChild(tokens[currentTokenID]); ok {
			currentStoreLevel = csv
		} else {
			break
		}
		currentTokenID++
	}

	return currentStoreLevel
}

func (cs *Store) getLastKeyCacheStoreValue(key string) *StoreValue {
	tokens := strings.Split(key, ".")
	currentTokenID := 0
	currentStoreLevel := cs.rootValue
	for currentTokenID < len(tokens) {
		if csv, ok := currentStoreLevel.LoadChild(tokens[currentTokenID]); ok {
			currentStoreLevel = csv
		} else {
			return nil
		}
		currentTokenID++
	}
	return currentStoreLevel
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
	PublishOperation(txID string, opTime int64, opType OpType, key string, value []byte) error
	PublishCommit(txID string, opsCount int) error
	GenerateTransactionID() string
}

func (cs *Store) SetTransactionGenerator(tg TransactionGenerator) {
	cs.transactionGenerator = tg
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

// hasPendingPublishes returns true if there are still in-flight WAL publish
// requests for the given txTime (ops queued but not yet sent to NATS).
func (cs *Store) hasPendingPublishes(txTime int64) bool {
	if v, ok := cs.pendingPublishes.Load(txTime); ok {
		return v.(*atomic.Int32).Load() > 0
	}
	return false
}

// HasActiveOperationsUpTo returns true if there is at least one in-flight
// write operation whose opTime is ≤ barrierTime.
func (cs *Store) HasActiveOperationsUpTo(barrierTime int64) bool {
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
