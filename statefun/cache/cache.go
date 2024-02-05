// Copyright 2023 NJWS Inc.

// Foliage statefun cache package.
// Provides cache system that lives between stateful functions and NATS key/value
package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
	"sort"
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

var (
	keyValidationRegexp *regexp.Regexp = regexp.MustCompile(`^[a-zA-Z0-9=_-][a-zA-Z0-9=._-]+[a-zA-Z0-9=_-]$|^[a-zA-Z0-9=_-]*$`)
)

type KeyValue struct {
	Key   interface{}
	Value interface{}
}

type StoreValue struct {
	parent      *StoreValue
	keyInParent interface{}
	value       interface{}
	valueExists bool
	// 0 - do not purge, 1 - wait for KV update confirmation and go to state 2, 2 - purge
	purgeState int
	store      map[interface{}]*StoreValue
	// "0" if store contains all keys and all subkeys (no lru purged ones at any next level)
	storeConsistencyWithKVLossTime int64
	valueUpdateTime                int64
	storeMutex                     sync.Mutex
	notifyUpdates                  sync.Map
	syncNeeded                     bool
	syncedWithKV                   bool
}

func notifySubscriber(c chan KeyValue, key interface{}, value interface{}) {
	c <- KeyValue{Key: key, Value: value}
}

func (csv *StoreValue) Lock(caller string) {
	//lg.Logf("------- Locking '%s' by '%s'\n", csv.keyInParent, caller)
	csv.storeMutex.Lock()
	//lg.Logf(">>>>>>> Locked '%s' by '%s'\n", csv.keyInParent, caller)
}

func (csv *StoreValue) Unlock(caller string) {
	//lg.Logf(">>>>>>> Unlocking '%s' by '%s'\n", csv.keyInParent, caller)
	csv.storeMutex.Unlock()
	//lg.Logf("------- Unlocked '%s' by '%s'\n", csv.keyInParent, caller)
}

func (csv *StoreValue) GetFullKeyString() string {
	if csv.parent != nil {
		if keyStr, ok := csv.keyInParent.(string); ok {
			prefix := csv.parent.GetFullKeyString()
			if len(prefix) > 0 {
				return csv.parent.GetFullKeyString() + "." + keyStr
			}
			return keyStr
		}
	} else {
		if keyStr, ok := csv.keyInParent.(string); ok {
			return keyStr
		}
	}
	return ""
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

func (csv *StoreValue) LoadChild(key interface{}, safe bool) (*StoreValue, bool) {
	if safe {
		csv.Lock("LoadChild")
		defer csv.Unlock("LoadChild")
	}
	if v, ok := csv.store[key]; ok {
		return v, true
	}
	return nil, false
}

func (csv *StoreValue) StoreChild(key interface{}, child *StoreValue, safe bool) {
	child.Lock("StoreChild child")
	defer child.Unlock("StoreChild child")

	child.parent = csv
	child.keyInParent = key

	if safe {
		csv.Lock("StoreChild")
	}
	csv.store[key] = child
	if safe {
		csv.Unlock("StoreChild")
	}
	csv.notifyUpdates.Range(func(_, v interface{}) bool {
		notifySubscriber(v.(chan KeyValue), key, child.value)
		return true
	})
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
	csv.syncNeeded = updateInKV
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

	if !csv.valueExists && len(csv.store) == 0 && csv.syncedWithKV {
		csv.TryPurgeReady(false)
		csv.TryPurgeConfirm(false)
	}

	noNotifySubscribers := true
	csv.notifyUpdates.Range(func(_, _ interface{}) bool {
		noNotifySubscribers = false
		return false
	})
	canBeDeletedFromParent = csv.purgeState == 2 && len(csv.store) == 0 && !csv.syncNeeded && csv.syncedWithKV && noNotifySubscribers
	csv.Unlock("collectGarbage")

	if csv.parent != nil && canBeDeletedFromParent {
		csv.parent.Lock("collectGarbageParent")
		delete(csv.parent.store, csv.keyInParent)
		//lg.Logln("____________ PURGING " + fmt.Sprintln(csv.keyInParent))
		csv.parent.Unlock("collectGarbageParent")

		go csv.parent.collectGarbage()
	}
}

func (csv *StoreValue) TryPurgeReady(safe bool) bool {
	if safe {
		csv.Lock("TryPurgeReady")
		defer csv.Unlock("TryPurgeReady")
	}
	if csv.purgeState == 0 {
		csv.purgeState = 1
		return true
	}
	return false
}

func (csv *StoreValue) TryPurgeConfirm(safe bool) bool {
	if safe {
		csv.Lock("TryPurgeConfirm")
		defer csv.Unlock("TryPurgeConfirm")
	}
	if !csv.syncNeeded && csv.syncedWithKV && csv.purgeState == 1 {
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
		csv.syncNeeded = true
		csv.syncedWithKV = false
	} else {
		csv.purgeState = 2
		csv.syncNeeded = false
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
	csv.Lock("Range")
	defer csv.Unlock("Range")
	for key, value := range csv.store {
		if !f(key, value) {
			break
		}
	}
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
}

func NewCacheStore(ctx context.Context, cacheConfig *Config, js nats.JetStreamContext, kv nats.KeyValue) *Store {
	var inited atomic.Bool
	initChan := make(chan bool)
	cs := Store{
		cacheConfig: cacheConfig,
		js:          js,
		kv:          kv,
		rootValue: &StoreValue{
			parent:                         nil,
			value:                          nil,
			store:                          make(map[interface{}]*StoreValue),
			storeConsistencyWithKVLossTime: 0,
			valueExists:                    false,
			purgeState:                     0,
			syncNeeded:                     false,
			syncedWithKV:                   true,
			valueUpdateTime:                -1,
		},
		lruTresholdTime:             0,
		valuesInCache:               0,
		transactionsMutex:           &sync.Mutex{},
		getKeysByPatternFromKVMutex: &sync.Mutex{},
	}

	cs.ctx, cs.cancel = context.WithCancel(ctx)

	storeUpdatesHandler := func(cs *Store) {
		system.GlobalPrometrics.GetRoutinesCounter().Started("cache.storeUpdatesHandler")
		defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("cache.storeUpdatesHandler")
		if w, err := kv.Watch(cacheConfig.kvStorePrefix+".>", nats.IgnoreDeletes()); err == nil {
			activeKVSync := true
			for activeKVSync {
				select {
				case <-cs.ctx.Done():
					activeKVSync = false
				case entry := <-w.Updates():
					if entry != nil {
						key := cs.fromStoreKey(entry.Key())
						valueBytes := entry.Value()
						if len(valueBytes) >= 9 { // Update or delete signal from KV store
							appendFlag := valueBytes[8]
							kvRecordTime := int64(binary.BigEndian.Uint64(valueBytes[:8]))

							cacheRecordTime := cs.GetValueUpdateTime(key)
							if kvRecordTime > cacheRecordTime {
								if appendFlag == 1 {
									//lg.Logf("---CACHE_KV TF UPDATE: %s, %d, %d\n", key, kvRecordTime, appendFlag)
									cs.SetValue(key, valueBytes[9:], false, kvRecordTime, "")
								} else { // Someone else (other module) deleted a key from the cache
									//lg.Logf("---CACHE_KV TF DELETE: %s, %d, %d\n", key, kvRecordTime, appendFlag)

									//system.MsgOnErrorReturn(kv.Delete(entry.Key()))
									system.MsgOnErrorReturn(customNatsKv.DeleteKeyValueValue(cs.js, cs.kv, entry.Key()))

									//cs.rootValue.purgeReady
									//if csv := cs.getLastKeyCacheStoreValue(key); csv != nil {
									//	csv.Purge(true)
									//}
								}
							} else if kvRecordTime == cacheRecordTime { // KV confirmes update
								if appendFlag == 0 {
									//system.MsgOnErrorReturn(kv.Delete(entry.Key()))
									system.MsgOnErrorReturn(customNatsKv.DeleteKeyValueValue(cs.js, cs.kv, entry.Key()))
								}
								if csv := cs.getLastKeyCacheStoreValue(key); csv != nil {
									csv.Lock("storeUpdatesHandler")
									csv.syncedWithKV = true
									csv.TryPurgeConfirm(false)
									csv.Unlock("storeUpdatesHandler")
								}
								//lg.Logf("---CACHE_KV TF TOO OLD: %s, %d, %d\n", key, kvRecordTime, appendFlag)
							}
						} else if len(valueBytes) == 0 { // Complete delete signal from KV store
							if csv := cs.getLastKeyCacheStoreValue(key); csv != nil {
								csv.Lock("storeUpdatesHandler complete_delete")
								csv.syncedWithKV = true
								csv.TryPurgeReady(false)
								csv.TryPurgeConfirm(false)
								csv.Unlock("storeUpdatesHandler complete_delete")
							}
							//lg.Logf("---CACHE_KV EMPTY: %s\n", key)
							// Deletion notify - omitting cause value must already be deleted from the cache
						} else {
							//lg.Logf("---CACHE_KV !T!F: %s\n", key)
							lg.Logf(lg.ErrorLevel, "storeUpdatesHandler: received value without time and append flag!\n")
						}
					} else {
						if inited.CompareAndSwap(false, true) {
							close(initChan)
						}
					}
				}
			}
			system.MsgOnErrorReturn(w.Stop())
		} else {
			lg.Logf(lg.ErrorLevel, "storeUpdatesHandler kv.Watch error %s\n", err)
		}
	}
	kvLazyWriter := func(cs *Store) {
		system.GlobalPrometrics.GetRoutinesCounter().Started("cache.kvLazyWriter")
		defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("cache.kvLazyWriter")
		for {
			select {
			case <-cs.ctx.Done():
			default:
				cacheStoreValueStack := []*StoreValue{cs.rootValue}
				suffixPathsStack := []string{""}
				depthsStack := []int{0}

				lruTimes := []int64{}

				for len(cacheStoreValueStack) > 0 {
					lastID := len(cacheStoreValueStack) - 1

					currentStoreValue := cacheStoreValueStack[lastID]

					currentStoreValue.Lock("kvLazyWriter")
					lruTimes = append(lruTimes, currentStoreValue.valueUpdateTime)
					currentStoreValue.Unlock("kvLazyWriter")

					currentSuffix := suffixPathsStack[lastID]
					currentDepth := depthsStack[lastID]

					cacheStoreValueStack = cacheStoreValueStack[:lastID]
					suffixPathsStack = suffixPathsStack[:lastID]
					depthsStack = depthsStack[:lastID]

					noChildred := true
					currentStoreValue.Range(func(key, value interface{}) bool {
						noChildred = false

						var newSuffix string
						if currentDepth == 0 {
							newSuffix = currentSuffix + key.(string)
						} else {
							newSuffix = currentSuffix + "." + key.(string)
						}

						var finalBytes []byte = nil

						csvChild := value.(*StoreValue)
						var valueUpdateTime int64 = 0
						csvChild.Lock("kvLazyWriter")
						if csvChild.syncNeeded {
							valueUpdateTime = csvChild.valueUpdateTime
							timeBytes := make([]byte, 8)
							binary.BigEndian.PutUint64(timeBytes, uint64(csvChild.valueUpdateTime))
							if csvChild.valueExists {
								header := append(timeBytes, 1) // Add append flag "1"
								finalBytes = append(header, csvChild.value.([]byte)...)
							} else {
								finalBytes = append(timeBytes, 0) // Add delete flag "0"
							}
						} else {
							if csvChild.valueUpdateTime > 0 && csvChild.valueUpdateTime <= cs.lruTresholdTime && csvChild.purgeState == 0 { // Older than or equal to specific time
								// currentStoreValue locked by range no locking/unlocking needed
								currentStoreValue.ConsistencyLoss(system.GetCurrentTimeNs())
								//lg.Logf("Consistency lost for key=\"%s\" store\n", currentStoreValue.GetFullKeyString())
								//lg.Logln("Purging: " + newSuffix)
								csvChild.TryPurgeReady(false)
								csvChild.TryPurgeConfirm(false)
							}
						}
						csvChild.Unlock("kvLazyWriter")

						// Putting value into KV store ------------------
						if csvChild.syncNeeded {
							keyStr := key.(string)
							_, putErr := kv.Put(cs.toStoreKey(newSuffix), finalBytes)
							if putErr == nil {
								csvChild.Lock("kvLazyWriter")
								if valueUpdateTime == csvChild.valueUpdateTime {
									csvChild.syncNeeded = false
								}
								csvChild.Unlock("kvLazyWriter")
							} else {
								lg.Logf(lg.ErrorLevel, "Store kvLazyWriter cannot update key=%s\n: %s", keyStr, putErr)
							}
						}
						// ----------------------------------------------

						cacheStoreValueStack = append(cacheStoreValueStack, value.(*StoreValue))
						suffixPathsStack = append(suffixPathsStack, newSuffix)
						depthsStack = append(depthsStack, currentDepth+1)
						return true
					})

					if noChildred {
						currentStoreValue.collectGarbage()
					}
				}

				sort.Slice(lruTimes, func(i, j int) bool { return lruTimes[i] > lruTimes[j] })
				if len(lruTimes) > cacheConfig.lruSize {
					cs.lruTresholdTime = lruTimes[cacheConfig.lruSize-1]
				} else {
					cs.lruTresholdTime = lruTimes[len(lruTimes)-1]
				}

				/*// Debug info -----------------------------------------------------
				if cs.valuesInCache != len(lruTimes) {
					cmpr := []bool{}
					for i := 0; i < len(lruTimes); i++ {
						cmpr = append(cmpr, lruTimes[i] > 0 && lruTimes[i] <= cs.lruTresholdTime)
					}
					lg.Logf("LEFT IN CACHE: %d (%d) - %s %s\n", len(lruTimes), cs.lruTresholdTime, fmt.Sprintln(cmpr), fmt.Sprintln(lruTimes))
				}
				// ----------------------------------------------------------------*/

				cs.valuesInCache = len(lruTimes)

				if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_values", "", []string{"id"}); err == nil {
					gaugeVec.With(prometheus.Labels{"id": cs.cacheConfig.id}).Set(float64(cs.valuesInCache))
				}

				time.Sleep(100 * time.Millisecond) // Prevents too many locks and prevents too much processor time consumption
			}
		}
	}
	go storeUpdatesHandler(&cs)
	go kvLazyWriter(&cs)
	<-initChan
	return &cs
}

// key - level callback key, for e.g. "a.b.c.*"
// callbackID - unique id for this subscription
func (cs *Store) SubscribeLevelCallback(key string, callbackID string) chan KeyValue {
	if _, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true); parentCacheStoreValue != nil {
		onBufferOverflow := func() {
			lg.Logf(lg.WarnLevel, "SubscribeLevelCallback SubscriptionNotificationsBuffer overflow for key=%s!\n", key)
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
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken, true); ok {
			csv.Lock("GetValueUpdateTime")
			if csv.valueExists {
				result = csv.valueUpdateTime
			}
			csv.Unlock("GetValueUpdateTime")
		}
	}
	return result
}

func (cs *Store) GetValue(key string) ([]byte, error) {
	var result []byte = nil
	var resultError error = nil

	cacheMiss := true

	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken, true); ok {
			cacheMiss = false // Value exists in cache - no cache miss then
			csv.Lock("GetValue")
			if csv.ValueExists() {
				if bv, ok := csv.value.([]byte); ok {
					result = bv
				}
			} else { // Value was intenionally deleted and was marked so, no cache miss policy can be applied here
				resultError = fmt.Errorf("Value for for key=%s does not exist", key)
			}
			csv.Unlock("GetValue")
		}
	}

	// Cache miss -----------------------------------------
	if cacheMiss {
		if entry, err := cs.kv.Get(cs.toStoreKey(key)); err == nil {
			key := cs.fromStoreKey(entry.Key())
			valueBytes := entry.Value()
			result = valueBytes[9:]

			if len(valueBytes) >= 9 { // Updated or deleted value exists in KV store
				appendFlag := valueBytes[8]
				kvRecordTime := int64(binary.BigEndian.Uint64(valueBytes[:8]))
				if appendFlag == 1 { // Valid value exists in KV store
					cs.SetValue(key, result, false, kvRecordTime, "")
					resultError = nil
				}
			}
		} else {
			resultError = err
		}
	}
	// ----------------------------------------------------

	return result, resultError
}

func (cs *Store) GetValueAsJSON(key string) (*easyjson.JSON, error) {
	value, err := cs.GetValue(key)
	if err == nil {
		if j, ok := easyjson.JSONFromBytes(value); ok {
			return &j, nil
		}
		return nil, fmt.Errorf("Value for key=%s is not a JSON", key)
	}
	return nil, err
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

/*func (cs *Store) SetValueIfEquals(key string, newValue []byte, updateInKV bool, customSetTime int64, compareValue []byte) bool {
	if customSetTime < 0 {
		customSetTime = GetCurrentTimeNs()
	}
	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		parentCacheStoreValue.Lock("SetValueIfEquals parent")
		defer parentCacheStoreValue.Unlock("SetValueIfEquals parent")

		var csvUpdate *StoreValue = nil
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken, false); ok {
			if currentByteValue, ok := csv.value.([]byte); ok && bytes.Equal(currentByteValue, compareValue) {
				csv.Put(newValue, updateInKV, customSetTime)
				return true
			}
			return false
		} else {
			csvUpdate = &StoreValue{value: newValue, storeMutex: &sync.Mutex{}, store: make(map[interface{}]*StoreValue), storeConsistencyWithKVLossTime: 0, valueExists: true, purgeState: 0, syncNeeded: updateInKV, syncedWithKV: !updateInKV, valueUpdateTime: customSetTime}
			parentCacheStoreValue.StoreChild(keyLastToken, csvUpdate)
			return true
		}
	}
	return false
}*/

func (cs *Store) SetValueIfDoesNotExist(key string, newValue []byte, updateInKV bool, customSetTime int64) bool {
	if !keyValidationRegexp.MatchString(key) {
		return false
	}

	if customSetTime < 0 {
		customSetTime = system.GetCurrentTimeNs()
	}
	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(key, true); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		parentCacheStoreValue.Lock("SetValueIfEquals parent")
		defer parentCacheStoreValue.Unlock("SetValueIfEquals parent")

		var csvUpdate *StoreValue
		if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken, false); ok {
			if csv.value == nil && !csv.valueExists {
				csv.Put(newValue, updateInKV, customSetTime)
				return true
			}
		} else {
			csvUpdate = &StoreValue{value: newValue, store: make(map[interface{}]*StoreValue), storeConsistencyWithKVLossTime: 0, valueExists: true, purgeState: 0, syncNeeded: updateInKV, syncedWithKV: !updateInKV, valueUpdateTime: customSetTime}
			parentCacheStoreValue.StoreChild(keyLastToken, csvUpdate, false)
			return true
		}
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
			if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken, true); ok {
				//lg.Logln(">>3 " + key)
				csv.Put(value, updateInKV, customSetTime)
			} else {
				//lg.Logln(">>4 " + key)
				csvUpdate = &StoreValue{value: value, store: make(map[interface{}]*StoreValue), storeConsistencyWithKVLossTime: 0, valueExists: true, purgeState: 0, syncNeeded: updateInKV, syncedWithKV: !updateInKV, valueUpdateTime: customSetTime}
				//lg.Logln(">>5 " + key)
				parentCacheStoreValue.StoreChild(keyLastToken, csvUpdate, true)
				//lg.Logln(">>6 " + key)
			}
		}
	} else {
		if v, ok := cs.transactions.Load(transactionID); ok {
			transaction := v.(*Transaction)
			transaction.mutex.Lock()
			transaction.operators = append(transaction.operators, &TransactionOperator{operatorType: 0, key: key, value: value, updateInKV: updateInKV, customTime: customSetTime})
			transaction.mutex.Unlock()
		} else {
			lg.Logf(lg.ErrorLevel, "SetValue: transaction with id=%s doesn't exist\n", transactionID)
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
			if csv, ok := parentCacheStoreValue.LoadChild(keyLastToken, true); ok {
				if csv.valueExists {
					csv.Delete(updateInKV, customDeleteTime)
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
			lg.Logf(lg.ErrorLevel, "DeleteValue: transaction with id=%s doesn't exist\n", transactionID)
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
			for entry := range w.Updates() {
				if entry != nil && len(entry.Value()) >= 9 {
					keys[cs.fromStoreKey(entry.Key())] = true
				} else {
					break
				}
			}
		} else {
			lg.Logf(lg.ErrorLevel, "GetKeysByPattern kv.Watch error %s\n", err)
		}
		//lg.Logln("!!! GetKeysByPattern ended appendKeysFromKV")
		cs.getKeysByPatternFromKVMutex.Unlock()
	}

	if keyLastToken, parentCacheStoreValue := cs.getLastKeyTokenAndItsParentCacheStoreValue(pattern, false); len(keyLastToken) > 0 && parentCacheStoreValue != nil {
		keyWithoutLastToken := pattern[:len(pattern)-1]
		if keyLastToken == "*" {
			// Gettting time of when CSV became inconsistent with KV
			consistencyWithKVLossTime := atomic.LoadInt64(&parentCacheStoreValue.storeConsistencyWithKVLossTime)
			// ------------------------------------------------------

			childrenStoresAreConsistentWithKV := true
			parentCacheStoreValue.Range(func(key, value interface{}) bool {
				childCSV := value.(*StoreValue)
				if atomic.LoadInt64(&childCSV.storeConsistencyWithKVLossTime) > 0 { // Child store not consistent with KV
					childrenStoresAreConsistentWithKV = false
				}
				if childCSV.ValueExists() {
					keys[keyWithoutLastToken+key.(string)] = true
				}
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
					//lg.Logf("Consistency restored for key=\"%s\" store\n", parentCacheStoreValue.GetFullKeyString())
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
					if value.(*StoreValue).ValueExists() {
						keys[newSuffix] = true
					}
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
						//lg.Logf("Consistency restored for key=\"%s\" store\n", subCSV.GetFullKeyString())
						//}
						atomic.CompareAndSwapInt64(&subCSV.storeConsistencyWithKVLossTime, subCSVConsistencyWithKVLossTime, 0)
					}
				}
			}
			// ------------------------------------------------------
		} else {
			// Gettting time of when CSV became inconsistent with KV
			consistencyWithKVLossTime := atomic.LoadInt64(&parentCacheStoreValue.storeConsistencyWithKVLossTime)
			// ------------------------------------------------------

			if _, ok := parentCacheStoreValue.LoadChild(keyLastToken, true); ok {
				keys[pattern] = true
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
			lg.Logf(lg.ErrorLevel, "GetKeysByPattern: getLastExistingCacheStoreValueByKey returns nil\n")
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
		if csv, ok := currentStoreLevel.LoadChild(tokens[currentTokenID], true); ok {
			currentStoreLevel = csv
		} else {
			if createIfNotexists {
				csv := StoreValue{
					value:                          nil,
					store:                          make(map[interface{}]*StoreValue),
					storeConsistencyWithKVLossTime: 0,
					valueExists:                    false,
					purgeState:                     0,
					syncNeeded:                     false,
					syncedWithKV:                   true,
					valueUpdateTime:                system.GetCurrentTimeNs(),
				}
				currentStoreLevel.StoreChild(tokens[currentTokenID], &csv, true)
				currentStoreLevel = &csv
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
		if csv, ok := currentStoreLevel.LoadChild(tokens[currentTokenID], true); ok {
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
		if csv, ok := currentStoreLevel.LoadChild(tokens[currentTokenID], true); ok {
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
