// Copyright 2023 NJWS Inc.

package statefun

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

var (
	keyValueMutexOperationMutex *sync.Mutex = &sync.Mutex{}
)

func KeyMutexLock(runtime *Runtime, key string, errorOnLocked bool, debugCaller ...string) (uint64, error) {
	caller := strings.Join(debugCaller, "-")
	kv := runtime.kv
	mutexResetLock := func(keyMutex string, now int64) (uint64, error) {
		lockRevisionId, err := kv.Put(keyMutex, system.Int64ToBytes(now))
		if err == nil {
			fmt.Printf("============== %s: Locked %s\n", caller, keyMutex)
			return lockRevisionId, nil
		} else {
			return 0, err
		}
	}
	mutexMereLock := func(entry nats.KeyValueEntry, now int64) (uint64, error) {
		// Try to lock mutex by updating it with current time value using revision obtained during last Get
		lockRevisionId, err := kv.Update(entry.Key(), system.Int64ToBytes(now), entry.Revision())
		if err != nil { // If no error appeared
			if strings.Contains(err.Error(), "nats: wrong last sequence") { // If error "wrong revision" appeared
				fmt.Printf("ERROR mutexMereLock: tried to lock with wrong revisionId\n")
			}
			return 0, err // Terminate with error
		}
		fmt.Printf("============== %s: Locked %s\n", caller, entry.Key())
		return lockRevisionId, nil // Successfully locked
	}
	mutexWaitForUnlock := func(keyMutex string) {
		for true {
			if w, err := kv.Watch(keyMutex); err == nil {
				defer w.Stop()

				for true {
					select {
					case entry := <-w.Updates():
						if entry != nil {
							lockTime := system.BytesToInt64(entry.Value())
							if lockTime == 0 {
								return
							}
						} else {
							break // All updates read - create new kv.Watch
						}
					// For too long waiting for mutex to be released, maybe it is dead and no updates will ever come - start again
					case <-time.After(time.Duration(runtime.config.kvMutexIsOldPollingIntervalSec) * time.Second):
						return
					}
				}

				w.Stop()
			} else {
				fmt.Printf("KeyMutexLock kv.Watch error %s\n", err)
			}
		}
	}

	keyMutex := key + ".mutex"
	mutexResetLockNeeded := false

	fmt.Printf("============== %s: Locking %s\n", caller, keyMutex)
	for true {
		now := time.Now().UnixNano()

		keyValueMutexOperationMutex.Lock()

		entry, err := kv.Get(keyMutex) // Getting last mutex state for key
		if err != nil {
			if err == nats.ErrKeyNotFound {
				mutexResetLockNeeded = true
			} else {
				keyValueMutexOperationMutex.Unlock()
				return 0, err
			}
		}
		if mutexResetLockNeeded {
			defer keyValueMutexOperationMutex.Unlock()
			return mutexResetLock(keyMutex, now)
		}

		lockTime := system.BytesToInt64(entry.Value())
		if lockTime == 0 { // Mutex is ready to be locked
			defer keyValueMutexOperationMutex.Unlock()
			return mutexMereLock(entry, now)
		} else if lockTime+int64(runtime.config.kvMutexLifeTimeSec)*int64(time.Second) < now { // Mutex was locked by someone else and its lock is too old
			fmt.Printf("WARNING: Context mutex for key=%s is too old, will be unlocked!\n", key)
			mutexResetLockNeeded = true
			keyValueMutexOperationMutex.Unlock()
			continue
		}

		keyValueMutexOperationMutex.Unlock()

		if errorOnLocked {
			return 0, fmt.Errorf("error: errorOnLocked")
		}
		mutexWaitForUnlock(keyMutex)
	}
	return 0, nil
}

func KeyMutexUnlock(runtime *Runtime, key string, lockRevisionId uint64, debugCaller ...string) error {
	caller := strings.Join(debugCaller, "-")
	kv := runtime.kv

	keyValueMutexOperationMutex.Lock()
	defer keyValueMutexOperationMutex.Unlock()

	keyMutex := key + ".mutex"
	entry, err := kv.Get(keyMutex)
	if err != nil {
		return err
	}
	if entry.Revision() != lockRevisionId {
		fmt.Printf("WARNING: Context mutex for key=%s with revision=%d was violated, new revision=%d!\n", key, lockRevisionId, entry.Revision())
	}
	lockTime := system.BytesToInt64(entry.Value())
	if lockTime != 0 {
		_, err := kv.Update(keyMutex, system.Int64ToBytes(0), entry.Revision())
		if err != nil {
			return err
		}
	} else {
		fmt.Printf("WARNING: Context mutex for key=%s was already unlocked!\n", key)
	}
	fmt.Printf("============== %s: Unlocked %s\n", caller, keyMutex)
	return nil // Successfully unlocked
}

func ContextMutexLock(ft *FunctionType, id string, errorOnLocked bool) (uint64, error) {
	return KeyMutexLock(ft.runtime, ft.name+"."+id, errorOnLocked, "ContextMutexLock")
}

func ContextMutexUnlock(ft *FunctionType, id string, lockRevisionId uint64) error {
	return KeyMutexUnlock(ft.runtime, ft.name+"."+id, lockRevisionId, "ContextMutexUnlock")
}

func FunctionTypeMutexLock(ft *FunctionType, errorOnLocked bool) (uint64, error) {
	return KeyMutexLock(ft.runtime, ft.name, errorOnLocked, "FunctionTypeMutexLock")
}

func FunctionTypeMutexUnlock(ft *FunctionType, lockRevisionId uint64) error {
	return KeyMutexUnlock(ft.runtime, ft.name, lockRevisionId, "FunctionTypeMutexUnlock")
}
