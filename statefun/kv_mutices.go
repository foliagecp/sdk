

package statefun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

var (
	//keyValueMutexOperationMutex sync.Mutex
	kwWatchMutex   sync.Mutex
	ErrMutexLocked = errors.New("mutex is locked")
)

// KeyMutexLock
// errorOnLocked - if mutex is already locked, exit with error (do not wait for unlocking)
func KeyMutexLock(ctx context.Context, runtime *Runtime, key string, errorOnLocked bool) (uint64, error) {
	le := lg.NewLogger(lg.Options{ReportCaller: true, Level: lg.TraceLevel})
	kv := runtime.Domain.kv
	mutexResetLock := func(keyMutex string, now int64) (uint64, error) {
		lockRevisionID, err := kv.Put(keyMutex, system.Int64ToBytes(now))
		if err == nil {
			le.Tracef(ctx, "============== Locked %s", keyMutex)
			return lockRevisionID, nil
		}
		return 0, err
	}
	mutexMereLock := func(entry nats.KeyValueEntry, now int64) (uint64, error) {
		// Try to lock mutex by updating it with current time value using revision obtained during last Get
		lockRevisionID, err := kv.Update(entry.Key(), system.Int64ToBytes(now), entry.Revision())
		if err != nil { // If no error appeared
			if strings.Contains(err.Error(), "nats: wrong last sequence") { // If error "wrong revision" appeared
				//le.Tracef(lg.ErrorLevel, "%s: ERROR mutexMereLock: tried to lock with wrong revisionId", caller)
				return 0, nil
			}
			return 0, err // Terminate with error
		}
		le.Tracef(ctx, "============== Locked %s", entry.Key())
		return lockRevisionID, nil // Successfully locked
	}
	getKeyWatch := func(keyMutex string) (nats.KeyWatcher, error) {
		kwWatchMutex.Lock()
		return kv.Watch(keyMutex, nats.IgnoreDeletes())
	}
	releaseKeyWatch := func(w nats.KeyWatcher) {
		system.MsgOnErrorReturn(w.Stop())
		kwWatchMutex.Unlock()
	}
	mutexWaitForUnlock := func(keyMutex string) {
		for {
			if w, err := getKeyWatch(keyMutex); err == nil {
				entry := <-w.Updates()
				if entry != nil {
					lockTime := system.BytesToInt64(entry.Value())
					if lockTime == 0 {
						releaseKeyWatch(w)
						return
					}
					if lockTime+int64(runtime.config.kvMutexLifeTimeSec)*int64(time.Second) < system.GetCurrentTimeNs() {
						le.Tracef(ctx, "======================= WAITING FOR UNLOCK DONE (MUTEX IS DEAD)")
						releaseKeyWatch(w)
						return
					}
				}
				releaseKeyWatch(w)
			} else {
				le.Errorf(ctx, "KeyMutexLock kv.Watch error %s", err)
			}
			// Maybe sleep is needed to prevent to often kv.Watch
			// time.Sleep(100 * time.Microsecond)
		}
	}

	keyMutex := key + ".mutex"
	mutexResetLockNeeded := false
	le.Tracef(ctx, "============== Locking %s", keyMutex)
	for {
		now := system.GetCurrentTimeNs()

		//keyValueMutexOperationMutex.Lock()

		entry, err := kv.Get(keyMutex) // Getting last mutex state for key
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) {
				mutexResetLockNeeded = true
			} else {
				//keyValueMutexOperationMutex.Unlock()
				return 0, err
			}
		}
		if mutexResetLockNeeded {
			//defer keyValueMutexOperationMutex.Unlock()
			return mutexResetLock(keyMutex, now)
		}

		lockTime := system.BytesToInt64(entry.Value())
		if lockTime == 0 { // Mutex is ready to be locked
			//defer keyValueMutexOperationMutex.Unlock()
			revId, err := mutexMereLock(entry, now)
			if revId == 0 && err == nil { // Did not succeed in locking, other lock was faster
				continue
			}
			return revId, err
		} else if lockTime+int64(runtime.config.kvMutexLifeTimeSec)*int64(time.Second) < now { // Mutex was locked by someone else and its lock is too old
			le.Warnf(ctx, "Context mutex for key=%s is too old, will be unlocked!", key)
			mutexResetLockNeeded = true
			//keyValueMutexOperationMutex.Unlock()
			continue
		}

		//keyValueMutexOperationMutex.Unlock()

		if errorOnLocked {
			return 0, ErrMutexLocked
		}
		mutexWaitForUnlock(keyMutex)
	}
}

func KeyMutexLockUpdate(ctx context.Context, runtime *Runtime, key string, lockRevisionID uint64) (uint64, error) {
	le := lg.NewLogger(lg.Options{ReportCaller: true, Level: lg.TraceLevel})
	kv := runtime.Domain.kv

	keyMutex := key + ".mutex"
	entry, err := kv.Get(keyMutex)
	if err != nil {
		return 0, err
	}
	if entry.Revision() != lockRevisionID {
		le.Warnf(ctx, "Context mutex for key=%s with revision=%d was violated, new revision=%d!", key, lockRevisionID, entry.Revision())
	}
	lockTime := system.BytesToInt64(entry.Value())
	if lockTime != 0 {
		revId, err := kv.Update(keyMutex, system.Int64ToBytes(system.GetCurrentTimeNs()), entry.Revision())
		if err != nil {
			return 0, err
		}
		le.Tracef(ctx, "============== Updated %s", keyMutex)
		return revId, err
	} else {
		return 0, fmt.Errorf("Context mutex for key=%s was already unlocked", key)
	}
}

func KeyMutexUnlock(ctx context.Context, runtime *Runtime, key string, lockRevisionID uint64) error {
	le := lg.NewLogger(lg.Options{ReportCaller: true, Level: lg.TraceLevel})
	kv := runtime.Domain.kv

	//keyValueMutexOperationMutex.Lock()
	//defer keyValueMutexOperationMutex.Unlock()

	keyMutex := key + ".mutex"
	entry, err := kv.Get(keyMutex)
	if err != nil {
		return err
	}
	if entry.Revision() != lockRevisionID {
		le.Warnf(ctx, "Context mutex for key=%s with revision=%d was violated, new revision=%d!", key, lockRevisionID, entry.Revision())
	}
	lockTime := system.BytesToInt64(entry.Value())
	if lockTime != 0 {
		_, err := kv.Update(keyMutex, system.Int64ToBytes(0), entry.Revision())
		if err != nil {
			return err
		}
	} else {
		le.Warnf(ctx, "Context mutex for key=%s was already unlocked!", key)
	}
	le.Tracef(ctx, "============== Unlocked %s", keyMutex)
	return nil // Successfully unlocked
}

func ContextMutexLock(ctx context.Context, ft *FunctionType, id string, errorOnLocked bool) (uint64, error) {
	return KeyMutexLock(ctx, ft.runtime, ft.name+"."+id, errorOnLocked)
}

func ContextMutexUnlock(ctx context.Context, ft *FunctionType, id string, lockRevisionID uint64) error {
	return KeyMutexUnlock(ctx, ft.runtime, ft.name+"."+id, lockRevisionID)
}

func FunctionTypeMutexLock(ctx context.Context, ft *FunctionType, errorOnLocked bool) (uint64, error) {
	return KeyMutexLock(ctx, ft.runtime, ft.name, errorOnLocked)
}

func FunctionTypeMutexUnlock(ctx context.Context, ft *FunctionType, lockRevisionID uint64) error {
	return KeyMutexUnlock(ctx, ft.runtime, ft.name, lockRevisionID)
}
