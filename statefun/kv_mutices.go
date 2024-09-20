package statefun

import (
	"context"
	"errors"
	"fmt"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

var (
	// ErrMutexLocked is returned when the mutex is already locked and errorOnLocked is true.
	ErrMutexLocked = errors.New("mutex is locked")
)

// KeyMutexLock attempts to acquire a distributed mutex for the given key.
// If errorOnLocked is true and the mutex is already locked, it returns an error immediately.
// Returns the lock revision ID if successful.
func KeyMutexLock(ctx context.Context, runtime *Runtime, key string, errorOnLocked bool) (uint64, error) {
	logger := lg.NewLogger(lg.Options{ReportCaller: true, Level: lg.TraceLevel})
	kv := runtime.Domain.kv
	keyMutex := key + ".mutex"
	mutexLifetime := int64(runtime.config.kvMutexLifeTimeSec) * int64(time.Second)

	logger.Trace(ctx, "Attempting to lock %s", keyMutex)

	for {
		now := system.GetCurrentTimeNs()

		// Attempt to get the current mutex value
		entry, err := kv.Get(keyMutex)
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) {
				// Mutex does not exist, create it
				revID, err := kv.Put(keyMutex, system.Int64ToBytes(now))
				if err == nil {
					logger.Trace(ctx, "Locked %s", keyMutex)
					return revID, nil
				}
				// If error occurs, retry
				continue
			}
			// Return other errors
			return 0, err
		}

		lockTime := system.BytesToInt64(entry.Value())

		if lockTime == 0 || (lockTime+mutexLifetime < now) {
			// Mutex is unlocked or expired, try to acquire it
			revID, err := kv.Update(keyMutex, system.Int64ToBytes(now), entry.Revision())
			if err == nil {
				logger.Trace(ctx, "Locked %s", keyMutex)
				return revID, nil
			}
			if errors.Is(err, &nats.APIError{ErrorCode: nats.JSErrCodeStreamWrongLastSequence}) {
				// Revision conflict, retry
				continue
			}
			// Return other errors
			return 0, err
		}

		// Mutex is locked by someone else
		if errorOnLocked {
			return 0, ErrMutexLocked
		}

		// Wait before retrying
		time.Sleep(100 * time.Millisecond)
	}
}

// KeyMutexLockUpdate refreshes the mutex lock for the given key to prevent expiration.
// It requires the lockRevisionID obtained when the lock was acquired.
// Returns the new lock revision ID.
func KeyMutexLockUpdate(ctx context.Context, runtime *Runtime, key string, lockRevisionID uint64) (uint64, error) {
	logger := lg.NewLogger(lg.Options{ReportCaller: true})
	kv := runtime.Domain.kv
	keyMutex := key + ".mutex"

	entry, err := kv.Get(keyMutex)
	if err != nil {
		return 0, err
	}

	if entry.Revision() != lockRevisionID {
		logger.Warn(ctx, "Lock revision mismatch for key=%s: expected %d, got %d", key, lockRevisionID, entry.Revision())
		return 0, fmt.Errorf("lock revision mismatch")
	}

	lockTime := system.BytesToInt64(entry.Value())
	if lockTime != 0 {
		now := system.GetCurrentTimeNs()
		revID, err := kv.Update(keyMutex, system.Int64ToBytes(now), entry.Revision())
		if err != nil {
			return 0, err
		}
		logger.Trace(ctx, "Updated lock for %s", keyMutex)
		return revID, nil
	}

	return 0, fmt.Errorf("mutex for key=%s was already unlocked", key)
}

// KeyMutexUnlock releases the distributed mutex lock for the given key.
// It requires the lockRevisionID obtained when the lock was acquired.
func KeyMutexUnlock(ctx context.Context, runtime *Runtime, key string, lockRevisionID uint64) error {
	logger := lg.NewLogger(lg.Options{ReportCaller: true, Level: lg.TraceLevel})
	kv := runtime.Domain.kv
	keyMutex := key + ".mutex"

	entry, err := kv.Get(keyMutex)
	if err != nil {
		return err
	}

	if entry.Revision() != lockRevisionID {
		logger.Warn(ctx, "Lock revision mismatch for key=%s: expected %d, got %d", key, lockRevisionID, entry.Revision())
		return fmt.Errorf("lock revision mismatch")
	}

	lockTime := system.BytesToInt64(entry.Value())
	if lockTime != 0 {
		_, err := kv.Update(keyMutex, system.Int64ToBytes(0), entry.Revision())
		if err != nil {
			return err
		}
		logger.Trace(ctx, "Unlocked %s", keyMutex)
		return nil
	}

	logger.Warn(ctx, "Mutex for key=%s was already unlocked", key)
	return nil
}

// ContextMutexLock acquires a mutex lock for a specific context ID within a function type.
func ContextMutexLock(ctx context.Context, ft *FunctionType, id string, errorOnLocked bool) (uint64, error) {
	return KeyMutexLock(ctx, ft.runtime, ft.name+"."+id, errorOnLocked)
}

// ContextMutexUnlock releases the mutex lock for a specific context ID within a function type.
func ContextMutexUnlock(ctx context.Context, ft *FunctionType, id string, lockRevisionID uint64) error {
	return KeyMutexUnlock(ctx, ft.runtime, ft.name+"."+id, lockRevisionID)
}

// FunctionTypeMutexLock acquires a mutex lock for the entire function type.
func FunctionTypeMutexLock(ctx context.Context, ft *FunctionType, errorOnLocked bool) (uint64, error) {
	return KeyMutexLock(ctx, ft.runtime, ft.name, errorOnLocked)
}

// FunctionTypeMutexUnlock releases the mutex lock for the entire function type.
func FunctionTypeMutexUnlock(ctx context.Context, ft *FunctionType, lockRevisionID uint64) error {
	return KeyMutexUnlock(ctx, ft.runtime, ft.name, lockRevisionID)
}
