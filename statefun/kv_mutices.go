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
	le := lg.GetLogger()
	kv := runtime.Domain.kv
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
		le.Tracef(ctx, "Locked %s", entry.Key())
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
						le.Trace(ctx, "Waiting for unlock done (mutex is dead)")
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
	le.Tracef(ctx, "Locking %s", keyMutex)
	for {
		now := system.GetCurrentTimeNs()

		entry, err := kv.Get(keyMutex) // Getting last mutex state for key
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) {
				// First-ever lock on this key: atomic create-if-absent. Losing
				// the race (someone created the entry between our Get and this
				// Create) means the lock is fresh and theirs — re-read and take
				// the locked path.
				lockRevisionID, cerr := kv.Create(keyMutex, system.Int64ToBytes(now))
				if cerr == nil {
					le.Tracef(ctx, "Locked %s", keyMutex)
					return lockRevisionID, nil
				}
				if errors.Is(cerr, nats.ErrKeyExists) {
					continue
				}
				return 0, cerr
			}
			return 0, err
		}

		lockTime := system.BytesToInt64(entry.Value())
		if lockTime == 0 { // Mutex is ready to be locked
			revId, err := mutexMereLock(entry, now)
			if revId == 0 && err == nil { // Did not succeed in locking, other lock was faster
				continue
			}
			return revId, err
		} else if lockTime+int64(runtime.config.kvMutexLifeTimeSec)*int64(time.Second) < now { // Mutex was locked by someone else and its lock is too old
			// Expired foreign lock: seize it with the SAME compare-and-swap as
			// a free one — an Update against the revision we just read. The
			// previous takeover did an unconditional kv.Put, so every
			// concurrent contender "won" (each overwrote the other; the loser
			// learned only on its next refresh tick, after having grabbed
			// per-function locks in the meantime — the ha-3-node failover
			// cascade), and a Put pending from an earlier loop iteration could
			// even stomp a FRESH lock legitimately taken in between. With CAS
			// exactly one contender wins; the rest re-read and see a fresh
			// foreign lock.
			le.Warnf(ctx, "Context mutex for key=%s is too old, will be unlocked!", key)
			revId, err := mutexMereLock(entry, now)
			if revId == 0 && err == nil { // lost the takeover race
				continue
			}
			return revId, err
		}

		if errorOnLocked {
			return 0, ErrMutexLocked
		}
		mutexWaitForUnlock(keyMutex)
	}
}

// ErrMutexRevisionViolated is returned by KeyMutexLockUpdate when the lock
// entry in KV no longer carries our last-known revision id. That means some
// other instance ran the lock-takeover path (mutexResetLock) and the lock is
// now legitimately theirs; we must NOT try to overwrite it back, because
// that would re-introduce a split-brain window where both instances think
// they are active.
var ErrMutexRevisionViolated = errors.New("mutex lock revision violated by another instance")

func KeyMutexLockUpdate(ctx context.Context, runtime *Runtime, key string, lockRevisionID uint64) (uint64, error) {
	le := lg.GetLogger()
	kv := runtime.Domain.kv

	keyMutex := key + ".mutex"
	entry, err := kv.Get(keyMutex)
	if err != nil {
		return 0, err
	}
	if entry.Revision() != lockRevisionID {
		// Another instance has touched our lock entry. Historically this was
		// only a warning and the code then optimistically tried to overwrite
		// the lock using the freshly-read revision — which let A "steal back"
		// a lock that B had just legitimately seized, leading to split-brain
		// (both A and B convinced they are active). Return a typed error so
		// the runtime-lifecycle code can demote immediately without the
		// "kept lock during soft-TTL window" tolerance.
		le.Warnf(ctx, "Context mutex for key=%s with revision=%d was violated, new revision=%d!", key, lockRevisionID, entry.Revision())
		return 0, ErrMutexRevisionViolated
	}
	lockTime := system.BytesToInt64(entry.Value())
	if lockTime == 0 {
		return 0, fmt.Errorf("Context mutex for key=%s was already unlocked", key)
	}
	revId, err := kv.Update(keyMutex, system.Int64ToBytes(system.GetCurrentTimeNs()), entry.Revision())
	if err != nil {
		return 0, err
	}
	le.Tracef(ctx, "Updated %s", keyMutex)
	return revId, nil
}

// lockRefreshAttemptTimeout bounds ONE attempt to refresh the runtime lock
// (env RUNTIME_LOCK_ATTEMPT_TIMEOUT_SEC, default 1s).
//
// The refresh is two KV calls, and each waits on the JetStream API — whose
// timeout is sized for data operations under saturation, several seconds. One
// slow call then consumed the whole tolerance window and the instance stepped
// down while it still held the lock. An attempt that takes longer than this is
// not worth waiting for: there is time for another before the window closes.
func lockRefreshAttemptTimeout() time.Duration {
	return time.Duration(system.GetEnvMustProceed("RUNTIME_LOCK_ATTEMPT_TIMEOUT_SEC", 1)) * time.Second
}

// lockRefreshAttemptTimeoutFor bounds one attempt so that at least two fit in a
// tick. A configured timeout longer than half the tick would make "retry within
// the tick" a promise the arithmetic cannot keep — on a short lock lifetime the
// tick itself is short.
func lockRefreshAttemptTimeoutFor(tick time.Duration) time.Duration {
	timeout := lockRefreshAttemptTimeout()
	if half := tick / 2; half > 0 && timeout > half {
		return half
	}
	return timeout
}

// KeyMutexLockUpdateWithRetries refreshes the lock, retrying short attempts
// until the deadline. It returns as soon as the answer is meaningful:
//
//	success            — the lock is ours for another TTL;
//	genuine loss       — someone else holds it, or it was released: the caller
//	                     must step down at once, and no retry can change that;
//	deadline exhausted — the last transient error, for the caller to weigh
//	                     against its tolerance window.
//
// Retrying inside the tick is what makes a slow KV survivable: a refresh that
// used to be one attempt against a saturated JetStream is now several.
func KeyMutexLockUpdateWithRetries(ctx context.Context, runtime *Runtime, key string, lockRevisionID uint64, attemptTimeout time.Duration, deadline time.Time) (uint64, int, error) {
	var lastErr error
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		started := time.Now()
		revID, err := KeyMutexLockUpdate(attemptCtx, runtime, key, lockRevisionID)
		cancel()
		runtime.recordLockRefreshLatency(time.Since(started), err == nil)

		if err == nil {
			return revID, attempt, nil
		}
		lastErr = err
		// Someone else's revision, or an unlocked mutex: the answer is final.
		if errors.Is(err, ErrMutexRevisionViolated) || strings.Contains(err.Error(), "already unlocked") {
			return 0, attempt, err
		}
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			return 0, attempt, lastErr
		}
	}
}

func KeyMutexUnlock(ctx context.Context, runtime *Runtime, key string, lockRevisionID uint64) error {
	le := lg.GetLogger()
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
	le.Tracef(ctx, "Unlocked %s", keyMutex)
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
