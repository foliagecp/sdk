package statefun

// KeyMutexLock takeover races: exactly ONE contender may win an expired or
// absent lock. The takeover path used to write with an unconditional kv.Put,
// so simultaneous contenders ALL "won" — each overwrote the other, and the
// loser learned it only on its next refresh tick, after having already
// grabbed per-function locks (the ha-3-node failover cascade). Worse, the
// takeover flag survived the retry loop, so a late Put could stomp a FRESH
// lock legitimately taken by someone else in between. Takeover must be the
// same compare-and-swap as locking a free mutex.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/foliagecp/sdk/statefun/system"
)

func Test_KeyMutexLock_ExpiredLockSeizedByExactlyOne(t *testing.T) {
	rt, srv := startWorkerPoolTestRuntime(t, func(*Runtime) {})
	defer srv.Shutdown()
	kv := rt.Domain.kv
	ttlNs := int64(rt.config.kvMutexLifeTimeSec) * int64(time.Second)

	const rounds = 30
	const contenders = 4
	key := "test.cas.seize"
	for r := 0; r < rounds; r++ {
		// Plant an expired foreign lock.
		expired := system.GetCurrentTimeNs() - ttlNs - int64(time.Second)
		_, err := kv.Put(key+".mutex", system.Int64ToBytes(expired))
		require.NoError(t, err)

		var wins atomic.Int32
		start := make(chan struct{})
		var wg sync.WaitGroup
		for c := 0; c < contenders; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if rev, lockErr := KeyMutexLock(context.TODO(), rt, key, true); lockErr == nil && rev != 0 {
					wins.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		require.EqualValuesf(t, 1, wins.Load(),
			"round %d: exactly one contender must seize an expired lock (multiple winners = the pre-CAS overwrite race)", r)
	}
}

func Test_KeyMutexLock_AbsentKeyCreatedByExactlyOne(t *testing.T) {
	rt, srv := startWorkerPoolTestRuntime(t, func(*Runtime) {})
	defer srv.Shutdown()

	const rounds = 30
	const contenders = 4
	for r := 0; r < rounds; r++ {
		key := fmt.Sprintf("test.cas.create.%d", r) // fresh, never-locked key each round

		var wins atomic.Int32
		start := make(chan struct{})
		var wg sync.WaitGroup
		for c := 0; c < contenders; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if rev, lockErr := KeyMutexLock(context.TODO(), rt, key, true); lockErr == nil && rev != 0 {
					wins.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		require.EqualValuesf(t, 1, wins.Load(),
			"round %d: exactly one contender must create the lock for a fresh key", r)
	}
}
