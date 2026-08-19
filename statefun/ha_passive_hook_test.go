package statefun

// Tests for RegisterOnBecamePassive — the active->passive teardown hook.
//
//   - Test_HA_OnBecamePassive_FiresOnDemotion drives a REAL demotion on an
//     embedded NATS (by releasing the runtime's own KV lock) and asserts the
//     callback fires exactly once with a deadline-bounded context.
//   - Test_OnBecamePassive_BoundedByTimeout asserts a callback that ignores its
//     context cannot wedge the demotion path (the call returns at its deadline).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
)

func Test_HA_OnBecamePassive_FiresOnDemotion(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir() // own auto-cleaned JetStream store (see ha_test.go)
	srv := natsservertest.RunServer(&opts)
	defer srv.Shutdown()
	url := srv.ClientURL()

	var passiveCount atomic.Int64
	var ctxBounded atomic.Bool

	cfg := NewRuntimeConfigSimple(url, "hadomain").SetKVMutexLifeTimeSec(4)
	rt, err := NewRuntime(*cfg)
	require.NoError(t, err)
	rt.RegisterOnBecamePassive(func(ctx context.Context) {
		if _, ok := ctx.Deadline(); ok {
			ctxBounded.Store(true)
		}
		passiveCount.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = rt.Start(ctx, cache.NewCacheConfig("hadomain_cache")) }()

	// Wait until Start has brought the runtime up (isReady, an atomic, also
	// establishes happens-before for Domain.kv, which Start initialises before
	// it). IsActiveInstance is unusable as a gate here: it defaults to true.
	require.Truef(t, waitForCond(30*time.Second, func() bool { return rt.IsReady() }),
		"runtime must finish starting")

	// Force a real demotion: release the runtime's own KV lock from the outside.
	// Done entirely KV-side (no race on the runtime's internal state): releasing a
	// HELD lock bumps its revision, so the runtime's next lock refresh sees a
	// revision violation and steps down (becomePassive -> the hook). The revision
	// rotates on every refresh, so retry until exactly one release lands; the loop
	// stops on that first success, so a single instance re-acquires and stays put
	// (exactly one demotion). Only release when the lock is actually held
	// (lockTime != 0), else KeyMutexUnlock no-ops without demoting anyone.
	lockKey := system.GetHashStr(RuntimeName)
	require.Truef(t, waitForCond(20*time.Second, func() bool {
		entry, err := rt.Domain.kv.Get(lockKey + ".mutex")
		if err != nil || system.BytesToInt64(entry.Value()) == 0 {
			return false
		}
		return KeyMutexUnlock(context.Background(), rt, lockKey, entry.Revision()) == nil
	}), "should release the runtime's own KV lock")

	require.Truef(t, waitForCond(15*time.Second, func() bool { return passiveCount.Load() >= 1 }),
		"OnBecamePassive must fire on the active->passive demotion")
	require.True(t, ctxBounded.Load(), "OnBecamePassive context must carry a deadline (bounded teardown)")

	// Exactly once per transition: the single becomePassive call site is
	// edge-gated by holding the lock, and after stepping down a single instance
	// merely re-acquires the now-free lock (a passive->active promotion, which
	// does NOT fire this hook). Settle through a re-acquisition, then confirm no
	// spurious extra fire.
	time.Sleep(3 * time.Second)
	require.EqualValues(t, 1, passiveCount.Load(), "OnBecamePassive must fire exactly once per demotion")
}

func Test_OnBecamePassive_BoundedByTimeout(t *testing.T) {
	saved := swapPassiveTeardownTimeout(300 * time.Millisecond)
	defer swapPassiveTeardownTimeout(saved)

	rt := &Runtime{} // runOnBecamePassiveFunctions only needs the registered callbacks
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	var ctxBounded atomic.Bool

	rt.RegisterOnBecamePassive(func(ctx context.Context) {
		if _, ok := ctx.Deadline(); ok {
			ctxBounded.Store(true)
		}
		close(entered)
		<-release // ignore the context: block until the test releases us
	})

	start := time.Now()
	rt.runOnBecamePassiveFunctions(context.Background())
	elapsed := time.Since(start)

	select {
	case <-entered:
	default:
		t.Fatal("OnBecamePassive callback was not invoked")
	}
	require.True(t, ctxBounded.Load(), "callback context must carry a deadline")
	require.Lessf(t, elapsed, 3*time.Second,
		"runOnBecamePassiveFunctions must return at its ~300ms deadline, not block on the hung callback; took %s", elapsed)
}
