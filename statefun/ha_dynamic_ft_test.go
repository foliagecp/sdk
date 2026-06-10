package statefun

// Failover guarantee for dynamically-registered single-instance function types.
//
// A single-instance FT registered via RegisterDynamicFunctionType (after Start)
// must get its NATS subscription (re)established when the instance is promoted to
// active — the same guarantee a statically-registered FT has. Before the fix in
// startFunctionSubscriptions, any single-instance FT missing from the startup
// `revisions` map was skipped on every promotion, so dynamic handlers stayed
// dead after the first failover (osm-app's per-pack slot handlers).
//
// This test registers one static and one dynamic single-instance FT, forces a
// real demotion (OnBecamePassive marks the moment subscriptions are torn down),
// then verifies BOTH receive signals again after the instance re-promotes: the
// static one proving no regression (T4), the dynamic one proving the fix
// (T1/T2). Runs on embedded NATS.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
)

func Test_HA_DynamicFunctionType_ResubscribesAfterFailover(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	srv := natsservertest.RunServer(&opts)
	defer srv.Shutdown()
	url := srv.ClientURL()

	var staticHits, dynamicHits atomic.Int64
	demoted := make(chan struct{}, 1)

	cfg := NewRuntimeConfigSimple(url, "hadomain").SetKVMutexLifeTimeSec(4)
	rt, err := NewRuntime(*cfg)
	require.NoError(t, err)

	// Static single-instance FT — registered BEFORE Start, so it enters the
	// startup revisions map. This is the control that must not regress.
	NewFunctionType(rt, "fn.static.failover", func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {
		staticHits.Add(1)
	}, *NewFunctionTypeConfig())

	rt.RegisterOnBecamePassive(func(_ context.Context) {
		select {
		case demoted <- struct{}{}:
		default:
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = rt.Start(ctx, cache.NewCacheConfig("hadomain_cache")) }()

	require.Truef(t, waitForCond(30*time.Second, func() bool { return rt.IsReady() }),
		"runtime must finish starting")

	// Dynamic single-instance FT — registered AFTER Start. Default config =>
	// single-instance (multipleInstancesAllowed=false). This is the regression.
	_, err = RegisterDynamicFunctionType(rt, "fn.dynamic.failover", func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {
		dynamicHits.Add(1)
	}, *NewFunctionTypeConfig())
	require.NoError(t, err)

	signalBoth := func() {
		_ = rt.Signal(sfPlugins.JetstreamGlobalSignal, "fn.static.failover", "x", easyjson.NewJSONObject().GetPtr(), nil)
		_ = rt.Signal(sfPlugins.JetstreamGlobalSignal, "fn.dynamic.failover", "x", easyjson.NewJSONObject().GetPtr(), nil)
	}

	// Baseline: both are subscribed while active and actually receive signals.
	require.Truef(t, waitForCond(20*time.Second, func() bool {
		signalBoth()
		return staticHits.Load() >= 1 && dynamicHits.Load() >= 1
	}), "both FTs must receive signals while active (static=%d dynamic=%d)", staticHits.Load(), dynamicHits.Load())

	// Force a real demotion by releasing the runtime's own KV lock from the
	// outside (NATS-side, no race on internal state). The instance steps down —
	// tearing down subscriptions — and, having no competitor, re-promotes.
	lockKey := system.GetHashStr(RuntimeName)
	require.Truef(t, waitForCond(20*time.Second, func() bool {
		entry, e := rt.Domain.kv.Get(lockKey + ".mutex")
		if e != nil || system.BytesToInt64(entry.Value()) == 0 {
			return false
		}
		return KeyMutexUnlock(context.Background(), rt, lockKey, entry.Revision()) == nil
	}), "should release the runtime's own KV lock")

	// Wait until the demotion actually happened (subscriptions torn down).
	select {
	case <-demoted:
	case <-time.After(20 * time.Second):
		t.Fatal("runtime did not step down to passive")
	}

	staticBase, dynBase := staticHits.Load(), dynamicHits.Load()

	// After the instance re-promotes, BOTH FTs must receive signals again. Re-send
	// each tick so a freshly delivered signal proves a live subscription
	// regardless of the consumer's deliver policy. The dynamic FT increasing here
	// is the fix; the static FT increasing is the no-regression control.
	require.Truef(t, waitForCond(40*time.Second, func() bool {
		signalBoth()
		return staticHits.Load() > staticBase && dynamicHits.Load() > dynBase
	}), "after failover both FTs must re-subscribe and receive signals; got static=%d (base %d), DYNAMIC=%d (base %d) — a stuck dynamic count is the bug",
		staticHits.Load(), staticBase, dynamicHits.Load(), dynBase)
}

// Test_HA_DynamicFunctionType_FailoverToSecondInstance is the two-instance
// failover (acceptance T1 + T3): the same dynamic single-instance FT is
// registered on BOTH a live and a standby runtime (as the same osm-app binary on
// every node does); only the active processes; when the active is killed the
// standby is promoted and MUST subscribe its dynamically-registered FT and take
// over processing — with no restart. Before the fix the promoted instance never
// subscribed the dynamic FT (absent from its startup revisions map), so the
// handler stayed dead after failover.
func Test_HA_DynamicFunctionType_FailoverToSecondInstance(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	srv := natsservertest.RunServer(&opts)
	defer srv.Shutdown()
	url := srv.ClientURL()

	const fn = "fn.dynamic.failover2"
	var activeHits, passiveHits atomic.Int64

	newRT := func() (*Runtime, context.CancelFunc) {
		cfg := NewRuntimeConfigSimple(url, "hadomain").SetKVMutexLifeTimeSec(4)
		rt, err := NewRuntime(*cfg)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = rt.Start(ctx, cache.NewCacheConfig("hadomain_cache")) }()
		return rt, cancel
	}

	rtA, cancelA := newRT()
	defer cancelA()
	rtB, cancelB := newRT()
	defer cancelB()

	activeCount := func() int {
		n := 0
		if rtA.IsActiveInstance() {
			n++
		}
		if rtB.IsActiveInstance() {
			n++
		}
		return n
	}
	require.Truef(t, waitForCond(40*time.Second, func() bool { return activeCount() == 1 }),
		"exactly one instance must become active; got %d", activeCount())

	active, passive := rtA, rtB
	if rtB.IsActiveInstance() {
		active, passive = rtB, rtA
	}

	mkHandler := func(hits *atomic.Int64) FunctionLogicHandler {
		return func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) { hits.Add(1) }
	}
	// Register the SAME dynamic single-instance FT on BOTH instances. The active
	// is registered first, so it owns the per-FT lock and subscribes; the standby
	// registers locally but (lock held by the active, and it is passive) does NOT
	// subscribe — exactly the "registered on a passive instance" case.
	_, err := RegisterDynamicFunctionType(active, fn, mkHandler(&activeHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(passive, fn, mkHandler(&passiveHits), *NewFunctionTypeConfig())
	require.NoError(t, err)

	payload := easyjson.NewJSONObject().GetPtr()

	// Baseline: only the active instance processes signals for the FT.
	require.Truef(t, waitForCond(20*time.Second, func() bool {
		_ = active.Signal(sfPlugins.JetstreamGlobalSignal, fn, "x", payload, nil)
		return activeHits.Load() >= 1
	}), "active instance must process the dynamic FT (activeHits=%d)", activeHits.Load())
	require.Zerof(t, passiveHits.Load(), "standby must not process before promotion (passiveHits=%d)", passiveHits.Load())

	// Kill the active. The standby recovers to active and MUST subscribe its
	// dynamically-registered FT on promotion and start processing. Re-send each
	// tick so a delivered signal proves a live subscription on the promoted node.
	active.Shutdown(true)

	require.Truef(t, waitForCond(45*time.Second, func() bool {
		_ = passive.Signal(sfPlugins.JetstreamGlobalSignal, fn, "x", payload, nil)
		return passiveHits.Load() >= 1
	}), "after failover the promoted instance must subscribe its dynamic FT and process signals (passiveHits=%d) — stuck at 0 is the bug", passiveHits.Load())
}
