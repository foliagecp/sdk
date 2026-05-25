package statefun

// Worker-pool regression tests for the 116 incident class: a stuck handler must
// never wedge teardown. SFWorkerPool.Stop() must return immediately (observing
// the in-flight drain in the background) even while a worker is blocked forever,
// and a stopped pool must be recreatable via ensureWorkerPool. Before the fix,
// Stop() did a blocking wg.Wait() inline, so a single wedged handler froze the
// lifecycle loop and recovery never ran.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
)

// startWorkerPoolTestRuntime brings up an embedded-NATS, single-instance
// (non-HA, always-active) runtime and runs `register` before Start so callers
// can register function types and capture their *FunctionType.
func startWorkerPoolTestRuntime(t *testing.T, register func(rt *Runtime)) (*Runtime, *server.Server) {
	t.Helper()
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	srv := natsservertest.RunServer(&opts)

	cfg := NewRuntimeConfigSimple(srv.ClientURL(), "wp_test").SetActivePassiveMode(false)
	rt, err := NewRuntime(*cfg)
	require.NoError(t, err)

	register(rt)

	ready := make(chan struct{})
	rt.RegisterOnAfterStartFunction(func(_ context.Context, _ *Runtime) error {
		close(ready)
		return nil
	}, false)

	go func() { _ = rt.Start(context.TODO(), cache.NewCacheConfig("wp_test_cache")) }()

	select {
	case <-ready:
	case <-time.After(20 * time.Second):
		srv.Shutdown()
		t.Fatal("runtime did not start in time")
	}

	deadline := time.Now().Add(5 * time.Second)
	for !rt.IsActiveInstance() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.True(t, rt.IsActiveInstance(), "runtime must be active before enqueuing")
	return rt, srv
}

func Test_WorkerPool_StopNonBlockingUnderStuckHandler_116(t *testing.T) {
	running := make(chan struct{})
	release := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }

	// A handler that blocks forever until released — the "stuck handler".
	blocking := func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {
		startOnce.Do(func() { close(running) })
		<-release
	}

	var ft *FunctionType
	_, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		cfg := *NewFunctionTypeConfig().
			SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
			SetMaxIdHandlers(-1)
		ft = NewFunctionType(rt, "test.wp.block", blocking, cfg)
	})
	defer srv.Shutdown()
	defer releaseHandler() // unblock the wedged worker so it can exit at teardown

	// Occupy a worker with the stuck handler (feed the pool directly).
	ft.sendMsg("id1", FunctionTypeMsg{
		Caller:          &sfPlugins.StatefunAddress{Typename: "test", ID: "caller"},
		RefusalCallback: func(bool) {},
		AckCallback:     func(bool) {},
		Payload:         easyjson.NewJSONObject().GetPtr(),
	})
	select {
	case <-running:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not start — worker was not occupied")
	}

	wp := ft.sfWorkerPool.Load()
	require.NotNil(t, wp, "worker pool must exist after start")

	// Stop() must return immediately despite the blocked worker (the 116 fix:
	// the wg.Wait drain runs in the background, not inline).
	start := time.Now()
	wp.Stop()
	elapsed := time.Since(start)
	require.Lessf(t, elapsed, 1*time.Second,
		"Stop() must be non-blocking even with a stuck worker (116); took %s", elapsed)
	require.True(t, wp.IsStopped(), "pool must report stopped after Stop()")

	// A stopped pool must be recreatable into a fresh, live pool.
	ft.ensureWorkerPool()
	wp2 := ft.sfWorkerPool.Load()
	require.NotNil(t, wp2)
	require.Truef(t, wp != wp2, "ensureWorkerPool must replace a stopped pool with a new instance")
	require.False(t, wp2.IsStopped(), "recreated pool must be live")
}
