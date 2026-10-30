package statefun

// Worker-pool regression tests for the 116 incident class: a stuck handler must
// never wedge teardown. SFWorkerPool.Stop() must return immediately (observing
// the in-flight drain in the background) even while a worker is blocked forever,
// and a stopped pool must be recreatable via ensureWorkerPool. Before the fix,
// Stop() did a blocking wg.Wait() inline, so a single wedged handler froze the
// lifecycle loop and recovery never ran.

import (
	"context"
	"fmt"
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
	// DefaultTestOptions leaves StoreDir empty → nats-server falls back to the
	// FIXED shared path $TMPDIR/jetstream, which persists across runs and is
	// shared with sibling packages during `go test ./...`. Give every run its
	// own auto-cleaned store dir so stale streams/KV cannot poison it.
	opts.StoreDir = t.TempDir()
	srv := natsservertest.RunServer(&opts)

	cfg := NewRuntimeConfigSimple(srv.ClientURL(), "wp_test").SetActivePassiveMode(false)
	rt, err := NewRuntime(*cfg)
	require.NoError(t, err)
	// Tear the runtime down when the test ends. Without this every test on
	// this harness leaked a live runtime for the rest of the package run:
	// its lifecycle ticker kept refreshing per-function locks against the
	// by-then-dead embedded NATS every 5s (each attempt a 5s context
	// deadline), and its cache lazy-writer kept retrying WAL publishes —
	// dozens of zombie goroutines of pure noise that skewed
	// timing-sensitive tests later in the package. Emergency mode is the
	// documented path for a throwaway test runtime: it cancels the
	// background contexts without draining and does not depend on NATS
	// reachability (the tests' own `defer srv.Shutdown()` runs BEFORE
	// t.Cleanup, so NATS is already gone by the time this fires).
	t.Cleanup(func() { rt.Shutdown(true) })

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

// A backlog for one stateful-function ID must not consume every worker while
// all but one of them wait for that ID's execution mutex. Other IDs are
// independent actors and must keep making progress.
//
// The queues are primed directly to make the scheduler state deterministic.
// Going through sendMsg here would test a different problem: once the first
// handler starts, sendMsg itself waits for the same per-ID mutex.
func Test_WorkerPool_HotIDDoesNotStarveColdID(t *testing.T) {
	const maxWorkers = 4

	hotStarted := make(chan struct{})
	coldHandled := make(chan struct{})
	releaseHot := make(chan struct{})
	var hotStartOnce, coldOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseHot) }) })

	var hotID, coldID string
	handler := func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		switch ctx.Self.ID {
		case hotID:
			hotStartOnce.Do(func() { close(hotStarted) })
			<-releaseHot
		case coldID:
			coldOnce.Do(func() { close(coldHandled) })
		}
	}

	var ft *FunctionType
	_, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		hotID = rt.Domain.CreateObjectIDWithThisDomain("hot", false)
		coldID = rt.Domain.CreateObjectIDWithThisDomain("cold", false)
		cfg := *NewFunctionTypeConfig().
			SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
			SetWorkerPoolConfig(SFWorkerPoolConfig{
				MinWorkers: 0, MaxWorkers: maxWorkers,
				IdleTimeout: time.Minute, TaskQueueLen: 16,
			})
		ft = NewFunctionType(rt, "test.wp.fairness", handler, cfg)
	})
	defer srv.Shutdown()

	wp := ft.sfWorkerPool.Load()
	require.NotNil(t, wp)

	newMsg := func() FunctionTypeMsg {
		return FunctionTypeMsg{
			Caller:          &sfPlugins.StatefunAddress{Typename: "test", ID: "caller"},
			RefusalCallback: func(bool) {},
			AckCallback:     func(bool) {},
			Payload:         easyjson.NewJSONObject().GetPtr(),
		}
	}

	hotQueue := make(chan FunctionTypeMsg, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		require.True(t, ft.TokenTryAcquire())
		hotQueue <- newMsg()
	}
	ft.idHandlersChannel.Store(hotID, hotQueue)
	ft.idHandlersLastMsgTime.Store(hotID, time.Now().UnixNano())
	ft.activeIDChannels.Add(1)

	coldQueue := make(chan FunctionTypeMsg, 1)
	require.True(t, ft.TokenTryAcquire())
	coldQueue <- newMsg()
	ft.idHandlersChannel.Store(coldID, coldQueue)
	ft.idHandlersLastMsgTime.Store(coldID, time.Now().UnixNano())
	ft.activeIDChannels.Add(1)

	// Keep submit blocked while notifications are ordered. The current manager
	// will schedule every hot task before it sees cold; a fair manager keeps at
	// most one hot task in flight and can immediately run cold on another worker.
	wp.mu.Lock()
	for i := 0; i < maxWorkers; i++ {
		wp.pendingIds <- hotID
	}
	wp.pendingIds <- coldID
	wp.mu.Unlock()

	select {
	case <-hotStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("hot handler did not start")
	}

	select {
	case <-coldHandled:
		// Desired behavior: an unrelated ID progresses while hot is blocked.
	case <-time.After(500 * time.Millisecond):
		releaseOnce.Do(func() { close(releaseHot) })
		select {
		case <-coldHandled:
			// Confirms that cold was queued correctly and only lacked a worker.
		case <-time.After(5 * time.Second):
			t.Fatal("cold ID was never handled after releasing the hot ID")
		}
		t.Fatal("cold ID starved while workers waited for the hot ID mutex")
	}
}

// NATS delivers callbacks for one asynchronous subscription serially. A
// message for an ID whose handler is already running must therefore be cheap
// to enqueue: if sendMsg waits for the handler's execution mutex, that callback
// also prevents later messages for every other ID of the function type from
// reaching the worker pool.
func Test_JetStreamIngress_HotIDDoesNotBlockColdID(t *testing.T) {
	hotStarted := make(chan struct{})
	coldHandled := make(chan struct{})
	releaseHot := make(chan struct{})
	var hotStartOnce, coldOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseHot) }) })

	var hotID, coldID string
	handler := func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		switch ctx.Self.ID {
		case hotID:
			hotStartOnce.Do(func() { close(hotStarted) })
			<-releaseHot
		case coldID:
			coldOnce.Do(func() { close(coldHandled) })
		}
	}

	const functionType = "test.wp.jetstream-hol"
	var ft *FunctionType
	rt, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		hotID = rt.Domain.CreateObjectIDWithThisDomain("hot", false)
		coldID = rt.Domain.CreateObjectIDWithThisDomain("cold", false)
		cfg := *NewFunctionTypeConfig().
			SetAllowedSignalProviders(sfPlugins.JetstreamGlobalSignal).
			SetWorkerPoolConfig(SFWorkerPoolConfig{
				MinWorkers: 2, MaxWorkers: 4,
				IdleTimeout: time.Minute, TaskQueueLen: 16,
			})
		ft = NewFunctionType(rt, functionType, handler, cfg)
	})
	defer srv.Shutdown()
	require.NotNil(t, ft)

	publish := func(id string) {
		subject := fmt.Sprintf(
			DomainIngressSubjectsTmpl,
			rt.Domain.name,
			fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, rt.Domain.name, functionType, id),
		)
		payload := easyjson.NewJSONObject()
		require.NoError(t, rt.nc.Publish(subject,
			buildNatsData(rt.Domain.name, "test", "caller", &payload, nil, nil)))
		require.NoError(t, rt.nc.Flush())
	}

	// The first hot message starts and holds the per-ID execution mutex.
	publish(hotID)
	select {
	case <-hotStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first hot JetStream message did not reach the handler")
	}

	// Both messages use the same JetStream subscription and are published in
	// order. The second hot enqueue must not prevent cold from being delivered.
	publish(hotID)
	publish(coldID)

	select {
	case <-coldHandled:
		// Desired behavior: ingress enqueue does not wait for handler execution.
	case <-time.After(500 * time.Millisecond):
		releaseOnce.Do(func() { close(releaseHot) })
		select {
		case <-coldHandled:
			// Confirms publication and subscription setup: cold advances as soon
			// as the callback blocked in sendMsg can acquire the hot-ID mutex.
		case <-time.After(5 * time.Second):
			t.Fatal("cold JetStream message was never handled after releasing the hot ID")
		}
		t.Fatal("cold ID was blocked in the JetStream subscription behind a hot ID enqueue")
	}
}
