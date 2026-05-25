package statefun

// Worker-pool correctness hunt: token balance / no leak.
//
// Every accepted task acquires a token (sendMsg) that must be released after the
// handler runs (worker -> TokenRelease). If any code path forgets to release,
// the bucket slowly drains and the function type eventually refuses all work.
// After draining a batch the load must return to 0 and no worker stays active.

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/stretchr/testify/require"
)

func Test_WorkerPool_TokenBalance_NoLeak(t *testing.T) {
	const n = 50
	var processed int64
	done := make(chan struct{})
	handler := func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {
		if atomic.AddInt64(&processed, 1) == n {
			close(done)
		}
	}

	var ft *FunctionType
	_, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		cfg := *NewFunctionTypeConfig().
			SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
			SetMaxIdHandlers(-1)
		ft = NewFunctionType(rt, "test.wp.fast", handler, cfg)
	})
	defer srv.Shutdown()

	for i := 0; i < n; i++ {
		ft.sendMsg(fmt.Sprintf("tok-%d", i), FunctionTypeMsg{
			Caller:          &sfPlugins.StatefunAddress{Typename: "test", ID: "caller"},
			RefusalCallback: func(bool) {},
			AckCallback:     func(bool) {},
			Payload:         easyjson.NewJSONObject().GetPtr(),
		})
	}

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("only %d/%d tasks processed", atomic.LoadInt64(&processed), n)
	}

	// Let workers finish releasing tokens and go idle.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ft.tokens.GetLoadPercentage() == 0 && ft.sfWorkerPool.Load().GetActiveWorkersCount() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	require.Equalf(t, float64(0), ft.tokens.GetLoadPercentage(),
		"token leak: not all tokens released after processing %d tasks", n)
	require.Equalf(t, 0, ft.sfWorkerPool.Load().GetActiveWorkersCount(),
		"active worker count must return to 0 after the batch drains")
}
