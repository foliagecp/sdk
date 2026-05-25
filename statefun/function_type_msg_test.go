package statefun

// Robustness hunt: a message with no Caller must not panic the worker.
//
// handleMsgForID used to dereference *msg.Caller unconditionally, so a malformed
// message (Caller == nil) panicked inside workerTaskExecutor — the panic was
// recovered, but the task was then silently dropped. The guard makes a nil
// Caller degrade to an empty address and the handler run normally. Before the
// guard this test fails (the handler never runs / times out).

import (
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func Test_HandleMsg_NilCaller_DoesNotPanicOrDrop(t *testing.T) {
	ran := make(chan struct{})
	var once sync.Once
	handler := func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {
		once.Do(func() { close(ran) })
	}

	var ft *FunctionType
	_, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		cfg := *NewFunctionTypeConfig().
			SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
			SetMaxIdHandlers(-1)
		ft = NewFunctionType(rt, "test.nilcaller", handler, cfg)
	})
	defer srv.Shutdown()

	// A message with NO Caller.
	ft.sendMsg("idn", FunctionTypeMsg{
		Caller:          nil,
		RefusalCallback: func(bool) {},
		AckCallback:     func(bool) {},
		Payload:         easyjson.NewJSONObject().GetPtr(),
	})

	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not run for a nil-Caller message (panicked & silently dropped)")
	}
}
