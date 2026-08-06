package statefun

// Accounting tests for ft_active_id_channels: the gauge is fed by an
// incremental counter (FunctionType.activeIDChannels) instead of a full
// idHandlersChannel.Range per message. The counter must stay exactly equal
// to the real map size through message churn and gc — including ids that
// arrive only via the in-process golang path, which own an
// idHandlersLastMsgTime entry but never get a channel.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/require"
)

func syncMapLen(m *sync.Map) int {
	n := 0
	m.Range(func(_, _ any) bool { n++; return true })
	return n
}

func waitGroupOrFatal(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: handlers did not finish in time", what)
	}
}

func Test_ActiveIDChannels_CounterMatchesMap_ChurnAndGC(t *testing.T) {
	var handled sync.WaitGroup
	handler := func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {
		handled.Done()
	}

	var ft *FunctionType
	_, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		cfg := *NewFunctionTypeConfig().
			SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
			// Plenty of token headroom so a burst of M sends never hits
			// TokenTryAcquire refusal (tokens = MaxWorkers + TaskQueueLen).
			SetWorkerPoolConfig(SFWorkerPoolConfig{
				MinWorkers: 2, MaxWorkers: 300, IdleTimeout: 5 * time.Second, TaskQueueLen: 300,
			})
		ft = NewFunctionType(rt, "test.idchan.counter", handler, cfg)
	})
	defer srv.Shutdown()

	newMsg := func() FunctionTypeMsg {
		return FunctionTypeMsg{
			Caller:          &sfPlugins.StatefunAddress{Typename: "test", ID: "caller"},
			RefusalCallback: func(bool) { t.Error("unexpected refusal"); handled.Done() },
			AckCallback:     func(bool) {},
			Payload:         easyjson.NewJSONObject().GetPtr(),
		}
	}

	// The worst-case unique-id stream: every message a fresh salted id.
	const m = 200
	handled.Add(m)
	for i := 0; i < m; i++ {
		ft.sendMsg(fmt.Sprintf("idchan-%d===%08x", i, i), newMsg())
	}
	waitGroupOrFatal(t, &handled, "salted churn")

	require.Equal(t, m, syncMapLen(&ft.idHandlersChannel), "map size after churn")
	require.EqualValues(t, m, ft.activeIDChannels.Load(), "counter after churn")

	// Ids delivered only through the in-process golang path: lastMsgTime
	// entry, no channel. gc must collect them without skewing the counter.
	const g = 7
	handled.Add(g)
	for i := 0; i < g; i++ {
		ft.workerTaskExecutor(fmt.Sprintf("golang-only-%d", i), newMsg())
	}
	waitGroupOrFatal(t, &handled, "golang-only path")
	require.EqualValues(t, m, ft.activeIDChannels.Load(), "golang path must not create channels")

	// lifetime 0 → every idle id is expired; drained channels are removable.
	collected, running := ft.gc(0)
	require.Equal(t, 0, running, "no handler may be running after the drain")
	require.Equal(t, m+g, collected, "gc must collect every idle id, channel-less ones included")

	require.Equal(t, 0, syncMapLen(&ft.idHandlersChannel), "map after gc")
	require.Equal(t, 0, syncMapLen(&ft.idHandlersLastMsgTime), "lastMsgTime after gc")
	require.EqualValues(t, 0, ft.activeIDChannels.Load(), "counter after gc")

	// Reuse after gc: the same id creates exactly one new channel.
	handled.Add(2)
	ft.sendMsg("idchan-reuse", newMsg())
	ft.sendMsg("idchan-reuse", newMsg())
	waitGroupOrFatal(t, &handled, "reuse")
	require.EqualValues(t, 1, ft.activeIDChannels.Load(), "same id must be counted once")
	require.Equal(t, 1, syncMapLen(&ft.idHandlersChannel))
}

// Regression beacon: the gauge update must be O(1) — its cost may not depend
// on the number of live id handlers. Before the counter it was a full
// sync.Map.Range (683 µs at N=10k, 6.7 ms at N=50k per call).
func benchmarkMeasureIdChannels(b *testing.B, n int) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	ft := &FunctionType{name: fmt.Sprintf("bench.idchan.n%d", n)}
	for i := 0; i < n; i++ {
		ft.idHandlersChannel.Store(fmt.Sprintf("id-%d===%08x", i, i), make(chan FunctionTypeMsg, 1))
		ft.activeIDChannels.Add(1)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ft.prometricsMeasureIdChannels()
	}
}

func BenchmarkPrometricsMeasureIdChannels_N100(b *testing.B)  { benchmarkMeasureIdChannels(b, 100) }
func BenchmarkPrometricsMeasureIdChannels_N100k(b *testing.B) { benchmarkMeasureIdChannels(b, 100_000) }
