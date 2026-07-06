package statefun

// Regression test for the per-call request timeout being dropped on the
// AutoRequestSelect path (io.go request()). Before the fix, the
// AutoRequestSelect branch recursed WITHOUT forwarding the caller's
// `timeout...` variadic, so a custom short timeout was silently replaced by the
// multi-second runtime default (config.requestTimeoutSec). This test pins the
// contract: a per-call timeout passed to Request(AutoRequestSelect, ...) must
// bound the NATS request path.

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/stretchr/testify/require"
)

// Test_Request_AutoSelect_HonorsPerCallTimeout verifies that a per-call timeout
// survives the AutoRequestSelect -> NatsCoreGlobalRequest recursion in
// (*Runtime).request.
//
// Setup rationale:
//   - The target function allows ONLY NatsCoreGlobalRequest. That (a) stands up
//     the NATS request receiver so nc.Request finds a responder and actually
//     waits, and (b) makes functionTypeIsReadyForGoLangCommunication return 3,
//     so AutoRequestSelect keeps selection == NatsCoreGlobalRequest (the local
//     path runs the handler synchronously and would not exercise the NATS
//     timeout at all).
//   - The handler blocks well past the 300ms per-call timeout but comfortably
//     under the 60s runtime default (RequestTimeoutSec), so the reply is
//     withheld inside the window under test. With the fix the call times out at
//     ~300ms; without it, the dropped timeout falls back to the 60s default and
//     the call would instead return only once the 3s handler replied.
func Test_Request_AutoSelect_HonorsPerCallTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	slow := func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {
		// Withhold any reply past the per-call timeout. Unblocks on teardown so
		// the worker goroutine does not linger for the full 3s.
		select {
		case <-release:
		case <-time.After(3 * time.Second):
		}
	}

	rt, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		cfg := *NewFunctionTypeConfig().
			SetAllowedRequestProviders(sfPlugins.NatsCoreGlobalRequest)
		NewFunctionType(rt, "test.timeout.slow", slow, cfg)
	})
	defer srv.Shutdown()

	const perCall = 300 * time.Millisecond

	start := time.Now()
	_, err := rt.Request(
		sfPlugins.AutoRequestSelect,
		"test.timeout.slow",
		"id1",
		easyjson.NewJSONObject().GetPtr(),
		nil,
		perCall,
	)
	elapsed := time.Since(start)

	// Must fail (the handler never replied within the window) ...
	require.Error(t, err, "request should time out, not return success on the runtime default")
	// ... and the failure must be a genuine timeout after ~perCall, not an
	// instant fast-fail (e.g. not-ready / no-responders), which would make the
	// timing bound below meaningless.
	require.Contains(t, err.Error(), "timeout", "error should be a request timeout: %v", err)
	require.GreaterOrEqual(t, elapsed, perCall-50*time.Millisecond,
		"request returned before the per-call timeout could elapse (elapsed=%v)", elapsed)
	// The core assertion: the per-call 300ms bound was applied, NOT the 60s
	// runtime default. Without the timeout-forwarding fix this call would block
	// until the 3s handler reply (or the 60s default) and blow past the ceiling.
	require.Less(t, elapsed, 1500*time.Millisecond,
		"per-call timeout was not honored through AutoRequestSelect; fell back to the runtime default (elapsed=%v)", elapsed)
}
