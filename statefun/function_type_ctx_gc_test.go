package statefun

// Behavior lock for the expired-context sweep in FunctionType.gc after it
// switched from getContext (deep-clones the whole stored context per key per
// tick) to GetValueJSONByPath (reads only the expiration mark). The sweep
// semantics must be byte-for-byte the same: contexts with an expired mark die,
// contexts with a future mark or no mark at all survive — salted keys
// (`<typename>.<id>===<hash>`) included.

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/stretchr/testify/require"
)

func Test_CtxGC_ExpiredContextsSweptWithoutTouchingOthers(t *testing.T) {
	noop := func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {}
	var ft *FunctionType
	rt, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		ft = NewFunctionType(rt, "test.ctxgc", noop, *NewFunctionTypeConfig())
	})
	defer srv.Shutdown()

	cs := rt.Domain.Cache()
	now := time.Now().UnixNano()

	putCtx := func(key string, expiresAtNs int64) {
		body := easyjson.NewJSONObject()
		body.SetByPath("payload", easyjson.NewJSON("ctx-data"))
		if expiresAtNs != 0 {
			body.SetByPath(contextExpirationKey, easyjson.NewJSON(expiresAtNs))
		}
		cs.SetValueJSON("test.ctxgc."+key, &body, true, -1)
	}

	putCtx("eternal", 0)                                     // no mark — must survive
	putCtx("expired", now-int64(time.Second))                // past mark — must be swept
	putCtx("future", now+int64(time.Hour))                   // future mark — must survive
	putCtx("salted===a1b2c3d4", now-int64(time.Second))      // salted, past — must be swept
	putCtx("salted-live===a1b2c3d4", now+int64(time.Hour))   // salted, future — must survive

	ft.gc(60_000)

	require.True(t, cs.ExistsJson("test.ctxgc.eternal"), "context without a TTL mark must survive gc")
	require.True(t, cs.ExistsJson("test.ctxgc.future"), "context with a future TTL must survive gc")
	require.True(t, cs.ExistsJson("test.ctxgc.salted-live===a1b2c3d4"), "salted context with a future TTL must survive gc")
	require.False(t, cs.ExistsJson("test.ctxgc.expired"), "expired context must be swept")
	require.False(t, cs.ExistsJson("test.ctxgc.salted===a1b2c3d4"), "expired salted context must be swept")

	// The surviving contexts must be intact (the sweep reads the mark only,
	// it must never rewrite or truncate the stored value).
	v, err := cs.GetValueJSON("test.ctxgc.eternal")
	require.NoError(t, err)
	require.Equal(t, "ctx-data", v.GetByPath("payload").AsStringDefault(""))
}
