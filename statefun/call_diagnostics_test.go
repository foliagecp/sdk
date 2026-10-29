package statefun

import (
	"github.com/foliagecp/easyjson"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestCallDiagnostics_RuntimeNoRespondersDoesNotGuessDomainOrRetry(t *testing.T) {
	var calls atomic.Int32
	rt, srv := startWorkerPoolTestRuntime(t, func(r *Runtime) {
		NewFunctionType(r, "test.diagnostic.echo", func(_ sfp.StatefunExecutor, ctx *sfp.StatefunContextProcessor) {
			calls.Add(1)
			ctx.Reply.With(ctx.Payload)
		}, *NewFunctionTypeConfig().SetAllowedRequestProviders(sfp.GolangLocalRequest, sfp.NatsCoreGlobalRequest))
	})
	defer srv.Shutdown()
	for _, tt := range []struct{ fn, id, domain string }{{"test.diagnostic.echo", "wrong/object", "wrong"}, {"test.diagnostic.absent", "object", rt.Domain.name}} {
		_, err := rt.Request(sfp.AutoRequestSelect, tt.fn, tt.id, nil, nil, 200*time.Millisecond)
		require.ErrorIs(t, err, nats.ErrNoResponders)
		require.Contains(t, err.Error(), tt.fn)
		require.Contains(t, err.Error(), `target_domain="`+tt.domain+`"`)
		require.Contains(t, err.Error(), "application readiness and Request/Reply support")
		require.Contains(t, err.Error(), "object existence are undetermined")
	}
	require.Zero(t, calls.Load(), "a wrong-domain call must not retry against the local function")
	_, err := rt.Request(sfp.GolangLocalRequest, "test.diagnostic.absent", "object", nil, nil)
	require.ErrorContains(t, err, "test.diagnostic.absent")
	require.Contains(t, err.Error(), `transport="Go local"`)
}
func TestCallDiagnostics_ValidRemoteAndLocalCallsKeepTheirResults(t *testing.T) {
	rt, srv := startWorkerPoolTestRuntime(t, func(r *Runtime) {
		NewFunctionType(r, "test.diagnostic.echo", func(_ sfp.StatefunExecutor, ctx *sfp.StatefunContextProcessor) { ctx.Reply.With(ctx.Payload) }, *NewFunctionTypeConfig().SetAllowedRequestProviders(sfp.GolangLocalRequest, sfp.NatsCoreGlobalRequest))
	})
	defer srv.Shutdown()
	payload := easyjson.NewJSONObjectWithKeyValue("value", easyjson.NewJSON("unchanged"))
	for _, provider := range []sfp.RequestProvider{sfp.GolangLocalRequest, sfp.NatsCoreGlobalRequest, sfp.AutoRequestSelect} {
		result, err := rt.Request(provider, "test.diagnostic.echo", "object", &payload, nil)
		require.NoError(t, err)
		require.Equal(t, "unchanged", result.GetByPath("value").AsStringDefault(""))
	}
	sub, err := rt.nc.Subscribe("request.remote.test.diagnostic.echo.remote/object", func(m *nats.Msg) { _ = m.Respond([]byte(`{"remote":true}`)) })
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, rt.nc.Flush())
	result, err := rt.Request(sfp.AutoRequestSelect, "test.diagnostic.echo", "remote/object", nil, nil)
	require.NoError(t, err)
	require.True(t, result.GetByPath("remote").AsBoolDefault(false))
}
func TestCallDiagnostics_ShadowErrorReportsActualDestination(t *testing.T) {
	rt, srv := startWorkerPoolTestRuntime(t, func(*Runtime) {})
	defer srv.Shutdown()
	id := rt.Domain.name + "/remote#object"
	_, err := rt.Request(sfp.NatsCoreGlobalRequest, "test.missing", id, nil, nil)
	require.ErrorIs(t, err, nats.ErrNoResponders)
	require.Contains(t, err.Error(), `target_domain="remote"`)
	require.Contains(t, err.Error(), `domain_source="shadow_object_target"`)
	require.Contains(t, err.Error(), id)
}
