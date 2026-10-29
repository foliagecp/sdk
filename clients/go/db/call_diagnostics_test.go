package db

import (
	"github.com/foliagecp/easyjson"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats-server/v2/server"
	ntest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func diagnosticDBNode(t *testing.T) (*nats.Conn, *server.Server) {
	t.Helper()
	opts := ntest.DefaultTestOptions
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	srv := ntest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc, srv
}
func TestCallDiagnostics_StandaloneClient(t *testing.T) {
	nc, _ := diagnosticDBNode(t)
	request := getRequestFunc(nc, 1, "hub")
	for _, id := range []string{"object", "wrong/object"} {
		_, err := request(sfp.AutoRequestSelect, "functions.missing", id, nil, nil)
		require.ErrorIs(t, err, nats.ErrNoResponders)
		require.Contains(t, err.Error(), id)
		require.Contains(t, err.Error(), "functions.missing")
		require.Contains(t, err.Error(), "check the domain prefix")
	}
	client, err := NewDBSyncClientFromRequestFunction(request)
	require.NoError(t, err)
	_, err = client.CMDB.ObjectRead("wrong/object")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNotFound)
	require.Contains(t, err.Error(), nats.ErrNoResponders.Error())
	require.Contains(t, err.Error(), `target_domain="wrong"`)
}
func TestCallDiagnostics_StandaloneRemoteSuccessAndTimeout(t *testing.T) {
	nc, _ := diagnosticDBNode(t)
	release := make(chan struct{})
	defer close(release)
	sub, err := nc.Subscribe("request.remote.functions.test.remote/object", func(m *nats.Msg) {
		data, _ := easyjson.JSONFromBytes(m.Data)
		if data.GetByPath("payload.slow").AsBoolDefault(false) {
			<-release
		}
		_ = m.Respond([]byte(`{"status":"ok","data":{"remote":true}}`))
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, nc.Flush())
	request := getRequestFunc(nc, 60, "hub")
	result, err := request(sfp.AutoRequestSelect, "functions.test", "remote/object", nil, nil)
	require.NoError(t, err)
	require.True(t, result.GetByPath("data.remote").AsBoolDefault(false))
	start := time.Now()
	_, err = request(sfp.AutoRequestSelect, "functions.test", "remote/object", easyjson.NewJSONObjectWithKeyValue("slow", easyjson.NewJSON(true)).GetPtr(), nil, 80*time.Millisecond)
	require.ErrorIs(t, err, nats.ErrTimeout)
	require.Less(t, time.Since(start), time.Second)
	require.Contains(t, err.Error(), "timeout=80ms")
}
