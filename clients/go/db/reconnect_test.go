package db

import (
	"net"
	"testing"
	"time"

	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats-server/v2/server"
	ntest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func reconnectTestServer(t *testing.T, port int) *server.Server {
	t.Helper()
	opts := ntest.DefaultTestOptions
	opts.Port = port
	srv := ntest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	return srv
}

// A client connection reconnects like the runtime's: no limit on attempts, at
// nats.go's default interval.
func TestClientConnection_ReconnectsLikeTheRuntime(t *testing.T) {
	srv := reconnectTestServer(t, -1)
	nc, err := connectClient(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	require.Equal(t, -1, nc.Opts.MaxReconnect)
	require.Equal(t, nats.DefaultReconnectWait, nc.Opts.ReconnectWait)
}

// A client created while NATS is unreachable fails at once, as before: only
// reconnection changed, not the first connection.
func TestClientConnection_FirstConnectionStillFailsFast(t *testing.T) {
	srv := reconnectTestServer(t, -1)
	url := srv.ClientURL()
	srv.Shutdown()
	started := time.Now()
	_, err := NewDBSyncClient(url, 1, "hub")
	require.ErrorIs(t, err, nats.ErrNoServers)
	require.Less(t, time.Since(started), 5*time.Second)
}

// Requests over a client connection work again once NATS is back, however long
// it was gone. Waits of 10 ms stand in for the real 2 s, so the outage outlasts
// nats.go's default of 60 attempts several times over: a control connection
// with the same waits and the default limit has given up by then.
func TestClientConnection_OutlivesALongOutage(t *testing.T) {
	srv := reconnectTestServer(t, -1)
	url := srv.ClientURL()
	port := srv.Addr().(*net.TCPAddr).Port
	fast := []nats.Option{nats.ReconnectWait(10 * time.Millisecond), nats.ReconnectJitter(0, 0)}

	client, err := connectClient(url, fast...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	control, err := nats.Connect(url, fast...)
	require.NoError(t, err)
	t.Cleanup(control.Close)

	srv.Shutdown()
	require.Eventually(t, control.IsClosed, 10*time.Second, 10*time.Millisecond,
		"with the default limit the control connection gives up")
	time.Sleep(time.Second) // another 100 attempts or so
	require.False(t, client.IsClosed(), "the client connection must keep trying")

	srv = reconnectTestServer(t, port)
	responder, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(responder.Close)
	sub, err := responder.Subscribe("request.hub.functions.test.hub/x", func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"status":"ok","data":{}}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	require.NoError(t, responder.Flush())

	request := getRequestFunc(client, 1, "hub")
	require.Eventually(t, func() bool {
		_, err := request(sfp.AutoRequestSelect, "functions.test", "hub/x", nil, nil)
		return err == nil
	}, 10*time.Second, 50*time.Millisecond, "requests must work again once NATS is back")
}
