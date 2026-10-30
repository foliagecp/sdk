package statefun

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Concurrent runtime startup creates broker resources in parallel. Exercise
// it against a real broker process: embedded NATS 2.10.12 has an upstream
// reservation-accounting race which otherwise fails the SDK race gate.
// SDK runtime and client code remain instrumented; dependency versions do not
// change and no detector exclusions or expected failures are introduced.
func newHABroker(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	broker, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.12.4",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-js"},
			WaitingFor:   wait.ForLog("Server is ready").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := broker.Terminate(cleanup); err != nil {
			t.Errorf("stop HA test broker: %v", err)
		}
	})
	host, err := broker.Host(ctx)
	require.NoError(t, err)
	port, err := broker.MappedPort(ctx, "4222/tcp")
	require.NoError(t, err)
	return "nats://" + net.JoinHostPort(host, port.Port())
}
