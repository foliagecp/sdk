package cache

// The startup sequence against a real broker.
//
// A runtime loads the bucket twice when it starts: once to build the cache and
// once more after WAL recovery. The first load reports every delete marker it
// meets to the purger, so the second load runs with the purger's queue full.
// On nats-server 2.12 a marker removed by its sequence before the second load
// reached it left that load waiting for an end nats.go's Watch never reported,
// and a runtime restarted after a large rebuild stayed alive and deaf. The
// embedded 2.10 broker counts such removals correctly and never showed it,
// which is why this one test brings a broker of its own.

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newBrokerKV(t *testing.T) (nats.JetStreamContext, *nats.Conn) {
	t.Helper()
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
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
			t.Errorf("stop test broker: %v", err)
		}
	})
	host, err := broker.Host(ctx)
	require.NoError(t, err)
	port, err := broker.MappedPort(ctx, "4222/tcp")
	require.NoError(t, err)
	nc, err := nats.Connect("nats://" + net.JoinHostPort(host, port.Port()))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096))
	require.NoError(t, err)
	return js, nc
}

// Test_Load_SecondLoadFinishesWithMarkersQueued — the bucket is loaded and at
// once loaded again, as startup does, with one marker per three keys waiting
// for the purger. Every round the second load must finish, hold every live
// key and none of the deleted ones; after the last round the markers must be
// gone from the stream.
//
// The failure it guards against was a race, met in two to four rounds of this
// size; the rounds are few enough to stay quick and enough to meet it.
func Test_Load_SecondLoadFinishesWithMarkersQueued(t *testing.T) {
	const (
		rounds = 6
		keys   = 150000
		step   = 3 // every step-th key is deleted
	)
	js, nc := newBrokerKV(t)
	for round := 0; round < rounds; round++ {
		bucket := fmt.Sprintf("second_load_%d", round)
		kvs, err := customNatsKv.CreateKeyValue(nc, js, &nats.KeyValueConfig{Bucket: bucket})
		require.NoError(t, err)
		key := func(i int) string { return fmt.Sprintf("dom/k%07d", i) }
		publish := func(i int, marker bool) {
			m := nats.NewMsg(customNatsKv.KVStoredSubject(kvs, KVStorePrefix+"."+key(i)))
			if marker {
				m.Header.Set("KV-Operation", "DEL")
			} else {
				m.Data = []byte(`{"body":{"hostname":"` + key(i) + `"}}`)
			}
			_, err := js.PublishMsgAsync(m, nats.StallWait(30*time.Second))
			require.NoError(t, err)
		}
		for i := 0; i < keys; i++ {
			publish(i, false)
		}
		for i := 0; i < keys; i += step {
			publish(i, true)
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(2 * time.Minute):
			t.Fatalf("round %d: the bucket was not filled in time", round)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cs, err := NewCacheStore(ctx, NewCacheConfig(bucket), js, kvs)
		require.NoError(t, err)

		reload, stop := context.WithTimeout(ctx, 30*time.Second)
		started := time.Now()
		err = cs.RehydrateFromKV(reload)
		stop()
		require.NoErrorf(t, err, "round %d: the second load did not finish (waited %s)", round, time.Since(started).Round(time.Millisecond))

		for i := 0; i < keys; i += 997 {
			require.Equalf(t, i%step != 0, cs.Exists(key(i)), "round %d: key %s after the second load", round, key(i))
		}
		if round == rounds-1 {
			waitPurged(t, cs, 2*time.Minute)
			require.EqualValues(t, keys-(keys+step-1)/step, streamMsgs(t, js, kvs), "only the live keys may be left in the stream")
		}

		cancel()
		require.NoError(t, js.DeleteKeyValue(bucket))
	}
}
