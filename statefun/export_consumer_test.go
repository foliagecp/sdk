package statefun_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runConsumerTestServer(t *testing.T) (*server.Server, nats.JetStreamContext) {
	t.Helper()
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	srv := natsservertest.RunServer(&opts)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(func() { nc.Close() })

	js, err := nc.JetStream()
	require.NoError(t, err)

	return srv, js
}

func createTestExportStream(t *testing.T, js nats.JetStreamContext, domain string) {
	t.Helper()
	ec := statefun.NewExportCommitter(js, domain)
	err := ec.CreateExportStream(1000, 64*1024*1024, 1*time.Hour, 1)
	require.NoError(t, err)
}

func publishTestEvents(t *testing.T, js nats.JetStreamContext, domain string, count int) {
	t.Helper()
	subject := fmt.Sprintf(statefun.ExportSubjectTmpl, domain)
	for i := 0; i < count; i++ {
		event := statefun.ExportEvent{
			TxID:      fmt.Sprintf("tx-%04d", i),
			Domain:    domain,
			Timestamp: time.Now().UnixNano(),
			Ops: []statefun.ExportOp{
				{Op: "vertex_put", ID: fmt.Sprintf("hub/v%04d", i), Body: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))},
			},
		}
		data, err := json.Marshal(event)
		require.NoError(t, err)
		_, err = js.Publish(subject, data)
		require.NoError(t, err)
	}
}

func fetchAllEvents(t *testing.T, sub *nats.Subscription, expected int, timeout time.Duration) []statefun.ExportEvent {
	t.Helper()
	var events []statefun.ExportEvent
	deadline := time.After(timeout)

	for len(events) < expected {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %d/%d events", len(events), expected)
		default:
		}

		remaining := expected - len(events)
		batchSize := 10
		if remaining < batchSize {
			batchSize = remaining
		}

		msgs, err := sub.Fetch(batchSize, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			continue
		}
		for _, msg := range msgs {
			var event statefun.ExportEvent
			require.NoError(t, json.Unmarshal(msg.Data, &event))
			events = append(events, event)
			require.NoError(t, msg.Ack())
		}
	}
	return events
}

func TestMultipleConsumers_IndependentReading(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)

	// Publish 10 events
	publishTestEvents(t, js, domain, 10)

	// Create 3 independent consumers
	sub1, err := statefun.CreateExportConsumer(js, domain, "dumper-pg")
	require.NoError(t, err)
	sub2, err := statefun.CreateExportConsumer(js, domain, "dumper-neo4j")
	require.NoError(t, err)
	sub3, err := statefun.CreateExportConsumer(js, domain, "dumper-clickhouse")
	require.NoError(t, err)

	// Each consumer should receive all 10 events independently
	events1 := fetchAllEvents(t, sub1, 10, 5*time.Second)
	events2 := fetchAllEvents(t, sub2, 10, 5*time.Second)
	events3 := fetchAllEvents(t, sub3, 10, 5*time.Second)

	assert.Len(t, events1, 10)
	assert.Len(t, events2, 10)
	assert.Len(t, events3, 10)

	// Verify all three got the same events in the same order
	for i := 0; i < 10; i++ {
		assert.Equal(t, events1[i].TxID, events2[i].TxID)
		assert.Equal(t, events1[i].TxID, events3[i].TxID)
		assert.Equal(t, fmt.Sprintf("tx-%04d", i), events1[i].TxID)
	}
}

func TestMultipleConsumers_DifferentPace(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)

	publishTestEvents(t, js, domain, 20)

	// Fast consumer reads all
	subFast, err := statefun.CreateExportConsumer(js, domain, "fast-dumper")
	require.NoError(t, err)
	eventsFast := fetchAllEvents(t, subFast, 20, 5*time.Second)
	assert.Len(t, eventsFast, 20)

	// Slow consumer reads first 5, pauses, then reads the rest
	subSlow, err := statefun.CreateExportConsumer(js, domain, "slow-dumper")
	require.NoError(t, err)
	eventsSlow1 := fetchAllEvents(t, subSlow, 5, 5*time.Second)
	assert.Len(t, eventsSlow1, 5)

	time.Sleep(500 * time.Millisecond) // simulate slow processing

	eventsSlow2 := fetchAllEvents(t, subSlow, 15, 5*time.Second)
	assert.Len(t, eventsSlow2, 15)

	// Total: slow consumer also got all 20
	allSlow := append(eventsSlow1, eventsSlow2...)
	assert.Len(t, allSlow, 20)

	// Same order as fast consumer
	for i := 0; i < 20; i++ {
		assert.Equal(t, eventsFast[i].TxID, allSlow[i].TxID)
	}
}

func TestConsumer_Reconnect(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	publishTestEvents(t, js, domain, 10)

	// Consumer reads first 5, then disconnects
	sub1, err := statefun.CreateExportConsumer(js, domain, "reconnect-dumper")
	require.NoError(t, err)
	events1 := fetchAllEvents(t, sub1, 5, 5*time.Second)
	assert.Len(t, events1, 5)

	// Unsubscribe (simulate disconnect)
	require.NoError(t, sub1.Unsubscribe())

	// Reconnect — create new subscription on the same durable consumer
	sub2, err := js.PullSubscribe(
		fmt.Sprintf(statefun.ExportSubjectTmpl, domain),
		"reconnect-dumper",
	)
	require.NoError(t, err)

	// Should continue from message 6, not message 1
	events2 := fetchAllEvents(t, sub2, 5, 5*time.Second)
	assert.Len(t, events2, 5)

	// Verify ordering: events2 starts where events1 left off
	assert.Equal(t, "tx-0005", events2[0].TxID)
	assert.Equal(t, "tx-0009", events2[4].TxID)
}

func TestConsumer_Ordering(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	publishTestEvents(t, js, domain, 100)

	sub, err := statefun.CreateExportConsumer(js, domain, "order-checker")
	require.NoError(t, err)

	events := fetchAllEvents(t, sub, 100, 10*time.Second)
	assert.Len(t, events, 100)

	// Verify strict ordering
	for i, evt := range events {
		expected := fmt.Sprintf("tx-%04d", i)
		assert.Equal(t, expected, evt.TxID, "event %d out of order", i)
	}
}

func TestMultipleConsumers_10Consumers(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	publishTestEvents(t, js, domain, 50)

	// Create 10 consumers concurrently
	const numConsumers = 10
	subs := make([]*nats.Subscription, numConsumers)
	for i := 0; i < numConsumers; i++ {
		var err error
		subs[i], err = statefun.CreateExportConsumer(js, domain, fmt.Sprintf("consumer-%02d", i))
		require.NoError(t, err)
	}

	// All consumers read in parallel
	var wg sync.WaitGroup
	results := make([][]statefun.ExportEvent, numConsumers)

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = fetchAllEvents(t, subs[idx], 50, 10*time.Second)
		}(i)
	}

	wg.Wait()

	// All consumers got exactly 50 events
	for i := 0; i < numConsumers; i++ {
		assert.Len(t, results[i], 50, "consumer %d", i)
	}

	// All in same order
	for i := 1; i < numConsumers; i++ {
		for j := 0; j < 50; j++ {
			assert.Equal(t, results[0][j].TxID, results[i][j].TxID,
				"consumer %d event %d differs from consumer 0", i, j)
		}
	}
}

func TestDeleteExportConsumer(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)

	_, err := statefun.CreateExportConsumer(js, domain, "temp-consumer")
	require.NoError(t, err)

	err = statefun.DeleteExportConsumer(js, domain, "temp-consumer")
	require.NoError(t, err)

	// Verify consumer is gone
	streamName := fmt.Sprintf(statefun.ExportStreamNameTmpl, domain)
	_, err = js.ConsumerInfo(streamName, "temp-consumer")
	assert.Error(t, err)
}

func BenchmarkConsumerThroughput(b *testing.B) {
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = b.TempDir()
	srv := natsservertest.RunServer(&opts)
	defer srv.Shutdown()

	nc, _ := nats.Connect(srv.ClientURL())
	js, _ := nc.JetStream()

	domain := "hub"
	ec := statefun.NewExportCommitter(js, domain)
	_ = ec.CreateExportStream(100000, 512*1024*1024, 1*time.Hour, 1)

	// Pre-publish b.N events
	subject := fmt.Sprintf(statefun.ExportSubjectTmpl, domain)
	event := statefun.ExportEvent{
		TxID:   "bench-tx",
		Domain: domain,
		Ops:    []statefun.ExportOp{{Op: "vertex_put", ID: "hub/v1", Body: json.RawMessage(`{"a":1}`)}},
	}
	data, _ := json.Marshal(event)
	for i := 0; i < b.N; i++ {
		_, _ = js.Publish(subject, data)
	}

	sub, _ := statefun.CreateExportConsumer(js, domain, "bench-consumer")

	b.ResetTimer()

	received := 0
	for received < b.N {
		msgs, err := sub.Fetch(100, nats.MaxWait(1*time.Second))
		if err != nil {
			continue
		}
		for _, msg := range msgs {
			_ = msg.Ack()
			received++
		}
	}
}
