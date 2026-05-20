package statefun_test

import (
	"fmt"
	"strings"
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

// ---- helpers ----------------------------------------------------------------

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
	for i := 0; i < count; i++ {
		txID := fmt.Sprintf("tx-%04d", i)
		subject := fmt.Sprintf(statefun.ExportSubjectTmpl, domain) + "." + txID
		// Minimal buildNatsData wrapper: handler only needs ctx.Payload.
		data := []byte(fmt.Sprintf(
			`{"caller_typename":"","caller_id":"","payload":{"tx_id":%q,"domain":%q,"timestamp":0,"ops":[]}}`,
			txID, domain))
		_, err := js.Publish(subject, data)
		require.NoError(t, err)
	}
}

// createDumperSub creates a per-dumper sourced stream and returns a pull subscription
// bound to a durable consumer on that stream.
func createDumperSub(t *testing.T, js nats.JetStreamContext, domain, dumperName string) *nats.Subscription {
	t.Helper()
	// Generate a function type name matching the export-handler convention.
	functionTypeName := "export." + dumperName + ".handler"
	require.NoError(t, statefun.CreateExportDumperStream(js, domain, dumperName, functionTypeName, 1000, 64*1024*1024, time.Hour))

	dumperStreamName := fmt.Sprintf(statefun.ExportDumperStreamNameTmpl, domain, dumperName)
	filterSubject := fmt.Sprintf(statefun.ExportSubjectFilterTmpl, domain)
	consumerName := dumperName + "-test"

	_, err := js.AddConsumer(dumperStreamName, &nats.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	require.NoError(t, err)

	sub, err := js.PullSubscribe(filterSubject, "", nats.Bind(dumperStreamName, consumerName))
	require.NoError(t, err)
	return sub
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
			// Extract TxID from the last token of the message subject.
			tokens := strings.Split(msg.Subject, ".")
			events = append(events, statefun.ExportEvent{TxID: tokens[len(tokens)-1]})
			require.NoError(t, msg.Ack())
		}
	}
	return events
}

// ---- tests ------------------------------------------------------------------

// TestMultipleConsumers_IndependentReading verifies that each dumper's sourced
// stream independently delivers all events regardless of other dumpers.
func TestMultipleConsumers_IndependentReading(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	publishTestEvents(t, js, domain, 10)

	// Each dumper gets its own sourced stream.
	sub1 := createDumperSub(t, js, domain, "dumper-pg")
	sub2 := createDumperSub(t, js, domain, "dumper-neo4j")
	sub3 := createDumperSub(t, js, domain, "dumper-clickhouse")

	events1 := fetchAllEvents(t, sub1, 10, 10*time.Second)
	events2 := fetchAllEvents(t, sub2, 10, 10*time.Second)
	events3 := fetchAllEvents(t, sub3, 10, 10*time.Second)

	assert.Len(t, events1, 10)
	assert.Len(t, events2, 10)
	assert.Len(t, events3, 10)

	// All three dumpers receive the same events in the same order.
	for i := 0; i < 10; i++ {
		assert.Equal(t, fmt.Sprintf("tx-%04d", i), events1[i].TxID)
		assert.Equal(t, events1[i].TxID, events2[i].TxID)
		assert.Equal(t, events1[i].TxID, events3[i].TxID)
	}
}

// TestMultipleConsumers_DifferentPace verifies that a slow dumper does not
// block a fast one — each reads at its own pace from its own sourced stream.
func TestMultipleConsumers_DifferentPace(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	publishTestEvents(t, js, domain, 20)

	subFast := createDumperSub(t, js, domain, "fast-dumper")
	eventsFast := fetchAllEvents(t, subFast, 20, 10*time.Second)
	assert.Len(t, eventsFast, 20)

	subSlow := createDumperSub(t, js, domain, "slow-dumper")
	eventsSlow1 := fetchAllEvents(t, subSlow, 5, 10*time.Second)
	assert.Len(t, eventsSlow1, 5)

	time.Sleep(500 * time.Millisecond) // simulate slow processing

	eventsSlow2 := fetchAllEvents(t, subSlow, 15, 10*time.Second)
	assert.Len(t, eventsSlow2, 15)

	allSlow := append(eventsSlow1, eventsSlow2...)
	assert.Len(t, allSlow, 20)

	for i := 0; i < 20; i++ {
		assert.Equal(t, eventsFast[i].TxID, allSlow[i].TxID)
	}
}

// TestConsumer_Reconnect verifies that a durable consumer on a sourced stream
// resumes from the last acknowledged position after reconnection.
func TestConsumer_Reconnect(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	publishTestEvents(t, js, domain, 10)

	// Read first 5, then disconnect.
	sub1 := createDumperSub(t, js, domain, "reconnect-dumper")
	events1 := fetchAllEvents(t, sub1, 5, 10*time.Second)
	assert.Len(t, events1, 5)
	require.NoError(t, sub1.Unsubscribe())

	// Reconnect to the same durable consumer on the same sourced stream.
	dumperStreamName := fmt.Sprintf(statefun.ExportDumperStreamNameTmpl, domain, "reconnect-dumper")
	sub2, err := js.PullSubscribe(fmt.Sprintf(statefun.ExportSubjectFilterTmpl, domain), "", nats.Bind(dumperStreamName, "reconnect-dumper-test"))
	require.NoError(t, err)

	// Should continue from message 6, not restart from message 1.
	events2 := fetchAllEvents(t, sub2, 5, 10*time.Second)
	assert.Len(t, events2, 5)
	assert.Equal(t, "tx-0005", events2[0].TxID)
	assert.Equal(t, "tx-0009", events2[4].TxID)
}

// TestConsumer_Ordering verifies that events arrive in strict publish order.
func TestConsumer_Ordering(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	publishTestEvents(t, js, domain, 100)

	sub := createDumperSub(t, js, domain, "order-checker")
	events := fetchAllEvents(t, sub, 100, 20*time.Second)
	assert.Len(t, events, 100)

	for i, evt := range events {
		assert.Equal(t, fmt.Sprintf("tx-%04d", i), evt.TxID, "event %d out of order", i)
	}
}

// TestMultipleConsumers_10Consumers verifies that 10 concurrent dumpers all
// receive all events independently without interfering with each other.
func TestMultipleConsumers_10Consumers(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	publishTestEvents(t, js, domain, 50)

	const numConsumers = 10
	subs := make([]*nats.Subscription, numConsumers)
	for i := 0; i < numConsumers; i++ {
		subs[i] = createDumperSub(t, js, domain, fmt.Sprintf("consumer-%02d", i))
	}

	var wg sync.WaitGroup
	results := make([][]statefun.ExportEvent, numConsumers)
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = fetchAllEvents(t, subs[idx], 50, 20*time.Second)
		}(i)
	}
	wg.Wait()

	for i := 0; i < numConsumers; i++ {
		assert.Len(t, results[i], 50, "consumer %d", i)
	}
	for i := 1; i < numConsumers; i++ {
		for j := 0; j < 50; j++ {
			assert.Equal(t, results[0][j].TxID, results[i][j].TxID,
				"consumer %d event %d differs from consumer 0", i, j)
		}
	}
}

// TestDeleteExportDumperStream verifies that a per-dumper sourced stream can
// be deleted and is no longer accessible afterwards.
func TestDeleteExportDumperStream(t *testing.T) {
	srv, js := runConsumerTestServer(t)
	defer srv.Shutdown()

	domain := "hub"
	createTestExportStream(t, js, domain)
	require.NoError(t, statefun.CreateExportDumperStream(js, domain, "temp-dumper", "export.temp-dumper.handler", 1000, 64*1024*1024, time.Hour))

	dumperStreamName := fmt.Sprintf(statefun.ExportDumperStreamNameTmpl, domain, "temp-dumper")
	_, err := js.StreamInfo(dumperStreamName)
	require.NoError(t, err, "stream should exist before delete")

	require.NoError(t, statefun.DeleteExportDumperStream(js, domain, "temp-dumper"))

	_, err = js.StreamInfo(dumperStreamName)
	assert.Error(t, err, "stream should be gone after delete")
}

// BenchmarkConsumerThroughput measures pull-consumer throughput on a sourced stream.
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
	_ = ec.CreateExportStream(100000, 512*1024*1024, time.Hour, 1)

	subject := fmt.Sprintf(statefun.ExportSubjectTmpl, domain) + ".bench-tx"
	data := []byte(`{"caller_typename":"","caller_id":"","payload":{"tx_id":"bench-tx","domain":"hub","timestamp":0,"ops":[]}}`)
	for i := 0; i < b.N; i++ {
		_, _ = js.Publish(subject, data)
	}

	_ = statefun.CreateExportDumperStream(js, domain, "bench-dumper", "export.bench-dumper.handler", 100000, 512*1024*1024, time.Hour)
	dumperStreamName := fmt.Sprintf(statefun.ExportDumperStreamNameTmpl, domain, "bench-dumper")
	consumerName := "bench-consumer"
	filterSubject := fmt.Sprintf(statefun.ExportSubjectFilterTmpl, domain)
	_, _ = js.AddConsumer(dumperStreamName, &nats.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	sub, _ := js.PullSubscribe(filterSubject, "", nats.Bind(dumperStreamName, consumerName))

	b.ResetTimer()

	received := 0
	for received < b.N {
		msgs, err := sub.Fetch(100, nats.MaxWait(time.Second))
		if err != nil {
			continue
		}
		for _, msg := range msgs {
			_ = msg.Ack()
			received++
		}
	}
}
