package statefun_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var initPrometrics sync.Once

func ensurePrometrics() {
	initPrometrics.Do(func() {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	})
}

func runTestServer(t *testing.T) *server.Server {
	t.Helper()
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	return natsservertest.RunServer(&opts)
}

func TestExportCommitter_Integration(t *testing.T) {
	ensurePrometrics()

	srv := runTestServer(t)
	defer srv.Shutdown()

	natsURL := srv.ClientURL()

	// Create runtime with export enabled
	cfg := statefun.NewRuntimeConfigSimple(natsURL, "export_test").
		SetExportEnabled(true)

	runtime, err := statefun.NewRuntime(*cfg)
	require.NoError(t, err)

	// Register LL CRUD functions
	crud.RegisterAllFunctionTypes(runtime)

	// Track export events received
	exportEventCh := make(chan statefun.ExportEvent, 10)

	// Subscribe to export stream after runtime starts
	runtime.RegisterOnAfterStartFunction(func(ctx context.Context, r *statefun.Runtime) error {
		// Connect separately to subscribe to export stream
		nc, err := nats.Connect(natsURL)
		if err != nil {
			return err
		}

		js, err := nc.JetStream()
		if err != nil {
			return err
		}

		exportStreamName := fmt.Sprintf(statefun.ExportStreamNameTmpl, r.Domain.Name())
		exportSubject := fmt.Sprintf(statefun.ExportSubjectTmpl, r.Domain.Name())

		// Create durable consumer on export stream
		consumerName := "test-consumer"
		_, err = js.AddConsumer(exportStreamName, &nats.ConsumerConfig{
			Name:           consumerName,
			Durable:        consumerName,
			DeliverSubject: consumerName,
			FilterSubject:  exportSubject,
			AckPolicy:      nats.AckExplicitPolicy,
		})
		if err != nil {
			return fmt.Errorf("failed to create export consumer: %w", err)
		}

		_, err = nc.Subscribe(consumerName, func(msg *nats.Msg) {
			var event statefun.ExportEvent
			if jsonErr := json.Unmarshal(msg.Data, &event); jsonErr == nil {
				exportEventCh <- event
			}
			_ = msg.Ack()
		})
		if err != nil {
			return fmt.Errorf("failed to subscribe: %w", err)
		}

		// Now perform CRUD operations
		// Create a vertex
		vertexPayload := easyjson.NewJSONObjectWithKeyValue("body",
			*easyjson.NewJSONObjectWithKeyValue("cpu", easyjson.NewJSON(16)).GetPtr())

		_, err = r.Request(sfPlugins.AutoRequestSelect,
			"functions.graph.api.vertex.create",
			r.Domain.CreateObjectIDWithThisDomain("srv01", true),
			vertexPayload.GetPtr(), nil)
		if err != nil {
			return fmt.Errorf("failed to create vertex: %w", err)
		}

		// Create another vertex for link target
		_, err = r.Request(sfPlugins.AutoRequestSelect,
			"functions.graph.api.vertex.create",
			r.Domain.CreateObjectIDWithThisDomain("rackA", true),
			nil, nil)
		if err != nil {
			return fmt.Errorf("failed to create vertex rackA: %w", err)
		}

		// Create a link between them
		linkPayload := easyjson.NewJSONObject()
		linkPayload.SetByPath("to", easyjson.NewJSON(r.Domain.CreateObjectIDWithThisDomain("rackA", true)))
		linkPayload.SetByPath("name", easyjson.NewJSON("hosts"))
		linkPayload.SetByPath("type", easyjson.NewJSON("__object"))
		linkBody := easyjson.NewJSONObjectWithKeyValue("zone", easyjson.NewJSON(1))
		linkPayload.SetByPath("body", *linkBody.GetPtr())

		_, err = r.Request(sfPlugins.AutoRequestSelect,
			"functions.graph.api.link.create",
			r.Domain.CreateObjectIDWithThisDomain("srv01", true),
			linkPayload.GetPtr(), nil)
		if err != nil {
			return fmt.Errorf("failed to create link: %w", err)
		}

		return nil
	}, false) // synchronous

	// Start runtime in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runtime.Start(ctx, cache.NewCacheConfig("test_cache"))
	}()

	// Wait for export events (WAL flush is ~5s, but cache config may differ)
	// Give up to 15 seconds for the full pipeline
	var events []statefun.ExportEvent
	timeout := time.After(15 * time.Second)

	for {
		select {
		case evt := <-exportEventCh:
			events = append(events, evt)
			// We expect at least one event with our vertices and link
			if hasAllOps(events) {
				goto verify
			}
		case <-timeout:
			goto verify
		case err := <-errCh:
			if err != nil {
				t.Fatalf("runtime error: %v", err)
			}
		}
	}

verify:
	cancel()

	// Verify we got export events
	require.NotEmpty(t, events, "expected at least one export event")

	// Collect all ops across all events
	var allOps []statefun.ExportOp
	for _, evt := range events {
		assert.NotEmpty(t, evt.TxID)
		assert.NotEmpty(t, evt.Domain)
		assert.True(t, evt.Timestamp > 0)
		allOps = append(allOps, evt.Ops...)
	}

	// Verify vertex and link presence
	foundVertexSrv01 := false
	foundVertexRackA := false
	foundLink := false

	domainName := events[0].Domain

	for _, op := range allOps {
		switch op.Op {
		case "vertex_put":
			if op.ID == domainName+"/srv01" {
				foundVertexSrv01 = true
				assert.NotNil(t, op.Body)
			}
			if op.ID == domainName+"/rackA" {
				foundVertexRackA = true
			}
		case "link_put":
			if op.From == domainName+"/srv01" && op.Name == "hosts" {
				foundLink = true
				assert.Equal(t, domainName+"/rackA", op.To)
				assert.Equal(t, "__object", op.LinkType)
			}
		}
	}

	assert.True(t, foundVertexSrv01, "expected vertex_put for srv01")
	assert.True(t, foundVertexRackA, "expected vertex_put for rackA")
	assert.True(t, foundLink, "expected link_put for srv01->rackA")
}

func hasAllOps(events []statefun.ExportEvent) bool {
	vertexCount := 0
	linkCount := 0
	for _, evt := range events {
		for _, op := range evt.Ops {
			switch op.Op {
			case "vertex_put":
				vertexCount++
			case "link_put":
				linkCount++
			}
		}
	}
	return vertexCount >= 2 && linkCount >= 1
}

func TestExportCommitter_Disabled(t *testing.T) {
	ensurePrometrics()

	srv := runTestServer(t)
	defer srv.Shutdown()

	natsURL := srv.ClientURL()

	// Create runtime WITHOUT export enabled (default)
	cfg := statefun.NewRuntimeConfigSimple(natsURL, "no_export_test")

	runtime, err := statefun.NewRuntime(*cfg)
	require.NoError(t, err)

	crud.RegisterAllFunctionTypes(runtime)

	streamChecked := make(chan bool, 1)

	runtime.RegisterOnAfterStartFunction(func(ctx context.Context, r *statefun.Runtime) error {
		// Verify export stream does NOT exist
		nc, err := nats.Connect(natsURL)
		if err != nil {
			return err
		}
		js, err := nc.JetStream()
		if err != nil {
			return err
		}

		exportStreamName := fmt.Sprintf(statefun.ExportStreamNameTmpl, r.Domain.Name())
		_, err = js.StreamInfo(exportStreamName)
		streamChecked <- (err != nil) // Should be error (stream not found)
		return nil
	}, false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = runtime.Start(ctx, cache.NewCacheConfig("test_cache"))
	}()

	select {
	case noStream := <-streamChecked:
		assert.True(t, noStream, "export stream should NOT exist when export is disabled")
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for stream check")
	}

	cancel()
}
