// Foliage export test — runtime + PG dumper integration.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/tests/export/dumper"
	"github.com/nats-io/nats.go"
)

var (
	NatsURL   = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	PgURL     = system.GetEnvMustProceed("PG_URL", "postgres://foliage:foliage@postgres:5432/foliage?sslmode=disable")
	EnableTLS = system.GetEnvMustProceed("ENABLE_TLS", false)
)

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	afterStart := func(ctx context.Context, runtime *statefun.Runtime) error {
		lg.Logln(lg.InfoLevel, "=== Export test: afterStart begin ===")

		// --- Start PG Dumper consumer ---
		pgDumper, err := dumper.NewPGDumper(ctx, PgURL)
		if err != nil {
			lg.Logf(lg.ErrorLevel, "Failed to connect to PostgreSQL: %s", err)
			return err
		}
		if err := pgDumper.InitSchema(ctx); err != nil {
			lg.Logf(lg.ErrorLevel, "Failed to init PG schema: %s", err)
			return err
		}
		lg.Logln(lg.InfoLevel, "PostgreSQL schema initialized")

		domainName := runtime.Domain.Name()

		nc, err := nats.Connect(NatsURL)
		if err != nil {
			return fmt.Errorf("dumper NATS connect: %w", err)
		}
		js, err := nc.JetStream()
		if err != nil {
			return fmt.Errorf("dumper JetStream: %w", err)
		}

		// Use the SDK helper to create a durable pull consumer on the export stream.
		sub, err := statefun.CreateExportConsumer(js, domainName, "pg-dumper")
		if err != nil {
			return fmt.Errorf("create export consumer: %w", err)
		}

		var eventsApplied atomic.Int64
		go func() {
			for {
				msgs, fetchErr := sub.Fetch(32, nats.MaxWait(2*time.Second))
				if fetchErr != nil {
					// Timeout is normal when the stream is idle — just keep polling.
					continue
				}
				for _, msg := range msgs {
					var event statefun.ExportEvent
					if jsonErr := json.Unmarshal(msg.Data, &event); jsonErr != nil {
						lg.Logf(lg.ErrorLevel, "Dumper: failed to unmarshal event: %s", jsonErr)
						_ = msg.Ack()
						continue
					}
					if applyErr := pgDumper.ApplyEvent(context.Background(), event); applyErr != nil {
						lg.Logf(lg.ErrorLevel, "Dumper: failed to apply event tx=%s: %s", event.TxID, applyErr)
						_ = msg.Nak()
						continue
					}
					n := eventsApplied.Add(1)
					lg.Logf(lg.DebugLevel, "Dumper: applied event tx=%s (%d ops), total events: %d",
						event.TxID, len(event.Ops), n)
					_ = msg.Ack()
				}
			}
		}()
		lg.Logln(lg.InfoLevel, "PG Dumper consumer started")

		// --- Perform CRUD operations ---
		dbc, err := db.NewDBSyncClientFromRequestFunction(runtime.Request)
		if err != nil {
			return err
		}

		lg.Logln(lg.InfoLevel, "=== Creating types ===")
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("server"))
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("rack"))
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("nic"))

		lg.Logln(lg.InfoLevel, "=== Creating type links ===")
		system.MsgOnErrorReturn(dbc.CMDB.TypesLinkCreate("server", "rack", "hosted_in", nil))
		system.MsgOnErrorReturn(dbc.CMDB.TypesLinkCreate("server", "nic", "has_nic", nil))

		lg.Logln(lg.InfoLevel, "=== Creating objects ===")
		for i := 0; i < 5; i++ {
			body := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(fmt.Sprintf("srv-%d", i)))
			body.SetByPath("cpu", easyjson.NewJSON(4*(i+1)))
			body.SetByPath("ram", easyjson.NewJSON(8*(i+1)))
			system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate(fmt.Sprintf("srv-%d", i), "server", *body.GetPtr()))
		}

		rackBody := easyjson.NewJSONObjectWithKeyValue("location", easyjson.NewJSON("DC1-Row3"))
		system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate("rack-A", "rack", *rackBody.GetPtr()))

		for i := 0; i < 3; i++ {
			nicBody := easyjson.NewJSONObjectWithKeyValue("speed", easyjson.NewJSON("10G"))
			system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate(fmt.Sprintf("nic-%d", i), "nic", *nicBody.GetPtr()))
		}

		lg.Logln(lg.InfoLevel, "=== Creating object links ===")
		for i := 0; i < 5; i++ {
			system.MsgOnErrorReturn(dbc.CMDB.ObjectsLinkCreate(
				fmt.Sprintf("srv-%d", i), "rack-A",
				fmt.Sprintf("rack-link-%d", i), nil,
			))
		}
		for i := 0; i < 3; i++ {
			system.MsgOnErrorReturn(dbc.CMDB.ObjectsLinkCreate(
				"srv-0", fmt.Sprintf("nic-%d", i),
				fmt.Sprintf("nic-link-%d", i), nil,
			))
		}

		lg.Logln(lg.InfoLevel, "=== Updating objects ===")
		updateBody := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON("active"))
		system.MsgOnErrorReturn(dbc.CMDB.ObjectUpdate("srv-0", *updateBody.GetPtr(), false))

		lg.Logln(lg.InfoLevel, "=== Deleting an object ===")
		system.MsgOnErrorReturn(dbc.CMDB.ObjectDelete("srv-4"))

		lg.Logln(lg.InfoLevel, "=== CRUD operations complete, waiting for export pipeline... ===")

		// Wait for WAL flush + export + PG apply
		time.Sleep(15 * time.Second)

		// --- Verify PG state ---
		lg.Logln(lg.InfoLevel, "=== Verification ===")

		vertexCount, err := pgDumper.CountVertices(ctx)
		if err != nil {
			lg.Logf(lg.ErrorLevel, "Failed to count vertices: %s", err)
		} else {
			lg.Logf(lg.InfoLevel, "Total vertices in PG: %d", vertexCount)
		}

		linkCount, err := pgDumper.CountLinks(ctx)
		if err != nil {
			lg.Logf(lg.ErrorLevel, "Failed to count links: %s", err)
		} else {
			lg.Logf(lg.InfoLevel, "Total links in PG: %d", linkCount)
		}

		// Check specific objects
		testIDs := []string{"srv-0", "srv-1", "rack-A", "nic-0"}
		for _, id := range testIDs {
			fullID := domainName + "/" + id
			body, err := pgDumper.ReadVertex(ctx, fullID)
			if err != nil {
				lg.Logf(lg.WarnLevel, "Vertex %s NOT found in PG: %s", fullID, err)
			} else {
				lg.Logf(lg.InfoLevel, "Vertex %s in PG: %s", fullID, string(body))
			}
		}

		// Check that deleted object is gone
		deletedID := domainName + "/srv-4"
		if _, err := pgDumper.ReadVertex(ctx, deletedID); err != nil {
			lg.Logf(lg.InfoLevel, "Deleted vertex %s correctly absent from PG", deletedID)
		} else {
			lg.Logf(lg.WarnLevel, "Deleted vertex %s still present in PG!", deletedID)
		}

		lg.Logf(lg.InfoLevel, "Total export events applied: %d", eventsApplied.Load())

		if vertexCount > 0 {
			lg.Logln(lg.InfoLevel, "=== EXPORT TEST PASSED ===")
		} else {
			lg.Logln(lg.ErrorLevel, "=== EXPORT TEST FAILED: no vertices in PG ===")
		}

		return nil
	}

	cfg := statefun.NewRuntimeConfigSimple(NatsURL, "export_test").
		UseJSDomainAsHubDomainName().
		SetTLS(EnableTLS).
		SetExportEnabled(true)

	runtime, err := statefun.NewRuntime(*cfg)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime: %s", err)
		return
	}

	graphCRUD.RegisterAllFunctionTypes(runtime)
	runtime.RegisterOnAfterStartFunction(afterStart, true)

	if err := runtime.Start(context.Background(), cache.NewCacheConfig("export_cache")); err != nil {
		lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
	}
}
