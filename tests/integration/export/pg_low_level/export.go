// Foliage export test — runtime + PG dumper integration.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/tests/integration/export/pg_low_level/dumper"
)

var (
	NatsURL   = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	PgURL     = system.GetEnvMustProceed("PG_URL", "postgres://foliage:foliage@postgres:5432/foliage?sslmode=disable")
	EnableTLS = system.GetEnvMustProceed("ENABLE_TLS", false)
)

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	graphCRUD.RegisterAllFunctionTypes(runtime)
	statefun.RegisterExportDumper(
		runtime,
		"pg-dumper",
		"export.pg.handler",
		PGExportHandler,
		statefun.NewFunctionTypeConfig().SetMaxAckPending(1),
	)
}

func afterStart(ctx context.Context, runtime *statefun.Runtime) error {
	lg.Logln(lg.InfoLevel, "=== Export test: afterStart begin ===")

	// Retry PG connection — the healthcheck marks postgres ready but the first
	// TCP accept can still fail for a moment after pg_isready returns OK.
	var pgErr error
	for attempt := 1; attempt <= 10; attempt++ {
		var d *dumper.PGDumper
		d, pgErr = dumper.NewPGDumper(ctx, PgURL)
		if pgErr == nil {
			pgDumper = d
			break
		}
		lg.Logf(lg.WarnLevel, "PG connect attempt %d/10: %s", attempt, pgErr)
		time.Sleep(2 * time.Second)
	}
	if pgErr != nil {
		return fmt.Errorf("connect to PostgreSQL after retries: %w", pgErr)
	}
	if err := pgDumper.InitSchema(ctx); err != nil {
		return fmt.Errorf("init PG schema: %w", err)
	}
	lg.Logln(lg.InfoLevel, "PostgreSQL schema initialized")

	domainName := runtime.Domain.Name()

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

	lg.Logln(lg.InfoLevel, "=== CRUD complete, waiting for export pipeline... ===")

	// Wait for WAL flush → ExportCommitter → bridge → Foliage signal → handler → PG.
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

	deletedID := domainName + "/srv-4"
	if _, err := pgDumper.ReadVertex(ctx, deletedID); err != nil {
		lg.Logf(lg.InfoLevel, "Deleted vertex %s correctly absent from PG", deletedID)
	} else {
		lg.Logf(lg.WarnLevel, "Deleted vertex %s still present in PG!", deletedID)
	}

	if vertexCount > 0 {
		lg.Logln(lg.InfoLevel, "=== EXPORT TEST PASSED ===")
	} else {
		lg.Logln(lg.ErrorLevel, "=== EXPORT TEST FAILED: no vertices in PG ===")
	}

	return nil
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "export_test").
		UseJSDomainAsHubDomainName().
		SetTLS(EnableTLS).
		SetExportEnabled(true))
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime: %s", err)
		return
	}

	RegisterFunctionTypes(runtime)
	runtime.RegisterOnAfterStartFunction(afterStart, true)

	if err := runtime.Start(context.Background(), cache.NewCacheConfig("export_cache")); err != nil {
		lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
	}
}
