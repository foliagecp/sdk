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
	"github.com/foliagecp/sdk/tests/integration/export/pg_neo4j/dumper"
)

var (
	NatsURL      = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	PgURL        = system.GetEnvMustProceed("PG_URL", "postgres://foliage:foliage@postgres:5432/foliage?sslmode=disable")
	Neo4jURI     = system.GetEnvMustProceed("NEO4J_URI", "bolt://neo4j:7687")
	Neo4jUser    = system.GetEnvMustProceed("NEO4J_USER", "neo4j")
	Neo4jPass    = system.GetEnvMustProceed("NEO4J_PASS", "foliage123")
	EnableTLS    = system.GetEnvMustProceed("ENABLE_TLS", false)

	pgDumper    *dumper.SemanticPGDumper
	neo4jDumper *dumper.SemanticNeo4jDumper
)

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	graphCRUD.RegisterAllFunctionTypes(runtime)
	statefun.RegisterExportDumper(
		runtime,
		"pg-dumper",
		"export.pg.handler",
		SemanticPGHandler,
		statefun.NewFunctionTypeConfig().SetMaxAckPending(1),
	)
	statefun.RegisterExportDumper(
		runtime,
		"neo4j-dumper",
		"export.neo4j.handler",
		SemanticNeo4jHandler,
		statefun.NewFunctionTypeConfig().SetMaxAckPending(1),
	)
}

func afterStart(ctx context.Context, runtime *statefun.Runtime) error {
	lg.Logln(lg.InfoLevel, "=== PG+Neo4j export test: afterStart begin ===")

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

	lg.Logln(lg.InfoLevel, "=== Updating object ===")
	updateBody := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON("active"))
	system.MsgOnErrorReturn(dbc.CMDB.ObjectUpdate("srv-0", *updateBody.GetPtr(), false))

	lg.Logln(lg.InfoLevel, "=== Deleting an object ===")
	system.MsgOnErrorReturn(dbc.CMDB.ObjectDelete("srv-4"))

	lg.Logln(lg.InfoLevel, "=== CRUD complete, waiting for export pipeline... ===")
	time.Sleep(15 * time.Second)

	// --- PG Verification ---
	lg.Logln(lg.InfoLevel, "=== PostgreSQL Verification ===")

	pgTypes, _ := pgDumper.CountTypes(ctx)
	pgObjects, _ := pgDumper.CountObjects(ctx)
	pgTypeLinks, _ := pgDumper.CountTypeLinks(ctx)
	pgObjLinks, _ := pgDumper.CountObjectLinks(ctx)

	lg.Logf(lg.InfoLevel, "PG Types: %d", pgTypes)
	lg.Logf(lg.InfoLevel, "PG Objects: %d", pgObjects)
	lg.Logf(lg.InfoLevel, "PG Type links: %d", pgTypeLinks)
	lg.Logf(lg.InfoLevel, "PG Object links: %d", pgObjLinks)

	// --- Neo4j Verification ---
	lg.Logln(lg.InfoLevel, "=== Neo4j Verification ===")

	n4jTypes, _ := neo4jDumper.CountNodes(ctx, "Type")
	n4jObjects, _ := neo4jDumper.CountNodes(ctx, "Object")
	n4jTypeLinks, _ := neo4jDumper.CountRelationships(ctx, "TYPE_LINK")
	n4jObjLinks, _ := neo4jDumper.CountRelationships(ctx, "OBJECT_LINK")

	lg.Logf(lg.InfoLevel, "Neo4j Types: %d", n4jTypes)
	lg.Logf(lg.InfoLevel, "Neo4j Objects: %d", n4jObjects)
	lg.Logf(lg.InfoLevel, "Neo4j Type links: %d", n4jTypeLinks)
	lg.Logf(lg.InfoLevel, "Neo4j Object links: %d", n4jObjLinks)

	// --- Sample data ---
	testIDs := []string{"srv-0", "srv-1", "rack-A", "nic-0"}
	for _, id := range testIDs {
		fullID := domainName + "/" + id
		typeID, body, err := pgDumper.ReadObject(ctx, fullID)
		if err != nil {
			lg.Logf(lg.WarnLevel, "PG Object %s NOT found: %s", fullID, err)
		} else {
			lg.Logf(lg.InfoLevel, "PG Object %s -> type=%s body=%s", fullID, typeID, string(body))
		}
	}

	deletedID := domainName + "/srv-4"
	if _, _, err := pgDumper.ReadObject(ctx, deletedID); err != nil {
		lg.Logf(lg.InfoLevel, "Deleted object %s correctly absent from PG", deletedID)
	} else {
		lg.Logf(lg.WarnLevel, "Deleted object %s still present in PG!", deletedID)
	}

	if pgObjects > 0 && n4jObjects > 0 {
		lg.Logln(lg.InfoLevel, "=== PG+NEO4J EXPORT TEST PASSED ===")
	} else {
		lg.Logln(lg.ErrorLevel, "=== PG+NEO4J EXPORT TEST FAILED ===")
	}

	lg.Logln(lg.InfoLevel, "Neo4j Browser available at http://localhost:7474")

	return nil
}

func initPGAndTranslator(ctx context.Context) error {
	var pgErr error
	for attempt := 1; attempt <= 10; attempt++ {
		pgDumper, pgErr = dumper.NewSemanticPGDumper(ctx, PgURL)
		if pgErr == nil {
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
	pgTranslator = statefun.NewSemanticTranslator(pgDumper)
	return nil
}

func initNeo4jAndTranslator(ctx context.Context) error {
	var n4jErr error
	for attempt := 1; attempt <= 10; attempt++ {
		neo4jDumper, n4jErr = dumper.NewSemanticNeo4jDumper(ctx, Neo4jURI, Neo4jUser, Neo4jPass)
		if n4jErr == nil {
			break
		}
		lg.Logf(lg.WarnLevel, "Neo4j connect attempt %d/10: %s", attempt, n4jErr)
		time.Sleep(2 * time.Second)
	}
	if n4jErr != nil {
		return fmt.Errorf("connect to Neo4j after retries: %w", n4jErr)
	}
	if err := neo4jDumper.InitSchema(ctx); err != nil {
		return fmt.Errorf("init Neo4j schema: %w", err)
	}
	lg.Logln(lg.InfoLevel, "Neo4j schema initialized")
	neo4jTranslator = statefun.NewSemanticTranslator(neo4jDumper)
	return nil
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	ctx := context.Background()

	if err := initPGAndTranslator(ctx); err != nil {
		lg.Logf(lg.ErrorLevel, "Cannot initialize PG: %s", err)
		return
	}

	if err := initNeo4jAndTranslator(ctx); err != nil {
		lg.Logf(lg.ErrorLevel, "Cannot initialize Neo4j: %s", err)
		return
	}

	runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "pg_neo4j_export_test").
		UseJSDomainAsHubDomainName().
		SetTLS(EnableTLS).
		SetExportEnabled(true))
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime: %s", err)
		return
	}

	RegisterFunctionTypes(runtime)
	runtime.RegisterOnAfterStartFunction(afterStart, true)

	if err := runtime.Start(ctx, cache.NewCacheConfig("pg_neo4j_export_cache")); err != nil {
		lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
	}
}
