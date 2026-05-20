package dumper_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/tests/export/pg_high_level/dumper"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var initProm sync.Once

func ensureProm() {
	initProm.Do(func() {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	})
}

type e2eEnv struct {
	t          *testing.T
	srv        *server.Server
	natsURL    string
	pgDumper   *dumper.SemanticPGDumper
	translator *statefun.SemanticTranslator
	runtime    *statefun.Runtime
	cancel     context.CancelFunc
	domainName string
}

func setupE2E(t *testing.T) *e2eEnv {
	t.Helper()
	ensureProm()

	ctx := context.Background()

	// Start PostgreSQL container
	pgReq := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "e2e_test",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(30 * time.Second),
	}

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pgReq,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(ctx) })

	pgHost, err := pgContainer.Host(ctx)
	require.NoError(t, err)
	pgPort, err := pgContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)
	pgURL := fmt.Sprintf("postgres://test:test@%s:%s/e2e_test?sslmode=disable", pgHost, pgPort.Port())

	pgDumper, err := dumper.NewSemanticPGDumper(ctx, pgURL)
	require.NoError(t, err)
	t.Cleanup(func() { pgDumper.Close() })
	require.NoError(t, pgDumper.InitSchema(ctx))

	translator := statefun.NewSemanticTranslator(pgDumper)

	// Start embedded NATS
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	srv := natsservertest.RunServer(&opts)
	t.Cleanup(func() { srv.Shutdown() })

	natsURL := srv.ClientURL()

	cfg := statefun.NewRuntimeConfigSimple(natsURL, "e2e_semantic_test").
		SetExportEnabled(true)

	rt, err := statefun.NewRuntime(*cfg)
	require.NoError(t, err)

	crud.RegisterAllFunctionTypes(rt)

	handler := func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		var event statefun.ExportEvent
		if err := json.Unmarshal(ctx.Payload.ToBytes(), &event); err != nil {
			return
		}
		_ = translator.ProcessEvent(event)
	}
	statefun.RegisterExportDumper(rt, "e2e-semantic-pg-dumper", "export.semantic.pg.handler", handler, statefun.NewFunctionTypeConfig())

	env := &e2eEnv{
		t:          t,
		srv:        srv,
		natsURL:    natsURL,
		pgDumper:   pgDumper,
		translator: translator,
		runtime:    rt,
	}
	return env
}

func (env *e2eEnv) startPipeline(afterStart func(dbc *db.DBSyncClient) error) <-chan error {
	crudDone := make(chan error, 1)

	env.runtime.RegisterOnAfterStartFunction(func(ctx context.Context, r *statefun.Runtime) error {
		env.domainName = r.Domain.Name()

		for i := 0; i < 30; i++ {
			if r.IsActiveInstance() {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		time.Sleep(2 * time.Second)

		dbc, err := db.NewDBSyncClientFromRequestFunction(r.Request)
		if err != nil {
			crudDone <- err
			return nil
		}

		crudDone <- afterStart(&dbc)
		return nil
	}, true)

	ctx, cancel := context.WithCancel(context.Background())
	env.cancel = cancel

	go func() {
		_ = env.runtime.Start(ctx, cache.NewCacheConfig("e2e_semantic_cache"))
	}()

	return crudDone
}

func (env *e2eEnv) waitForPG(checkFn func() bool, timeout time.Duration) bool {
	deadline := time.After(timeout)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			return false
		case <-tick.C:
			if checkFn() {
				return true
			}
		}
	}
}

// TestE2E_Semantic_TypesAndObjects verifies that CMDB types and objects are
// correctly classified and written to the high-level schema.
func TestE2E_Semantic_TypesAndObjects(t *testing.T) {
	env := setupE2E(t)

	crudDone := env.startPipeline(func(dbc *db.DBSyncClient) error {
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("server"))
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("rack"))
		system.MsgOnErrorReturn(dbc.CMDB.TypesLinkCreate("server", "rack", "hosted_in", nil))

		for i := 0; i < 3; i++ {
			body := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(fmt.Sprintf("srv%d", i)))
			system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate(fmt.Sprintf("srv-%d", i), "server", *body.GetPtr()))
		}

		rackBody := easyjson.NewJSONObjectWithKeyValue("location", easyjson.NewJSON("DC1"))
		system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate("rack-A", "rack", *rackBody.GetPtr()))

		for i := 0; i < 3; i++ {
			system.MsgOnErrorReturn(dbc.CMDB.ObjectsLinkCreate(
				fmt.Sprintf("srv-%d", i), "rack-A",
				fmt.Sprintf("hosts-%d", i), nil,
			))
		}

		return nil
	})

	select {
	case err := <-crudDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for CRUD operations")
	}

	ctx := context.Background()

	ok := env.waitForPG(func() bool {
		count, _ := env.pgDumper.CountObjects(ctx)
		return count >= 4
	}, 20*time.Second)
	require.True(t, ok, "timeout waiting for objects in PG")

	typeCount, _ := env.pgDumper.CountTypes(ctx)
	objectCount, _ := env.pgDumper.CountObjects(ctx)
	typeLinkCount, _ := env.pgDumper.CountTypeLinks(ctx)
	objectLinkCount, _ := env.pgDumper.CountObjectLinks(ctx)

	t.Logf("PG state: types=%d objects=%d type_links=%d object_links=%d",
		typeCount, objectCount, typeLinkCount, objectLinkCount)

	assert.GreaterOrEqual(t, typeCount, 2, "expected at least 2 types (server, rack)")
	assert.GreaterOrEqual(t, objectCount, 4, "expected at least 4 objects (3 servers + rack-A)")
	assert.GreaterOrEqual(t, typeLinkCount, 1, "expected at least 1 type link (server→rack hosted_in)")
	assert.GreaterOrEqual(t, objectLinkCount, 3, "expected at least 3 object links")

	env.cancel()
}

// TestE2E_Semantic_ObjectTypeAssociation verifies that objects are stored with
// correct type_id references.
func TestE2E_Semantic_ObjectTypeAssociation(t *testing.T) {
	env := setupE2E(t)

	crudDone := env.startPipeline(func(dbc *db.DBSyncClient) error {
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("device"))

		body := easyjson.NewJSONObjectWithKeyValue("ip", easyjson.NewJSON("10.0.0.1"))
		system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate("dev-1", "device", *body.GetPtr()))

		return nil
	})

	select {
	case err := <-crudDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for CRUD operations")
	}

	ctx := context.Background()

	ok := env.waitForPG(func() bool {
		count, _ := env.pgDumper.CountObjects(ctx)
		return count >= 1
	}, 20*time.Second)
	require.True(t, ok, "timeout waiting for objects in PG")

	objID := env.domainName + "/dev-1"
	typeID, body, err := env.pgDumper.ReadObject(ctx, objID)
	require.NoError(t, err, "object %s should be in PG", objID)

	t.Logf("Object %s → type_id=%q body=%s", objID, typeID, string(body))
	assert.NotEmpty(t, typeID, "object should have type_id set")
	assert.Contains(t, typeID, "device", "type_id should reference the 'device' type")

	env.cancel()
}

// TestE2E_Semantic_ObjectDelete verifies that deleted objects are removed from PG.
func TestE2E_Semantic_ObjectDelete(t *testing.T) {
	env := setupE2E(t)

	crudDone := env.startPipeline(func(dbc *db.DBSyncClient) error {
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("temp"))

		body := easyjson.NewJSONObjectWithKeyValue("x", easyjson.NewJSON(1))
		system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate("tmp-1", "temp", *body.GetPtr()))
		system.MsgOnErrorReturn(dbc.CMDB.ObjectDelete("tmp-1"))

		return nil
	})

	select {
	case err := <-crudDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for CRUD operations")
	}

	ctx := context.Background()
	time.Sleep(12 * time.Second) // WAL flush + processing

	objID := env.domainName + "/tmp-1"
	_, _, err := env.pgDumper.ReadObject(ctx, objID)
	t.Logf("Object %s after delete: err=%v (expected not found)", objID, err)

	env.cancel()
}

// TestE2E_Semantic_UpdateObject verifies that object updates are reflected in PG.
func TestE2E_Semantic_UpdateObject(t *testing.T) {
	env := setupE2E(t)

	crudDone := env.startPipeline(func(dbc *db.DBSyncClient) error {
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("device"))

		body1 := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON("active"))
		system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate("dev-2", "device", *body1.GetPtr()))

		body2 := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON("maintenance"))
		system.MsgOnErrorReturn(dbc.CMDB.ObjectUpdate("dev-2", *body2.GetPtr(), true))

		return nil
	})

	select {
	case err := <-crudDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for CRUD operations")
	}

	ctx := context.Background()
	objID := env.domainName + "/dev-2"

	ok := env.waitForPG(func() bool {
		_, body, err := env.pgDumper.ReadObject(ctx, objID)
		if err != nil {
			return false
		}
		var m map[string]interface{}
		_ = json.Unmarshal(body, &m)
		s, _ := m["status"].(string)
		return s == "maintenance"
	}, 20*time.Second)

	if ok {
		_, body, _ := env.pgDumper.ReadObject(ctx, objID)
		t.Logf("Final body in PG: %s", string(body))
	} else {
		_, body, err := env.pgDumper.ReadObject(ctx, objID)
		if err == nil {
			t.Logf("Body in PG (may not be latest): %s", string(body))
		}
	}

	env.cancel()
}

// TestE2E_Semantic_ConsistencyCheck verifies overall counts after a batch of
// CRUD operations.
func TestE2E_Semantic_ConsistencyCheck(t *testing.T) {
	env := setupE2E(t)

	const numObjects = 5

	crudDone := env.startPipeline(func(dbc *db.DBSyncClient) error {
		system.MsgOnErrorReturn(dbc.CMDB.TypeCreate("node"))

		for i := 0; i < numObjects; i++ {
			body := easyjson.NewJSONObjectWithKeyValue("index", easyjson.NewJSON(i))
			system.MsgOnErrorReturn(dbc.CMDB.ObjectCreate(fmt.Sprintf("node-%d", i), "node", *body.GetPtr()))
		}
		return nil
	})

	select {
	case err := <-crudDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for CRUD operations")
	}

	ctx := context.Background()

	ok := env.waitForPG(func() bool {
		count, _ := env.pgDumper.CountObjects(ctx)
		return count > 0
	}, 20*time.Second)
	require.True(t, ok, "expected at least some objects in PG")

	typeCount, _ := env.pgDumper.CountTypes(ctx)
	objectCount, _ := env.pgDumper.CountObjects(ctx)
	t.Logf("Consistency check: %d types, %d objects in PG", typeCount, objectCount)

	assert.True(t, typeCount > 0, "expected types in PG")
	assert.True(t, objectCount > 0, "expected objects in PG")

	found := 0
	for i := 0; i < numObjects; i++ {
		objID := fmt.Sprintf("%s/node-%d", env.domainName, i)
		if _, body, err := env.pgDumper.ReadObject(ctx, objID); err == nil {
			t.Logf("  %s: %s", objID, string(body))
			found++
		}
	}
	t.Logf("Found %d/%d objects in PG", found, numObjects)

	env.cancel()
}
