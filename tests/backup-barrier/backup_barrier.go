// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"context"
	"fmt"
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
	"github.com/foliagecp/sdk/embedded/graph/search"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
	"strings"
	"time"

	graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	// NatsURL - nats server url
	NatsURL  = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	dbClient db.DBSyncClient
	js       nats.JetStreamContext
	kv       nats.KeyValue
)

func runBackupBarrierTest() {
	le := lg.GetLogger()
	le.Infof(context.TODO(), "Starting Backup Barrier Test")

	if err := initKVConnection(); err != nil {
		le.Errorf(context.TODO(), "Failed to init KV connection: %v", err)
		return
	}

	body := easyjson.NewJSONObject()
	system.MsgOnErrorReturn(dbClient.CMDB.TypeUpdate("typea", body, true, true))

	initialKVCount := countKVKeys()
	initialTestObjectsCount := countTestObjectsInKV()
	le.Infof(context.TODO(), "Initial state - Total KV keys: %d, Test objects: %d",
		initialKVCount, initialTestObjectsCount)

	le.Infof(context.TODO(), ">> 1: Creating 50 objects...")
	for i := 0; i < 50; i++ {
		body.SetByPath("field1", easyjson.NewJSON(1))
		body.SetByPath("field2", easyjson.NewJSON(2))
		body.SetByPath("field3", easyjson.NewJSON(3))
		body.SetByPath("created_at", easyjson.NewJSON(time.Now().UnixNano()))
		body.SetByPath("object_index", easyjson.NewJSON(i))

		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(fmt.Sprintf("test_%d", i), "typea", body))
		le.Tracef(context.TODO(), "object %d created", i)
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	afterCreateKVCount := countKVKeys()
	afterCreateTestObjectsCount := countTestObjectsInKV()
	le.Infof(context.TODO(), "After creation - Total KV keys: %d (+%d), Test objects: %d (+%d)",
		afterCreateKVCount, afterCreateKVCount-initialKVCount,
		afterCreateTestObjectsCount, afterCreateTestObjectsCount-initialTestObjectsCount)

	le.Infof(context.TODO(), "Waiting 10 seconds for KV synchronization...")
	time.Sleep(10 * time.Second)

	duringWaitKVCount := countKVKeys()
	duringWaitTestObjectsCount := countTestObjectsInKV()
	le.Infof(context.TODO(), "During wait - Total KV keys: %d (+%d from start), Test objects: %d (+%d from start)",
		duringWaitKVCount, duringWaitKVCount-initialKVCount,
		duringWaitTestObjectsCount, duringWaitTestObjectsCount-initialTestObjectsCount)

	le.Infof(context.TODO(), ">> 2: Deleting 50 objects...")
	for i := 0; i < 50; i++ {
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectDelete(fmt.Sprintf("test_%d", i)))
		le.Tracef(context.TODO(), "object %d deleted", i)
	}

	time.Sleep(2 * time.Second)

	afterDeleteKVCount := countKVKeys()
	afterDeleteTestObjectsCount := countTestObjectsInKV()
	le.Infof(context.TODO(), "After deletion - Total KV keys: %d (%d from initial), Test objects: %d (%d from initial)",
		afterDeleteKVCount, afterDeleteKVCount-initialKVCount,
		afterDeleteTestObjectsCount, afterDeleteTestObjectsCount-initialTestObjectsCount)

	le.Infof(context.TODO(), "Waiting additional 10 seconds for final KV cleanup...")
	time.Sleep(10 * time.Second)

	finalKVCount := countKVKeys()
	finalTestObjectsCount := countTestObjectsInKV()
	le.Infof(context.TODO(), "Final state - Total KV keys: %d (%d from initial), Test objects: %d (%d from initial)",
		finalKVCount, finalKVCount-initialKVCount,
		finalTestObjectsCount, finalTestObjectsCount-initialTestObjectsCount)

	le.Infof(context.TODO(), "-----------------------")
	le.Infof(context.TODO(), "=== KV KEYS SUMMARY ===")
	le.Infof(context.TODO(), "Initial:       %d total, %d test objects", initialKVCount, initialTestObjectsCount)
	le.Infof(context.TODO(), "After create:  %d total, %d test objects", afterCreateKVCount, afterCreateTestObjectsCount)
	le.Infof(context.TODO(), "During wait:   %d total, %d test objects", duringWaitKVCount, duringWaitTestObjectsCount)
	le.Infof(context.TODO(), "After delete:  %d total, %d test objects", afterDeleteKVCount, afterDeleteTestObjectsCount)
	le.Infof(context.TODO(), "Final:         %d total, %d test objects", finalKVCount, finalTestObjectsCount)
	le.Infof(context.TODO(), "-----------------------")
	le.Infof(context.TODO(), "change: %d total keys, %d test objects",
		finalKVCount-initialKVCount, finalTestObjectsCount-initialTestObjectsCount)

	le.Infof(context.TODO(), "test completed!")
}

func backupBarrierTestStatefun(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	runBackupBarrierTest()
}

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime)
	fpl.RegisterAllFunctionTypes(runtime)
	search.RegisterAllFunctionTypes(runtime)

	statefun.NewFunctionType(
		runtime,
		"functions.tests.backup_barrier.run",
		backupBarrierTestStatefun,
		*statefun.NewFunctionTypeConfig(),
	)
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	afterStart := func(ctx context.Context, runtime *statefun.Runtime) error {
		dbc, err := db.NewDBSyncClientFromRequestFunction(runtime.Request)
		if err != nil {
			return err
		}
		dbClient = dbc

		runBackupBarrierTest()

		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "backup_barrier").UseJSDomainAsHubDomainName()); err == nil {
		RegisterFunctionTypes(runtime)
		runtime.RegisterOnAfterStartFunction(afterStart, true)
		if err := runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s", err)
	}
}

func countKVKeys() int {
	entries, err := kv.Keys()
	if err != nil {
		le := lg.GetLogger()
		le.Errorf(context.TODO(), "Failed to get KV keys: %v", err)
		return -1
	}

	count := 0
	for range entries {
		count++
	}

	return count
}

func countTestObjectsInKV() int {
	entries, err := kv.Keys()
	if err != nil {
		le := lg.GetLogger()
		le.Errorf(context.TODO(), "Failed to get KV keys: %v", err)
		return -1
	}

	count := 0
	for _, key := range entries {
		if strings.Contains(key, "store.hub/test") {
			count++
		}
	}

	return count
}

func initKVConnection() error {
	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %v", err)
	}

	js, err = nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %v", err)
	}

	kv, err = js.KeyValue("hub_main_cache_cache_bucket")
	if err != nil {
		return fmt.Errorf("failed to get KeyValue store: %v", err)
	}

	return nil
}
