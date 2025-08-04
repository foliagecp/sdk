// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"context"
	"github.com/foliagecp/sdk/clients/go/db"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"time"

	graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"

	statefun "github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

var (
	// NatsURL - nats server url
	NatsURL string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	// NatsURL - nats server url
	PrometricsServerPort string = system.GetEnvMustProceed("PROMETRICS_PORT", "9901")
	// CreateSimpleGraphTest - create a simple graph on runtime start
	CreateSimpleGraphTest bool = system.GetEnvMustProceed("CREATE_SIMPLE_GRAPH_TEST", true)
	// TriggersTest - test the Foliage cmdb crud triggers
	TriggersTest bool = system.GetEnvMustProceed("TRIGGERS_TEST", true)

	dbClient db.DBSyncClient
)

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime)
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":"+PrometricsServerPort)

	afterStart := func(ctx context.Context, runtime *statefun.Runtime) error {
		dbc, err := db.NewDBSyncClientFromRequestFunction(runtime.Request)
		if err != nil {
			return err
		}
		dbClient = dbc

		CreateTestCMDB(runtime)

		if runtime.Domain.Name() == runtime.Domain.HubDomainName() {
			time.Sleep(10 * time.Second)
			servers, err := dbc.Query.JPGQLCtraQuery("root", "..*[l:type('rack-server')]")
			if err != nil {
				lg.Logf(lg.ErrorLevel, "Error: %s", err)
				return err
			}

			for _, objectID := range servers {
				if runtime.Domain.GetDomainFromObjectID(objectID) != "hub" {
					dbClient.CMDB.ShadowObjectCanBeRecevier = true
					system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate(runtime.Domain.GetShadowObjectShadowId(objectID), "rack"))
					dbClient.CMDB.ShadowObjectCanBeRecevier = false
				}
			}
		}

		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "distributed")); err == nil {
		RegisterFunctionTypes(runtime)

		runtime.RegisterOnAfterStartFunction(afterStart, true)
		if err := runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s", err)
	}
}

// --------------------------------------------------------------------------------------
