// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"context"
	"fmt"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/graphql"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"

	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
	"github.com/foliagecp/sdk/embedded/graph/search"
	lg "github.com/foliagecp/sdk/statefun/logger"

	graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	// NatsURL - nats server url
	NatsURL string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")

	dbClient db.DBSyncClient
)

func testObjectCall(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	le := lg.GetLogger()
	le.Info(context.TODO(), "====================> TestObjectRequest")
	query := statefun.NewLinkQuery("datacenter-rack")
	system.MsgOnErrorReturn(statefun.ObjectCall(ctx, query, "function.tests.object.reader", nil, nil))
}

func testReader(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	le := lg.GetLogger()
	objectCtx := ctx.GetObjectContext()
	if objectCtx != nil {
		body, ok := objectCtx.AsObject()
		if !ok {
			return
		}
		le.Infof(context.TODO(), "====================> Object context: %+v", body)
	} else {
		le.Infof(context.TODO(), "====================> Object context is nil")
	}
}

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime)
	fpl.RegisterAllFunctionTypes(runtime)
	search.RegisterAllFunctionTypes(runtime)

	statefun.NewFunctionType(
		runtime,
		"functions.tests.object.call",
		testObjectCall,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect),
	)

	//statefun.NewFunctionType(
	//	runtime,
	//	"functions.tests.object.request",
	//	testObjectRequest,
	//	*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect),
	//)

	statefun.NewFunctionType(
		runtime,
		"function.tests.object.reader",
		testReader,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect),
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

		CreateTestCMDB()

		fmt.Println("Starting GraphQL")
		graphql.StartGraphqlServer("8080", &dbClient)

		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "object").UseJSDomainAsHubDomainName()); err == nil {
		RegisterFunctionTypes(runtime)
		runtime.RegisterOnAfterStartFunction(afterStart, true)
		if err := runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s", err)
	}
}
