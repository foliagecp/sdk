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
	NatsURL = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")

	dbClient db.DBSyncClient
)

func testObjectRequest(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	le := lg.GetLogger()
	le.Info(context.TODO(), "====================> TestObjectRequest")
	query := sfPlugins.NewLinkQuery("datacenter-rack")
	res, err := ctx.ObjectRequest(sfPlugins.AutoRequestSelect, query, "functions.cmdb.api.object.read", ctx.Self.ID, nil, nil)
	if err != nil {
		le.Errorf(context.TODO(), "====================> failed to call: %v", err)
		return
	}

	for k, v := range res {
		if v.ReqError == nil && v.ReqReply != nil {
			le.Infof(context.TODO(), "====================> TestObjectRequest on %v response: %+v", k, v.ReqReply)
		}
	}
}

func testObjectSignal(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	le := lg.GetLogger()
	le.Info(context.TODO(), "====================> TestObjectSignal")
	query := sfPlugins.NewLinkQuery("datacenter-rack")
	res, err := ctx.ObjectSignal(sfPlugins.AutoSignalSelect, query, "functions.cmdb.api.object.read", ctx.Self.ID, nil, nil)
	if err != nil {
		le.Errorf(context.TODO(), "====================> failed to call: %v", err)
		return
	}

	for k, v := range res {
		le.Infof(context.TODO(), "====================> TestObjectSignal on %v error: %+v", k, v)
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
		"functions.tests.objectsignal",
		testObjectSignal,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect),
	)

	statefun.NewFunctionType(
		runtime,
		"functions.tests.objectrequest",
		testObjectRequest,
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
