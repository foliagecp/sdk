package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/embedded/graph/search"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	NatsURL  = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	dbClient db.DBSyncClient
)

func CollectInventoryInfo(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	le := lg.GetLogger()

	system.MsgOnErrorReturn(ctx.Egress(sfPlugins.NatsCoreEgress, easyjson.NewJSONObject().GetPtr(), "__counter"))

	tag, ok := ctx.Payload.GetByPath("tag").AsString()
	if !ok {
		le.Errorf(context.TODO(), "Cannot get tag from payload")
		return
	}

	linkQuery := sfPlugins.NewLinkQuery("")
	linkQuery.WithCustom(fmt.Sprintf(".*[l:tags('%s')]", tag))

	system.MsgOnErrorReturn(ctx.ObjectSignal(sfPlugins.AutoSignalSelect, ctx.Self.ID, linkQuery, "functions.tests.type_composition.collect_inventory_info", ctx.Self.ID, ctx.Payload, nil))

	objectTypes, err := ctx.GetObjectImplTypes()
	if err != nil {
		le.Errorf(context.TODO(), "Cannot get object types: %s", err)
		return
	}

	pl := ctx.GetObjectContext()

	egressPayload := easyjson.NewJSONObject()
	egressPayload.SetByPath("id", easyjson.NewJSON(ctx.Self.ID))
	egressPayload.SetByPath("caller", easyjson.NewJSON(ctx.Caller.ID))

	if slices.Contains(objectTypes, "hub/inventory") {
		sn := pl.GetByPath("SN").AsStringDefault("empty")
		egressPayload.SetByPath("serial", easyjson.NewJSON(sn))
		system.MsgOnErrorReturn(ctx.Egress(sfPlugins.NatsCoreEgress, &egressPayload))
	} else {
		egressPayload.SetByPath("serial", easyjson.NewJSON("virtual"))
		system.MsgOnErrorReturn(ctx.Egress(sfPlugins.NatsCoreEgress, &egressPayload))
	}
}

func runTypeCompositionTest() {
	CreateTestCmdb()
}

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime)
	fpl.RegisterAllFunctionTypes(runtime)
	search.RegisterAllFunctionTypes(runtime)

	statefun.NewFunctionType(
		runtime,
		"functions.tests.type_composition.collect_inventory_info",
		CollectInventoryInfo,
		*statefun.NewFunctionTypeConfig())
}

func Start() {
	le := lg.GetLogger()

	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	afterStart := func(ctx context.Context, runtime *statefun.Runtime) error {
		dbc, err := db.NewDBSyncClientFromRequestFunction(runtime.Request)
		if err != nil {
			return err
		}
		dbClient = dbc

		runTypeCompositionTest()

		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "type_compositing_test").UseJSDomainAsHubDomainName()); err == nil {
		RegisterFunctionTypes(runtime)
		runtime.RegisterOnAfterStartFunction(afterStart, true)
		if err = runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			le.Errorf(context.TODO(), "Cannot start due to an error: %s", err)
		}
	} else {
		le.Errorf(context.TODO(), "Cannot create runtime due to an error: %s", err)
	}
}
