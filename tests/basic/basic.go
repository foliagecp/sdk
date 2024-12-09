// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/foliagecp/sdk/clients/go/db"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
	"github.com/foliagecp/sdk/embedded/graph/graphql"
	"github.com/foliagecp/sdk/embedded/graph/search"
	lg "github.com/foliagecp/sdk/statefun/logger"

	graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfPluginJS "github.com/foliagecp/sdk/statefun/plugins/js"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	// NatsURL - nats server url
	NatsURL string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	// MasterFunctionContextIncrement - does the master stateful function do the increment operation on each call in its context
	MasterFunctionContextIncrement bool = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INCREMENT", true)
	// MasterFunctionContextIncrementOption - Default increment value
	MasterFunctionContextIncrementOption int = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INCREMENT_OPTION", 1)
	// MasterFunctionObjectContextProcess - make master function read and write its object context in idle mode
	MasterFunctionObjectContextProcess bool = system.GetEnvMustProceed("MASTER_FUNC_OBJECT_CONTEXT_PROCESS", false)
	// MasterFunctionJSPlugin - enable js plugin for the master function
	MasterFunctionJSPlugin bool = system.GetEnvMustProceed("MASTER_FUNC_JS_PLUGIN", false)
	// MasterFunctionLogs - enable logging of the master function
	MasterFunctionLogs bool = system.GetEnvMustProceed("MASTER_FUNC_LOGS", true)
	// CreateSimpleGraphTest - create a simple graph on runtime start
	CreateSimpleGraphTest bool = system.GetEnvMustProceed("CREATE_SIMPLE_GRAPH_TEST", true)
	// KVMuticesTest - test the Foliage global key/value mutices
	KVMuticesTest bool = system.GetEnvMustProceed("KV_MUTICES_TEST", true)
	// RequestReplyTest - test the Foliage sync calls
	RequestReplyTest bool = system.GetEnvMustProceed("REQUEST_REPLY_TEST", true)
	// TriggersTest - test the Foliage cmdb crud triggers
	TriggersTest bool = system.GetEnvMustProceed("TRIGGERS_TEST", true)
	// KVMuticesTestDurationSec - key/value mutices test duration

	dbClient db.DBSyncClient
)

func MasterFunction(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	start := time.Now()

	var functionContext *easyjson.JSON
	if MasterFunctionContextIncrement {
		functionContext = ctx.GetFunctionContext()
	}

	options := ctx.Options
	increment := int(options.GetByPath("increment").AsNumericDefault(0))

	if MasterFunctionLogs {
		lg.Logf(lg.DebugLevel, "-------> %s:%s", ctx.Self.Typename, ctx.Self.ID)
		lg.Logln(lg.DebugLevel, "== Payload:", ctx.Payload.ToString())
		lg.Logln(lg.DebugLevel, "== Context:", functionContext.ToString())
	}

	var objectContext *easyjson.JSON
	if MasterFunctionObjectContextProcess {
		objectContext = ctx.GetObjectContext()
		if MasterFunctionLogs {
			lg.Logln(lg.DebugLevel, "== Object context:", objectContext.ToString())
		}
	}

	if MasterFunctionJSPlugin {
		if executor != nil {
			if err := executor.BuildError(); err != nil {
				lg.Logln(lg.ErrorLevel, err.Error())
			} else {
				if err := executor.Run(ctx); err != nil {
					lg.Logln(lg.ErrorLevel, err.Error())
				}
			}
		}
		functionContext = ctx.GetFunctionContext()
	}

	incrementValue := 0
	if MasterFunctionContextIncrement {
		if v, ok := functionContext.GetByPath("counter").AsNumeric(); ok {
			incrementValue = int(v)
		}
		incrementValue += increment
		functionContext.SetByPath("counter", easyjson.NewJSON(incrementValue))
		lg.Logf(lg.DebugLevel, "++ Function context's counter value incrementated by %d", increment)
	}

	if MasterFunctionObjectContextProcess {
		ctx.SetObjectContext(objectContext)
	}

	if MasterFunctionContextIncrement {
		ctx.SetFunctionContext(functionContext)
	}

	if ctx.Reply != nil { // Request call is being made
		ctx.Reply.With(easyjson.NewJSONObjectWithKeyValue("counter", easyjson.NewJSON(incrementValue)).GetPtr())
	}

	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("master_function", "", []string{"id"}); err == nil {
		gaugeVec.With(prometheus.Labels{"id": ctx.Self.ID}).Set(float64(time.Since(start).Microseconds()))
	}
}

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	// Create new typename function "functions.tests.basic.master" each stateful instance of which uses go function "MasterFunction"
	ftOptions := easyjson.NewJSONObjectWithKeyValue("increment", easyjson.NewJSON(MasterFunctionContextIncrementOption))
	ft := statefun.NewFunctionType(runtime, "functions.tests.basic.master", MasterFunction, *statefun.NewFunctionTypeConfig().SetOptions(&ftOptions).SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	// Add TypenameExecutorPlugin which will provide StatefunExecutor for each stateful instance for this typename function (skip this if TypenameExecutorPlugin is not needed)

	if MasterFunctionJSPlugin {
		jsFileName := "./js/master_function_plugin.js"
		if content, err := os.ReadFile(jsFileName); err == nil {
			// Assign JavaScript StatefunExecutor for TypenameExecutorPlugin
			system.MsgOnErrorReturn(ft.SetExecutor(jsFileName, string(content), sfPluginJS.StatefunExecutorPluginJSContructor))
		} else {
			lg.Logf(lg.ErrorLevel, "Could not load JS script: %v", err)
		}
	}

	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime)
	fpl.RegisterAllFunctionTypes(runtime)
	search.RegisterAllFunctionTypes(runtime)
}

func RunRequestReplyTest(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: request reply calls")

	funcTypename := "functions.tests.basic.master"
	replyJson, err := runtime.Request(sfPlugins.GolangLocalRequest, funcTypename, "synctest", easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		system.MsgOnErrorReturn(err)
	} else {
		if _, ok := replyJson.GetByPath("counter").AsNumeric(); ok {
			lg.Logf(lg.DebugLevel, "GolangLocalRequest test passed! Got reply from %s: %s", funcTypename, replyJson.ToString())
		}
	}

	replyJson, err = runtime.Request(sfPlugins.NatsCoreGlobalRequest, funcTypename, "synctest", easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		system.MsgOnErrorReturn(err)
	} else {
		if _, ok := replyJson.GetByPath("counter").AsNumeric(); ok {
			lg.Logf(lg.DebugLevel, "NatsCoreGlobalRequest test passed! Got reply from %s: %s", funcTypename, replyJson.ToString())
		}
	}

	lg.Logln(lg.DebugLevel, "<<< Test ended: request reply calls")
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	afterStart := func(ctx context.Context, runtime *statefun.Runtime) error {

		dbc, err := db.NewDBSyncClientFromRequestFunction(runtime.Request)
		if err != nil {
			return err
		}
		dbClient = dbc

		if TriggersTest {
			RunTriggersTest(runtime)
		}
		if RequestReplyTest {
			RunRequestReplyTest(runtime)
		}
		if CreateSimpleGraphTest {
			CreateTestGraph(runtime)
		}

		body := easyjson.NewJSONObjectWithKeyValue("search_fields", easyjson.JSONFromArray([]string{"f1.f11", "f2"}))
		system.MsgOnErrorReturn(dbClient.CMDB.TypeUpdate("typea", body, false))

		body = easyjson.NewJSONObjectWithKeyValue("search_fields", easyjson.JSONFromArray([]string{"f1", "f2"}))
		system.MsgOnErrorReturn(dbClient.CMDB.TypeUpdate("typeb", body, false))

		b := easyjson.NewJSONObjectWithKeyValue("f2", easyjson.NewJSON(true))
		b.SetByPath("f1.f11", easyjson.NewJSON(123.13))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("test1", "typea", b))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("test2", "typea", easyjson.NewJSONObjectWithKeyValue("f2", easyjson.NewJSON("bar"))))
		b = easyjson.NewJSONObjectWithKeyValue("f1", easyjson.NewJSON("data1"))
		b.SetByPath("f2", easyjson.NewJSON(119))
		system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("test3", "typeb", b))

		fmt.Println("Starting GraphQL")
		graphql.StartGraphqlServer("8080", &dbClient)
		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "basic").UseJSDomainAsHubDomainName()); err == nil {
		RegisterFunctionTypes(runtime)
		if TriggersTest {
			registerTriggerFunctions(runtime)
		}
		runtime.RegisterOnAfterStartFunction(afterStart, true)
		if err := runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s", err)
	}
}
