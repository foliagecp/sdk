// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	/*"time"

	"github.com/foliagecp/easyjson"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"

	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"


	lg "github.com/foliagecp/sdk/statefun/logger"

	// Comment out and no not use graphDebug for resolving the cgo conflict between go-graphviz and rogchap (when --ldflags '-extldflags "-Wl,--allow-multiple-definition"' does not help)
	graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	graphTX "github.com/foliagecp/sdk/embedded/graph/tx"
	statefun "github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"*/

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/sharding"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"time"
)

var (
	// NatsURL - nats server url
	NatsURL string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	/*// MasterFunctionContextIncrement - does the master stateful function do the increment operation on each call in its context
	MasterFunctionContextIncrement bool = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INCREMENT", true)
	// MasterFunctionContextIncrementOption - Default increment value
	MasterFunctionContextIncrementOption int = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INCREMENT_OPTION", 1)
	// MasterFunctionLogs - enable logging of the master function
	MasterFunctionLogs bool = system.GetEnvMustProceed("MASTER_FUNC_LOGS", true)*/
)

/*func MasterFunction(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	start := time.Now()

	var functionContext *easyjson.JSON
	if MasterFunctionContextIncrement {
		functionContext = contextProcessor.GetFunctionContext()
	}

	options := contextProcessor.Options
	increment := int(options.GetByPath("increment").AsNumericDefault(0))

	if MasterFunctionLogs {
		lg.Logf(lg.DebugLevel, "-------> %s:%s\n", contextProcessor.Self.Typename, contextProcessor.Self.ID)
		lg.Logln(lg.DebugLevel, "== Payload:", contextProcessor.Payload.ToString())
		lg.Logln(lg.DebugLevel, "== Context:", functionContext.ToString())
	}

	incrementValue := 0
	if MasterFunctionContextIncrement {
		if v, ok := functionContext.GetByPath("counter").AsNumeric(); ok {
			incrementValue = int(v)
		}
		incrementValue += increment
		functionContext.SetByPath("counter", easyjson.NewJSON(incrementValue))
		lg.Logf(lg.DebugLevel, "++ Function context's counter value incrementated by %d\n", increment)
	}

	if MasterFunctionContextIncrement {
		contextProcessor.SetFunctionContext(functionContext)
	}

	if contextProcessor.Reply != nil { // Request call is being made
		contextProcessor.Reply.With(easyjson.NewJSONObjectWithKeyValue("counter", easyjson.NewJSON(incrementValue)).GetPtr())
	}

	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("master_function", "", []string{"id"}); err == nil {
		gaugeVec.With(prometheus.Labels{"id": contextProcessor.Self.ID}).Set(float64(time.Since(start).Microseconds()))
	}
}

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	ftOptions := easyjson.NewJSONObjectWithKeyValue("increment", easyjson.NewJSON(MasterFunctionContextIncrementOption))
	statefun.NewFunctionType(runtime, "functions.tests.basic.master", MasterFunction, *statefun.NewFunctionTypeConfig().SetOptions(&ftOptions).SetServiceState(true))

	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphTX.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime, 30)
}*/

func Start() {
	/*system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	afterStart := func(runtime *statefun.Runtime) error {
		CreateTestGraph(runtime)
		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "basic")); err == nil {
		RegisterFunctionTypes(runtime)
		if err := runtime.Start(cache.NewCacheConfig("main_cache"), afterStart); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s\n", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s\n", err)
	}*/

	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return
	}

	s, e := sharding.NewShard(nc, js, "hub")
	if e == nil {
		s.Start()
	} else {
		lg.Logln(lg.ErrorLevel, e)
	}

	for true {
		time.Sleep(1 * time.Second)
	}
}

// --------------------------------------------------------------------------------------
