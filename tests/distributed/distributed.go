// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"time"

	//"github.com/foliagecp/easyjson"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"

	// Comment out and no not use graphDebug for resolving the cgo conflict between go-graphviz and rogchap (when --ldflags '-extldflags "-Wl,--allow-multiple-definition"' does not help)
	//graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	//graphTX "github.com/foliagecp/sdk/embedded/graph/tx"

	statefun "github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

var (
	// NatsURL - nats server url
	NatsURL string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	// NatsURL - nats server url
	PrometricsServerPort string = system.GetEnvMustProceed("PROMETRICS_PORT", "9901")
	/*// MasterFunctionContextIncrement - does the master stateful function do the increment operation on each call in its context
	MasterFunctionContextIncrement bool = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INCREMENT", true)
	// MasterFunctionContextIncrementOption - Default increment value
	MasterFunctionContextIncrementOption int = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INCREMENT_OPTION", 1)
	// MasterFunctionLogs - enable logging of the master function
	MasterFunctionLogs bool = system.GetEnvMustProceed("MASTER_FUNC_LOGS", true)*/
)

func TestFunction(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	hubDomain := contextProcessor.Domain.HubDomainName()
	callerDomain := contextProcessor.Domain.GetDomainFromObjectID(contextProcessor.Caller.ID)
	functionDomain := contextProcessor.Domain.GetDomainFromObjectID(contextProcessor.Self.ID)
	if contextProcessor.Reply == nil { // Signal came
		lg.Logf(
			lg.InfoLevel,
			">>> Signal from caller %s:%s on %s:%s\n",
			contextProcessor.Caller.Typename,
			contextProcessor.Caller.ID,
			contextProcessor.Self.Typename,
			contextProcessor.Self.ID,
		)
		if functionDomain == hubDomain { // Function on HUB
			contextProcessor.Signal(
				sfPlugins.JetstreamGlobalSignal,
				contextProcessor.Self.Typename,
				contextProcessor.Domain.CreateObjectIDWithDomain("leaf", contextProcessor.Self.ID+"A"),
				contextProcessor.Payload,
				contextProcessor.Options,
			)
		} else { // Function on LEAF
			if callerDomain == hubDomain { // from HUB
				contextProcessor.Signal(
					sfPlugins.JetstreamGlobalSignal,
					contextProcessor.Self.Typename,
					contextProcessor.Domain.CreateObjectIDWithDomain("leaf", contextProcessor.Self.ID+"B"),
					contextProcessor.Payload,
					contextProcessor.Options,
				)
			} else { // from LEAF
				contextProcessor.Request(
					sfPlugins.NatsCoreGlobalRequest,
					contextProcessor.Self.Typename,
					contextProcessor.Domain.CreateObjectIDWithDomain(hubDomain, contextProcessor.Self.ID+"C"),
					contextProcessor.Payload,
					contextProcessor.Options,
				)
			}
		}
	} else { // Request came
		lg.Logf(
			lg.InfoLevel,
			">>>>>> Request from caller %s:%s on %s:%s\n",
			contextProcessor.Caller.Typename,
			contextProcessor.Caller.ID,
			contextProcessor.Self.Typename,
			contextProcessor.Self.ID,
		)
		if functionDomain == hubDomain { // Function on HUB
			contextProcessor.Request(
				sfPlugins.NatsCoreGlobalRequest,
				contextProcessor.Self.Typename,
				contextProcessor.Domain.CreateObjectIDWithDomain("leaf", contextProcessor.Self.ID+"D"),
				contextProcessor.Payload,
				contextProcessor.Options,
			)
		}
	}

	/*start := time.Now()

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
	}*/
}

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(runtime, "domains.test", TestFunction, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	graphCRUD.RegisterAllFunctionTypes(runtime)
	//graphTX.RegisterAllFunctionTypes(runtime)
	//graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime, 30)
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":"+PrometricsServerPort)

	afterStart := func(runtime *statefun.Runtime) error {
		if runtime.Domain.Name() == runtime.Domain.HubDomainName() {
			time.Sleep(5 * time.Second) // Wait for everithing to bring up
			CreateTestGraph(runtime)
			//time.Sleep(1 * time.Second)
			//runtime.Signal(sfPlugins.JetstreamGlobalSignal, "domains.test", "foo", easyjson.NewJSONObject().GetPtr(), nil)
		}
		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "distributed")); err == nil {
		RegisterFunctionTypes(runtime)
		if err := runtime.Start(cache.NewCacheConfig("main_cache"), afterStart); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s\n", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s\n", err)
	}
}

// --------------------------------------------------------------------------------------
