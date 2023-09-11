// Copyright 2023 NJWS Inc.

// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package basic

import (
	"fmt"
	"json_easy"
	"os"

	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	statefun "github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfPluginJS "github.com/foliagecp/sdk/statefun/plugins/js"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	// NatsURL - nats server url
	NatsURL string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	// MasterFunctionContextIncrement - does the master stateful function do the increment operation on each call in its context
	MasterFunctionContextIncrement bool = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INREMENT", true)
	// MasterFunctionContextIncrementOption - Default increment value
	MasterFunctionContextIncrementOption int = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INREMENT_OPTION", 1)
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
	// KVMuticesTestDurationSec - key/value mutices test duration
	KVMuticesTestDurationSec int = system.GetEnvMustProceed("KV_MUTICES_TEST_DURATION_SEC", 10)
	// KVMuticesTestWorkers - key/value mutices workers to apply in the test
	KVMuticesTestWorkers int = system.GetEnvMustProceed("KV_MUTICES_TEST_WORKERS", 4)
)

func MasterFunction(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	var functionContext *json_easy.JSON
	if MasterFunctionContextIncrement {
		functionContext = contextProcessor.GetFunctionContext()
	}

	options := contextProcessor.Options
	increment := int(options.GetByPath("increment").AsNumericDefault(0))

	if MasterFunctionLogs {
		fmt.Printf("-------> %s:%s\n", contextProcessor.Self.Typename, contextProcessor.Self.ID)
		fmt.Println("== Payload:", contextProcessor.Payload.ToString())
		fmt.Println("== Context:", functionContext.ToString())
	}

	var objectContext *json_easy.JSON
	if MasterFunctionObjectContextProcess {
		objectContext = contextProcessor.GetObjectContext()
		if MasterFunctionLogs {
			fmt.Println("== Object context:", objectContext.ToString())
		}
	}

	if MasterFunctionJSPlugin {
		if executor != nil {
			if err := executor.BuildError(); err != nil {
				fmt.Println(err)
			} else {
				if err := executor.Run(contextProcessor); err != nil {
					fmt.Println(err)

				}
			}
		}
		functionContext = contextProcessor.GetFunctionContext()
	}

	if MasterFunctionContextIncrement {
		if v, ok := functionContext.GetByPath("counter").AsNumeric(); ok {
			functionContext.SetByPath("counter", json_easy.NewJSON(int(v)+increment))
			fmt.Printf("++ Function context's counter value incrementated by %d\n", increment)
		} else {
			functionContext.SetByPath("counter", json_easy.NewJSON(0))
			fmt.Printf("++ Function context's counter value initialized with 0\n")
		}
	}

	if MasterFunctionObjectContextProcess {
		contextProcessor.SetObjectContext(objectContext)
	}

	if MasterFunctionContextIncrement {
		contextProcessor.SetFunctionContext(functionContext)
	}
}

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	// Create new typename function "functions.tests.basic.master" each stateful instance of which uses go function "MasterFunction"
	ftOptions := json_easy.NewJSONObjectWithKeyValue("increment", json_easy.NewJSON(MasterFunctionContextIncrementOption))
	ft := statefun.NewFunctionType(runtime, "functions.tests.basic.master", MasterFunction, *statefun.NewFunctionTypeConfig().SetOptions(&ftOptions))
	// Add TypenameExecutorPlugin which will provide StatefunExecutor for each stateful instance for this typename function (skip this if TypenameExecutorPlugin is not needed)

	if MasterFunctionJSPlugin {
		jsFileName := "master_function_plugin.js"
		if content, err := os.ReadFile(jsFileName); err == nil {
			// Assign JavaScript StatefunExecutor for TypenameExecutorPlugin
			system.MsgOnErrorReturn(ft.SetExecutor(jsFileName, string(content), sfPluginJS.StatefunExecutorPluginJSContructor))
		} else {
			fmt.Printf("ERROR: Could not load JS script: %v\n", err)
		}
	}

	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime, 30)
}

func Start() {
	afterStart := func(runtime *statefun.Runtime) {
		if CreateSimpleGraphTest {
			CreateTestGraph(runtime)
		}
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "basic")); err == nil {
		if KVMuticesTest {
			KVMuticesSimpleTest(runtime, KVMuticesTestDurationSec, KVMuticesTestWorkers, 1000, 200)
		}

		RegisterFunctionTypes(runtime)
		if err := runtime.Start(cache.NewCacheConfig(), afterStart); err != nil {
			fmt.Printf("Cannot start due to an error: %s\n", err)
		}
	} else {
		fmt.Printf("Cannot create statefun runtime due to an error: %s\n", err)
	}
}

// --------------------------------------------------------------------------------------
