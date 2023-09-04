

package main

import (
	"fmt"
	"json_easy"
	"os"

	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	statefun "github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfPluginJS "github.com/foliagecp/sdk/statefun/plugins/js"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	natsURL                              string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")
	masterFunctionContextIncrement       bool   = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INREMENT", true)
	masterFunctionContextIncrementOption int    = system.GetEnvMustProceed("MASTER_FUNC_CONTEXT_INREMENT_OPTION", 1)
	masterFunctionObjectContextProcess   bool   = system.GetEnvMustProceed("MASTER_FUNC_OBJECT_CONTEXT_PROCESS", false)
	masterFunctionJSPlugin               bool   = system.GetEnvMustProceed("MASTER_FUNC_JS_PLUGIN", false)
	masterFunctionLogs                   bool   = system.GetEnvMustProceed("MASTER_FUNC_LOGS", true)
	createSimpleGraphTest                bool   = system.GetEnvMustProceed("CREATE_SIMPLE_GRAPH_TEST", true)
	kvMuticesTest                        bool   = system.GetEnvMustProceed("KV_MUTICES_TEST", true)
	kvMuticesTestDurationSec             int    = system.GetEnvMustProceed("KV_MUTICES_TEST_DURATION_SEC", 10)
	kvMuticesTestWorkers                 int    = system.GetEnvMustProceed("KV_MUTICES_TEST_WORKERS", 4)
)

func masterFunction(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	var functionContext *json_easy.JSON
	if masterFunctionContextIncrement {
		functionContext = contextProcessor.GetFunctionContext()
	}

	options := contextProcessor.Options
	increment := int(options.GetByPath("increment").AsNumericDefault(0))

	if masterFunctionLogs {
		fmt.Printf("-------> %s:%s\n", contextProcessor.Self.Typename, contextProcessor.Self.ID)
		fmt.Println("== Payload:", contextProcessor.Payload.ToString())
		fmt.Println("== Context:", functionContext.ToString())
	}

	var objectContext *json_easy.JSON
	if masterFunctionObjectContextProcess {
		objectContext = contextProcessor.GetObjectContext()
		if masterFunctionLogs {
			fmt.Println("== Object context:", objectContext.ToString())
		}
	}

	if masterFunctionJSPlugin {
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

	if masterFunctionContextIncrement {
		if v, ok := functionContext.GetByPath("counter").AsNumeric(); ok {
			functionContext.SetByPath("counter", json_easy.NewJSON(int(v)+increment))
			fmt.Printf("++ Function context's counter value incrementated by %d\n", increment)
		} else {
			functionContext.SetByPath("counter", json_easy.NewJSON(0))
			fmt.Printf("++ Function context's counter value initialized with 0\n")
		}
	}

	if masterFunctionObjectContextProcess {
		contextProcessor.SetObjectContext(objectContext)
	}

	if masterFunctionContextIncrement {
		contextProcessor.SetFunctionContext(functionContext)
	}
}

func registerFunctionTypes(runtime *statefun.Runtime) {
	// Create new typename function "functions.app1.json.master" each stateful instance of which uses go function "masterFunction"
	ftOptions := json_easy.NewJSONObjectWithKeyValue("increment", json_easy.NewJSON(masterFunctionContextIncrementOption))
	ft := statefun.NewFunctionType(runtime, "functions.app1.json.master", masterFunction, statefun.NewFunctionTypeConfig().SetOptions(&ftOptions))
	// Add TypenameExecutorPlugin which will provide StatefunExecutor for each stateful instance for this typename function (skip this if TypenameExecutorPlugin is not needed)

	if masterFunctionJSPlugin {
		jsFileName := "statefun_context_increment.js"
		if content, err := os.ReadFile("./" + jsFileName); err == nil {
			// Assign JavaScript StatefunExecutor for TypenameExecutorPlugin
			ft.SetExecutor(jsFileName, string(content), sfPluginJS.StatefunExecutorPluginJSContructor)
		} else {
			fmt.Printf("ERROR: Could not load JS script: %v\n", err)
		}
	}

	graphCRUD.RegisterAllFunctionTypes(runtime)
	RegisterAllGraphDebugFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime, 30)
}

func main() {
	afterStart := func(runtime *statefun.Runtime) {
		if createSimpleGraphTest {
			CreateTestGraph(runtime)
		}
	}

	if runtime, err := statefun.NewRuntime(statefun.NewRuntimeConfigSimple(natsURL, "basic")); err == nil {
		if kvMuticesTest {
			kvMuticesSimpleTest(runtime, kvMuticesTestDurationSec, kvMuticesTestWorkers, 1000, 200)
		}

		registerFunctionTypes(runtime)
		if err := runtime.Start(cache.NewCacheConfig(), afterStart); err != nil {
			fmt.Printf("Cannot start due to an error: %s\n", err)
		}
	} else {
		fmt.Printf("Cannot create statefun runtime due to an error: %s\n", err)
	}
}

// --------------------------------------------------------------------------------------
