// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"context"
	"fmt"

	"github.com/foliagecp/easyjson"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
	"github.com/foliagecp/sdk/embedded/graph/search"
	"github.com/foliagecp/sdk/embedded/workflow"
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

	workflowEngine    = workflow.NewWorkflowEngine(TestWorkflow, "functions.workflow.engine")
	workflowActivity1 = workflow.NewWorkflowActivity(Activity1, "functions.workflow.activity1")
	workflowActivity2 = workflow.NewWorkflowActivity(Activity2, "functions.workflow.activity2")
)

func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime)
	fpl.RegisterAllFunctionTypes(runtime)
	search.RegisterAllFunctionTypes(runtime)
}

func TestWorkflow(tools workflow.WorkflowTools) {
	fmt.Println("TestWorkflow: 0")

	greet := ""

	data1 := easyjson.NewJSONObjectWithKeyValue("val", easyjson.NewJSON("olleh"))
	result1 := tools.ExecActivity(workflowActivity1, data1)

	greet += result1.GetByPathPtr("val").AsStringDefault("ERROR1")
	greet += " "

	fmt.Println("TestWorkflow: 1")

	data2 := easyjson.NewJSONObjectWithKeyValue("val", easyjson.NewJSON("wolfkrow"))
	result2 := tools.ExecActivity(workflowActivity2, data2)

	greet += result2.GetByPathPtr("val").AsStringDefault("ERROR2")
	greet += "!"

	fmt.Println("TestWorkflow: 2")

	fmt.Println(greet)
}

func Activity1(tools workflow.ActivityTools) {
	fmt.Println("  Activity1: 0")

	val := tools.SFctx.Payload.GetByPath("val").AsStringDefault("0000")
	replyData := easyjson.NewJSONObjectWithKeyValue("val", easyjson.NewJSON(reverseString(val)))
	tools.ReplyWith(replyData)

	fmt.Println("  Activity1: 1")
}

func Activity2(tools workflow.ActivityTools) {
	fmt.Println("  Activity2: 0")

	val := tools.SFctx.Payload.GetByPath("val").AsStringDefault("0000")
	replyData := easyjson.NewJSONObjectWithKeyValue("val", easyjson.NewJSON(reverseString(val)))
	tools.ReplyWith(replyData)

	fmt.Println("  Activity2: 1")
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "clean").UseJSDomainAsHubDomainName()); err == nil {
		workflowEngine.RegisterStatefun(runtime)
		workflowActivity1.RegisterStatefun(runtime)
		workflowActivity2.RegisterStatefun(runtime)

		RegisterFunctionTypes(runtime)
		runtime.RegisterOnAfterStartFunction(nil, true)
		if err := runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s", err)
	}
}
