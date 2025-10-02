// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"context"
	"fmt"
	"time"

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
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	// NatsURL - nats server url
	NatsURL string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")

	workflowEngine    = workflow.NewWorkflowEngine(TestWorkflow, "functions.workflow.engine")
	workflowActivity1 = workflow.NewWorkflowActivity(Activity1, "functions.workflow.activity1")
	workflowActivity2 = workflow.NewWorkflowActivity(Activity2, "functions.workflow.activity2")

	activity2Counter int = 0
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
	result1 := tools.ExecActivity(workflowActivity1, data1, &workflow.ActivityOptions{Timeout: 10 * time.Second})

	tools.SetStageProgressInfo("half way there")

	greet += result1.GetByPathPtr("val").AsStringDefault("ERROR1")
	greet += " "

	fmt.Println("TestWorkflow: 1")

	data2 := easyjson.NewJSONObjectWithKeyValue("val", easyjson.NewJSON("wolfkrow"))
	result2 := tools.ExecActivity(workflowActivity2, data2, &workflow.ActivityOptions{Timeout: 10 * time.Second, Retries: 3})

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

	activity2Counter++
	if activity2Counter >= 2 {
		time.Sleep(5 * time.Second)
		activity2Counter = 0
		tools.ReplyWith(replyData)
	}

	fmt.Println("  Activity2: 1")
}

func TimerSink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	fc := ctx.GetFunctionContext()
	count := int(fc.GetByPath("hits").AsNumericDefault(0))
	count++
	fc.SetByPath("hits", easyjson.NewJSON(count))
	ctx.SetFunctionContextImmediately(fc)

	lg.Logf(lg.InfoLevel, "[SINK] %s:%s hits=%d payload=%s", ctx.Self.Typename, ctx.Self.ID, count, ctx.Payload.ToString())
}

func startTimerTest(runtime *statefun.Runtime) {
	log := func(msg string, a ...interface{}) {
		lg.Logf(lg.InfoLevel, "[BOOT] "+msg, a...)
	}

	// helper: send a schedule_once
	scheduleOnce := func(taskID, targetID string, dueInMs int64, dueAtMs int64, payload map[string]interface{}) {
		pl := easyjson.NewJSONObject()
		pl.SetByPath("cmd", easyjson.NewJSON("schedule_once"))
		pl.SetByPath("task.id", easyjson.NewJSON(taskID))
		pl.SetByPath("task.target_typename", easyjson.NewJSON("functions.tests.timing.sink"))
		pl.SetByPath("task.target_id", easyjson.NewJSON(targetID))
		if payload != nil {
			pl.SetByPath("task.payload", easyjson.NewJSONObjectFromMap(payload))
		}
		if dueInMs > 0 {
			pl.SetByPath("task.due_in_ms", easyjson.NewJSON(dueInMs))
		}
		if dueAtMs > 0 {
			pl.SetByPath("task.due_at_unix_ms", easyjson.NewJSON(dueAtMs))
		}
		_ = runtime.Signal(sfPlugins.JetstreamGlobalSignal, workflow.DelayedSignalGeneratorTypename, "wheel1", &pl, nil)
	}

	// helper: send a schedule_every
	scheduleEvery := func(taskID, targetID string, periodMs int64, firstInMs int64, payload map[string]interface{}) {
		pl := easyjson.NewJSONObject()
		pl.SetByPath("cmd", easyjson.NewJSON("schedule_every"))
		pl.SetByPath("task.id", easyjson.NewJSON(taskID))
		pl.SetByPath("task.target_typename", easyjson.NewJSON("functions.tests.timing.sink"))
		pl.SetByPath("task.target_id", easyjson.NewJSON(targetID))
		pl.SetByPath("task.period_ms", easyjson.NewJSON(periodMs))
		if payload != nil {
			pl.SetByPath("task.payload", easyjson.NewJSONObjectFromMap(payload))
		}
		if firstInMs > 0 {
			pl.SetByPath("task.first_in_ms", easyjson.NewJSON(firstInMs))
		}
		_ = runtime.Signal(sfPlugins.JetstreamGlobalSignal, workflow.DelayedSignalGeneratorTypename, "wheel1", &pl, nil)
	}

	// Optional: set per-id config (1s tick, 3600 slots)
	// apply on first message
	{
		cfg := easyjson.NewJSONObject()
		cfg.SetByPath("cmd", easyjson.NewJSON("_tick")) // any cmd path flows through ensureWheelInitialized
		cfg.SetByPath("config.tick_ms", easyjson.NewJSON(1000))
		cfg.SetByPath("config.wheel_size", easyjson.NewJSON(3600))
		_ = runtime.Signal(sfPlugins.JetstreamGlobalSignal, workflow.DelayedSignalGeneratorTypename, "wheel1", &cfg, nil)
	}

	// 1) ONE-SHOT after due_in_ms (5s)
	log("schedule once due_in_ms=5s")
	scheduleOnce("once:5s", "sink-oneshot1", 5000, 0, map[string]interface{}{"case": "once_due_in_5s"})

	// 2) ONE-SHOT after due_in_ms (2min)
	log("schedule once due_in_ms=2m")
	scheduleOnce("once:2m", "sink-oneshot2", 120000, 0, map[string]interface{}{"case": "once_due_in_2m"})

	// 3) ONE-SHOT at absolute time (now + 30s)
	log("schedule once absolute now+30s")
	scheduleOnce("once:absolute30s", "sink-absolute", 0, time.Now().Add(30*time.Second).UnixMilli(), map[string]interface{}{"case": "once_absolute_30s"})

	// 4) PERIODIC: every 15s, first after 5s
	log("schedule every 15s, first in 5s")
	scheduleEvery("every:15s", "sink-p15", 15000, 5000, map[string]interface{}{"case": "periodic_15s"})

	// 5) PERIODIC: every 60s, default first (60s)
	log("schedule every 60s (default first)")
	scheduleEvery("every:60s", "sink-p60", 60000, 0, map[string]interface{}{"case": "periodic_60s"})

	time.Sleep(40 * time.Second)

	log("cancel of task every:15s in 40s")
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("cmd", easyjson.NewJSON("cancel"))
		payload.SetByPath("task.id", easyjson.NewJSON("every:15s"))

		_ = runtime.Signal(sfPlugins.JetstreamGlobalSignal, workflow.DelayedSignalGeneratorTypename, "wheel1", &payload, nil)
	}

	time.Sleep(30 * time.Second)

	log("cancel_all in 70s")
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("cmd", easyjson.NewJSON("cancel_all"))

		_ = runtime.Signal(sfPlugins.JetstreamGlobalSignal, workflow.DelayedSignalGeneratorTypename, "wheel1", &payload, nil)
	}
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	afterStart := func(ctx context.Context, runtime *statefun.Runtime) error {
		startTimerTest(runtime)

		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "clean").UseJSDomainAsHubDomainName()); err == nil {
		workflow.RegisterDelayedSignalGenerator(runtime)
		workflowEngine.RegisterStatefun(runtime)
		workflowActivity1.RegisterStatefun(runtime)
		workflowActivity2.RegisterStatefun(runtime)

		RegisterFunctionTypes(runtime)

		statefun.NewFunctionType(runtime, "functions.tests.timing.sink", TimerSink, *statefun.NewFunctionTypeConfig())

		runtime.RegisterOnAfterStartFunction(afterStart, true)
		if err := runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s", err)
	}
}
