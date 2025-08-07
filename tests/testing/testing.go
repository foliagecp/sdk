// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"context"
	"math"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/foliagecp/sdk/clients/go/db"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
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
	// EnableTLS - TLS flag
	EnableTLS = system.GetEnvMustProceed("ENABLE_TLS", false)
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

	dbClient db.DBSyncClient
)

func Testing(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	pl := ctx.Payload
	if pl == nil {
		lg.Logln(lg.ErrorLevel, "Payload is nil")
		return
	}

	id := pl.GetByPath("id").AsStringDefault("server10")

	count := pl.GetByPath("count").AsNumericDefault(1)

	payload := easyjson.NewJSONObject()
	payload.SetByPath("query", easyjson.NewJSON(".*"))

	var maxTime, minTime, sum, verticesCount float64
	times := make([]int, 0, int(count))

	for i := 0; i < int(count); i++ {
		if i%20 == 0 {
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			lg.Logf(lg.DebugLevel, "Iteration %d: Memory=%dMB, Goroutines=%d",
				i, memStats.Alloc/1024/1024, runtime.NumGoroutine())
		}
		reply, err := ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.query.jpgql.ctra", id, payload.GetPtr(), nil)
		if err != nil {
			lg.Logln(lg.ErrorLevel, err.Error())
			return
		}
		queryTime := reply.GetByPath("data.stats.duration.query_nano").AsNumericDefault(0)
		times = append(times, int(queryTime))

		if i == 0 {
			verticesCount = reply.GetByPath("data.stats.call_tree.vertices_passed").AsNumericDefault(0)
			maxTime = queryTime
			minTime = queryTime
		}

		if maxTime < queryTime {
			maxTime = queryTime
		} else if minTime > queryTime {
			minTime = queryTime
		}

		sum += queryTime

	}

	avgTime := sum / count

	lg.Logf(lg.DebugLevel, "Performance test results:")
	if count >= 100 {
		median, p95, p99, stdDev := calculateAdvancedStats(times, avgTime)

		lg.Logf(lg.DebugLevel, ">>> Repeats: %.0f, Vertices: %.0f", count, verticesCount)
		lg.Logf(lg.DebugLevel, ">>> Avg: %.2f μs, Median: %.2f μs", avgTime/1000, median/1000)
		lg.Logf(lg.DebugLevel, ">>> Min: %.2f μs, Max: %.2f μs", minTime/1000, maxTime/1000)
		lg.Logf(lg.DebugLevel, ">>> P95: %.2f μs, P99: %.2f μs", p95/1000, p99/1000)
		lg.Logf(lg.DebugLevel, ">>> StdDev: %.2f μs", stdDev/1000)
	} else {
		lg.Logf(lg.DebugLevel, ">>> Num of repeats: %.f, Verticies: %.f, Avg time: %.2f μs, Max time: %.2f μs, Min time: %.2f μs",
			count, verticesCount, avgTime/1000, maxTime/1000, minTime/1000)
	}
}

func calculateAdvancedStats(times []int, avgTime float64) (median, p95, p99, stdDev float64) {
	n := len(times)
	if n == 0 {
		return 0, 0, 0, 0
	}

	sort.Slice(times, func(i, j int) bool {
		return times[i] < times[j]
	})

	median = float64(times[n/2])
	if n%2 == 0 {
		median = (float64(times[n/2-1]) + float64(times[n/2])) / 2
	}

	p95Index := int(float64(n) * 0.95)
	if p95Index >= n {
		p95Index = n - 1
	}
	p95 = float64(times[p95Index])

	p99Index := int(float64(n) * 0.99)
	if p99Index >= n {
		p99Index = n - 1
	}
	p99 = float64(times[p99Index])

	var variance float64
	for _, t := range times {
		diff := float64(t) - avgTime
		variance += diff * diff
	}
	variance /= float64(n)
	stdDev = math.Sqrt(variance)

	return median, p95, p99, stdDev
}

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
		lg.Logln(lg.DebugLevel, "== Payload: %s", ctx.Payload.ToString())
		lg.Logln(lg.DebugLevel, "== Context: %s", functionContext.ToString())
	}

	var objectContext *easyjson.JSON
	if MasterFunctionObjectContextProcess {
		objectContext = ctx.GetObjectContext()
		if MasterFunctionLogs {
			lg.Logln(lg.DebugLevel, "== Object context: %s", objectContext.ToString())
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

	statefun.NewFunctionType(runtime, "functions.testing.latency", Testing, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))

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

		CreateTestGraph(runtime)

		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "testing").UseJSDomainAsHubDomainName().SetTLS(EnableTLS)); err == nil {
		RegisterFunctionTypes(runtime)
		runtime.RegisterOnAfterStartFunction(afterStart, true)
		if err := runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			lg.Logf(lg.ErrorLevel, "Cannot start due to an error: %s", err)
		}
	} else {
		lg.Logf(lg.ErrorLevel, "Cannot create statefun runtime due to an error: %s", err)
	}
}
