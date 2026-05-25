package main

import (
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

// dynamicEcho is a trivial stateful function: logs the call, replies with
// echo of the payload plus a server-side timestamp.
func dynamicEcho(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	lg.Logf(lg.InfoLevel, "[DYNAMIC ECHO] typename=%s id=%s payload=%s",
		ctx.Self.Typename, ctx.Self.ID, ctx.Payload.ToString())

	reply := easyjson.NewJSONObject()
	reply.SetByPath("echo", *ctx.Payload)
	reply.SetByPath("handled_by", easyjson.NewJSON(ctx.Self.Typename))
	reply.SetByPath("handled_for_id", easyjson.NewJSON(ctx.Self.ID))
	reply.SetByPath("ts_ns", easyjson.NewJSON(system.GetCurrentTimeNs()))

	if ctx.Reply != nil {
		ctx.Reply.With(&reply)
	}
}

// RunDynamicFunctionTest waits 30 seconds after runtime start, then registers
// a new stateful function type at runtime and invokes it to verify the
// dynamic registration path works end-to-end.
func RunDynamicFunctionTest(runtime *statefun.Runtime) {
	const delay = 30 * time.Second
	lg.Logf(lg.InfoLevel, ">>> Dynamic function registration test scheduled in %s", delay)
	time.Sleep(delay)

	typename := "functions.tests.basic.dynamic_echo"
	lg.Logf(lg.InfoLevel, ">>> Registering dynamic function type: %s", typename)

	cfg := statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetAllowedSignalProviders(sfPlugins.JetstreamGlobalSignal)

	if _, err := statefun.RegisterDynamicFunctionType(runtime, typename, dynamicEcho, *cfg); err != nil {
		lg.Logf(lg.ErrorLevel, "RegisterDynamicFunctionType failed: %s", err)
		return
	}
	lg.Logf(lg.InfoLevel, ">>> Dynamic function registered: %s", typename)

	// Give NATS subscription a moment to be fully live before first call.
	time.Sleep(500 * time.Millisecond)

	payload := easyjson.NewJSONObjectWithKeyValue("hello", easyjson.NewJSON("from-dynamic-test"))
	payload.SetByPath("n", easyjson.NewJSON(42))

	start := time.Now()
	reply, err := runtime.Request(sfPlugins.GolangLocalRequest, typename, "probe-1", &payload, nil)
	elapsed := time.Since(start)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "[DYNAMIC TEST] GolangLocal request FAILED in %s: %s", elapsed, err)
	} else if reply == nil {
		lg.Logf(lg.ErrorLevel, "[DYNAMIC TEST] GolangLocal request returned nil reply")
	} else {
		lg.Logf(lg.InfoLevel, "[DYNAMIC TEST] GolangLocal OK in %s reply=%s", elapsed, reply.ToString())
	}

	start = time.Now()
	reply, err = runtime.Request(sfPlugins.NatsCoreGlobalRequest, typename, "probe-2", &payload, nil)
	elapsed = time.Since(start)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "[DYNAMIC TEST] NatsCoreGlobal request FAILED in %s: %s", elapsed, err)
	} else if reply == nil {
		lg.Logf(lg.ErrorLevel, "[DYNAMIC TEST] NatsCoreGlobal request returned nil reply")
	} else {
		lg.Logf(lg.InfoLevel, "[DYNAMIC TEST] NatsCoreGlobal OK in %s reply=%s", elapsed, reply.ToString())
	}

	// Fire-and-forget signal via JetStream (async, no reply).
	sigPayload := easyjson.NewJSONObjectWithKeyValue("event", easyjson.NewJSON("dynamic-signal"))
	sigPayload.SetByPath("n", easyjson.NewJSON(100))
	start = time.Now()
	if err := runtime.Signal(sfPlugins.JetstreamGlobalSignal, typename, "probe-signal", &sigPayload, nil); err != nil {
		lg.Logf(lg.ErrorLevel, "[DYNAMIC TEST] JetstreamGlobalSignal FAILED in %s: %s", time.Since(start), err)
	} else {
		lg.Logf(lg.InfoLevel, "[DYNAMIC TEST] JetstreamGlobalSignal dispatched in %s (watch for [DYNAMIC ECHO] probe-signal log below)", time.Since(start))
	}
	// Give the signal time to be delivered and processed asynchronously.
	time.Sleep(500 * time.Millisecond)

	// Double-registration should fail gracefully.
	if _, err := statefun.RegisterDynamicFunctionType(runtime, typename, dynamicEcho, *cfg); err == nil {
		lg.Logf(lg.ErrorLevel, "[DYNAMIC TEST] expected duplicate registration to fail, got nil error")
	} else {
		lg.Logf(lg.InfoLevel, "[DYNAMIC TEST] duplicate registration correctly rejected: %s", err)
	}

	lg.Logln(lg.InfoLevel, "<<< Dynamic function registration test finished")
}
