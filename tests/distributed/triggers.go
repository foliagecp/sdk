package main

import (
	"runtime"
	"time"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	triggersTestStatefun1 = "functions.tests.basic.trigger1"
	triggersTestStatefun2 = "functions.tests.basic.trigger2"
)

func IsTransactionOperationOk(j *easyjson.JSON, err error) bool {
	le := lg.GetCustomLogEntry(runtime.Caller(1))
	if err != nil {
		le.Logf(lg.ErrorLevel, "Transaction operation failed: %s\n", err)
		return false
	}
	if s, ok := j.GetByPath("payload.status").AsString(); ok {
		if s != "ok" {
			le.Logf(lg.WarnLevel, "Transaction status is not ok, raw data: %s\n", j.ToString())
			return false
		}
	} else {
		le.Logf(lg.WarnLevel, "Transaction operation status format is unknown, raw data: %s\n", j.ToString())
		return false
	}
	return true
}

func initTriggersTest(runtime *statefun.Runtime) {
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", "typea", nil, nil))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", "typeb", nil, nil))

	v := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("typeb"))
	v.SetByPath("object_type", easyjson.NewJSON("a2b"))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.create", "typea", &v, nil))
}

func TriggersTestIteration(runtime *statefun.Runtime) {
	// Create A
	payload := easyjson.NewJSONObjectWithKeyValue("origin_type", easyjson.NewJSON("typea"))
	payload.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("a_state", easyjson.NewJSON("created")))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "hub/a", &payload, nil))

	// Create B
	payload = easyjson.NewJSONObjectWithKeyValue("origin_type", easyjson.NewJSON("typeb"))
	payload.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("b_state", easyjson.NewJSON("created")))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "leaf/b", &payload, nil))

	// Create A -> B link
	payload = easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("leaf/b"))
	payload.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("ab_state", easyjson.NewJSON("created")))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.create", "hub/a", &payload, nil))

	// Delete A
	payload = easyjson.NewJSONObject()
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", "hub/a", &payload, nil))

	// Create A
	payload = easyjson.NewJSONObjectWithKeyValue("origin_type", easyjson.NewJSON("typea"))
	payload.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("a_state", easyjson.NewJSON("recreated")))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "hub/a", &payload, nil))

	// Create A -> B link
	payload = easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("leaf/b"))
	payload.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("ab_state", easyjson.NewJSON("recreated")))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.create", "hub/a", &payload, nil))

	// Update B
	payload = easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObjectWithKeyValue("b_state", easyjson.NewJSON("updated")))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.update", "leaf/b", &payload, nil))

	// Update A -> B link
	payload = easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("leaf/b"))
	payload.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("ab_state", easyjson.NewJSON("updated")))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.update", "hub/a", &payload, nil))

	// Delete A -> B link
	payload = easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("leaf/b"))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", "hub/a", &payload, nil))

	// Delete B
	payload = easyjson.NewJSONObject()
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", "leaf/b", &payload, nil))

	// Delete A
	payload = easyjson.NewJSONObject()
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", "hub/a", &payload, nil))
}

func registerTriggers1(runtime *statefun.Runtime) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body.triggers.create", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.update", "typea", &payload, nil))

	payload = easyjson.NewJSONObject()
	payload.SetByPath("body.triggers.update", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	payload.SetByPath("body.triggers.delete", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.update", "typeb", &payload, nil))

	payload = easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON("typeb"))
	payload.SetByPath("body.triggers.update", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.update", "typea", &payload, nil))
}

func registerTriggers2(runtime *statefun.Runtime) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body.triggers.update", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	payload.SetByPath("body.triggers.delete", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.update", "typea", &payload, nil))

	payload = easyjson.NewJSONObject()
	payload.SetByPath("body.triggers.create", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.update", "typeb", &payload, nil))

	payload = easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON("typeb"))
	payload.SetByPath("body.triggers.create", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	payload.SetByPath("body.triggers.delete", easyjson.JSONFromArray([]string{triggersTestStatefun1}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.update", "typea", &payload, nil))
}

func registerTriggers3(runtime *statefun.Runtime) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body.triggers.create", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	payload.SetByPath("body.triggers.update", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	payload.SetByPath("body.triggers.delete", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.update", "typea", &payload, nil))

	payload = easyjson.NewJSONObject()
	payload.SetByPath("body.triggers.create", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	payload.SetByPath("body.triggers.update", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	payload.SetByPath("body.triggers.delete", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.update", "typeb", &payload, nil))

	payload = easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON("typeb"))
	payload.SetByPath("body.triggers.create", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	payload.SetByPath("body.triggers.update", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	payload.SetByPath("body.triggers.delete", easyjson.JSONFromArray([]string{triggersTestStatefun2}))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.update", "typea", &payload, nil))
}

func triggersStatefun1(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	lg.Logf(lg.DebugLevel, "-------> %s:%s\n", ctx.Self.Typename, ctx.Self.ID)
	lg.Logln(lg.DebugLevel, "== Payload:", ctx.Payload.ToString())
}

func triggersStatefun2(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	lg.Logf(lg.DebugLevel, "-------> %s:%s\n", ctx.Self.Typename, ctx.Self.ID)
	lg.Logln(lg.DebugLevel, "== Payload:", ctx.Payload.ToString())
}

func registerTriggerFunctions(runtime *statefun.Runtime) {
	statefun.NewFunctionType(runtime, triggersTestStatefun1, triggersStatefun1, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, triggersTestStatefun2, triggersStatefun2, *statefun.NewFunctionTypeConfig())
}

func RunTriggersTest(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: crud API triggers")

	initTriggersTest(runtime)

	lg.Logln(lg.DebugLevel, "### crud API triggers 1/4: No triggers registered")
	TriggersTestIteration(runtime)
	time.Sleep(1 * time.Second)

	lg.Logln(lg.DebugLevel, "### crud API triggers 2/4: Some triggers registered")
	registerTriggers1(runtime)
	TriggersTestIteration(runtime)
	time.Sleep(1 * time.Second)

	lg.Logln(lg.DebugLevel, "### crud API triggers 3/4: All triggers registered")
	registerTriggers2(runtime)
	TriggersTestIteration(runtime)
	time.Sleep(1 * time.Second)

	lg.Logln(lg.DebugLevel, "### crud API triggers 4/4: More triggers registered")
	registerTriggers3(runtime)
	TriggersTestIteration(runtime)
	time.Sleep(1 * time.Second)

	lg.Logln(lg.DebugLevel, "<<< Test ended: crud API triggers")
}
