package main

import (
	"runtime"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
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
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("typea"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("typeb"))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("typea", "typeb", "a2b", nil))
}

func TriggersTestIteration(runtime *statefun.Runtime) {
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("a", "typea", easyjson.NewJSONObjectWithKeyValue("a_state", easyjson.NewJSON("created"))))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("b", "typeb", easyjson.NewJSONObjectWithKeyValue("b_state", easyjson.NewJSON("created"))))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate("a", "b", "2b", nil, easyjson.NewJSONObjectWithKeyValue("ab_state", easyjson.NewJSON("created"))))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectDelete("a"))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("a", "typea", easyjson.NewJSONObjectWithKeyValue("a_state", easyjson.NewJSON("recreated"))))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate("a", "b", "2b", nil, easyjson.NewJSONObjectWithKeyValue("ab_state", easyjson.NewJSON("recreated"))))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectUpdate("b", easyjson.NewJSONObjectWithKeyValue("b_state", easyjson.NewJSON("updated")), true))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkUpdate("a", "b", nil, easyjson.NewJSONObjectWithKeyValue("ab_state", easyjson.NewJSON("updated")), true))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkDelete("a", "b"))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectDelete("b"))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectDelete("a"))
}

func registerTriggers1(runtime *statefun.Runtime) {
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typea", db.CreateTrigger, triggersTestStatefun1))

	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typeb", db.UpdateTrigger, triggersTestStatefun1))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typeb", db.DeleteTrigger, triggersTestStatefun1))

	system.MsgOnErrorReturn(dbClient.CMDB.TriggerLinkSet("typea", "typeb", db.UpdateTrigger, triggersTestStatefun1))
}

func registerTriggers2(runtime *statefun.Runtime) {
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typea", db.UpdateTrigger, triggersTestStatefun1))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typea", db.DeleteTrigger, triggersTestStatefun1))

	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typeb", db.CreateTrigger, triggersTestStatefun1))

	system.MsgOnErrorReturn(dbClient.CMDB.TriggerLinkSet("typea", "typeb", db.CreateTrigger, triggersTestStatefun1))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerLinkSet("typea", "typeb", db.DeleteTrigger, triggersTestStatefun1))
}

func registerTriggers3(runtime *statefun.Runtime) {
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typea", db.CreateTrigger, triggersTestStatefun2))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typea", db.UpdateTrigger, triggersTestStatefun2))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typea", db.DeleteTrigger, triggersTestStatefun2))

	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typeb", db.CreateTrigger, triggersTestStatefun2))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typeb", db.UpdateTrigger, triggersTestStatefun2))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typeb", db.DeleteTrigger, triggersTestStatefun2))

	system.MsgOnErrorReturn(dbClient.CMDB.TriggerLinkSet("typea", "typeb", db.CreateTrigger, triggersTestStatefun2))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerLinkSet("typea", "typeb", db.UpdateTrigger, triggersTestStatefun2))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerLinkSet("typea", "typeb", db.DeleteTrigger, triggersTestStatefun2))
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
