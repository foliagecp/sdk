package main

import (
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
)

func CreateTestCMDB(runtime *statefun.Runtime) {
	lg.Logf(lg.DebugLevel, ">>> Test started: distributed graph with shadow objects in %v", runtime.Domain.Name())

	rackType := easyjson.NewJSONObject()
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("rack", rackType))

	serverType := easyjson.NewJSONObject()
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("server", serverType))

	vmType := easyjson.NewJSONObject()
	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("vm", vmType))

	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("rack", "server", "rack-server", nil))
	system.MsgOnErrorReturn(dbClient.CMDB.TypesLinkCreate("server", "vm", "server-vm", nil))

	system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("rack1", "rack"))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("server1", "server"))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectCreate("vm1", "vm"))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate("rack1", "server1", "rack-server", nil))
	system.MsgOnErrorReturn(dbClient.CMDB.ObjectsLinkCreate("server1", "vm1", "server-vm", nil))

	lg.Logln(lg.DebugLevel, "<<< Test ended: distributed graph with shadow objects")
}
