package main

import (
	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/system"
)

func CreateTestGraph(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: simple graph creation")

	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("hub/rt"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("hub/a"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("hub/b"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("hub/c"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("leaf/d"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("leaf/e"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("leaf/f"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("leaf/g"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("leaf/h"))

	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("hub/rt", "hub/a", "2a", "type1", []string{"t1", "t2"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("hub/rt", "hub/a", "name0", "type2", []string{"t2", "t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("hub/rt", "hub/b", "2b", "type2", []string{"t2"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("hub/rt", "hub/c", "2c", "type1", nil))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("hub/a", "leaf/e", "2e", "type3", nil))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("hub/b", "leaf/e", "2e", "type4", []string{"t1", "t2", "t3"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("hub/c", "leaf/d", "2d", "type3", []string{"t1"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("leaf/d", "hub/b", "2b", "type1", []string{"t1", "t3"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("leaf/e", "hub/b", "2b", "type2", []string{"t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("leaf/e", "leaf/f", "2f", "type1", []string{"t1", "t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("leaf/f", "leaf/g", "2g", "type5", []string{"t1", "t2", "t3", "t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("leaf/g", "leaf/d", "2d", "type2", []string{"t5"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("leaf/g", "leaf/h", "2h", "type2", nil))

	lg.Logln(lg.DebugLevel, "<<< Test ended: simple graph creation")
}
