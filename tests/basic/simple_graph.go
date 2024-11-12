package main

import (
	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/system"
)

func CreateTestGraph(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: simple graph creation")

	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("rt"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("a"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("b"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("c"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("d"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("e"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("f"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("g"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("h"))

	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("rt", "a", "2a", "type1", []string{"t1", "t2"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("rt", "a", "name0", "type2", []string{"t2", "t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("rt", "b", "2b", "type2", []string{"t2"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("rt", "c", "2c", "type1", nil))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("a", "e", "2e", "type3", nil))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("b", "e", "2e", "type4", []string{"t1", "t2", "t3"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("c", "d", "2d", "type3", []string{"t1"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("d", "b", "2b", "type1", []string{"t1", "t3"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("e", "b", "2b", "type2", []string{"t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("e", "f", "2f", "type1", []string{"t1", "t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("f", "g", "2g", "type5", []string{"t1", "t2", "t3", "t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("g", "d", "2d", "type2", []string{"t5"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexLinkCreate("g", "h", "2h", "type2", nil))

	lg.Logln(lg.DebugLevel, "<<< Test ended: simple graph creation")
}

func NewDBClient() {
	panic("unimplemented")
}
