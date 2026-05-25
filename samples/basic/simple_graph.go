package main

import (
	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/system"
)

func CreateTestGraph(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: simple graph creation")

	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("rt"))

	// Vertex "a": top-level scalars + nested object + string array
	b1 := easyjson.NewJSONObject()
	b1.SetByPath("val1", easyjson.NewJSON(15.32))
	b1.SetByPath("val2", easyjson.NewJSON("Hello World"))
	b1.SetByPath("val3", easyjson.NewJSON(true))
	b1.SetByPath("info.mac", easyjson.NewJSON("aa:bb:cc:dd:ee:01"))
	b1.SetByPath("info.role", easyjson.NewJSON("spine"))
	b1.SetByPath("tags", easyjson.NewJSON([]string{"core", "prod", "dc1"}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("a", b1))

	// Vertex "b": top-level scalars + nested object + string array + numeric array
	b2 := easyjson.NewJSONObject()
	b2.SetByPath("val1", easyjson.NewJSON(25.99))
	b2.SetByPath("val2", easyjson.NewJSON("something"))
	b2.SetByPath("val3", easyjson.NewJSON(false))
	b2.SetByPath("info.mac", easyjson.NewJSON("aa:bb:cc:dd:ee:02"))
	b2.SetByPath("info.role", easyjson.NewJSON("leaf"))
	b2.SetByPath("tags", easyjson.NewJSON([]string{"edge", "staging"}))
	b2.SetByPath("ports", easyjson.NewJSON([]int{1, 2, 3, 4}))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("b", b2))

	// Vertex "c": top-level scalars + nested object + string array + deep nested config
	b3 := easyjson.NewJSONObject()
	b3.SetByPath("val1", easyjson.NewJSON(-33))
	b3.SetByPath("val2", easyjson.NewJSON("tea Peach rock"))
	b3.SetByPath("val3", easyjson.NewJSON(false))
	b3.SetByPath("info.mac", easyjson.NewJSON("aa:bb:cc:dd:ee:03"))
	b3.SetByPath("info.role", easyjson.NewJSON("spine"))
	b3.SetByPath("tags", easyjson.NewJSON([]string{"core", "staging", "dc2"}))
	b3.SetByPath("config.vlans", easyjson.NewJSON([]int{100, 200, 300}))
	b3.SetByPath("config.mtu", easyjson.NewJSON(9000))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("c", b3))

	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("d"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("e"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("f"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("g"))
	system.MsgOnErrorReturn(dbClient.Graph.VertexCreate("h"))

	// Link rt→a (2a): scalars + nested metrics + string array protocols
	lb1 := easyjson.NewJSONObject()
	lb1.SetByPath("lval1", easyjson.NewJSON(11.11))
	lb1.SetByPath("lval2", easyjson.NewJSON("disk"))
	lb1.SetByPath("lval3", easyjson.NewJSON(true))
	lb1.SetByPath("metrics.bw", easyjson.NewJSON(1000))
	lb1.SetByPath("metrics.latency", easyjson.NewJSON(5))
	lb1.SetByPath("protocols", easyjson.NewJSON([]string{"bgp", "ospf"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("rt", "a", "2a", "type1", []string{"t1", "t2"}, lb1))

	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("rt", "a", "name0", "type2", []string{"t2", "t4"}))

	// Link rt→b (2b): scalars + nested metrics + string array protocols
	lb2 := easyjson.NewJSONObject()
	lb2.SetByPath("lval1", easyjson.NewJSON(22.22))
	lb2.SetByPath("lval2", easyjson.NewJSON("cpu"))
	lb2.SetByPath("lval3", easyjson.NewJSON(false))
	lb2.SetByPath("metrics.bw", easyjson.NewJSON(500))
	lb2.SetByPath("metrics.latency", easyjson.NewJSON(10))
	lb2.SetByPath("protocols", easyjson.NewJSON([]string{"bgp"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("rt", "b", "2b", "type2", []string{"t2"}, lb2))

	// Link rt→c (2c): scalars + nested metrics + string array protocols
	lb3 := easyjson.NewJSONObject()
	lb3.SetByPath("lval1", easyjson.NewJSON(33.33))
	lb3.SetByPath("lval2", easyjson.NewJSON("nic"))
	lb3.SetByPath("lval3", easyjson.NewJSON(true))
	lb3.SetByPath("metrics.bw", easyjson.NewJSON(2000))
	lb3.SetByPath("metrics.latency", easyjson.NewJSON(2))
	lb3.SetByPath("protocols", easyjson.NewJSON([]string{"isis", "ldp"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("rt", "c", "2c", "type1", nil, lb3))

	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("a", "e", "2e", "type3", nil))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("b", "e", "2e", "type4", []string{"t1", "t2", "t3"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("c", "d", "2d", "type3", []string{"t1"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("d", "b", "2b", "type1", []string{"t1", "t3"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("e", "b", "2b", "type2", []string{"t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("e", "f", "2f", "type1", []string{"t1", "t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("f", "g", "2g", "type5", []string{"t1", "t2", "t3", "t4"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("g", "d", "2d", "type2", []string{"t5"}))
	system.MsgOnErrorReturn(dbClient.Graph.VerticesLinkCreate("g", "h", "2h", "type2", nil))

	lg.Logln(lg.DebugLevel, "<<< Test ended: simple graph creation")
}
