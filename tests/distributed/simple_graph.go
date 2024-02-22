package main

import (
	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CreateTestGraph(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: simple graph creation")

	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "hub/rt", easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "hub/a", easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "hub/b", easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "hub/c", easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "leaf/d", easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "leaf/e", easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "leaf/f", easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "leaf/g", easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.vertex.create", "leaf/h", easyjson.NewJSONObject().GetPtr(), nil))

	var v easyjson.JSON

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("hub/a"))
	v.SetByPath("type", easyjson.NewJSON("type1"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t1", "t2"}))
	v.SetByPath("name", easyjson.NewJSON("2a"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "hub/rt", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("hub/a"))
	v.SetByPath("type", easyjson.NewJSON("type2"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t2", "t4"}))
	v.SetByPath("name", easyjson.NewJSON("2a"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "hub/rt", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("hub/b"))
	v.SetByPath("type", easyjson.NewJSON("type2"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t2"}))
	v.SetByPath("name", easyjson.NewJSON("2b"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "hub/rt", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("hub/c"))
	v.SetByPath("type", easyjson.NewJSON("type1"))
	v.SetByPath("tags", easyjson.NewJSONObject())
	v.SetByPath("name", easyjson.NewJSON("2c"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "hub/rt", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("leaf/e"))
	v.SetByPath("type", easyjson.NewJSON("type3"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t3"}))
	v.SetByPath("name", easyjson.NewJSON("2e"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "hub/a", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("leaf/e"))
	v.SetByPath("type", easyjson.NewJSON("type4"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t1", "t2", "t3"}))
	v.SetByPath("name", easyjson.NewJSON("2e"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "hub/b", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("leaf/d"))
	v.SetByPath("type", easyjson.NewJSON("type3"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t1"}))
	v.SetByPath("name", easyjson.NewJSON("2d"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "hub/c", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("hub/b"))
	v.SetByPath("type", easyjson.NewJSON("type1"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t1", "t3"}))
	v.SetByPath("name", easyjson.NewJSON("2b"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "leaf/d", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("hub/b"))
	v.SetByPath("type", easyjson.NewJSON("type2"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t4"}))
	v.SetByPath("name", easyjson.NewJSON("2b"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "leaf/e", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("leaf/f"))
	v.SetByPath("type", easyjson.NewJSON("type1"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t1", "t4"}))
	v.SetByPath("name", easyjson.NewJSON("2f"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "leaf/e", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("leaf/g"))
	v.SetByPath("type", easyjson.NewJSON("type5"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t1", "t2", "t3", "t4"}))
	v.SetByPath("name", easyjson.NewJSON("2g"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "leaf/f", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("leaf/d"))
	v.SetByPath("type", easyjson.NewJSON("type2"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{"t5"}))
	v.SetByPath("name", easyjson.NewJSON("2d"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "leaf/g", &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON("leaf/h"))
	v.SetByPath("type", easyjson.NewJSON("type2"))
	v.SetByPath("tags", easyjson.JSONFromArray([]string{}))
	v.SetByPath("name", easyjson.NewJSON("2h"))
	system.MsgOnErrorReturn(runtime.Request(plugins.AutoRequestSelect, "functions.graph.api.link.create", "leaf/g", &v, nil))

	lg.Logln(lg.DebugLevel, "<<< Test ended: simple graph creation")
}
