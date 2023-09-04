

package main

import (
	"fmt"
	"json_easy"

	"github.com/foliagecp/sdk/statefun"
)

func CreateTestGraph(runtime *statefun.Runtime) {
	fmt.Println(">>> Test started: simple graph creation")

	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "root", json_easy.NewJSONObject().GetPtr(), nil)
	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "a", json_easy.NewJSONObject().GetPtr(), nil)
	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "b", json_easy.NewJSONObject().GetPtr(), nil)
	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "c", json_easy.NewJSONObject().GetPtr(), nil)
	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "d", json_easy.NewJSONObject().GetPtr(), nil)
	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "e", json_easy.NewJSONObject().GetPtr(), nil)
	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "f", json_easy.NewJSONObject().GetPtr(), nil)
	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "g", json_easy.NewJSONObject().GetPtr(), nil)
	runtime.IngressGolangSync("functions.graph.ll.api.object.create", "h", json_easy.NewJSONObject().GetPtr(), nil)

	var v json_easy.JSON

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("a"))
	v.SetByPath("link_type", json_easy.NewJSON("type1"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t1", "t2"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "root", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("a"))
	v.SetByPath("link_type", json_easy.NewJSON("type2"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t2", "t4"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "root", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("b"))
	v.SetByPath("link_type", json_easy.NewJSON("type2"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t2"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "root", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("c"))
	v.SetByPath("link_type", json_easy.NewJSON("type1"))
	v.SetByPath("link_body.tags", json_easy.NewJSONObject())
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "root", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("e"))
	v.SetByPath("link_type", json_easy.NewJSON("type3"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t3"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "a", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("e"))
	v.SetByPath("link_type", json_easy.NewJSON("type4"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t1", "t2", "t3"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "b", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("d"))
	v.SetByPath("link_type", json_easy.NewJSON("type3"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t1"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "c", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("b"))
	v.SetByPath("link_type", json_easy.NewJSON("type1"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t1", "t3"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "d", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("b"))
	v.SetByPath("link_type", json_easy.NewJSON("type2"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t4"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "e", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("f"))
	v.SetByPath("link_type", json_easy.NewJSON("type1"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t1", "t4"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "e", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("g"))
	v.SetByPath("link_type", json_easy.NewJSON("type5"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t1", "t2", "t3", "t4"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "f", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("d"))
	v.SetByPath("link_type", json_easy.NewJSON("type2"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{"t5"}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "g", &v, nil)

	v = json_easy.NewJSONObject()
	v.SetByPath("descendant_uuid", json_easy.NewJSON("h"))
	v.SetByPath("link_type", json_easy.NewJSON("type2"))
	v.SetByPath("link_body.tags", json_easy.JSONFromArray([]string{}))
	runtime.IngressGolangSync("functions.graph.ll.api.link.create", "g", &v, nil)

	fmt.Println("<<< Test ended: simple graph creation")
}
