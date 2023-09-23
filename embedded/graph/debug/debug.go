

// Foliage graph store debug package.
// Provides debug stateful functions for the graph store
package debug

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	"github.com/foliagecp/sdk/statefun"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.debug.print", LLAPIObjectDebugPrint, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.debug.print.graph", LLAPIPrintGraph, *statefun.NewFunctionTypeConfig())
}

/*
Prints to caonsole the content of an object the function being called on along with all its input and output links.
*/
func LLAPIObjectDebugPrint(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	self := contextProcessor.Self

	objectContext := contextProcessor.GetObjectContext()
	fmt.Printf("************************* Object's body (id=%s):\n", self.ID)
	fmt.Println(objectContext.ToString())
	fmt.Printf("************************* In links:\n")
	for _, key := range contextProcessor.GlobalCache.GetKeysByPattern(self.ID + ".in.oid_ltp-nil.>") {
		fmt.Println(key)
	}
	fmt.Printf("************************* Out links:\n")
	for _, key := range contextProcessor.GlobalCache.GetKeysByPattern(self.ID + ".out.ltp_oid-bdy.>") {
		fmt.Println(key)
		if j, err := contextProcessor.GlobalCache.GetValueAsJSON(key); err == nil {
			fmt.Println(j.ToString())
		}
	}
	fmt.Println()
}

/*
uuid: [

	{
		uuid1: []
		link: "type"
	},
	{
		uuid2: []
		link: "type"
	},

]
*/
func LLAPIPrintGraph(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	self := contextProcessor.Self
	payload := contextProcessor.Payload
	qid := common.GetQueryID(contextProcessor)

	if !payload.PathExists("root") {
		payload.SetByPath("root", easyjson.NewJSON(self.ID))
	}

	childs := contextProcessor.GlobalCache.GetKeysByPattern(self.ID + ".out.ltp_oid-bdy.>")

	// add root check
	if len(childs) == 0 {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("root", payload.GetByPath("root"))
		reply.SetByPath("result", easyjson.NewJSONObjectWithKeyValue(self.ID, easyjson.NewJSONArray()))
		common.ReplyQueryID(qid, &reply, contextProcessor)
		return
	}

	reply := easyjson.NewJSONArray()

	for _, key := range childs {
		split := strings.Split(key, ".")
		if len(split) == 0 {
			continue
		}

		id := split[len(split)-1]
		linkType := split[len(split)-2]

		result, err := contextProcessor.GolangCallSync("functions.graph.ll.api.object.debug.print.graph", id, payload, nil)
		if err != nil {
			fmt.Printf("call child print graph error: %v\n", err)
			continue
		}

		for _, v := range result.GetByPath("result").ObjectKeys() {
			elem := easyjson.NewJSONObject()
			elem.SetByPath("link", easyjson.NewJSON(linkType))
			elem.SetByPath(v, result.GetByPath(v))
			reply.AddToArray(elem)
		}
	}

	result := easyjson.NewJSONObjectWithKeyValue(self.ID, reply)

	if root := payload.GetByPath("root").AsStringDefault(""); root == self.ID {
		gviz := graphviz.New()
		graph, err := gviz.Graph()
		if err != nil {
			return
		}

		defer func() {
			if err := graph.Close(); err != nil {
				return
			}

			if err := gviz.Close(); err != nil {
				return
			}
		}()

		rootNode, err := graph.CreateNode(root)
		if err != nil {
			return
		}

		if err := processGraph(graph, rootNode, &result); err != nil {
			return
		}

		if err := gviz.RenderFilename(graph, graphviz.PNG, "graph.png"); err != nil {
			return
		}

	} else {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("root", payload.GetByPath("root"))
		reply.SetByPath("result", result)
		common.ReplyQueryID(qid, &reply, contextProcessor)
		return
	}
}

func processGraph(g *cgraph.Graph, root *cgraph.Node, result *easyjson.JSON) error {
	body := result.GetByPath(root.Name())

	elems, ok := body.AsArray()
	if !ok || len(elems) == 0 {
		return nil
	}

	for _, v := range elems {
		elemBody := easyjson.NewJSON(v)

		var elemKey string
		for _, key := range elemBody.ObjectKeys() {
			if key != "link" {
				elemKey = key
				break
			}
		}

		if elemKey == "" {
			return fmt.Errorf("elem key is empty %v", root.Name())
		}

		link := elemBody.GetByPath("link").AsStringDefault("")
		if link == "" {
			return fmt.Errorf("link is empty %v", root.Name())
		}

		elemNode, err := g.CreateNode(elemKey)
		if err != nil {
			return err
		}

		if _, err := g.CreateEdge(link, root, elemNode); err != nil {
			return err
		}

		next := easyjson.NewJSONObjectWithKeyValue(elemKey, elemBody.GetByPath(elemKey))
		processGraph(g, elemNode, &next)
	}

	return nil
}
