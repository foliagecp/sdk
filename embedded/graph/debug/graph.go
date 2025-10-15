// Foliage graph store debug package.
// Provides debug stateful functions for the graph store

package debug

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

type gNode struct {
	id    string
	depth int
}

type gEdge struct {
	from string        // parent vertex id
	name string        // link name
	to   string        // child vertex id
	tp   string        // link type
	tags []string      // link tags
	body easyjson.JSON // link's body
}

type exportConfig struct {
	excludeVertexFields []string
	excludeEdgeFields   []string
}

/*
Print Graph from certain id using Graphviz

Algorithm: Sync BFS

	Payload: {
		"depth": uint // optional, default: -1
		"format": string // "dot" | "graphml" - optional, default: "dot"
		"json2xml": bool // whether to export bodies as json or xml (graphml only) - optional, default false
		"exclude": { // Fields to exclude during export, optional
			"vertex": ["__meta", "fieldX.fieldY" ...] // optional
			"edge": ["field1", ...] // optional
		}
	}
*/
func LLAPIPrintGraph(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	self := ctx.Self
	payload := ctx.Payload

	maxDepth := int(payload.GetByPath("depth").AsNumericDefault(-1))

	var conf exportConfig
	if arr, ok := payload.GetByPath("exclude.vertex").AsArrayString(); ok {
		conf.excludeVertexFields = arr
	}
	if arr, ok := payload.GetByPath("exclude.edge").AsArrayString(); ok {
		conf.excludeEdgeFields = arr
	}

	nodes := map[string]*easyjson.JSON{}
	uniqueEdges := map[string]struct{}{}
	queue := []gNode{}
	queue = append(queue, gNode{self.ID, 0})

	edges := []gEdge{}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if _, exists := nodes[node.id]; exists {
			continue
		}

		b, e := getVertexBodyAndOutLinks(ctx, node.id)
		nodes[node.id] = b

		for _, edge := range e {
			if maxDepth < 0 || node.depth < maxDepth {
				if _, ok := nodes[edge.to]; !ok {
					queue = append(queue, gNode{edge.to, node.depth + 1}) // Forward link itrospection
				}
				if _, ok := nodes[edge.from]; !ok {
					queue = append(queue, gNode{edge.from, node.depth + 1}) // Inward link introspection
				}
			}
			if edge.from == node.id {
				if _, ok := uniqueEdges[edge.from+edge.name]; !ok {
					uniqueEdges[edge.from+edge.name] = struct{}{}
					edges = append(edges, edge)
				}
			}
		}
	}

	om := sfMediators.NewOpMediator(ctx)

	var fileData string

	format := payload.GetByPath("format").AsStringDefault("dot")
	switch format {
	case "graphml":
		fileData = createGraphML(ctx.Self.ID, ctx.Domain, nodes, edges, conf, payload.GetByPath("json2xml").AsBoolDefault(false))
	case "dot":
		fileData = createGraphViz(ctx.Self.ID, ctx.Domain, nodes, edges)
	default:
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("%s – unsopported format", format))).Reply()
		return
	}

	reply := easyjson.NewJSONObjectWithKeyValue("file", easyjson.NewJSON(fileData))
	reply.SetByPath("format", easyjson.NewJSON(format))
	om.AggregateOpMsg(sfMediators.OpMsgOk(reply)).Reply()
}

func getVertexBodyAndOutLinks(ctx *sfPlugins.StatefunContextProcessor, id string) (*easyjson.JSON, []gEdge) {
	var outLinkNames []string
	var inLinks *easyjson.JSON

	var vertexBody *easyjson.JSON
	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", id, &payload, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if arr, ok := som.Data.GetByPath("links.out.names").AsArrayString(); ok {
			outLinkNames = arr
		}
		inLinks = som.Data.GetByPath("links.in").GetPtr()
		vertexBody = som.Data.GetByPathPtr("body")
	}
	if outLinkNames == nil {
		outLinkNames = []string{}
	}
	if inLinks == nil {
		inLinks = easyjson.NewJSONArray().GetPtr()
	}

	edges := []gEdge{}

	for _, outLinkName := range outLinkNames {
		lt := ""
		to := ""
		var body easyjson.JSON
		var tags []string

		payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(outLinkName))
		som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", id, &payload, nil))
		if som.Status == sfMediators.SYNC_OP_STATUS_OK {
			lt = som.Data.GetByPath("type").AsStringDefault(lt)
			to = som.Data.GetByPath("to").AsStringDefault(to)
			body = som.Data.GetByPath("body")
			if arr, ok := som.Data.GetByPath("tags").AsArrayString(); ok {
				tags = arr
			}
		}
		if tags == nil {
			tags = []string{}
		}

		if len(id) > 0 && len(outLinkName) > 0 && len(to) > 0 && len(lt) > 0 {
			edges = append(edges, gEdge{
				from: id,
				name: outLinkName,
				to:   to,
				tp:   lt,
				tags: tags,
				body: body,
			})
		}
	}

	for i := 0; i < inLinks.ArraySize(); i++ {
		inLink := inLinks.ArrayElement(i)
		from := inLink.GetByPath("from").AsStringDefault("")
		linkName := inLink.GetByPath("name").AsStringDefault("")

		lt := ""
		var tags []string

		payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(linkName))
		som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", from, &payload, nil))
		if som.Status == sfMediators.SYNC_OP_STATUS_OK {
			lt = som.Data.GetByPath("type").AsStringDefault(lt)
			if arr, ok := som.Data.GetByPath("tags").AsArrayString(); ok {
				tags = arr
			}
		}
		if tags == nil {
			tags = []string{}
		}

		if len(from) > 0 && len(linkName) > 0 && len(id) > 0 && len(lt) > 0 {
			edges = append(edges, gEdge{
				from: from,
				name: linkName,
				to:   id,
				tp:   lt,
				tags: tags,
			})
		}
	}

	return vertexBody, edges
}

/*
format: string // "graphml"
source: string // "file" | "payload"
data: string // "graph data" | "file path"

Example:
nats -s nats://nats:foliage@nats:4222 pub signal.hub.functions.graph.api.import.a "{\"payload\":{\"format\":\"graphml\",\"source\":\"file\",\"data\":\"./skala-xml.graphml\"}}"
*/
func LLAPIImportGraph(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	const workers = 10

	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload
	format := payload.GetByPath("format").AsStringDefault("dot")
	source := payload.GetByPath("source").AsStringDefault("payload")

	lg.Logln(lg.InfoLevel, "------------------ GRAPH <<< IMPORTING <<< DATA ------------------")

	switch format {
	case "graphml":
		lg.Logln(lg.InfoLevel, "Format: graphml")
		graph := RawGraph{}
		switch source {
		case "payload":
			lg.Logln(lg.InfoLevel, "Source: payload.data")
			reader := strings.NewReader(payload.GetByPath("data").AsStringDefault(""))
			g, err := ImportGraphML(reader)
			if err != nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
				lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", err.Error())
				return
			}
			graph = g
		case "file":
			fileName := payload.GetByPath("data").AsStringDefault("")
			lg.Logln(lg.InfoLevel, "Source: file %s", fileName)
			f, err := os.Open(fileName)
			if err != nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
				lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", err.Error())
				return
			}
			defer f.Close()
			g, err := ImportGraphML(f)
			if err != nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
				lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", err.Error())
				return
			}
			graph = g
		}

		dbc, err := db.NewDBSyncClientFromRequestFunction(ctx.Request)
		if err != nil {
			lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", err.Error())
			om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
			return
		}

		t0 := time.Now()
		// ---------- PHASE 1: vertices ----------
		lg.Logln(lg.InfoLevel, "Importing vertices with %d workers...", workers)

		var wg sync.WaitGroup
		vertexIdx := make(chan int, workers*4)

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range vertexIdx {
					n := graph.Nodes[i]
					uuid := ctx.Domain.CreateObjectIDWithHubDomain(n.Id, true)

					if err := dbc.Graph.VertexDelete(uuid); err != nil {
						system.MsgOnErrorReturn(err)
					}
					if body, _ := ExtractBodyAsJSON(n.Attributes); true {
						if err := dbc.Graph.VertexCreate(uuid, body); err != nil {
							system.MsgOnErrorReturn(err)
						}
					}
				}
			}()
		}

		for i := range graph.Nodes {
			vertexIdx <- i
		}
		close(vertexIdx)
		wg.Wait()

		// ---------- PHASE 2: edges ----------
		lg.Logln(lg.InfoLevel, "Importing edges with %d workers...", workers)

		var wgE sync.WaitGroup
		edgeIdx := make(chan int, workers*4)

		for w := 0; w < workers; w++ {
			wgE.Add(1)
			go func() {
				defer wgE.Done()
				for i := range edgeIdx {
					e := graph.Edges[i]
					uuidFrom := ctx.Domain.CreateObjectIDWithHubDomain(e.Source, true)
					uuidTo := ctx.Domain.CreateObjectIDWithHubDomain(e.Target, true)

					tp, name, tags := ExtractEdgeTypeAndNameAndTags(e.Attributes)
					body, _ := ExtractBodyAsJSON(e.Attributes)

					if err := dbc.Graph.VerticesLinkCreate(uuidFrom, uuidTo, name, tp, tags, body); err != nil {
						system.MsgOnErrorReturn(err)
					}
				}
			}()
		}

		for i := range graph.Edges {
			edgeIdx <- i
		}
		close(edgeIdx)
		wgE.Wait()

		lg.Logln(lg.InfoLevel, fmt.Sprintf("Import is done within %.2f sec", time.Since(t0).Seconds()))
		om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONObject())).Reply()

	default:
		msg := fmt.Sprintf("%s – unsopported format", format)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(msg)).Reply()
		lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", msg)
		return
	}
}
