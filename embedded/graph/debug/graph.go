

// Foliage graph store debug package.
// Provides debug stateful functions for the graph store

package debug

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/emicklei/dot"
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type gdNode struct {
	id    string
	depth int
}

type gdEdge struct {
	from string   // parent vertex id
	name string   // link name
	to   string   // child vertex id
	tp   string   // link type
	tags []string // link tags
}

/*
Print Graph from certain id using Graphviz

Algorithm: Sync BFS

	Payload: {
		"depth": uint // optional, default: -1
	}
*/
func LLAPIPrintGraph(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	self := ctx.Self
	payload := ctx.Payload

	maxDepth := int(payload.GetByPath("depth").AsNumericDefault(-1))

	nodes := map[string]struct{}{}
	uniqueEdges := map[string]struct{}{}
	queue := []gdNode{}
	queue = append(queue, gdNode{self.ID, 0})

	edges := []gdEdge{}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if _, exists := nodes[node.id]; exists {
			continue
		}
		nodes[node.id] = struct{}{}
		if maxDepth >= 0 && node.depth >= maxDepth {
			continue
		}

		for _, edge := range getEdges(ctx, node.id) {
			if _, ok := nodes[edge.to]; !ok {
				queue = append(queue, gdNode{edge.to, node.depth + 1}) // Forward link itrospection
			}
			if _, ok := nodes[edge.from]; !ok {
				queue = append(queue, gdNode{edge.from, node.depth + 1}) // Inward link introspection
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

	dotData := createGraphViz(ctx.Self.ID, ctx.Domain, nodes, edges)
	reply := easyjson.NewJSONObjectWithKeyValue("dot_file", easyjson.NewJSON(dotData))
	om.AggregateOpMsg(sfMediators.OpMsgOk(reply)).Reply()
}

func createGraphViz(sourceVertex string, domain sfPlugins.Domain, nodes map[string]struct{}, edges []gdEdge) string {
	dotGraph := dot.NewGraph(dot.Directed)

	domainColors := map[string]string{}

	randomHex := func(n int) (string, error) {
		bytes := make([]byte, n)
		if _, err := rand.Read(bytes); err != nil {
			return "", err
		}
		return hex.EncodeToString(bytes), nil
	}

	createGraphDotNode := func(nodeId string) (*dot.Node, error) {
		dm := domain.GetDomainFromObjectID(nodeId)
		color, ok := domainColors[dm]
		if !ok {
			c, err := randomHex(3)
			if err == nil {
				color = c
			} else {
				color = "ffffff"
			}
			domainColors[dm] = color
		}

		return createNode(dotGraph, nodeId, color)
	}

	dotNodes := map[string]*dot.Node{}
	for nodeId := range nodes {
		dotNode, err := createGraphDotNode(nodeId)
		if err != nil {
			continue
		}
		dotNodes[nodeId] = dotNode
	}

	for _, edge := range edges {
		fromNode, fromOK := dotNodes[edge.from]
		toNode, toOK := dotNodes[edge.to]
		if fromOK && toOK {
			if _, err := createEdge(dotGraph, edge, fromNode, toNode); err != nil {
				logger.Logf(logger.ErrorLevel, "in dot file cannot create link from %s to %s: %s\n", edge.from, edge.to, err.Error())
			}
		}
	}

	return dotGraph.String()
}

func getEdges(ctx *sfPlugins.StatefunContextProcessor, id string) []gdEdge {
	var outLinkNames []string
	var inLinks *easyjson.JSON

	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", id, &payload, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if arr, ok := som.Data.GetByPath("links.out.names").AsArrayString(); ok {
			outLinkNames = arr
		}
		inLinks = som.Data.GetByPath("links.in").GetPtr()
	}
	if outLinkNames == nil {
		outLinkNames = []string{}
	}
	if inLinks == nil {
		inLinks = easyjson.NewJSONArray().GetPtr()
	}

	edges := []gdEdge{}

	for _, outLinkName := range outLinkNames {
		lt := ""
		to := ""
		var tags []string

		payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(outLinkName))
		som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", id, &payload, nil))
		if som.Status == sfMediators.SYNC_OP_STATUS_OK {
			lt = som.Data.GetByPath("type").AsStringDefault(lt)
			to = som.Data.GetByPath("to").AsStringDefault(to)
			if arr, ok := som.Data.GetByPath("tags").AsArrayString(); ok {
				tags = arr
			}
		}
		if tags == nil {
			tags = []string{}
		}

		if len(id) > 0 && len(outLinkName) > 0 && len(to) > 0 && len(lt) > 0 {
			edges = append(edges, gdEdge{
				from: id,
				name: outLinkName,
				to:   to,
				tp:   lt,
				tags: tags,
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
			edges = append(edges, gdEdge{
				from: from,
				name: linkName,
				to:   id,
				tp:   lt,
				tags: tags,
			})
		}
	}

	return edges
}

func createNode(dotGraph *dot.Graph, name string, colorStr string) (*dot.Node, error) {
	node := dotGraph.Node(name).Box()
	node.Attr("color", fmt.Sprintf("#%s", colorStr))

	return &node, nil
}

func createEdge(dotGraph *dot.Graph, e gdEdge, start, end *dot.Node) (*dot.Edge, error) {
	edgeLabel := fmt.Sprintf("%s\ntype:%s\ntags:%v", e.name, e.tp, e.tags)
	edge := dotGraph.Edge(*start, *end, edgeLabel)

	return &edge, nil
}
