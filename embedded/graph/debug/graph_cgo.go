// Copyright 2023 NJWS Inc.

// Foliage graph store debug package.
// Provides debug stateful functions for the graph store

//go:build (cgo && !darwin) || graph_debug

package debug

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
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
		"ext": "dot" | "png" | "svg" // optional, default: "dot"
		"depth": uint // optional, default: 256
	}
*/
func LLAPIPrintGraph(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	self := ctx.Self
	payload := ctx.Payload

	graphvizExtension := graphviz.XDOT

	if payload.PathExists("ext") {
		ext := payload.GetByPath("ext").AsStringDefault("")
		switch ext {
		case "dot":
			graphvizExtension = graphviz.XDOT
		case "png":
			graphvizExtension = graphviz.PNG
		case "svg":
			graphvizExtension = graphviz.SVG
		}
	}

	maxDepth := int(payload.GetByPath("depth").AsNumericDefault(-1))

	gviz := graphviz.New()
	graph, err := gviz.Graph()
	if err != nil {
		logger.Logln(logger.WarnLevel, err)
		return
	}

	defer func() {
		_ = gviz.Close()
		_ = graph.Close()
	}()

	graph = graph.SetSize(500, 500)
	graph.SetRankSeparator(5)

	domainColors := map[string]string{}

	nodes := make(map[string]*cgraph.Node)
	queue := []gdNode{}
	queue = append(queue, gdNode{self.ID, 0})

	randomHex := func(n int) (string, error) {
		bytes := make([]byte, n)
		if _, err := rand.Read(bytes); err != nil {
			return "", err
		}
		return hex.EncodeToString(bytes), nil
	}

	createGraphDotNodeIfNotExists := func(nodeId string) (*cgraph.Node, error) {
		dm := ctx.Domain.GetDomainFromObjectID(nodeId)
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

		e, exists := nodes[nodeId]
		if !exists {
			newElem, err := createNode(graph, nodeId, color)
			if err == nil {
				nodes[nodeId] = newElem
			}
			return newElem, err
		}
		return e, nil
	}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		thisElem, err := createGraphDotNodeIfNotExists(node.id)
		if err != nil {
			continue
		}
		if maxDepth >= 0 && node.depth >= maxDepth {
			continue
		}

		for _, edge := range getEdges(ctx, node.id) {
			if _, ok := nodes[edge.to]; !ok {
				queue = append(queue, gdNode{edge.to, node.depth + 1})
			}
			if _, ok := nodes[edge.from]; !ok {
				queue = append(queue, gdNode{edge.from, node.depth + 1})
			}
			if node.id == edge.from {
				toElem, err := createGraphDotNodeIfNotExists(edge.to)
				if err == nil {
					createEdge(graph, edge, thisElem, toElem)
				}
			}
		}
	}

	outputPath := filepath.Join("graph." + string(graphvizExtension))

	if err := gviz.RenderFilename(graph, graphvizExtension, outputPath); err != nil {
		logger.Logln(logger.WarnLevel, err)
		return
	}
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

func createNode(graph *cgraph.Graph, name string, colorStr string) (*cgraph.Node, error) {
	node, err := graph.CreateNode(name)
	if err != nil {
		return nil, err
	}

	node.SetHeight(1)
	node.SetArea(1)
	node.SetShape(cgraph.BoxShape)
	node.SetFontSize(24)
	node.SetColor(fmt.Sprintf("#%s", colorStr))
	node.SetStyle(cgraph.FilledNodeStyle)

	return node, nil
}

func createEdge(graph *cgraph.Graph, e gdEdge, start, end *cgraph.Node) (*cgraph.Edge, error) {
	edge, err := graph.CreateEdge(e.name, start, end)
	if err != nil {
		return nil, err
	}

	edge.SetLabel(fmt.Sprintf("%s\ntype:%s\ntags:%v", e.name, e.tp, e.tags))
	edge.SetFontSize(18)

	return edge, nil
}
