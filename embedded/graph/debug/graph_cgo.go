// Copyright 2023 NJWS Inc.

// Foliage graph store debug package.
// Provides debug stateful functions for the graph store

//go:build cgo
// +build cgo

package debug

import (
	"container/list"
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/logger"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
)

type node struct {
	parent string // parent id
	id     string
	lt     string // link type
}

/*
Print Graph from certain id using Graphviz

Algorithm: Sync BFS

	Payload: {
		"ext": "dot" | "png" | "svg" // optional, default: "dot"
		"verbose": true | false // optional, default: false
		"depth": uint // optional, default: 256
	}
*/
func LLAPIPrintGraph(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	self := contextProcessor.Self
	payload := contextProcessor.Payload

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

	verboseGraph := payload.GetByPath("verbose").AsBoolDefault(true)
	maxDepth := uint(payload.GetByPath("depth").AsNumericDefault(math.MaxUint8))

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

	if verboseGraph {
		graph.SetRankSeparator(5)
	}

	nodes := make(map[string]*cgraph.Node)

	root := node{
		id: self.ID,
	}

	queue := list.New()
	queue.PushBack(root)
	queue.PushBack(nil)

	currentDepth := uint(0)
	for queue.Len() > 0 {
		e := queue.Front()
		node, ok := e.Value.(node)

		queue.Remove(e)

		if !ok {
			currentDepth++
			queue.PushBack(nil)

			if queue.Front().Value == nil {
				break
			}

			continue
		}

		elem, exists := nodes[node.id]
		if !exists {
			newElem, err := createNode(graph, node.id)
			if err != nil {
				continue
			}

			nodes[node.id] = newElem
			elem = newElem
		}

		if verboseGraph {
			addParents(contextProcessor, graph, nodes, elem)
		}

		if node.parent != "" && node.lt != "" {
			if _, err := createEdge(graph, node.lt, nodes[node.parent], elem); err != nil {
				continue
			}
		}

		if currentDepth < maxDepth {
			for _, n := range getChildren(contextProcessor, node.id) {
				if _, ok := nodes[n.id]; !ok {
					queue.PushBack(n)
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

func getParents(ctx *sfplugins.StatefunContextProcessor, id string) []node {
	pattern := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff1Pattern, id, ">")
	parents := ctx.GlobalCache.GetKeysByPattern(pattern)

	nodes := make([]node, 0, len(parents))

	for _, v := range parents {
		split := strings.Split(v, ".")
		if len(split) == 0 {
			continue
		}

		nodes = append(nodes, node{
			parent: id,
			id:     split[len(split)-2],
			lt:     split[len(split)-1],
		})
	}

	return nodes
}

func getChildren(ctx *sfplugins.StatefunContextProcessor, id string) []node {
	children := ctx.GlobalCache.GetKeysByPattern(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff1Pattern, id, ">"))

	nodes := make([]node, 0, len(children))

	for _, v := range children {
		split := strings.Split(v, ".")
		if len(split) == 0 {
			continue
		}

		nodes = append(nodes, node{
			parent: id,
			id:     split[len(split)-1],
			lt:     split[len(split)-2],
		})
	}

	return nodes
}

func createNode(graph *cgraph.Graph, name string) (*cgraph.Node, error) {
	node, err := graph.CreateNode(name)
	if err != nil {
		return nil, err
	}

	node.SetHeight(1)
	node.SetArea(1)
	node.SetShape(cgraph.BoxShape)
	node.SetFontSize(24)

	return node, nil
}

func createEdge(graph *cgraph.Graph, name string, start, end *cgraph.Node) (*cgraph.Edge, error) {
	edge, err := graph.CreateEdge(name, start, end)
	if err != nil {
		return nil, err
	}

	edge.SetLabel(name)
	edge.SetFontSize(18)

	return edge, nil
}

func addParents(ctx *sfplugins.StatefunContextProcessor, g *cgraph.Graph, allNodes map[string]*cgraph.Node, root *cgraph.Node) {
	rootID := root.Name()

	for _, parentNode := range getParents(ctx, rootID) {
		parent, exists := allNodes[parentNode.id]
		if !exists {
			newElem, err := createNode(g, parentNode.id)
			if err != nil {
				continue
			}

			allNodes[parentNode.id] = newElem
			parent = newElem
		}

		if _, err := createEdge(g, parentNode.lt, parent, root); err != nil {
			continue
		}
	}
}
