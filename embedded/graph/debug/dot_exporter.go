package debug

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/emicklei/dot"
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func createGraphViz(sourceVertex string, domain sfPlugins.Domain, nodes map[string]*easyjson.JSON, edges []gEdge) string {
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

func createNode(dotGraph *dot.Graph, name string, colorStr string) (*dot.Node, error) {
	node := dotGraph.Node(name).Box()
	node.Attr("color", fmt.Sprintf("#%s", colorStr))

	return &node, nil
}

func createEdge(dotGraph *dot.Graph, e gEdge, start, end *dot.Node) (*dot.Edge, error) {
	edgeLabel := fmt.Sprintf("%s\ntype:%s\ntags:%v", e.name, e.tp, e.tags)
	edge := dotGraph.Edge(*start, *end, edgeLabel)

	return &edge, nil
}
