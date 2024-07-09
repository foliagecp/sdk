package debug

import (
	"encoding/xml"
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type GraphML struct {
	XMLName xml.Name `xml:"graphml"`
	Xmlns   string   `xml:"xmlns,attr"`
	Keys    []Key    `xml:"key"`
	Graph   Graph    `xml:"graph"`
}

type Key struct {
	XMLName  xml.Name `xml:"key"`
	Id       string   `xml:"id,attr"`
	For      string   `xml:"for,attr"`
	AttrName string   `xml:"attr.name,attr"`
	AttrType string   `xml:"attr.type,attr"`
}

type Graph struct {
	XMLName     xml.Name `xml:"graph"`
	Id          string   `xml:"id,attr"`
	Edgedefault string   `xml:"edgedefault,attr"`
	Nodes       []Node   `xml:"node"`
	Edges       []Edge   `xml:"edge"`
}

type Node struct {
	XMLName    xml.Name    `xml:"node"`
	Id         string      `xml:"id,attr"`
	Attributes []Attribute `xml:"data"`
}

type Edge struct {
	XMLName    xml.Name    `xml:"edge"`
	Source     string      `xml:"source,attr"`
	Target     string      `xml:"target,attr"`
	Attributes []Attribute `xml:"data"`
}

type Attribute struct {
	XMLName xml.Name    `xml:"data"`
	Key     string      `xml:"key,attr"`
	Data    interface{} `xml:",innerxml"`
}

var (
	predefinedAttributesTypes = []Key{
		{Id: "d0", For: "all", AttrName: "body", AttrType: "string"},
		{Id: "d1", For: "edge", AttrName: "name", AttrType: "string"},
		{Id: "d2", For: "edge", AttrName: "type", AttrType: "string"},
		{Id: "d3", For: "edge", AttrName: "tag", AttrType: "string"},
	}
)

func exportToGraphMLString(nodes []Node, edges []Edge) (string, error) {
	graphml := GraphML{
		Xmlns: "http://graphml.graphdrawing.org/xmlns",
		Keys:  predefinedAttributesTypes,
		Graph: Graph{
			Id:          "G",
			Edgedefault: "directed",
			Nodes:       nodes,
			Edges:       edges,
		},
	}

	output, err := xml.MarshalIndent(graphml, "", "  ")
	if err != nil {
		return "", err
	}

	xmlHeader := []byte(xml.Header)
	output = append(xmlHeader, output...)

	return string(output), nil
}

type Element struct {
	XMLName xml.Name
	Type    string    `xml:"type,attr,omitempty"`
	Content string    `xml:",chardata"`
	Items   []Element `xml:",any"`
}

func ConvertToXML(name string, value interface{}) []Element {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case map[string]interface{}:
		element := Element{XMLName: xml.Name{Local: name}}
		for key, val := range v {
			childElements := ConvertToXML(key, val)
			element.Items = append(element.Items, childElements...)
		}
		return []Element{element}
	case []interface{}:
		elements := []Element{}
		for _, val := range v {
			childElements := ConvertToXML(name, val)
			elements = append(elements, childElements...)
		}
		return elements
	case string:
		return []Element{{XMLName: xml.Name{Local: name}, Type: "string", Content: v}}
	case float64:
		return []Element{{XMLName: xml.Name{Local: name}, Type: "number", Content: fmt.Sprintf("%v", v)}}
	case bool:
		return []Element{{XMLName: xml.Name{Local: name}, Type: "boolean", Content: fmt.Sprintf("%v", v)}}
	default:
		return []Element{{XMLName: xml.Name{Local: name}, Type: "unknown", Content: fmt.Sprintf("%v", v)}}
	}
}

func createGraphML(sourceVertex string, domain sfPlugins.Domain, nodes map[string]*easyjson.JSON, edges []gEdge) string {
	outNodes := []Node{}
	for id, body := range nodes {
		outNode := Node{
			Id:         id,
			Attributes: []Attribute{},
		}
		if body.IsNonEmptyObject() {
			if b, ok := body.AsObject(); ok {
				outNode.Attributes = append(outNode.Attributes, Attribute{Key: "d0", Data: ConvertToXML("body", b)})
			}
		}
		outNodes = append(outNodes, outNode)
	}
	outEdges := []Edge{}
	for _, edge := range edges {
		outEdge := Edge{
			Source: edge.from,
			Target: edge.to,
			Attributes: []Attribute{
				{Key: "d1", Data: edge.name},
				{Key: "d2", Data: edge.tp},
			},
		}
		if edge.body.IsNonEmptyObject() {
			if b, ok := edge.body.AsObject(); ok {
				outEdge.Attributes = append(outEdge.Attributes, Attribute{Key: "d0", Data: ConvertToXML("body", b)})
			}
		}
		for _, tag := range edge.tags {
			outEdge.Attributes = append(outEdge.Attributes, Attribute{Key: "d3", Data: tag})
		}
		outEdges = append(outEdges, outEdge)
	}
	s, err := exportToGraphMLString(outNodes, outEdges)
	if err != nil {
		logger.Logf(logger.ErrorLevel, "createGraphML error: %s\n", err.Error())
	}
	return s
}
