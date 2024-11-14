package debug

import (
	"encoding/xml"
	"fmt"
	"sort"

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
	XMLName    xml.Name      `xml:"node"`
	Id         string        `xml:"id,attr"`
	Attributes []interface{} `xml:"data"`
}

type Edge struct {
	XMLName    xml.Name      `xml:"edge"`
	Source     string        `xml:"source,attr"`
	Target     string        `xml:"target,attr"`
	Attributes []interface{} `xml:"data"`
}

type AttributeInnerXML struct {
	XMLName xml.Name    `xml:"data"`
	Key     string      `xml:"key,attr"`
	Data    interface{} `xml:",innerxml"`
}

type AttributeCharData struct {
	XMLName xml.Name    `xml:"data"`
	Key     string      `xml:"key,attr"`
	Data    interface{} `xml:",chardata"`
}

const (
	BODY_JSON_ATTR_ID        = "bdj"
	BODY_XML_ATTR_ID         = "bdx"
	LINK_NAME_STRING_ATTR_ID = "nms"
	LINK_TYPE_STRING_ATTR_ID = "tps"
	LINK_TAG_STRING_ATTR_ID  = "tgs"
)

var (
	predefinedAttributesTypes = []Key{
		{Id: BODY_JSON_ATTR_ID, For: "all", AttrName: "body", AttrType: "string"},
		{Id: BODY_XML_ATTR_ID, For: "all", AttrName: "body", AttrType: "xml"},
		{Id: LINK_NAME_STRING_ATTR_ID, For: "edge", AttrName: "name", AttrType: "string"},
		{Id: LINK_TYPE_STRING_ATTR_ID, For: "edge", AttrName: "type", AttrType: "string"},
		{Id: LINK_TAG_STRING_ATTR_ID, For: "edge", AttrName: "tag", AttrType: "string"},
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
	XMLName xml.Name  `xml:"v"`
	Key     string    `xml:"key,attr"`
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
		element := Element{Key: name}
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
		return []Element{{Key: name, Type: "string", Content: v}}
	case float64:
		return []Element{{Key: name, Type: "number", Content: fmt.Sprintf("%v", v)}}
	case bool:
		return []Element{{Key: name, Type: "boolean", Content: fmt.Sprintf("%v", v)}}
	default:
		return []Element{{Key: name, Type: "unknown", Content: fmt.Sprintf("%v", v)}}
	}
}

func createGraphML(sourceVertex string, domain sfPlugins.Domain, nodes map[string]*easyjson.JSON, edges []gEdge, json2xml bool) string {
	outNodes := []Node{}
	nodeIds := make([]string, 0, len(nodes))
	for nodeId := range nodes {
		nodeIds = append(nodeIds, nodeId)
	}
	sort.Strings(nodeIds)
	for _, nodeId := range nodeIds {
		body := nodes[nodeId]
		outNode := Node{
			Id:         nodeId,
			Attributes: []interface{}{},
		}
		if body.IsNonEmptyObject() {
			if json2xml {
				if b, ok := body.AsObject(); ok {
					outNode.Attributes = append(outNode.Attributes, AttributeInnerXML{Key: BODY_XML_ATTR_ID, Data: ConvertToXML("body", b)})
				}
			} else {
				outNode.Attributes = append(outNode.Attributes, AttributeCharData{Key: BODY_JSON_ATTR_ID, Data: body.ToString()})
			}
		}
		outNodes = append(outNodes, outNode)
	}

	edgesMap := map[string]int{}
	for edgeNum, edge := range edges {
		edgesMap[edge.from+"."+edge.to] = edgeNum
	}
	edgeIds := make([]string, 0, len(edgesMap))
	for edgeId := range edgesMap {
		edgeIds = append(edgeIds, edgeId)
	}
	sort.Strings(edgeIds)

	outEdges := []Edge{}
	for _, edgeId := range edgeIds {
		edge := edges[edgesMap[edgeId]]
		outEdge := Edge{
			Source: edge.from,
			Target: edge.to,
			Attributes: []interface{}{
				AttributeInnerXML{Key: LINK_NAME_STRING_ATTR_ID, Data: edge.name},
				AttributeInnerXML{Key: LINK_TYPE_STRING_ATTR_ID, Data: edge.tp},
			},
		}
		if edge.body.IsNonEmptyObject() {
			if json2xml {
				if b, ok := edge.body.AsObject(); ok {
					outEdge.Attributes = append(outEdge.Attributes, AttributeInnerXML{Key: BODY_XML_ATTR_ID, Data: ConvertToXML("body", b)})
				}
			} else {
				outEdge.Attributes = append(outEdge.Attributes, AttributeCharData{Key: BODY_JSON_ATTR_ID, Data: edge.body.ToString()})
			}
		}
		for _, tag := range edge.tags {
			outEdge.Attributes = append(outEdge.Attributes, AttributeInnerXML{Key: LINK_TAG_STRING_ATTR_ID, Data: tag})
		}
		outEdges = append(outEdges, outEdge)
	}
	s, err := exportToGraphMLString(outNodes, outEdges)
	if err != nil {
		logger.Logf(logger.ErrorLevel, "createGraphML error: %s", err.Error())
	}
	return s
}
