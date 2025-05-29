package debug

import (
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/foliagecp/easyjson"
)

type RawAttributeInnerXML struct {
	XMLName xml.Name     `xml:"data"`
	Key     string       `xml:"key,attr"`
	Data    xml.CharData `xml:",innerxml"`
}

type RawNode struct {
	XMLName    xml.Name               `xml:"node"`
	Id         string                 `xml:"id,attr"`
	Attributes []RawAttributeInnerXML `xml:"data"`
}

type RawEdge struct {
	XMLName    xml.Name               `xml:"edge"`
	Source     string                 `xml:"source,attr"`
	Target     string                 `xml:"target,attr"`
	Attributes []RawAttributeInnerXML `xml:"data"`
}

type RawGraph struct {
	XMLName     xml.Name  `xml:"graph"`
	Id          string    `xml:"id,attr"`
	Edgedefault string    `xml:"edgedefault,attr"`
	Nodes       []RawNode `xml:"node"`
	Edges       []RawEdge `xml:"edge"`
}

type RawGraphML struct {
	XMLName xml.Name `xml:"graphml"`
	Graph   RawGraph `xml:"graph"`
}

func ImportGraphML(r io.Reader) (RawGraph, error) {
	var rawGraphML RawGraphML
	if err := xml.NewDecoder(r).Decode(&rawGraphML); err != nil {
		return RawGraph{}, fmt.Errorf("XML decode error: %w", err)
	}

	return rawGraphML.Graph, nil
}

func parseTypedValue(typ, raw string) interface{} {
	switch typ {
	case "number":
		var f float64
		fmt.Sscanf(raw, "%f", &f)
		return f
	case "boolean":
		return raw == "true"
	default:
		return raw
	}
}

func convertElementsToMap(elements []Element) interface{} {
	result := map[string]interface{}{}

	for _, el := range elements {
		if len(el.Items) > 0 {
			child := convertElementsToMap(el.Items)

			if existing, exists := result[el.Key]; exists {
				switch existing := existing.(type) {
				case []interface{}:
					result[el.Key] = append(existing, child)
				default:
					result[el.Key] = []interface{}{existing, child}
				}
			} else {
				result[el.Key] = child
			}
		} else {
			value := parseTypedValue(el.Type, el.Content)

			if existing, exists := result[el.Key]; exists {
				switch existing := existing.(type) {
				case []interface{}:
					result[el.Key] = append(existing, value)
				default:
					result[el.Key] = []interface{}{existing, value}
				}
			} else {
				result[el.Key] = value
			}
		}
	}

	return result
}

func ExtractBodyAsJSON(attrs []RawAttributeInnerXML) (easyjson.JSON, error) {
	var bdjRaw string
	var bdxRaw string

	for _, attr := range attrs {
		switch attr.Key {
		case BODY_JSON_ATTR_ID:
			bdjRaw = strings.TrimSpace(string(attr.Data))
			bdjRaw = html.UnescapeString(bdjRaw)
		case BODY_XML_ATTR_ID:
			bdxRaw = strings.TrimSpace(string(attr.Data))
		}
	}

	// Попробуем JSON-строку
	if bdjRaw != "" {
		if js, ok := easyjson.JSONFromString(bdjRaw); ok {
			return js, nil
		}
	}

	// Попробуем XML-декодинг тела
	if bdxRaw != "" {
		var elements []Element
		if err := xml.Unmarshal([]byte(bdxRaw), &elements); err == nil {
			root := convertElementsToMap(elements)
			if m, ok := root.(map[string]interface{}); ok {
				js := easyjson.NewJSON(m)
				return js, nil
			}
		}
	}

	return easyjson.NewJSONObject(), fmt.Errorf("no valid body found")
}

func ExtractEdgeTypeAndNameAndTags(attrs []RawAttributeInnerXML) (tp string, name string, tags []string) {
	for _, attr := range attrs {
		val := strings.TrimSpace(string(attr.Data))

		switch attr.Key {
		case LINK_TYPE_STRING_ATTR_ID:
			tp = val
		case LINK_NAME_STRING_ATTR_ID:
			name = val
		case LINK_TAG_STRING_ATTR_ID:
			if val != "" {
				tags = append(tags, val)
			}
		}
	}

	return
}
