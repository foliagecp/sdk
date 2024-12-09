

package jpgql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/foliagecp/sdk/embedded/graph/crud"

	"github.com/PaesslerAG/gval"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
)

const QueryResultTopic = "functions.graph.query"

var jsonPathPartsExtractRegexp *regexp.Regexp = regexp.MustCompile(`\.[*a-zA-Z0-9/_-]*(\[\]|\[([^[]+]*|.*?\[.*?\].*?)\]|("(?:.|[\n])+))?`)
var filterParseLanguage = gval.NewLanguage(gval.Base(), gval.PropositionalLogic(),
	gval.InfixOperator("||", func(a, b interface{}) (interface{}, error) {
		filterA := a.(*FilterData)
		filterB := b.(*FilterData)
		filterA.disjunctiveNormalFormOfFeatures = append(filterA.disjunctiveNormalFormOfFeatures, filterB.disjunctiveNormalFormOfFeatures...)
		return filterA, nil
	}),
	gval.InfixOperator("&&", func(a, b interface{}) (interface{}, error) {
		filterA := a.(*FilterData)
		filterB := b.(*FilterData)
		for _, tagsB := range filterB.disjunctiveNormalFormOfFeatures {
			for i := 0; i < len(filterA.disjunctiveNormalFormOfFeatures); i++ {
				filterA.disjunctiveNormalFormOfFeatures[i] = append(filterA.disjunctiveNormalFormOfFeatures[i], tagsB...)
			}
		}
		return filterA, nil
	}),
	gval.Function("l_tags", func(args ...interface{}) (interface{}, error) {
		if len(args) == 0 {
			return nil, fmt.Errorf("at least one tag must be declared")
		}
		tagFeatures := []filterFeature{}
		for _, arg := range args {
			tagFeatures = append(tagFeatures, filterFeature{"l_tag", map[string]string{"idx": arg.(string)}})
		}
		return NewFilterDataWithConjunctionFeatures(tagFeatures), nil
	}),
	gval.Function("l_type", func(args ...interface{}) (interface{}, error) {
		if len(args) > 1 {
			return nil, fmt.Errorf("multiple types are not permitted")
		}
		if len(args) < 1 {
			return nil, fmt.Errorf("name must be declared")
		}
		t := args[0].(string)
		if len(t) == 0 {
			return nil, fmt.Errorf("name must not be empty")
		}
		return NewFilterDataWithOneFeature(filterFeature{"l_type", map[string]string{"idx": t}}), nil
	}),
	gval.Function("v_has", func(args ...interface{}) (interface{}, error) { // vertex body filter
		if len(args) != 4 {
			return nil, fmt.Errorf("required args are: key; value type; target value; operation;")
		}
		value := map[string]string{
			"key":          args[0].(string),
			"value_type":   args[1].(string), // "numeric", "string", "bool"
			"operation":    args[2].(string), // "==", "!=", ">", "<"
			"target_value": args[3].(string),
		}
		return NewFilterDataWithOneFeature(filterFeature{"v_has", value}), nil
	}),
	gval.Function("l_has", func(args ...interface{}) (interface{}, error) { // link body filter
		if len(args) != 4 {
			return nil, fmt.Errorf("required args are: key; value type; target value; operation;")
		}
		value := map[string]string{
			"key":          args[0].(string),
			"value_type":   args[1].(string), // "numeric", "string", "bool"
			"operation":    args[2].(string), // "==", "!=", ">", "<"
			"target_value": args[3].(string),
		}
		return NewFilterDataWithOneFeature(filterFeature{"l_has", value}), nil
	}),
)

type filterFeature struct {
	name  string
	value map[string]string
}

type FilterData struct {
	disjunctiveNormalFormOfFeatures [][]filterFeature // [[tag:tag1, tag:tag2], [tag:tag3, name:link001]] == tag:tag1 && tag:tag2 || tag:tag3 && name:link001
}

type AnyDepthStop struct {
	LinkName    string
	FilterQeury string
	QueryTail   string
}

func NewFilterDataWithConjunctionFeatures(conjunctionFeatures []filterFeature) *FilterData {
	filterData := &FilterData{}
	filterData.disjunctiveNormalFormOfFeatures = [][]filterFeature{conjunctionFeatures}
	return filterData
}

func NewFilterDataWithOneFeature(feature filterFeature) *FilterData {
	filterData := &FilterData{}
	filterData.disjunctiveNormalFormOfFeatures = [][]filterFeature{{feature}}
	return filterData
}

func ParseFilter(filterQuery string) (*FilterData, error) {
	filterQuery = strings.ReplaceAll(filterQuery, `'`, `"`) // Allow to use single quotes
	filterQuery = strings.ReplaceAll(filterQuery, `:`, `_`) // Allow to use colon
	value, err := filterParseLanguage.Evaluate(filterQuery, nil)
	if err != nil {
		return nil, err
	}
	filterData, ok := value.(*FilterData)
	if !ok {
		return nil, fmt.Errorf("parseFilter error: cannot parse filterData")
	}
	return filterData, nil
}

func GetQueryHeadAndTailsParts(query string) (string, string, string, *AnyDepthStop, error) {
	if query[:1] != "." {
		return "", "", "", nil, fmt.Errorf(`getQueryHeadAndTailsParts error: query must start from ".", query="%s"`, query)
	}
	if len(query) == 1 {
		return "", "", "", nil, nil
	}
	var anyDepthStop *AnyDepthStop = nil
	if query[:2] == ".." {
		anyDepthStop = &AnyDepthStop{"", "", ""}
		query = query[1:]
	}

	queryHeadFilter := ""
	res := jsonPathPartsExtractRegexp.FindAllStringSubmatch(query, 1)
	queryWithoutFilters := query
	if len(res) > 0 && len(res[0]) > 1 {
		queryWithoutFilters = strings.Replace(queryWithoutFilters, res[0][1], "", 1)
		queryHeadFilter = res[0][2]
	}
	queryHeadLinkName := strings.Split(queryWithoutFilters[1:], ".")[0]
	queryTail := strings.Replace(queryWithoutFilters, "."+queryHeadLinkName, "", 1)
	if anyDepthStop != nil {
		anyDepthStop.LinkName = queryHeadLinkName
		anyDepthStop.FilterQeury = queryHeadFilter
		anyDepthStop.QueryTail = queryTail
		queryHeadLinkName = "*"
		queryHeadFilter = ""
		queryTail = "." + query
	}
	return queryHeadLinkName, queryHeadFilter, queryTail, anyDepthStop, nil
}

func GetLinkNamesFromJPGQLLinkName(cacheStore *cache.Store, sourceId string, jpgqlLinkName string) []string {
	result := []string{}
	for _, key := range cacheStore.GetKeysByPattern(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, sourceId, jpgqlLinkName)) {
		keyTokens := strings.Split(key, ".")
		if len(keyTokens) != 4 {
			break
		}
		result = append(result, keyTokens[3])
	}
	return result
}

func GetSpecificLinkIndices(cacheStore *cache.Store, fromObjectID string, linkName string) map[string]struct{} { // Returns map which contains [<indexName>.<indexValue>, <indexName1>.<indexValue1>, ...]
	resultIndices := map[string]struct{}{}

	linksQuery := fmt.Sprintf(crud.OutLinkIndexPrefPattern+crud.KeySuff2Pattern, fromObjectID, linkName, ">")
	// Get all links matching defined link type ---------------------------
	for _, key := range cacheStore.GetKeysByPattern(linksQuery) {
		linkKeyTokens := strings.Split(key, ".")
		if len(linkKeyTokens) != 6 {
			return resultIndices
		}
		indexName := linkKeyTokens[len(linkKeyTokens)-2]
		indexValue := linkKeyTokens[len(linkKeyTokens)-1]
		resultIndices[indexName+"."+indexValue] = struct{}{}
	}
	// --------------------------------------------------------------------

	return resultIndices
}

func IsVertexBodyHasIndexValue(cacheStore *cache.Store, vertexId, key, valueType, operation, targetValue string) bool {
	typeStr := strings.ToLower(valueType)[:1]
	indexKeys := cacheStore.GetKeysByPattern(fmt.Sprintf(crud.VertexBodyValueIndexPrefPattern+crud.KeySuff2Pattern, vertexId, typeStr, key))
	return IsIndexedKeyMeetsRequirements(cacheStore, indexKeys, typeStr, operation, targetValue)
}

func IsLinkBodyHasIndexValue(cacheStore *cache.Store, fromVertexId, linkName, key, valueType, operation, targetValue string) bool {
	typeStr := strings.ToLower(valueType)[:1]
	indexKeys := cacheStore.GetKeysByPattern(fmt.Sprintf(crud.LinkBodyValueIndexPrefPattern+crud.KeySuff3Pattern, fromVertexId, linkName, typeStr, key))
	return IsIndexedKeyMeetsRequirements(cacheStore, indexKeys, typeStr, operation, targetValue)
}

func IsIndexedKeyMeetsRequirements(cacheStore *cache.Store, indexKeys []string, typeStr, operation, targetValue string) bool {
	for _, indexKey := range indexKeys {
		if v, err := cacheStore.GetValue(indexKey); err == nil {
			switch typeStr {
			case "b":
				valBool := system.BytesToBool(v)
				targetValBool := system.Str2Bool(targetValue)
				switch operation {
				case "==":
					return valBool == targetValBool
				case "!=":
					fallthrough
				case "<":
					fallthrough
				case ">":
					return valBool != targetValBool
				}
			case "n":
				valNumeric := system.BytesToFloat64(v)
				targetValNumeric := system.StringToFloat(targetValue)
				switch operation {
				case "==":
					return valNumeric == targetValNumeric
				case "!=":
					return valNumeric != targetValNumeric
				case "<":
					return valNumeric < targetValNumeric
				case ">":
					return valNumeric > targetValNumeric
				}
			case "s":
				valString := string(v)
				switch operation {
				case "==":
					return valString == targetValue
				case "!=":
					return valString != targetValue
				case "<":
					return strings.Contains(targetValue, valString)
				case ">":
					return strings.Contains(valString, targetValue)
				}
			}
		}
	}
	return false
}

func IsLinkSatifiesFilterCreteria(cacheStore *cache.Store, fromVertexId string, toVertexId string, linkName string, linkFilterQuery string) bool {
	if len(linkFilterQuery) == 0 {
		return true
	}
	if filterData, err := ParseFilter(linkFilterQuery); err == nil {
		if len(filterData.disjunctiveNormalFormOfFeatures) == 0 {
			return true
		}
		linkIndicesMap := GetSpecificLinkIndices(cacheStore, fromVertexId, linkName)
		for _, features := range filterData.disjunctiveNormalFormOfFeatures {
			featuresFromDisjunctionFound := true
			for _, feature := range features {
				tokens := strings.Split(feature.name, "_")
				if len(tokens) == 2 {
					if tokens[0] == "l" {
						if tokens[1] == "has" {
							if !IsLinkBodyHasIndexValue(cacheStore, fromVertexId, linkName, feature.value["key"], feature.value["value_type"], feature.value["operation"], feature.value["target_value"]) {
								featuresFromDisjunctionFound = false
								break
							}
						} else {
							if _, ok := linkIndicesMap[tokens[1]+"."+feature.value["idx"]]; !ok {
								featuresFromDisjunctionFound = false
								break
							}
						}
					}
					if tokens[0] == "v" {
						if tokens[1] == "has" {
							if !IsVertexBodyHasIndexValue(cacheStore, toVertexId, feature.value["key"], feature.value["value_type"], feature.value["operation"], feature.value["target_value"]) {
								featuresFromDisjunctionFound = false
								break
							}
						}
					}
				}
			}
			if featuresFromDisjunctionFound {
				return true
			}
		}
	}
	return false
}

func GetTargetIdFromSourceIdAndLinkName(cacheStore *cache.Store, sourceId string, linkName string) string {
	linkTargetBytes, err := cacheStore.GetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, sourceId, linkName))
	if err != nil {
		return ""
	}
	linkTargetStr := string(linkTargetBytes)
	linkTargetTokens := strings.Split(linkTargetStr, ".")
	if len(linkTargetTokens) != 2 {
		return ""
	}
	return linkTargetTokens[1]
}

func GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(cacheStore *cache.Store, sourceId string, jpgqlLinkName string, linkFilterQuery string) map[string]bool {
	result := map[string]bool{}
	if len(jpgqlLinkName) == 0 {
		return result
	}

	for _, linkName := range GetLinkNamesFromJPGQLLinkName(cacheStore, sourceId, jpgqlLinkName) {
		targetId := GetTargetIdFromSourceIdAndLinkName(cacheStore, sourceId, linkName)
		if IsLinkSatifiesFilterCreteria(cacheStore, sourceId, targetId, linkName, linkFilterQuery) {
			// Is targetSatisfies
			if len(targetId) > 0 {
				result[targetId] = false
			}
		}
	}

	return result
}

func GetObjectIDsFromLinkNameAndLinkFilterQueryWithAnyDepthStop(cacheStore *cache.Store, sourceId string, jpgqlLinkName string, linkFilterQuery string, anyDepthStop *AnyDepthStop) map[string]bool {
	resultObjects := GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(cacheStore, sourceId, jpgqlLinkName, linkFilterQuery)

	if anyDepthStop != nil {
		anyDepthStopResultObjects := GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(cacheStore, sourceId, anyDepthStop.LinkName, anyDepthStop.FilterQeury)
		for linkObjectID := range anyDepthStopResultObjects {
			resultObjects[linkObjectID] = true
		}
	}

	return resultObjects
}
