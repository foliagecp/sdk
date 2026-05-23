package jpgql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/foliagecp/easyjson"
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
	gval.Function("v_array_has", func(args ...interface{}) (interface{}, error) { // vertex body array element filter
		if len(args) != 4 {
			return nil, fmt.Errorf("required args are: key; element type; operation; target value;")
		}
		value := map[string]string{
			"key":          args[0].(string),
			"value_type":   args[1].(string), // "numeric", "string", "bool"
			"operation":    args[2].(string), // "==", "!=", ">", "<"
			"target_value": args[3].(string),
		}
		return NewFilterDataWithOneFeature(filterFeature{"v_array_has", value}), nil
	}),
	gval.Function("l_array_has", func(args ...interface{}) (interface{}, error) { // link body array element filter
		if len(args) != 4 {
			return nil, fmt.Errorf("required args are: key; element type; operation; target value;")
		}
		value := map[string]string{
			"key":          args[0].(string),
			"value_type":   args[1].(string), // "numeric", "string", "bool"
			"operation":    args[2].(string), // "==", "!=", ">", "<"
			"target_value": args[3].(string),
		}
		return NewFilterDataWithOneFeature(filterFeature{"l_array_has", value}), nil
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

// replaceColonsOutsideQuotes replaces ':' with '_' only outside of double-quoted strings,
// so that filter function names like v:has become v_has while values like "aa:bb:cc" stay intact.
func replaceColonsOutsideQuotes(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inQuotes := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '"' {
			inQuotes = !inQuotes
			b.WriteByte(ch)
		} else if ch == ':' && !inQuotes {
			b.WriteByte('_')
		} else {
			b.WriteByte(ch)
		}
	}
	return b.String()
}

func ParseFilter(filterQuery string) (*FilterData, error) {
	filterQuery = strings.ReplaceAll(filterQuery, `'`, `"`) // Allow to use single quotes
	filterQuery = replaceColonsOutsideQuotes(filterQuery)   // Replace colons in function names (v:has -> v_has) but not inside quoted values
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

// IsVertexBodyHasIndexValue reports whether the vertex body field `key`
// satisfies the scalar requirement. The name retains "Index" for backward
// API compatibility, but there is no longer a separate body-value index:
// the value is read straight from the (already in-memory, parsed) vertex
// body. The per-vertex index it used to consult never served reverse
// lookups — it was only ever a point check on a known vertex — so reading
// the body directly is equivalent, and avoids the write-time/memory cost of
// maintaining the index. GetValueJSONByPath reads only the requested field
// without cloning the whole body.
func IsVertexBodyHasIndexValue(cacheStore *cache.Store, vertexId, key, valueType, operation, targetValue string) bool {
	typeStr := strings.ToLower(valueType)[:1]
	value, err := cacheStore.GetValueJSONByPath(vertexId, key)
	if err != nil || value == nil {
		return false
	}
	return isFieldValueMeetsRequirements(*value, typeStr, operation, targetValue)
}

// IsLinkBodyHasIndexValue is the link-body counterpart of
// IsVertexBodyHasIndexValue. See that function for why no index is involved.
func IsLinkBodyHasIndexValue(cacheStore *cache.Store, fromVertexId, linkName, key, valueType, operation, targetValue string) bool {
	typeStr := strings.ToLower(valueType)[:1]
	value, err := cacheStore.GetValueJSONByPath(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, fromVertexId, linkName), key)
	if err != nil || value == nil {
		return false
	}
	return isFieldValueMeetsRequirements(*value, typeStr, operation, targetValue)
}

// isFieldValueMeetsRequirements evaluates a scalar comparison against an
// already-resolved field value (no body traversal — the caller passes the
// field directly). A missing field arrives as JSON{nil}, which fails every
// Is* type check below and yields false.
func isFieldValueMeetsRequirements(value easyjson.JSON, typeStr, operation, targetValue string) bool {
	switch typeStr {
	case "b":
		if !value.IsBool() {
			return false
		}
		valBool := value.AsBoolDefault(false)
		targetValBool := system.Str2Bool(targetValue)
		switch operation {
		case "==":
			return valBool == targetValBool
		default:
			return valBool != targetValBool
		}
	case "n":
		if !value.IsNumeric() {
			return false
		}
		valNumeric := value.AsNumericDefault(0)
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
		if !value.IsString() {
			return false
		}
		valString := value.AsStringDefault("")
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
	return false
}

func isArrayElementMeetsRequirements(arr easyjson.JSON, typeStr, operation, targetValue string) bool {
	if !arr.IsArray() {
		return false
	}
	elements, ok := arr.AsArray()
	if !ok {
		return false
	}
	targetNumeric := system.StringToFloat(targetValue)
	targetBool := system.Str2Bool(targetValue)
	for _, elem := range elements {
		elemJSON := easyjson.NewJSON(elem)
		switch typeStr {
		case "b":
			if elemJSON.IsBool() {
				valBool := elemJSON.AsBoolDefault(false)
				switch operation {
				case "==":
					if valBool == targetBool {
						return true
					}
				default:
					if valBool != targetBool {
						return true
					}
				}
			}
		case "n":
			if elemJSON.IsNumeric() {
				valNumeric := elemJSON.AsNumericDefault(0)
				switch operation {
				case "==":
					if valNumeric == targetNumeric {
						return true
					}
				case "!=":
					if valNumeric != targetNumeric {
						return true
					}
				case "<":
					if valNumeric < targetNumeric {
						return true
					}
				case ">":
					if valNumeric > targetNumeric {
						return true
					}
				}
			}
		case "s":
			if elemJSON.IsString() {
				valString := elemJSON.AsStringDefault("")
				switch operation {
				case "==":
					if valString == targetValue {
						return true
					}
				case "!=":
					if valString != targetValue {
						return true
					}
				case "<":
					if strings.Contains(targetValue, valString) {
						return true
					}
				case ">":
					if strings.Contains(valString, targetValue) {
						return true
					}
				}
			}
		}
	}
	return false
}

func IsVertexBodyHasArrayValue(cacheStore *cache.Store, vertexId, key, valueType, operation, targetValue string) bool {
	typeStr := strings.ToLower(valueType)[:1]
	value, err := cacheStore.GetValueJSONByPath(vertexId, key)
	if err != nil || value == nil {
		return false
	}
	return isArrayElementMeetsRequirements(*value, typeStr, operation, targetValue)
}

func IsLinkBodyHasArrayValue(cacheStore *cache.Store, fromVertexId, linkName, key, valueType, operation, targetValue string) bool {
	typeStr := strings.ToLower(valueType)[:1]
	value, err := cacheStore.GetValueJSONByPath(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, fromVertexId, linkName), key)
	if err != nil || value == nil {
		return false
	}
	return isArrayElementMeetsRequirements(*value, typeStr, operation, targetValue)
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
							// TODO: FIX IS NEEDED: toVertexId may live in another domain!!!!! Then nothing can be cheked!!!
							// Probobal solution: toVertexId body' indices must be built not in its own domain, but in the domains of those vertices from which links lead to it
							if !IsVertexBodyHasIndexValue(cacheStore, toVertexId, feature.value["key"], feature.value["value_type"], feature.value["operation"], feature.value["target_value"]) {
								featuresFromDisjunctionFound = false
								break
							}
						}
					}
				}
				if len(tokens) == 3 && tokens[1] == "array" && tokens[2] == "has" {
					if tokens[0] == "v" {
						if !IsVertexBodyHasArrayValue(cacheStore, toVertexId, feature.value["key"], feature.value["value_type"], feature.value["operation"], feature.value["target_value"]) {
							featuresFromDisjunctionFound = false
							break
						}
					}
					if tokens[0] == "l" {
						if !IsLinkBodyHasArrayValue(cacheStore, fromVertexId, linkName, feature.value["key"], feature.value["value_type"], feature.value["operation"], feature.value["target_value"]) {
							featuresFromDisjunctionFound = false
							break
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
