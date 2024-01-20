// Copyright 2023 NJWS Inc.

package jpgql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/foliagecp/sdk/embedded/graph/crud"

	"github.com/PaesslerAG/gval"
	"github.com/foliagecp/sdk/statefun/cache"
)

const QueryResultTopic = "functions.graph.query"

var jsonPathPartsExtractRegexp *regexp.Regexp = regexp.MustCompile(`\.[*a-zA-Z0-9_-]*(\[\]|\[([^[]+]*|.*?\[.*?\].*?)\]|("(?:.|[\n])+))?`)
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
	gval.Function("tags", func(args ...interface{}) (interface{}, error) {
		if len(args) == 0 {
			return nil, fmt.Errorf("at least one tag must be declared")
		}
		tagFeatures := []filterFeature{}
		for _, arg := range args {
			tagFeatures = append(tagFeatures, filterFeature{"tag", arg.(string)})
		}
		return NewFilterDataWithConjunctionFeatures(tagFeatures), nil
	}),
	gval.Function("name", func(args ...interface{}) (interface{}, error) {
		if len(args) > 1 {
			return nil, fmt.Errorf("multiple names are not permitted")
		}
		if len(args) < 1 {
			return nil, fmt.Errorf("name must be declared")
		}
		name := args[0].(string)
		if len(name) == 0 {
			return nil, fmt.Errorf("name must not be empty")
		}
		return NewFilterDataWithOneFeature(filterFeature{"name", name}), nil
	}),
)

type filterFeature struct {
	name  string
	value string
}

type FilterData struct {
	disjunctiveNormalFormOfFeatures [][]filterFeature // [[tag:tag1, tag:tag2], [tag:tag3, name:link001]] == tag:tag1 && tag:tag2 || tag:tag3 && name:link001
}

type AnyDepthStop struct {
	LinkType    string
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
	queryHeadLinkType := strings.Split(queryWithoutFilters[1:], ".")[0]
	queryTail := strings.Replace(queryWithoutFilters, "."+queryHeadLinkType, "", 1)
	if anyDepthStop != nil {
		anyDepthStop.LinkType = queryHeadLinkType
		anyDepthStop.FilterQeury = queryHeadFilter
		anyDepthStop.QueryTail = queryTail
		queryHeadLinkType = "*"
		queryHeadFilter = ""
		queryTail = "." + query
	}
	return queryHeadLinkType, queryHeadFilter, queryTail, anyDepthStop, nil
}

func GetObjectIDsFromLinkType(cacheStore *cache.Store, objectID string, linkType string) map[string]int {
	resultObjects := map[string]int{}

	if len(linkType) == 0 { // No link type - return object itself
		resultObjects[objectID] = 0
		return resultObjects
	}

	linksQuery := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff1Pattern, objectID, ">")
	if linkType != "*" {
		linksQuery = fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, objectID, linkType, ">")
	}
	// Get all links matching defined link type ---------------------------
	for _, key := range cacheStore.GetKeysByPattern(linksQuery) {
		linkKeyTokens := strings.Split(key, ".")
		targetObjectID := linkKeyTokens[len(linkKeyTokens)-1]
		resultObjects[targetObjectID] = 0
	}
	// --------------------------------------------------------------------

	return resultObjects
}

func GetAllLinksFromSpecifiedLinkType(cacheStore *cache.Store, objectID string, linkType string) [][]string { // Returns pairs [["type1", "toObjectId1"], ["type2", "toObjectId2"], ...]
	resultPairs := [][]string{}

	if len(linkType) == 0 { // No link type - return object itself
		return resultPairs
	}

	linksQuery := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff1Pattern, objectID, ">")
	if linkType != "*" {
		linksQuery = fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, objectID, linkType, ">")
	}
	// Get all links matching defined link type ---------------------------
	for _, key := range cacheStore.GetKeysByPattern(linksQuery) {
		linkKeyTokens := strings.Split(key, ".")
		linkTypeToObject := linkKeyTokens[len(linkKeyTokens)-2]
		targetObjectID := linkKeyTokens[len(linkKeyTokens)-1]
		pair := []string{linkTypeToObject, targetObjectID}
		resultPairs = append(resultPairs, pair)
	}
	// --------------------------------------------------------------------

	return resultPairs
}

func GetSpecificLinkIndices(cacheStore *cache.Store, fromObjectID string, linkType string, toObjectId string) map[string]struct{} { // Returns map which contains [<indexName>.<indexValue>, <indexName1>.<indexValue1>, ...]
	resultIndices := map[string]struct{}{}

	if len(linkType) == 0 { // No link type - return object itself
		return resultIndices
	}

	linksQuery := fmt.Sprintf(crud.OutLinkIndexPrefPattern+crud.LinkKeySuff3Pattern, fromObjectID, linkType, toObjectId, ">")
	// Get all links matching defined link type ---------------------------
	for _, key := range cacheStore.GetKeysByPattern(linksQuery) {
		linkKeyTokens := strings.Split(key, ".")
		indexName := linkKeyTokens[len(linkKeyTokens)-2]
		indexValue := linkKeyTokens[len(linkKeyTokens)-1]
		resultIndices[indexName+"."+indexValue] = struct{}{}
	}
	// --------------------------------------------------------------------

	return resultIndices
}

func GetObjectIDsFromLinkTypeAndFilterData(cacheStore *cache.Store, objectID string, linkType string, filterData *FilterData) map[string]int {
	if len(filterData.disjunctiveNormalFormOfFeatures) == 0 {
		return GetObjectIDsFromLinkType(cacheStore, objectID, linkType)
	}
	resultObjects := map[string]int{}
	linkTypeObjectIdPairs := GetAllLinksFromSpecifiedLinkType(cacheStore, objectID, linkType)
	for _, pair := range linkTypeObjectIdPairs {
		realLinkType := pair[0]
		realObjectId := pair[1]
		linkIndicesMap := GetSpecificLinkIndices(cacheStore, objectID, realLinkType, realObjectId)
		if _, added := resultObjects[realObjectId]; !added {
			for _, features := range filterData.disjunctiveNormalFormOfFeatures {
				featuresFromDisjunctionFound := true
				for _, feature := range features {
					if _, ok := linkIndicesMap[feature.name+"."+feature.value]; !ok {
						featuresFromDisjunctionFound = false
						break
					}
				}
				if featuresFromDisjunctionFound {
					resultObjects[realObjectId] = 0
					break // No need to test other disjunctions
				}
			}
		}
	}
	return resultObjects
}

func GetObjectIDsFromLinkTypeAndLinkFilterQuery(cacheStore *cache.Store, objectID string, linkType string, linkFilterQuery string) map[string]int {
	if len(linkFilterQuery) == 0 {
		return GetObjectIDsFromLinkType(cacheStore, objectID, linkType)
	}
	if filterData, err := ParseFilter(linkFilterQuery); err == nil {
		return GetObjectIDsFromLinkTypeAndFilterData(cacheStore, objectID, linkType, filterData)
	}
	return map[string]int{}
}

func GetObjectIDsFromLinkTypeAndLinkFilterQueryWithAnyDepthStop(cacheStore *cache.Store, objectID string, linkType string, linkFilterQuery string, anyDepthStop *AnyDepthStop) map[string]int {
	resultObjects := GetObjectIDsFromLinkTypeAndLinkFilterQuery(cacheStore, objectID, linkType, linkFilterQuery)

	if anyDepthStop != nil {
		anyDepthStopResultObjects := GetObjectIDsFromLinkTypeAndLinkFilterQuery(cacheStore, objectID, anyDepthStop.LinkType, anyDepthStop.FilterQeury)
		for linkObjectID := range anyDepthStopResultObjects {
			resultObjects[linkObjectID] = 1
		}
	}

	return resultObjects
}
