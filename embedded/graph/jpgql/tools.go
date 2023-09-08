

package jpgql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/PaesslerAG/gval"
	"github.com/foliagecp/sdk/statefun/cache"
)

const QueryResultTopic = "functions.graph.query"

var jsonPathPartsExtractRegexp *regexp.Regexp = regexp.MustCompile(`\.[*a-zA-Z0-9_-]*(\[\]|\[([^[]+]*|.*?\[.*?\].*?)\]|("(?:.|[\n])+))?`)
var filterParseLanguage = gval.NewLanguage(gval.Base(), gval.PropositionalLogic(),
	gval.InfixOperator("||", func(a, b interface{}) (interface{}, error) {
		filterA := a.(*FilterData)
		filterB := b.(*FilterData)
		filterA.disjunctiveSlicesOfTags = append(filterA.disjunctiveSlicesOfTags, filterB.disjunctiveSlicesOfTags...)
		return filterA, nil
	}),
	gval.InfixOperator("&&", func(a, b interface{}) (interface{}, error) {
		filterA := a.(*FilterData)
		filterB := b.(*FilterData)
		for _, tagsB := range filterB.disjunctiveSlicesOfTags {
			for i := 0; i < len(filterA.disjunctiveSlicesOfTags); i++ {
				filterA.disjunctiveSlicesOfTags[i] = append(filterA.disjunctiveSlicesOfTags[i], tagsB...)
			}
		}
		return filterA, nil
	}),
	gval.Function("tags", func(args ...interface{}) (interface{}, error) {
		filterData := NewFilterData()
		tags := []string{}
		for _, arg := range args {
			tag2Check := arg.(string)
			tags = append(tags, tag2Check)
		}
		filterData.disjunctiveSlicesOfTags = append(filterData.disjunctiveSlicesOfTags, tags)
		return filterData, nil
	}),
)

type FilterData struct {
	disjunctiveSlicesOfTags [][]string // [["tag1", "tag2"], ["tag3", "tag4"]] == "tag1" && "tag2" || "tag3" && "tag4"
}
type AnyDepthStop struct {
	LinkType    string
	FilterQeury string
	QueryTail   string
}

func NewFilterData() *FilterData {
	filterData := &FilterData{}
	filterData.disjunctiveSlicesOfTags = make([][]string, 0)
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

	linksQuery := objectID + ".out.ltp_oid-bdy.>"
	if linkType != "*" {
		linksQuery = objectID + ".out.ltp_oid-bdy." + linkType + ".>"
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

func GetObjectIDsFromLinkTypeAndTag(cacheStore *cache.Store, objectID string, linkType string, tag string) map[string]int {
	if len(tag) == 0 {
		return GetObjectIDsFromLinkType(cacheStore, objectID, linkType)
	}

	resultObjects := map[string]int{}

	linksQuery := objectID + ".out.tag_ltp_oid-nil." + tag + ".>"
	if linkType != "*" {
		linksQuery = objectID + ".out.tag_ltp_oid-nil." + tag + "." + linkType + ".*"
	}

	// Get all links matching defined link type ---------------------------
	for _, key := range cacheStore.GetKeysByPattern(linksQuery) {
		if tokens := strings.Split(key, "."); len(tokens) == 6 {
			objectID := string(tokens[len(tokens)-1])
			resultObjects[objectID] = 0
		} else {
			fmt.Printf("ERROR getObjectIDsFromLinkTypeAndTag: linksQuery GetKeysByPattern key %s must consist from 6 tokens, but consists from %d\n", key, len(tokens))
		}
	}
	// --------------------------------------------------------------------

	return resultObjects
}

func GetObjectIDsFromLinkTypeAndFilterData(cacheStore *cache.Store, objectID string, linkType string, filterData *FilterData) map[string]int {
	if len(filterData.disjunctiveSlicesOfTags) == 0 {
		return GetObjectIDsFromLinkType(cacheStore, objectID, linkType)
	}
	disjunctionResultObjects := map[string]int{}
	for _, tags := range filterData.disjunctiveSlicesOfTags {
		conjunctionResultObjects := map[string]int{}
		for _, tag := range tags {
			linksWithTypeAngTag := GetObjectIDsFromLinkTypeAndTag(cacheStore, objectID, linkType, tag)
			for linkObjectID := range linksWithTypeAngTag {
				if _, ok := conjunctionResultObjects[linkObjectID]; ok {
					conjunctionResultObjects[linkObjectID] = conjunctionResultObjects[linkObjectID] + 1
				} else {
					conjunctionResultObjects[linkObjectID] = 1
				}
			}
		}
		for linkObjectID, tagsCount := range conjunctionResultObjects {
			if tagsCount == len(tags) {
				disjunctionResultObjects[linkObjectID] = 0
			}
		}
	}
	return disjunctionResultObjects
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
