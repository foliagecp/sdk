// Foliage graph store search package.
// Provides stateful functions for search in graph
package search

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(
		runtime,
		"functions.graph.api.search.objects.fvpm",
		FieldValuePartialMatch,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false).SetMaxIdHandlers(-1),
	)
}

func getQueryFromPayload(ctx *sfPlugins.StatefunContextProcessor) (string, error) {
	jpQuery, ok := ctx.Payload.GetByPath("query").AsString()
	if !ok {
		return "", fmt.Errorf("\"query\" must be a string")
	}
	return jpQuery, nil
}

func getObjectTypeFilterFromPayload(ctx *sfPlugins.StatefunContextProcessor) map[string]struct{} {
	result := map[string]struct{}{}
	if objectTypeFilter, ok := ctx.Payload.GetByPath("object_type_filter").AsArrayString(); ok {
		for _, v := range objectTypeFilter {
			result[ctx.Domain.GetObjectIDWithoutDomain(v)] = struct{}{}
		}
	}
	return result
}

/*
Uses JPGQL call-tree result aggregation algorithm to find objects

Request:

	payload: json - required
		query: string - required // May be empty - will find all objects
		object_type_filter: []string - optional // Searches only for declared types. If empty or not defined - searches through all objects without type exclusions
*/

func FieldValuePartialMatch(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	searchQuery, err := getQueryFromPayload(ctx)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid query: %s", err.Error()))).Reply()
		return
	}

	dbc, err := db.NewDBSyncClientFromRequestFunction(ctx.Request)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("cannot create db client")).Reply()
		return
	}

	resultObjects := easyjson.NewJSONObject()
	commonFields := map[string]struct{}{}

	for _, domain := range ctx.Domain.GetWeakClusterDomains() {
		objectIds, err := dbc.Query.JPGQLCtraQuery(domain+statefun.ObjectIDDomainSeparator+crud.BUILT_IN_OBJECTS, fmt.Sprintf(".*[type('%s')]", crud.OBJECT_TYPELINK))
		if err != nil {
			logger.Logf(logger.ErrorLevel, "cannot gather object ids via JPGQLCtraQuery from domain with name=%s", domain)
			continue
		}

		objectTypesList := getObjectTypeFilterFromPayload(ctx)
		typeSearchFieldsIndex := map[string][]string{}

		for _, objId := range objectIds {
			data, err := dbc.CMDB.ObjectRead(ctx.Domain.GetShadowObjectShadowId(objId))
			if err == nil {
				otype := ctx.Domain.GetObjectIDWithoutDomain(data.GetByPath("type").AsStringDefault(""))
				if len(otype) > 0 {
					if len(objectTypesList) > 0 {
						if _, ok := objectTypesList[otype]; !ok {
							continue
						}
					}
					var searchFieldList []string
					if fl, ok := typeSearchFieldsIndex[otype]; ok {
						searchFieldList = fl
					} else {
						fieldList := []string{}
						data, err := dbc.Graph.VertexRead(ctx.Domain.GetShadowObjectShadowId(domain + statefun.ObjectIDDomainSeparator + otype))
						if err == nil {
							if fl, ok := data.GetByPath("body.search_fields").AsArrayString(); ok {
								fieldList = fl
							}
						}
						typeSearchFieldsIndex[otype] = fieldList
						searchFieldList = fieldList
					}

					body := data.GetByPath("body")
					objectSatisfiesSearch := false

					for _, field := range searchFieldList {
						if body.PathExists(field) {
							v := body.GetByPath(field)
							fieldValue := fmt.Sprintf("%v", v.Value)
							if strings.Contains(strings.ToLower(fieldValue), strings.ToLower(searchQuery)) {
								commonFields[field] = struct{}{}
								objectSatisfiesSearch = true
								break
							}
						}
					}
					if objectSatisfiesSearch {
						resultObjects.SetByPath(objId, data)
					}
				}
			}
		}
	}

	result := easyjson.NewJSONObject()
	commonFieldsArray := []string{}
	for k := range commonFields {
		commonFieldsArray = append(commonFieldsArray, k)
	}
	result.SetByPath("match.objects", resultObjects)
	result.SetByPath("match.fields", easyjson.JSONFromArray(commonFieldsArray))

	om.AggregateOpMsg(sfMediators.OpMsgOk(result)).Reply()
}
