package fpl

import (
	"fmt"
	"sync"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type JPGQLRequestData struct {
	request string
	uuid    string
}

/*
	{
		"jpgql_uoi": [ // Union of intersections
			[
				{"jpgql": "<jpgql query 1>", "from_uuid": "<vertex uuid x>"},
				{"jpgql": "<jpgql query 2>", "from_uuid": "<vertex uuid y>"},
				...
			],
			[
				{"jpgql": "<jpgql query 1>", "from_uuid": "<vertex uuid x>"},
				{"jpgql": "<jpgql query 2>", "from_uuid": "<vertex uuid y>"},
				...
			],
			...
		],
		"only_uuids": true,
		"sort_by": [
			{
				"field": "uuid",
				"reverse": false
			},
			{
				"field": "<field name 2>",
				"reverse": false
			},
			...
		],
		"group_by": [
			"<field name 1>",
			"<field name 2>",
			...
		]
	}
*/
func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(
		runtime,
		"functions.graph.api.query.fpl",
		FoliageProcessingLanguage,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false).SetMaxIdHandlers(-1),
	)
}

func FoliageProcessingLanguage(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	if !ctx.Payload.PathExists("jpgql_uoi") {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("request does not contain \"jpgql_uoi\" field")).Reply()
		return
	}
	jpgqlUoI := ctx.Payload.GetByPath("jpgql_uoi")
	if !jpgqlUoI.IsArray() {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("\"jpgql_uoi\" is not an array")).Reply()
		return
	}
	unionUUIDs := map[string]struct{}{}
	for i := 0; i < jpgqlUoI.ArraySize(); i++ {
		jpgqlIntersectionRequestsJSON := jpgqlUoI.ArrayElement(i)
		if !jpgqlIntersectionRequestsJSON.IsArray() {
			om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("\"jpgql_uoi\"'s element %d is not an array", i))).Reply()
			return
		}

		jpgqlIntersectionValidQueries := []JPGQLRequestData{}
		for j := 0; j < jpgqlIntersectionRequestsJSON.ArraySize(); j++ {
			jpgqlData := jpgqlIntersectionRequestsJSON.ArrayElement(j)
			jpgqlRequest := jpgqlData.GetByPath("jpgql").AsStringDefault("")
			if len(jpgqlRequest) == 0 {
				om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("\"jpgql_uoi\"'s element [%d, %d] does not contain a valid value at \"jpgql\" field", i, j))).Reply()
				return
			}
			jpgqlStartUUID := jpgqlData.GetByPath("from_uuid").AsStringDefault("")
			if len(jpgqlRequest) == 0 {
				om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("\"jpgql_uoi\"'s element [%d, %d] does not contain a valid value at \"from_uuid\" field", i, j))).Reply()
				return
			}
			req := JPGQLRequestData{request: jpgqlRequest, uuid: jpgqlStartUUID}
			jpgqlIntersectionValidQueries = append(jpgqlIntersectionValidQueries, req)
		}

		var intersectionUUIDsMutex sync.Mutex
		intersectionUUIDs := map[string]struct{}{}

		var wg sync.WaitGroup
		for _, jpgqlQuery := range jpgqlIntersectionValidQueries {
			wg.Add(1)
			go func() {
				defer wg.Done()

				payload := easyjson.NewJSONObjectWithKeyValue("query", easyjson.NewJSON(jpgqlQuery.request))
				om := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.query.jpgql.ctra", jpgqlQuery.uuid, &payload, nil))

				if om.Status == sfMediators.SYNC_OP_STATUS_OK {
					intersectionUUIDsMutex.Lock()
					defer intersectionUUIDsMutex.Unlock()

					newIntersectionUUIDs := map[string]struct{}{}
					for _, foundUUID := range om.Data.ObjectKeys() {
						if _, ok := intersectionUUIDs[foundUUID]; len(intersectionUUIDs) == 0 || ok {
							newIntersectionUUIDs[foundUUID] = struct{}{}
						}
					}
					intersectionUUIDs = newIntersectionUUIDs
				}
			}()
			wg.Wait()
		}

		// Append result into finalUUIDs ------------------
		for uuid := range intersectionUUIDs {
			unionUUIDs[uuid] = struct{}{}
		}
		// ------------------------------------------------
	}

	resultUUID := []string{}
	for uuid := range unionUUIDs {
		resultUUID = append(resultUUID, uuid)
	}

	resultData := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.JSONFromArray(resultUUID))
	system.MsgOnErrorReturn(om.ReplyWithData(&resultData))
}
