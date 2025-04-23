package fpl

import (
	"fmt"
	"strings"
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

const (
	MAX_ACK_WAIT_MS = 60 * 1000
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(
		runtime,
		"functions.graph.api.query.fpl",
		FoliageProcessingLanguage,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false).SetMsgAckWaitMs(MAX_ACK_WAIT_MS).SetWorkerPoolLoadType(statefun.WPLoadHigh),
	)
	statefun.NewFunctionType(
		runtime,
		"functions.graph.api.query.fpl.pp.vbody",
		PostProcessorVertexBody,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false).SetMsgAckWaitMs(MAX_ACK_WAIT_MS).SetWorkerPoolLoadType(statefun.WPLoadHigh),
	)
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
		"sort": "asc"|"dsc",
		"post_processor_func": { // Arbitrary
			name: "<statefun name>",
			data: {
				....
			}
		}
	}
*/
func FoliageProcessingLanguage(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	if !ctx.Payload.PathExists("jpgql_uoi") {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("request does not contain \"jpgql_uoi\" field")).Reply()
		return
	}
	jpgqlUoI := ctx.Payload.GetByPath("jpgql_uoi")
	if !jpgqlUoI.IsArray() {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("\"jpgql_uoi\" is not an array")).Reply()
		return
	}

	unionUUIDs := map[string]struct{}{}
	for i := 0; i < jpgqlUoI.ArraySize(); i++ {
		jpgqlIntersectionRequestsJSON := jpgqlUoI.ArrayElement(i)
		if !jpgqlIntersectionRequestsJSON.IsArray() {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("\"jpgql_uoi\"'s element %d is not an array", i))).Reply()
			return
		}

		jpgqlIntersectionValidQueries := []JPGQLRequestData{}
		for j := 0; j < jpgqlIntersectionRequestsJSON.ArraySize(); j++ {
			jpgqlData := jpgqlIntersectionRequestsJSON.ArrayElement(j)
			jpgqlRequest := jpgqlData.GetByPath("jpgql").AsStringDefault("")
			if len(jpgqlRequest) == 0 {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("\"jpgql_uoi\"'s element [%d, %d] does not contain a valid value at \"jpgql\" field", i, j))).Reply()
				return
			}
			jpgqlStartUUID := jpgqlData.GetByPath("from_uuid").AsStringDefault(ctx.Self.ID)
			if len(jpgqlRequest) == 0 {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("\"jpgql_uoi\"'s element [%d, %d] does not contain a valid value at \"from_uuid\" field", i, j))).Reply()
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
					for _, foundUUID := range om.Data.GetByPath("uuids").ObjectKeys() {
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
	uuidSortDir := strings.ToLower(ctx.Payload.GetByPath("sort").AsStringDefault(""))
	if len(uuidSortDir) > 0 {
		resultUUID = system.SortUUIDs(resultUUID, uuidSortDir == "asc")
	}

	// Running post processing function
	postProcessorFunc := ctx.Payload.GetByPath("post_processor_func.name").AsStringDefault("")
	if len(postProcessorFunc) > 0 {
		postProcessorPayload := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.JSONFromArray(resultUUID))
		if ctx.Payload.PathExists("post_processor_func.data") {
			postProcessorPayload.SetByPath("data", ctx.Payload.GetByPath("post_processor_func.data"))
		}
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, postProcessorFunc, ctx.Self.ID, &postProcessorPayload, nil))).Reply()
		return
	}

	resultJson := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.JSONFromArray(resultUUID))
	om.AggregateOpMsg(sfMediators.OpMsgOk(resultJson)).Reply()
}

/*
	{
		"uuids": [...],
		"data": {
			"sort_by_field": [
				"<field name 1>[:asc|:dsc]",
				"<field name 2>[:asc|:dsc]",
				...
			]
		}
	}
*/
func PostProcessorVertexBody(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	uuids := []string{}
	if arr, ok := ctx.Payload.GetByPath("uuids").AsArrayString(); ok {
		uuids = arr
	}

	var wg sync.WaitGroup
	uuidDatas := make([]*easyjson.JSON, len(uuids))
	var uuiDataMutex sync.Mutex
	for i, uuid := range uuids {
		wg.Add(1)
		go func() {
			defer wg.Done()

			payload := easyjson.NewJSONObject()
			om := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", uuid, &payload, nil))

			uuiDataMutex.Lock()
			defer uuiDataMutex.Unlock()
			if om.Status == sfMediators.SYNC_OP_STATUS_OK {
				uuidDatas[i] = &om.Data
			} else {
				uuidDatas[i] = easyjson.NewJSONObject().GetPtr()
			}
			uuidDatas[i].SetByPath("uuid", easyjson.NewJSON(uuid))
		}()
		wg.Wait()
	}

	if sortFields, ok := ctx.Payload.GetByPath("data.sort_by_field").AsArrayString(); ok {
		uuidDatas = system.SortJSONs(uuidDatas, sortFields)
	}

	resultJsonArray := easyjson.NewJSONArray()
	for _, uuidData := range uuidDatas {
		resultJsonArray.AddToArray(*uuidData)
	}

	resultJson := easyjson.NewJSONObjectWithKeyValue("arr", resultJsonArray)
	om.AggregateOpMsg(sfMediators.OpMsgOk(resultJson)).Reply()
}
