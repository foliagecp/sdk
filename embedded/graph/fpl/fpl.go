package fpl

import (
	"fmt"
	"sort"
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
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false).SetMsgAckWaitMs(MAX_ACK_WAIT_MS),
	)
	statefun.NewFunctionType(
		runtime,
		"functions.graph.api.query.fpl.pp.vbody",
		PostProcessorVertexBody,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false).SetMsgAckWaitMs(MAX_ACK_WAIT_MS),
	)
}

/*
Request:

	payload: json - required
		jpgql_uoi: array - required  // Union of intersections
			[
				[
					{"jpgql": "<jpgql query 1>", "from_uuid": "<vertex uuid x>"},
					{"jpgql": "<jpgql query 2>", "from_uuid": "<vertex uuid y>"},
					...
				],
				...
			]
		sort: "asc"|"dsc" - optional
		post_processor_func: object - optional  // Arbitrary follow-up call
			name: "<statefun name>"
			data: { ... }

	options: json - optional
		qds_timeout_sec: float - optional // forwarded to each jpgql.ctra sub-call;
		                                  // default = 5 (matches jpgql)
		max_depth: int - optional         // forwarded to each jpgql.ctra sub-call;
		                                  // default = -1 (matches jpgql)

Response:

	uuids: array of string  // union-of-intersections result
	stats: object
	  jpgql_calls: array of object  // one entry per individual jpgql.ctra call
	    uoi_index: int        // outer "Union" index
	    intersection_index: int  // inner "Intersection" index within that union
	    query: string         // the jpgql query string used
	    from_uuid: string     // the start vertex id used
	    status: "ok"|"failed"|"incomplete"|"idle"  // mediator status of the call
	    stats: object         // raw stats object returned by jpgql.ctra (see jpgql.go),
	                          // including call_tree.*, paths_skipped.*, times.*, duration.*
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

	// Per-call options forwarded to every jpgql.ctra sub-call. jpgql normalises
	// these defaults itself (qds_timeout_sec=5, max_depth=-1) but we set them
	// here so they show up in the call stats explicitly even when the caller
	// omits them — easier diagnostics from a single fpl response.
	qdsTimeoutSec := ctx.Options.GetByPath("qds_timeout_sec").AsNumericDefault(5)
	maxDepth := int(ctx.Options.GetByPath("max_depth").AsNumericDefault(-1))
	jpgqlOptions := easyjson.NewJSONObject()
	jpgqlOptions.SetByPath("qds_timeout_sec", easyjson.NewJSON(qdsTimeoutSec))
	jpgqlOptions.SetByPath("max_depth", easyjson.NewJSON(maxDepth))

	// jpgqlCallStat records what one jpgql.ctra invocation looked like, for
	// inclusion in the fpl response under stats.jpgql_calls. We capture it
	// before merging into the intersection so a single misbehaving query is
	// still individually attributable in the response — even if its result
	// did not survive intersection.
	type jpgqlCallStat struct {
		uoiIndex          int
		intersectionIndex int
		query             string
		fromUUID          string
		status            string
		stats             easyjson.JSON
	}

	var (
		statsMu        sync.Mutex
		allJpgqlCalls  []jpgqlCallStat
	)
	recordCallStat := func(s jpgqlCallStat) {
		statsMu.Lock()
		defer statsMu.Unlock()
		allJpgqlCalls = append(allJpgqlCalls, s)
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
		for j, jpgqlQuery := range jpgqlIntersectionValidQueries {
			wg.Add(1)
			go func(uoiIdx, intersectionIdx int, jpgqlQuery JPGQLRequestData) {
				defer wg.Done()

				payload := easyjson.NewJSONObjectWithKeyValue("query", easyjson.NewJSON(jpgqlQuery.request))
				// Append a unique "===<id>" suffix to bypass the FT per-id mutex
				// in jpgql.ctra. getOriginalID() inside JPGQL strips the suffix,
				// so graph/cache operations target the same vertex, but the
				// FunctionType scheduler treats each call as an independent id,
				// enabling true parallel execution across multiple FPL workers.
				jpgqlCallID := jpgqlQuery.uuid + "===" + system.GetUniqueStrID()
				subOptions := jpgqlOptions.Clone()
				subOpMsg := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.query.jpgql.ctra", jpgqlCallID, &payload, &subOptions))

				// Record per-call stats regardless of status — failed/incomplete
				// calls are part of the diagnostic story too. Status name lookup
				// uses OpStatusNames (mediator/msg.go) so the response surfaces
				// "ok"|"idle"|"incomplete"|"failed" verbatim instead of an int.
				statusName := "unknown"
				if int(subOpMsg.Status) >= 0 && int(subOpMsg.Status) < len(sfMediators.OpStatusNames) {
					statusName = sfMediators.OpStatusNames[subOpMsg.Status]
				}
				recordCallStat(jpgqlCallStat{
					uoiIndex:          uoiIdx,
					intersectionIndex: intersectionIdx,
					query:             jpgqlQuery.request,
					fromUUID:          jpgqlQuery.uuid,
					status:            statusName,
					stats:             subOpMsg.Data.GetByPath("stats"),
				})

				if subOpMsg.Status == sfMediators.SYNC_OP_STATUS_OK {
					intersectionUUIDsMutex.Lock()
					defer intersectionUUIDsMutex.Unlock()

					newIntersectionUUIDs := map[string]struct{}{}
					for _, foundUUID := range subOpMsg.Data.GetByPath("uuids").ObjectKeys() {
						if _, ok := intersectionUUIDs[foundUUID]; len(intersectionUUIDs) == 0 || ok {
							newIntersectionUUIDs[foundUUID] = struct{}{}
						}
					}
					intersectionUUIDs = newIntersectionUUIDs
				}
			}(i, j, jpgqlQuery)
		}
		wg.Wait()

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

	// Build the stats.jpgql_calls array. Sorted by (uoi_index, intersection_index)
	// so the array order matches the input jpgql_uoi structure — easier to read
	// when diagnosing a slow query.
	sort.Slice(allJpgqlCalls, func(a, b int) bool {
		if allJpgqlCalls[a].uoiIndex != allJpgqlCalls[b].uoiIndex {
			return allJpgqlCalls[a].uoiIndex < allJpgqlCalls[b].uoiIndex
		}
		return allJpgqlCalls[a].intersectionIndex < allJpgqlCalls[b].intersectionIndex
	})
	statsArray := easyjson.NewJSONArray()
	for _, s := range allJpgqlCalls {
		entry := easyjson.NewJSONObject()
		entry.SetByPath("uoi_index", easyjson.NewJSON(s.uoiIndex))
		entry.SetByPath("intersection_index", easyjson.NewJSON(s.intersectionIndex))
		entry.SetByPath("query", easyjson.NewJSON(s.query))
		entry.SetByPath("from_uuid", easyjson.NewJSON(s.fromUUID))
		entry.SetByPath("status", easyjson.NewJSON(s.status))
		entry.SetByPath("stats", s.stats)
		statsArray.AddToArray(entry)
	}

	// Running post processing function
	postProcessorFunc := ctx.Payload.GetByPath("post_processor_func.name").AsStringDefault("")
	if len(postProcessorFunc) > 0 {
		postProcessorPayload := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.NewJSON(resultUUID))
		if ctx.Payload.PathExists("post_processor_func.data") {
			postProcessorPayload.SetByPath("data", ctx.Payload.GetByPath("post_processor_func.data"))
		}
		// Same "===<uid>" trick as for jpgql.ctra above — lets multiple concurrent
		// FPL requests reach the post-processor without serializing on its FT mutex.
		ppCallID := ctx.Self.ID + "===" + system.GetUniqueStrID()
		ppReply := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, postProcessorFunc, ppCallID, &postProcessorPayload, nil))
		// Attach the per-jpgql stats to whatever the post-processor returns so
		// the caller still has access to the same diagnostics regardless of
		// whether a post-processor was configured.
		if ppReply.Status == sfMediators.SYNC_OP_STATUS_OK {
			ppReply.Data.SetByPath("stats.jpgql_calls", statsArray)
		}
		om.AggregateOpMsg(ppReply).Reply()
		return
	}

	resultJson := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.NewJSON(resultUUID))
	resultJson.SetByPath("stats.jpgql_calls", statsArray)
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
		go func(i int, uuid string) {
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
		}(i, uuid)
	}
	wg.Wait()

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
