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
	statefun.NewFunctionType(
		runtime,
		"functions.graph.api.query.fpl.pp.obody",
		PostProcessorObjectBody,
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
		statsMu       sync.Mutex
		allJpgqlCalls []jpgqlCallStat
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
				// calls are part of the diagnostic story too.
				recordCallStat(jpgqlCallStat{
					uoiIndex:          uoiIdx,
					intersectionIndex: intersectionIdx,
					query:             jpgqlQuery.request,
					fromUUID:          jpgqlQuery.uuid,
					status:            sfMediators.OpStatusNames[subOpMsg.Status],
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

	// Was the answer assembled from everything it was supposed to be assembled
	// from? A sub-query that was truncated (partial) or did not succeed at all
	// leaves the intersection narrower than the caller asked for — and only the
	// OK ones are merged into it at all (see the merge above), so a failed
	// sub-query used to disappear into stats while the answer came back OK.
	partial := false
	for _, s := range allJpgqlCalls {
		if s.status != sfMediators.OpStatusNames[sfMediators.SYNC_OP_STATUS_OK] ||
			s.stats.GetByPath("paths_skipped.timeout").AsNumericDefault(0) > 0 ||
			s.stats.GetByPath("paths_skipped.backpressure").AsNumericDefault(0) > 0 {
			partial = true
			break
		}
	}
	strict := ctx.Payload.GetByPath("strict").AsBoolDefault(false) || ctx.Options.GetByPath("strict").AsBoolDefault(false)

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
			// The post-processor works on what the sub-queries produced, so a
			// narrowed input makes its output narrowed too.
			ppReply.Data.SetByPath("partial", easyjson.NewJSON(partial))
			if partial && strict {
				ppReply = sfMediators.MakeOpMsg(sfMediators.SYNC_OP_STATUS_INCOMPLETE,
					"query result is partial: a sub-query was truncated or did not succeed", "", ppReply.Data)
			}
		}
		om.AggregateOpMsg(ppReply).Reply()
		return
	}

	resultJson := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.NewJSON(resultUUID))
	resultJson.SetByPath("stats.jpgql_calls", statsArray)
	resultJson.SetByPath("partial", easyjson.NewJSON(partial))

	if partial && strict {
		// Strict mode reports incompleteness as a status. Everything failing is
		// not a partial answer, it is no answer.
		allFailed := len(allJpgqlCalls) > 0
		for _, s := range allJpgqlCalls {
			if s.status == sfMediators.OpStatusNames[sfMediators.SYNC_OP_STATUS_OK] {
				allFailed = false
				break
			}
		}
		if allFailed {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("every sub-query failed")).Reply()
			return
		}
		om.AggregateOpMsg(sfMediators.MakeOpMsg(sfMediators.SYNC_OP_STATUS_INCOMPLETE,
			"query result is partial: a sub-query was truncated or did not succeed", "", resultJson)).Reply()
		return
	}
	om.AggregateOpMsg(sfMediators.OpMsgOk(resultJson)).Reply()
}

// linkBodyData reads one link's body via functions.graph.api.link.read WITHOUT the
// "details" flag — that returns just the body and takes no per-key lock (light, and
// outside the link-read/object-delete deadlock class). fromID is the link OWNER: for
// an out-link it is the vertex itself; for an in-link it is the link's source
// ("from"). A missing/failed read yields an empty object so the entry shape stays
// stable.
func linkBodyData(ctx *sfPlugins.StatefunContextProcessor, fromID, name string) easyjson.JSON {
	if len(fromID) == 0 || len(name) == 0 {
		return easyjson.NewJSONObject()
	}
	payload := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(name))
	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", fromID, &payload, nil))
	if m.Status == sfMediators.SYNC_OP_STATUS_OK {
		return m.Data.GetByPath("body")
	}
	return easyjson.NewJSONObject()
}

// attachLinkBodies enriches the structured links.in / links.out arrays of one
// per-uuid result with each link's "body", controlled per direction. An out-link
// {to,name,type} of selfID has its body stored on selfID; an in-link {from,name,type}
// has its body stored on the source vertex ("from"). Link bodies are read
// sequentially within one uuid — callers already fan out uuids concurrently, so the
// goroutine count stays bounded by the number of uuids.
func attachLinkBodies(ctx *sfPlugins.StatefunContextProcessor, vertexData *easyjson.JSON, selfID string, inBody, outBody bool) {
	enrich := func(path, sourceField string) {
		arr := vertexData.GetByPath(path)
		if !arr.IsArray() {
			return
		}
		enriched := easyjson.NewJSONArray()
		for i := 0; i < arr.ArraySize(); i++ {
			link := arr.ArrayElement(i)
			owner := selfID
			if sourceField != "" {
				owner = link.GetByPath(sourceField).AsStringDefault("")
			}
			link.SetByPath("body", linkBodyData(ctx, owner, link.GetByPath("name").AsStringDefault("")))
			enriched.AddToArray(link)
		}
		vertexData.SetByPath(path, enriched)
	}
	if outBody {
		enrich("links.out", "") // out-link body lives on the vertex itself
	}
	if inBody {
		enrich("links.in", "from") // in-link body lives on the source vertex
	}
}

/*
	{
		"uuids": [...],
		"data": {
			"sort_by_field": [
				"<field name 1>[:asc|:dsc]",
				"<field name 2>[:asc|:dsc]",
				...
			],
			"links_in_body":  bool,  // optional, default false
			"links_out_body": bool   // optional, default false
		}
	}

When links_in_body and/or links_out_body is set, each per-vertex result additionally
carries the structured links.in / links.out arrays (fetched via vertex.read
"details_v2"), with the "body" of each link in the requested direction attached to its
entry. Without these flags the behavior is unchanged: only the vertex body is returned
and no links are enumerated.
*/
func PostProcessorVertexBody(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	uuids := []string{}
	if arr, ok := ctx.Payload.GetByPath("uuids").AsArrayString(); ok {
		uuids = arr
	}

	inBody := ctx.Payload.GetByPath("data.links_in_body").AsBoolDefault(false)
	outBody := ctx.Payload.GetByPath("data.links_out_body").AsBoolDefault(false)
	withLinks := inBody || outBody

	var wg sync.WaitGroup
	uuidDatas := make([]*easyjson.JSON, len(uuids))
	var uuiDataMutex sync.Mutex
	for i, uuid := range uuids {
		wg.Add(1)
		go func(i int, uuid string) {
			defer wg.Done()

			payload := easyjson.NewJSONObject()
			if withLinks {
				// details_v2 returns the structured links.in / links.out arrays the
				// flags attach bodies to; omitted otherwise to keep the default light.
				payload.SetByPath("details_v2", easyjson.NewJSON(true))
			}
			om := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", uuid, &payload, nil))

			var data *easyjson.JSON
			if om.Status == sfMediators.SYNC_OP_STATUS_OK {
				data = &om.Data
				if withLinks {
					attachLinkBodies(ctx, data, uuid, inBody, outBody)
				}
			} else {
				data = easyjson.NewJSONObject().GetPtr()
			}
			data.SetByPath("uuid", easyjson.NewJSON(uuid))

			uuiDataMutex.Lock()
			uuidDatas[i] = data
			uuiDataMutex.Unlock()
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

/*
PostProcessorObjectBody is the CMDB-object counterpart of PostProcessorVertexBody.

Layout of the request is identical to .pp.vbody:

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

The only difference is the per-uuid fetch: instead of the raw graph-level
"functions.graph.api.vertex.read", this post-processor calls the CMDB-level
"functions.cmdb.api.object.read" with the "details_v2" payload flag. That gives
the caller the v2-shaped CMDB object (structured links.out array, body, type)
rather than the raw vertex projection — which is what report exports usually
want, identical to ObjectReadV2 in clients/go/db.

Each uuid is fetched concurrently; on a failed read a placeholder empty object
is inserted (with the "uuid" field still set) so the result array length and
caller-visible indexing match the input uuids list, mirroring vbody behavior.

links_in_body / links_out_body in "data" behave exactly as in .pp.vbody: when set,
the body of each link in the requested direction is attached to its entry in the
links.in / links.out arrays (object.read already returns those arrays via details_v2,
so unlike vbody no extra request shape is needed to expose them).
*/
func PostProcessorObjectBody(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	uuids := []string{}
	if arr, ok := ctx.Payload.GetByPath("uuids").AsArrayString(); ok {
		uuids = arr
	}

	inBody := ctx.Payload.GetByPath("data.links_in_body").AsBoolDefault(false)
	outBody := ctx.Payload.GetByPath("data.links_out_body").AsBoolDefault(false)

	var wg sync.WaitGroup
	uuidDatas := make([]*easyjson.JSON, len(uuids))
	var uuiDataMutex sync.Mutex
	for i, uuid := range uuids {
		wg.Add(1)
		go func(i int, uuid string) {
			defer wg.Done()

			payload := easyjson.NewJSONObject()
			payload.SetByPath("details_v2", easyjson.NewJSON(true))
			om := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.read", uuid, &payload, nil))

			var data *easyjson.JSON
			if om.Status == sfMediators.SYNC_OP_STATUS_OK {
				data = &om.Data
				if inBody || outBody {
					attachLinkBodies(ctx, data, uuid, inBody, outBody)
				}
			} else {
				data = easyjson.NewJSONObject().GetPtr()
			}
			data.SetByPath("uuid", easyjson.NewJSON(uuid))

			uuiDataMutex.Lock()
			uuidDatas[i] = data
			uuiDataMutex.Unlock()
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
