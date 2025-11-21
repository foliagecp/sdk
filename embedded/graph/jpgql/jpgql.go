// Foliage graph store jpgql package.
// Provides stateful functions of json-path graph query language for the graph store
package jpgql

import (
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/sdk/statefun/system"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(
		runtime,
		"functions.graph.api.query.jpgql.ctra",
		JPGQLCallTreeResultAggregation,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false),
	)
}

func getQueryFromPayload(ctx *sfPlugins.StatefunContextProcessor) (string, error) {
	jpQuery, ok := ctx.Payload.GetByPath("query").AsString()
	if !ok || len(jpQuery) == 0 {
		return "", fmt.Errorf("Error LLAPIQueryJPGQLCallTreeResultAggregation: \"query\" must be a string with len>0")
	}
	return jpQuery, nil
}

// TODO: Objects can be deleted and created while graph is being traversed by JPGQL, need to do something about it
// seems that an infinite loop can appear

/*
Uses JPGQL call-tree result aggregation algorithm to find objects

Request:

	payload: json - required
		// Initial request from caller
		query: string - required // Json path query

	options: json - optional
		qds_timeout_sec: float - optional // Query Depth Spreading timeout (whole query timeout will be about twice longer), default = 5
		max_depth: int - optional // default = -1
		bp_timeout_ms: float // Back pressure timeout ms, default = 300

		query_started_nano: int // set by system from initial moment, will be overwritted if received
		call_started_nano: int // set by system for each call, will be overwritted if received
		depth: int // set by system from initial moment, will be overwritted if received
*/

/*func JPGQLCallTreeResultAggregationOLD(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	callTimeNano := time.Now().UnixNano()
	createReturnData := func(uuids map[string]bool, currentDepth, skippedByDepth, skippedByTimeout, skippedByBP, verticesPassed int, queryStartTimeNano int64, maxBPDurationNano int64) easyjson.JSON {
		resultUUIDS := map[string]bool{}
		if uuids != nil {
			resultUUIDS = uuids
		}
		returnData := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.NewJSON(resultUUIDS))
		returnData.SetByPath("stats.paths_skipped.depth", easyjson.NewJSON(skippedByDepth))
		returnData.SetByPath("stats.paths_skipped.timeout", easyjson.NewJSON(skippedByTimeout))
		returnData.SetByPath("stats.paths_skipped.backpressure", easyjson.NewJSON(skippedByBP))
		returnData.SetByPath("stats.call_tree.max_depth_reached", easyjson.NewJSON(currentDepth))
		returnData.SetByPath("stats.call_tree.vertices_passed", easyjson.NewJSON(verticesPassed))
		returnData.SetByPath("stats.times.qds_end_nano", easyjson.NewJSON(callTimeNano))
		returnData.SetByPath("stats.times.query_start_nano", easyjson.NewJSON(queryStartTimeNano))
		returnData.SetByPath("stats.duration.max_backpressure_nano", easyjson.NewJSON(maxBPDurationNano))
		return returnData
	}
	aggregateData := func(returnData, aggregatedReturnData easyjson.JSON) easyjson.JSON {
		// Merging uuids ----------------------------------
		resultUUIDs := returnData.GetByPath("uuids")
		if !resultUUIDs.IsNonEmptyObject() {
			resultUUIDs = easyjson.NewJSONObject()
		}
		uuids := aggregatedReturnData.GetByPath("uuids")
		for _, objectUUID := range uuids.ObjectKeys() {
			resultUUIDs.SetByPath(objectUUID, easyjson.NewJSON(true))
		}
		// ------------------------------------------------
		// Map's reduce -----------------------------------
		retMDR := returnData.GetByPath("stats.call_tree.max_depth_reached").AsNumericDefault(0)
		aggMDR := aggregatedReturnData.GetByPath("stats.call_tree.max_depth_reached").AsNumericDefault(0)
		if aggMDR > retMDR {
			retMDR = aggMDR
		}

		retPSBD := returnData.GetByPath("stats.paths_skipped.depth").AsNumericDefault(0)
		aggPSBD := aggregatedReturnData.GetByPath("stats.paths_skipped.depth").AsNumericDefault(0)
		retPSBD += aggPSBD

		retPSBT := returnData.GetByPath("stats.paths_skipped.timeout").AsNumericDefault(0)
		aggPSBT := aggregatedReturnData.GetByPath("stats.paths_skipped.timeout").AsNumericDefault(0)
		retPSBT += aggPSBT

		retPSBBP := returnData.GetByPath("stats.paths_skipped.backpressure").AsNumericDefault(0)
		aggPSBBP := aggregatedReturnData.GetByPath("stats.paths_skipped.backpressure").AsNumericDefault(0)
		retPSBBP += aggPSBBP

		retVP := returnData.GetByPath("stats.call_tree.vertices_passed").AsNumericDefault(0)
		aggVP := aggregatedReturnData.GetByPath("stats.call_tree.vertices_passed").AsNumericDefault(0)
		retVP += aggVP

		retQETN := returnData.GetByPath("stats.times.qds_end_nano").AsNumericDefault(0)
		aggQETN := aggregatedReturnData.GetByPath("stats.times.qds_end_nano").AsNumericDefault(0)
		if aggQETN > retQETN {
			retQETN = aggQETN
		}

		retMBPDN := returnData.GetByPath("stats.duration.max_backpressure_nano").AsNumericDefault(0)
		aggMBPDN := aggregatedReturnData.GetByPath("stats.duration.max_backpressure_nano").AsNumericDefault(0)
		if aggMBPDN > retMBPDN {
			retMBPDN = aggMBPDN
		}

		aggQSTN := aggregatedReturnData.GetByPath("stats.times.query_start_nano").AsNumericDefault(0)
		// ------------------------------------------------

		newReturnData := easyjson.NewJSONObjectWithKeyValue("uuids", resultUUIDs)
		newReturnData.SetByPath("stats.paths_skipped.depth", easyjson.NewJSON(retPSBD))
		newReturnData.SetByPath("stats.paths_skipped.timeout", easyjson.NewJSON(retPSBT))
		newReturnData.SetByPath("stats.paths_skipped.backpressure", easyjson.NewJSON(retPSBBP))
		newReturnData.SetByPath("stats.call_tree.vertices_passed", easyjson.NewJSON(retVP))
		newReturnData.SetByPath("stats.call_tree.max_depth_reached", easyjson.NewJSON(retMDR))
		newReturnData.SetByPath("stats.times.qds_end_nano", easyjson.NewJSON(retQETN))
		newReturnData.SetByPath("stats.times.query_start_nano", easyjson.NewJSON(aggQSTN))
		newReturnData.SetByPath("stats.times.query_end_nano", easyjson.NewJSON(callTimeNano))
		newReturnData.SetByPath("stats.duration.max_backpressure_nano", easyjson.NewJSON(retMBPDN))

		newReturnData.SetByPath("stats.duration.query_nano", easyjson.NewJSON(callTimeNano-int64(aggQSTN)))
		newReturnData.SetByPath("stats.duration.qds_nano", easyjson.NewJSON(retQETN-aggQSTN))
		newReturnData.SetByPath("stats.duration.agg_nano", easyjson.NewJSON(callTimeNano-int64(retQETN)))

		return newReturnData
	}

	tokens := strings.Split(ctx.Self.ID, "===")
	var vId string = tokens[0]
	var pId string
	if len(tokens) == 2 {
		pId = tokens[1]
	} else {
		pId = system.GetUniqueStrID()
	}

	loopPreventIdGenerator := func() string {
		return system.GetHashStr(ctx.Caller.ID + pId + ctx.Payload.GetByPath("query").AsStringDefault(""))
	}
	om, noLoop := sfMediators.NewOpMediatorWithUniquenessControl(ctx, loopPreventIdGenerator)
	if !noLoop {
		om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSON(map[string]bool{}))).Reply()
		return
	}

	switch om.GetOpType() {
	case mediator.MereOp: // Initial call of jpgql
		newOptions := ctx.Options
		queryDepthSpreadTimeoutSec := ctx.Options.GetByPath("qds_timeout_sec").AsNumericDefault(5)
		newOptions.SetByPath("qds_timeout_sec", easyjson.NewJSON(queryDepthSpreadTimeoutSec))
		queryMaxDepth := ctx.Options.GetByPath("max_depth").AsNumericDefault(-1)
		newOptions.SetByPath("max_depth", easyjson.NewJSON(queryMaxDepth))
		bpTimeoutMs := ctx.Options.GetByPath("bp_timeout_ms").AsNumericDefault(300)
		newOptions.SetByPath("bp_timeout_ms", easyjson.NewJSON(bpTimeoutMs))

		newOptions.SetByPath("query_started_nano", easyjson.NewJSON(callTimeNano))
		newOptions.SetByPath("call_started_nano", easyjson.NewJSON(callTimeNano))
		newOptions.SetByPath("depth", easyjson.NewJSON(0))
		system.MsgOnErrorReturn(om.SignalWithAggregation(sfPlugins.JetstreamGlobalSignal, ctx.Self.Typename, vId+"==="+pId, ctx.Payload, newOptions))
	case mediator.WorkerIsTaskedByAggregatorOp:
		qdsTimeoutSec := float64(ctx.Options.GetByPath("qds_timeout_sec").AsNumericDefault(0))
		maxDepth := int(ctx.Options.GetByPath("max_depth").AsNumericDefault(0))
		bpTimeoutMs := float64(ctx.Options.GetByPath("bp_timeout_ms").AsNumericDefault(0))

		currentDepth := int(ctx.Options.GetByPath("depth").AsNumericDefault(0))
		queryStartedNano := int64(ctx.Options.GetByPath("query_started_nano").AsNumericDefault(-1))
		callStartedNano := int64(ctx.Options.GetByPath("call_started_nano").AsNumericDefault(-1))
		bpNano := callTimeNano - callStartedNano

		// Checking limits --------------------------------------------------------------
		if callStartedNano > 0 {
			if bpNano > int64(bpTimeoutMs*float64(time.Millisecond)) { // One jpgql execution takes too long - back pressure
				data := createReturnData(nil, currentDepth, 0, 0, 1, 0, queryStartedNano, bpNano)
				om.AggregateOpMsg(sfMediators.OpMsgOk(data)).Reply()
				return
			}
		}
		if maxDepth >= 0 && currentDepth >= maxDepth { // Will be cheking at depth + 1 if this condition returns false, thus currentDepth !!!>=!!! maxDepth
			data := createReturnData(nil, currentDepth, 1, 0, 0, 0, queryStartedNano, bpNano)
			om.AggregateOpMsg(sfMediators.OpMsgOk(data)).Reply()
			return
		}
		if queryStartedNano > 0 {
			if callTimeNano-queryStartedNano > int64(qdsTimeoutSec*float64(time.Second)) {
				data := createReturnData(nil, currentDepth, 0, 1, 0, 0, queryStartedNano, bpNano)
				om.AggregateOpMsg(sfMediators.OpMsgOk(data)).Reply()
				return
			}
		}
		// ------------------------------------------------------------------------------

		currentObjectLinksQuery, err := getQueryFromPayload(ctx)
		if err != nil {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid query: %s", err.Error()))).Reply()
			return
		}
		queryHeadLinkType, queryHeadFilter, queryTail, anyDepthStop, err := GetQueryHeadAndTailsParts(currentObjectLinksQuery)
		if err != nil {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("currentObjectLinksQuery is invalid: %s", err.Error()))).Reply()
			return
		}
		// Getting vertices through out links that satisfy query leftover
		resultObjects := GetObjectIDsFromLinkNameAndLinkFilterQueryWithAnyDepthStop(ctx.Domain.Cache(), vId, queryHeadLinkType, queryHeadFilter, anyDepthStop)

		if len(resultObjects) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONObject())).Reply()
			return
		} else {
			objectsToReturnAsAResult := map[string]bool{}
			workersToAggregateFrom := 0
			for objectID, anyDepthStopped := range resultObjects {
				nextQuery := queryTail
				if anyDepthStopped {
					nextQuery = anyDepthStop.QueryTail
				}
				if len(nextQuery) == 0 { // query ended!!!!
					objectsToReturnAsAResult[objectID] = true
				} else {
					workerPayload := easyjson.NewJSONObject()
					workerPayload.SetByPath("query", easyjson.NewJSON(nextQuery))

					newOptions := ctx.Options.Clone()
					newOptions.SetByPath("depth", easyjson.NewJSON(currentDepth+1))
					newOptions.SetByPath("call_started_nano", easyjson.NewJSON(callTimeNano))
					err := om.SignalWithAggregation(sfPlugins.JetstreamGlobalSignal, ctx.Self.Typename, objectID+"==="+pId, &workerPayload, &newOptions)
					if err != nil {
						om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
						return
					}
					workersToAggregateFrom++
				}
			}
			if workersToAggregateFrom == 0 { // This vertex' children ALL are leafs against query
				checkedVerticesInThisCall := 1 + len(resultObjects) // This vertex + all vertices-leafs
				data := createReturnData(objectsToReturnAsAResult, currentDepth+1, 0, 0, 0, checkedVerticesInThisCall, queryStartedNano, bpNano)
				om.AggregateOpMsg(sfMediators.OpMsgOk(data)).Reply()
				return
			} else { // This vertex' children SOME are leafs against query and SOME are tasked so this one waits for AggregatedWorkersOp
				checkedVerticesInThisCall := 1 + (len(resultObjects) - workersToAggregateFrom) // This vertex + all vertices-leafs
				data := createReturnData(objectsToReturnAsAResult, currentDepth+1, 0, 0, 0, checkedVerticesInThisCall, queryStartedNano, bpNano)
				om.AddIntermediateResult(ctx, &data)
			}
		}
	case mediator.AggregatorRepliedByWorkerOp:
	case mediator.AggregatedWorkersOp:
		aggregatedResult := easyjson.NewJSONObject()
		for _, opMsg := range om.GetAggregatedOpMsgs() {
			aggregatedResult = aggregateData(aggregatedResult, opMsg.Data)
		}
		system.MsgOnErrorReturn(om.ReplyWithData(&aggregatedResult))
	}
}*/

/*
Uses JPGQL call-tree result aggregation algorithm to find objects

Request:

	payload: json - required
		// Initial request from caller
		query: string - required // Json path query

	options: json - optional
		qds_timeout_sec: float - optional // Query Depth Spreading timeout (whole query timeout will be about twice longer), default = 5
		max_depth: int - optional // default = -1
		bp_timeout_ms: float // Back pressure timeout ms, default = 300

		query_started_nano: int // set by system from initial moment, will be overwritted if received
		call_started_nano: int // set by system for each call, will be overwritted if received
		depth: int // set by system from initial moment, will be overwritted if received
*/

func JPGQLCallTreeResultAggregation(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	callTimeNano := time.Now().UnixNano()

	om := sfMediators.NewOpMediator(ctx)

	// 1. Get query from payload
	jpQuery, err := getQueryFromPayload(ctx)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid query: %s", err.Error()))).Reply()
		return
	}

	// 2. Normalize options
	options := ctx.Options

	qdsTimeoutSec := options.GetByPath("qds_timeout_sec").AsNumericDefault(5)
	options.SetByPath("qds_timeout_sec", easyjson.NewJSON(qdsTimeoutSec))

	maxDepth := int(options.GetByPath("max_depth").AsNumericDefault(-1))
	options.SetByPath("max_depth", easyjson.NewJSON(maxDepth))

	// bp_timeout_ms is kept for compatibility, but is not actually used in this implementation
	bpTimeoutMs := options.GetByPath("bp_timeout_ms").AsNumericDefault(300)
	options.SetByPath("bp_timeout_ms", easyjson.NewJSON(bpTimeoutMs))

	queryStartedNano := int64(options.GetByPath("query_started_nano").AsNumericDefault(0))
	if queryStartedNano == 0 {
		queryStartedNano = callTimeNano
		options.SetByPath("query_started_nano", easyjson.NewJSON(queryStartedNano))
	}

	// 3. Root vertex ID (strip "===..." suffix if present)
	selfTokens := strings.Split(ctx.Self.ID, "===")
	rootVertexID := selfTokens[0]

	currentDomainName := ctx.Domain.Name()

	type stackItem struct {
		vertexID string
		query    string
		depth    int
	}

	// Iterative DFS stack
	stack := []stackItem{
		{
			vertexID: rootVertexID,
			query:    jpQuery,
			depth:    0,
		},
	}

	// 4. Loop protection: "vertex + remaining query" pair
	visited := map[string]struct{}{}
	visited[rootVertexID+"|"+jpQuery] = struct{}{}

	resultUUIDs := map[string]bool{}

	// Stats
	maxDepthReached := 0
	verticesPassed := 0
	skippedByDepth := 0
	skippedByTimeout := 0
	skippedByBP := 0
	var maxBackpressureNano int64 = 0

	for len(stack) > 0 {
		// Check global query timeout
		if qdsTimeoutSec > 0 {
			if time.Now().UnixNano()-queryStartedNano > int64(qdsTimeoutSec*float64(time.Second)) {
				skippedByTimeout++
				break
			}
		}

		// Pop from stack (DFS)
		idx := len(stack) - 1
		item := stack[idx]
		stack = stack[:idx]

		verticesPassed++

		// Depth limit: if current depth > maxDepth, we do not expand from this vertex
		if maxDepth >= 0 && item.depth > maxDepth {
			skippedByDepth++
			continue
		}

		// If query tail is empty, this vertex is already a final match
		if len(item.query) == 0 {
			resultUUIDs[item.vertexID] = true
			continue
		}

		// Parse "head" of the current query tail
		headLinkName, headFilter, tailQuery, anyDepthStop, err := GetQueryHeadAndTailsParts(item.query)
		if err != nil {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(
				fmt.Sprintf("currentObjectLinksQuery is invalid: %s", err.Error()),
			)).Reply()
			return
		}

		// Get neighbors via outgoing links that satisfy the current step of the query
		resultObjects := GetObjectIDsFromLinkNameAndLinkFilterQueryWithAnyDepthStop(
			ctx.Domain.Cache(),
			item.vertexID,
			headLinkName,
			headFilter,
			anyDepthStop,
		)

		if len(resultObjects) == 0 {
			// No neighbors from this vertex for the current query step
			continue
		}

		for targetID, anyDepthStopped := range resultObjects {
			nextQuery := tailQuery
			if anyDepthStopped && anyDepthStop != nil {
				nextQuery = anyDepthStop.QueryTail
			}

			// If query ends here, just add vertex to result
			if len(nextQuery) == 0 {
				resultUUIDs[targetID] = true
				continue
			}

			visitKey := targetID + "|" + nextQuery
			if _, seen := visited[visitKey]; seen {
				// Already visited this vertex with the same query tail — prevent cycles
				continue
			}
			visited[visitKey] = struct{}{}

			targetDomain := ctx.Domain.GetDomainFromObjectID(targetID)
			if targetDomain == "" || targetDomain == currentDomainName {
				// ---------- 5. Target vertex is in the same domain — just push to local DFS stack ----------
				nextDepth := item.depth + 1
				if nextDepth > maxDepthReached {
					maxDepthReached = nextDepth
				}
				stack = append(stack, stackItem{
					vertexID: targetID,
					query:    nextQuery,
					depth:    nextDepth,
				})
			} else {
				// ---------- 6. Cross-domain transition — synchronous recursive JPGQL call ----------
				childPayload := easyjson.NewJSONObject()
				childPayload.SetByPath("query", easyjson.NewJSON(nextQuery))

				childOptions := options.Clone()
				childOptions.SetByPath("depth", easyjson.NewJSON(item.depth+1))
				childOptions.SetByPath("call_started_nano", easyjson.NewJSON(time.Now().UnixNano()))

				// TODO: adjust to your actual ctx.Request signature and reply type.
				// We assume it returns something with a Data field of type easyjson.JSON or *easyjson.JSON.
				sfReply, err := ctx.Request(
					sfPlugins.AutoRequestSelect,
					ctx.Self.Typename,
					targetID+"==="+system.GetUniqueStrID(),
					&childPayload,
					&childOptions,
				)

				if err != nil {
					system.MsgOnErrorReturn(err)
					continue
				}

				subResult := sfReply.GetByPath("data")

				// Merge uuids
				uuidsJSON := subResult.GetByPath("uuids")
				if uuidsJSON.IsNonEmptyObject() {
					for _, id := range uuidsJSON.ObjectKeys() {
						resultUUIDs[id] = true
					}
				}

				// Merge stats
				subStats := subResult.GetByPath("stats")

				verticesPassed += int(subStats.GetByPath("call_tree.vertices_passed").AsNumericDefault(0))

				subMaxDepth := int(subStats.GetByPath("call_tree.max_depth_reached").AsNumericDefault(0))
				globalDepth := item.depth + 1 + subMaxDepth
				if globalDepth > maxDepthReached {
					maxDepthReached = globalDepth
				}

				skippedByDepth += int(subStats.GetByPath("paths_skipped.depth").AsNumericDefault(0))
				skippedByTimeout += int(subStats.GetByPath("paths_skipped.timeout").AsNumericDefault(0))
				skippedByBP += int(subStats.GetByPath("paths_skipped.backpressure").AsNumericDefault(0))

				subMaxBP := int64(subStats.GetByPath("duration.max_backpressure_nano").AsNumericDefault(0))
				if subMaxBP > maxBackpressureNano {
					maxBackpressureNano = subMaxBP
				}
			}
		}
	}

	// 7. Final response object
	qdsEndNano := time.Now().UnixNano()

	result := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.NewJSON(resultUUIDs))
	result.SetByPath("stats.paths_skipped.depth", easyjson.NewJSON(skippedByDepth))
	result.SetByPath("stats.paths_skipped.timeout", easyjson.NewJSON(skippedByTimeout))
	result.SetByPath("stats.paths_skipped.backpressure", easyjson.NewJSON(skippedByBP))
	result.SetByPath("stats.call_tree.vertices_passed", easyjson.NewJSON(verticesPassed))
	result.SetByPath("stats.call_tree.max_depth_reached", easyjson.NewJSON(maxDepthReached))
	result.SetByPath("stats.times.qds_end_nano", easyjson.NewJSON(qdsEndNano))
	result.SetByPath("stats.times.query_start_nano", easyjson.NewJSON(queryStartedNano))
	result.SetByPath("stats.times.query_end_nano", easyjson.NewJSON(callTimeNano))
	result.SetByPath("stats.duration.max_backpressure_nano", easyjson.NewJSON(maxBackpressureNano))
	result.SetByPath("stats.duration.query_nano", easyjson.NewJSON(qdsEndNano-queryStartedNano))
	result.SetByPath("stats.duration.qds_nano", easyjson.NewJSON(qdsEndNano-queryStartedNano))

	om.AggregateOpMsg(sfMediators.OpMsgOk(result)).Reply()
}
