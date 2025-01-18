

// Foliage graph store jpgql package.
// Provides stateful functions of json-path graph query language for the graph store
package jpgql

import (
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/sdk/statefun/mediator"
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
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false).SetMaxIdHandlers(-1),
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
		qds_timeout_sec: float - optional // Query Depth Spreading timeout (whole query timeout will be twice longer), default = 5
		max_depth: int - optional // default = -1
		started_nano: int // set by system from initial moment, will be overwritted if received
		depth: int // set by system from initial moment, will be overwritted if received
*/

func JPGQLCallTreeResultAggregation(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	createReturnData := func(uuids map[string]bool, currentDepth, skippedByDepth, skippedByTimeout, verticesPassed int, queryStartTimeNano int64) easyjson.JSON {
		resultUUIDS := map[string]bool{}
		if uuids != nil {
			resultUUIDS = uuids
		}
		returnData := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.NewJSON(resultUUIDS))
		returnData.SetByPath("map_reduce_stats.max_depth_reached", easyjson.NewJSON(currentDepth))
		returnData.SetByPath("map_reduce_stats.paths_skipped_by_depth", easyjson.NewJSON(skippedByDepth))
		returnData.SetByPath("map_reduce_stats.paths_skipped_by_timeout", easyjson.NewJSON(skippedByTimeout))
		returnData.SetByPath("map_reduce_stats.vertices_passed", easyjson.NewJSON(verticesPassed))
		returnData.SetByPath("map_reduce_stats.qds_end_time_nano", easyjson.NewJSON(time.Now().UnixNano()))
		returnData.SetByPath("map_reduce_stats.query_start_time_nano", easyjson.NewJSON(queryStartTimeNano))
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
		retMDR := returnData.GetByPath("map_reduce_stats.max_depth_reached").AsNumericDefault(0)
		aggMDR := aggregatedReturnData.GetByPath("map_reduce_stats.max_depth_reached").AsNumericDefault(0)
		if aggMDR > retMDR {
			retMDR = aggMDR
		}

		retPSBD := returnData.GetByPath("map_reduce_stats.paths_skipped_by_depth").AsNumericDefault(0)
		aggPSBD := aggregatedReturnData.GetByPath("map_reduce_stats.paths_skipped_by_depth").AsNumericDefault(0)
		retPSBD += aggPSBD

		retPSBT := returnData.GetByPath("map_reduce_stats.paths_skipped_by_timeout").AsNumericDefault(0)
		aggPSBT := aggregatedReturnData.GetByPath("map_reduce_stats.paths_skipped_by_timeout").AsNumericDefault(0)
		retPSBT += aggPSBT

		retVP := returnData.GetByPath("map_reduce_stats.vertices_passed").AsNumericDefault(0)
		aggVP := aggregatedReturnData.GetByPath("map_reduce_stats.vertices_passed").AsNumericDefault(0)
		retVP += aggVP

		retQETN := returnData.GetByPath("map_reduce_stats.qds_end_time_nano").AsNumericDefault(0)
		aggQETN := aggregatedReturnData.GetByPath("map_reduce_stats.qds_end_time_nano").AsNumericDefault(0)
		if aggQETN > retQETN {
			retQETN = aggQETN
		}

		aggQSTN := aggregatedReturnData.GetByPath("map_reduce_stats.query_start_time_nano").AsNumericDefault(0)
		// ------------------------------------------------

		thisTimeNano := time.Now().UnixNano()

		newReturnData := easyjson.NewJSONObjectWithKeyValue("uuids", resultUUIDs)
		newReturnData.SetByPath("map_reduce_stats.max_depth_reached", easyjson.NewJSON(retMDR))
		newReturnData.SetByPath("map_reduce_stats.paths_skipped_by_depth", easyjson.NewJSON(retPSBD))
		newReturnData.SetByPath("map_reduce_stats.paths_skipped_by_timeout", easyjson.NewJSON(retPSBT))
		newReturnData.SetByPath("map_reduce_stats.vertices_passed", easyjson.NewJSON(retVP))
		newReturnData.SetByPath("map_reduce_stats.qds_end_time_nano", easyjson.NewJSON(retQETN))
		newReturnData.SetByPath("map_reduce_stats.query_start_time_nano", easyjson.NewJSON(aggQSTN))
		newReturnData.SetByPath("map_reduce_stats.query_end_time_nano", easyjson.NewJSON(thisTimeNano))

		newReturnData.SetByPath("map_reduce_stats.query_duration_nano", easyjson.NewJSON(thisTimeNano-int64(aggQSTN)))
		newReturnData.SetByPath("map_reduce_stats.qds_duration_nano", easyjson.NewJSON(retQETN-aggQSTN))
		newReturnData.SetByPath("map_reduce_stats.agg_duration_nano", easyjson.NewJSON(thisTimeNano-int64(retQETN)))

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
		newOptions.SetByPath("started_nano", easyjson.NewJSON(time.Now().UnixNano()))
		queryDepthSpreadTimeoutSec := ctx.Options.GetByPath("qds_timeout_sec").AsNumericDefault(5)
		newOptions.SetByPath("qds_timeout_sec", easyjson.NewJSON(queryDepthSpreadTimeoutSec))
		newOptions.SetByPath("depth", easyjson.NewJSON(0))
		system.MsgOnErrorReturn(om.SignalWithAggregation(sfPlugins.JetstreamGlobalSignal, ctx.Self.Typename, vId+"==="+pId, ctx.Payload, newOptions))
	case mediator.WorkerIsTaskedByAggregatorOp:
		maxDepth := int(ctx.Options.GetByPath("max_depth").AsNumericDefault(-1))
		currentDepth := int(ctx.Options.GetByPath("depth").AsNumericDefault(0))
		qdsTimeoutSec := float64(ctx.Options.GetByPath("qds_timeout_sec").AsNumericDefault(5))
		startedNano := int64(ctx.Options.GetByPath("started_nano").AsNumericDefault(-1))

		// Checking limits --------------------------------------------------------------
		if maxDepth >= 0 && currentDepth >= maxDepth { // Will be cheking at depth + 1 if this condition returns false, thus currentDepth !!!>=!!! maxDepth
			data := createReturnData(nil, currentDepth, 1, 0, 0, startedNano)
			om.AggregateOpMsg(sfMediators.OpMsgOk(data)).Reply()
			return
		}
		if startedNano > 0 {
			if time.Now().UnixNano()-startedNano > int64(qdsTimeoutSec*float64(time.Second)) {
				data := createReturnData(nil, currentDepth, 0, 1, 0, startedNano)
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
				data := createReturnData(objectsToReturnAsAResult, currentDepth+1, 0, 0, checkedVerticesInThisCall, startedNano)
				om.AggregateOpMsg(sfMediators.OpMsgOk(data)).Reply()
				return
			} else { // This vertex' children SOME are leafs against query and SOME are tasked so this one waits for AggregatedWorkersOp
				checkedVerticesInThisCall := 1 + (len(resultObjects) - workersToAggregateFrom) // This vertex + all vertices-leafs
				data := createReturnData(objectsToReturnAsAResult, currentDepth+1, 0, 0, checkedVerticesInThisCall, startedNano)
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
}
