

// Foliage graph store jpgql package.
// Provides stateful functions of json-path graph query language for the graph store
package jpgql

import (
	"fmt"
	"strings"

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
*/

func JPGQLCallTreeResultAggregation(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
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
		system.MsgOnErrorReturn(om.SignalWithAggregation(sfPlugins.JetstreamGlobalSignal, ctx.Self.Typename, vId+"==="+pId, ctx.Payload, ctx.Options))
	case mediator.WorkerIsTaskedByAggregatorOp:
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
					err := om.SignalWithAggregation(sfPlugins.JetstreamGlobalSignal, ctx.Self.Typename, objectID+"==="+pId, &workerPayload, nil)
					if err != nil {
						om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
						return
					}
					workersToAggregateFrom++
				}
			}
			if workersToAggregateFrom == 0 {
				om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSON(objectsToReturnAsAResult))).Reply()
				return
			} else {
				om.AddIntermediateResult(ctx, easyjson.NewJSON(objectsToReturnAsAResult).GetPtr())
			}
		}
	case mediator.AggregatorRepliedByWorkerOp:
	case mediator.AggregatedWorkersOp:
		aggregatedResult := map[string]bool{}
		for _, opMsg := range om.GetAggregatedOpMsgs() {
			for _, objectId := range opMsg.Data.ObjectKeys() {
				aggregatedResult[objectId] = true
			}
		}
		immediateAggregationResult := easyjson.NewJSON(aggregatedResult)
		system.MsgOnErrorReturn(om.ReplyWithData(immediateAggregationResult.GetPtr()))
	}
}
