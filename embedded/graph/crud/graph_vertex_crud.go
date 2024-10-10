package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/mediator"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func GraphVertexCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, data *easyjson.JSON, opTime int64) {
	opStack := getOpStackFromOptions(ctx.Options)

	var vertexBody easyjson.JSON
	if data.GetByPath("body").IsObject() {
		vertexBody = data.GetByPath("body")
	} else {
		vertexBody = easyjson.NewJSONObject()
	}

	_, recordTime, err := ctx.Domain.Cache().GetValueWithRecordTime(ctx.Self.ID)
	if opTime != recordTime && err == nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s already exists", ctx.Self.ID))).Reply()
		return
	}

	// Set vertex body ------------------
	ctx.Domain.Cache().SetValueKVSync(ctx.Self.ID, vertexBody.ToBytes(), opTime) // Store vertex body in KV
	// ----------------------------------

	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, nil, &vertexBody)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

func GraphVertexCUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64) {
	operation := ctx.Payload.GetByPath("operation").AsStringDefault("")

	switch strings.ToLower(operation) {
	case "create":
		data := ctx.Payload.GetByPath("data")
		GraphVertexCreate(ctx, om, &data, opTime)
	default:

	}
}

/*
Graph vertex Create, Update, Delete function.
This function works via signals and request-reply.

Request:

	payload: json - optional
		operation: string - requred // supported values (case insensitive): "create", "update", "delete"
		data: json - required // operation data

	options: json - optional
		return_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string - optional, if any exists
		data: json - optional, id any exists
			op_stack: json array - optional
*/
func GraphVertexCUD(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	switch om.GetOpType() {
	case mediator.MereOp:
		if len(ctx.Options.GetByPath("op_time").AsStringDefault("")) == 0 {
			forwardOptions := ctx.Options.Clone()
			forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, ctx.Self.ID, ctx.Payload, &forwardOptions)
			return
		}
		fallthrough
	case mediator.WorkerIsTaskedByAggregatorOp:
		optTimeStr := ctx.Options.GetByPath("op_time").AsStringDefault("")
		if len(optTimeStr) > 0 {
			GraphVertexCUD_Dispatcher(ctx, om, system.Str2Int(optTimeStr))
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("GraphVertexCUD operation processor recevied no op_time")).Reply()
		}
	case mediator.AggregatedWorkersOp:
		aggregatedOpStack := easyjson.NewJSONNull()
		for _, opMsg := range om.GetAggregatedOpMsgs() {
			if opMsg.Data.PathExists("op_stack") {
				if aggregatedOpStack.IsNull() {
					aggregatedOpStack = opMsg.Data.GetByPath("op_stack").Clone()
				} else {
					aggregatedOpStack.DeepMerge(opMsg.Data.GetByPath("op_stack"))
				}
			}
		}
		immediateAggregationResult := easyjson.NewJSONObjectWithKeyValue("op_stack", aggregatedOpStack)
		system.MsgOnErrorReturn(om.ReplyWithData(immediateAggregationResult.GetPtr()))
	}
}
