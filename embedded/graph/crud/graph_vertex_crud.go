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

	addVertexOpToOpStack(opStack, "create", ctx.Self.ID, nil, &vertexBody)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

func GraphVertexUpdate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, data *easyjson.JSON, opTime int64) {
	opStack := getOpStackFromOptions(ctx.Options)

	upsert := data.GetByPath("upsert").AsBoolDefault(false)
	replace := data.GetByPath("replace").AsBoolDefault(false)

	if !upsert && (!data.PathExists("body") || !data.GetByPath("body").IsObject()) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("new body should be passed and must be a valid json-object when upsert==false"))).Reply()
		return
	}

	oldBody, _, err := ctx.Domain.Cache().GetValueWithRecordTimeAsJSON(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		if upsert {
			createVertexPayload := easyjson.NewJSONObjectWithKeyValue("operation", easyjson.NewJSON("create"))
			createVertexPayload.SetByPath("data.body", data.GetByPath("body"))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Caller.Typename, ctx.Self.ID, &createVertexPayload, ctx.Options)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist, nothing to update", ctx.Self.ID))).Reply()
		}
		return
	}

	body := data.GetByPath("body")
	if !replace { // merge
		newBody := oldBody.Clone().GetPtr()
		newBody.DeepMerge(body)
		body = *newBody
	}

	ctx.Domain.Cache().SetValueKVSync(ctx.Self.ID, body.ToBytes(), opTime) // Store vertex body in KV
	addVertexOpToOpStack(opStack, "update", ctx.Self.ID, oldBody, &body)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

func GraphVertexDelete(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, data *easyjson.JSON, opTime int64) {
	opStack := getOpStackFromOptions(ctx.Options)

	oldBody, _, err := ctx.Domain.Cache().GetValueWithRecordTimeAsJSON(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	ctx.Domain.Cache().DeleteValueKVSync(ctx.Self.ID, -1) // Delete vertex's body
	addVertexOpToOpStack(opStack, "delete", ctx.Self.ID, oldBody, nil)

	waiting4Aggregation := false
	// Delete all out links -------------------------------
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		inLinkPayload := easyjson.NewJSONObjectWithKeyValue("operation", easyjson.NewJSON("delete"))
		inLinkPayload.SetByPath("data.name", easyjson.NewJSON(linkName))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.link.cud", ctx.Self.ID, &inLinkPayload, ctx.Options)
		waiting4Aggregation = true
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		inLinkPayload := easyjson.NewJSONObjectWithKeyValue("operation", easyjson.NewJSON("delete"))
		inLinkPayload.SetByPath("data.name", easyjson.NewJSON(linkName))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.link.cud", fromObjectID, &inLinkPayload, ctx.Options)
		waiting4Aggregation = true
	}
	// ----------------------------------------------------

	if waiting4Aggregation {
		om.AddIntermediateResult(ctx, resultWithOpStack(nil, opStack).GetPtr())
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
	}
}

func GraphVertexCUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64) {
	operation := ctx.Payload.GetByPath("operation").AsStringDefault("")
	data := ctx.Payload.GetByPath("data")

	switch strings.ToLower(operation) {
	case "create":
		GraphVertexCreate(ctx, om, &data, opTime)
	case "update":
		GraphVertexUpdate(ctx, om, &data, opTime)
	case "delete":
		GraphVertexDelete(ctx, om, &data, opTime)
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
		var immediateAggregationResult *easyjson.JSON = nil
		if aggregatedOpStack.IsNonEmptyArray() {
			immediateAggregationResult = easyjson.NewJSONObjectWithKeyValue("op_stack", aggregatedOpStack).GetPtr()
		}
		system.MsgOnErrorReturn(om.ReplyWithData(immediateAggregationResult))
	}
}

func GraphVertexRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	opStack := getOpStackFromOptions(ctx.Options)

	body, _, err := ctx.Domain.Cache().GetValueWithRecordTimeAsJSON(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	result := easyjson.NewJSONObjectWithKeyValue("body", *body)
	if ctx.Payload.GetByPath("details").AsBoolDefault(false) {
		outLinkNames := []string{}
		outLinkTypes := []string{}
		outLinkIds := []string{}
		outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
		for _, outLinkKey := range outLinkKeys {
			linkKeyTokens := strings.Split(outLinkKey, ".")
			linkName := linkKeyTokens[len(linkKeyTokens)-1]
			outLinkNames = append(outLinkNames, linkName)

			linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
			if err == nil {
				tokens := strings.Split(string(linkTargetBytes), ".")
				if len(tokens) == 2 {
					outLinkTypes = append(outLinkTypes, tokens[0])
					outLinkIds = append(outLinkIds, tokens[1])
				}
			}
		}
		result.SetByPath("links.out.names", easyjson.JSONFromArray(outLinkNames))
		result.SetByPath("links.out.types", easyjson.JSONFromArray(outLinkTypes))
		result.SetByPath("links.out.ids", easyjson.JSONFromArray(outLinkIds))

		inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
		inLinks := easyjson.NewJSONArray()
		for _, inLinkKey := range inLinkKeys {
			linkKeyTokens := strings.Split(inLinkKey, ".")
			linkName := linkKeyTokens[len(linkKeyTokens)-1]
			linkFromVId := linkKeyTokens[len(linkKeyTokens)-2]
			inLinkJson := easyjson.NewJSONObjectWithKeyValue("from", easyjson.NewJSON(linkFromVId))
			inLinkJson.SetByPath("name", easyjson.NewJSON(linkName))
			inLinks.AddToArray(inLinkJson)
		}
		result.SetByPath("links.in", inLinks)
	}
	addVertexOpToOpStack(opStack, "read", ctx.Self.ID, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(result.GetPtr(), opStack))).Reply()
}
