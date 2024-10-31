package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func GraphVertexRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	opStack := getOpStackFromOptions(ctx.Options)

	body, _, err := ctx.Domain.Cache().GetValueWithRecordTimeAsJSON(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	result := easyjson.NewJSONObjectWithKeyValue("body", *body)
	if data.GetByPath("details").AsBoolDefault(false) {
		outLinkNames := []string{}
		outLinkTypes := []string{}
		outLinkUUIDs := []string{}
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
					outLinkUUIDs = append(outLinkUUIDs, tokens[1])
				}
			}
		}
		result.SetByPath("links.out.names", easyjson.JSONFromArray(outLinkNames))
		result.SetByPath("links.out.types", easyjson.JSONFromArray(outLinkTypes))
		result.SetByPath("links.out.uuids", easyjson.JSONFromArray(outLinkUUIDs))

		inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))

		inLinkNames := []string{}
		inLinkTypes := []string{}
		inLinkUUIDs := []string{}
		for _, inLinkKey := range inLinkKeys {
			if inLinkTypeBytes, err := ctx.Domain.Cache().GetValue(inLinkKey); err == nil {
				linkKeyTokens := strings.Split(inLinkKey, ".")

				linkType := string(inLinkTypeBytes)
				linkName := linkKeyTokens[len(linkKeyTokens)-1]
				linkFromVId := linkKeyTokens[len(linkKeyTokens)-2]

				inLinkNames = append(inLinkNames, linkName)
				inLinkTypes = append(inLinkTypes, linkType)
				inLinkUUIDs = append(inLinkUUIDs, linkFromVId)
			}
		}
		result.SetByPath("links.in.names", easyjson.JSONFromArray(inLinkNames))
		result.SetByPath("links.in.types", easyjson.JSONFromArray(inLinkTypes))
		result.SetByPath("links.in.uuids", easyjson.JSONFromArray(inLinkUUIDs))
	}
	addVertexOpToOpStack(opStack, "read", ctx.Self.ID, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(result.GetPtr(), opStack))).Reply()
}

func GraphVertexCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	opStack := getOpStackFromOptions(ctx.Options)

	if !FixateOperationIdTime(ctx, "", opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

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

func GraphVertexUpdate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	opStack := getOpStackFromOptions(ctx.Options)

	if !FixateOperationIdTime(ctx, "", opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	upsert := data.GetByPath("upsert").AsBoolDefault(false)
	replace := data.GetByPath("replace").AsBoolDefault(false)

	if !upsert && (!data.PathExists("body") || !data.GetByPath("body").IsObject()) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("new body should be passed and must be a valid json-object when upsert==false"))).Reply()
		return
	}

	oldBody, _, err := ctx.Domain.Cache().GetValueWithRecordTimeAsJSON(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		if upsert {
			createVertexPayload := easyjson.NewJSONObject()
			createVertexPayload.SetByPath("operation.type", easyjson.NewJSON("create"))
			createVertexPayload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
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

func GraphVertexDelete(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, _ *easyjson.JSON) {
	opStack := getOpStackFromOptions(ctx.Options)

	if !FixateOperationIdTime(ctx, "", opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

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

		inLinkPayload := easyjson.NewJSONObject()
		inLinkPayload.SetByPath("operation.type", easyjson.NewJSON("delete"))
		inLinkPayload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		inLinkPayload.SetByPath("data.name", easyjson.NewJSON(linkName))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &inLinkPayload, ctx.Options)
		waiting4Aggregation = true
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		inLinkPayload := easyjson.NewJSONObject()
		inLinkPayload.SetByPath("operation.type", easyjson.NewJSON("delete"))
		inLinkPayload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		inLinkPayload.SetByPath("data.name", easyjson.NewJSON(linkName))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", fromObjectID, &inLinkPayload, ctx.Options)
		waiting4Aggregation = true
	}
	// ----------------------------------------------------

	if waiting4Aggregation {
		om.AddIntermediateResult(ctx, resultWithOpStack(nil, opStack).GetPtr())
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
	}
}

func GraphVertexCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch strings.ToLower(operation) {
	case "create":
		GraphVertexCreate(ctx, om, opTime, data)
	case "update":
		GraphVertexUpdate(ctx, om, opTime, data)
	case "delete":
		GraphVertexDelete(ctx, om, opTime, data)
	case "read":
		GraphVertexRead(ctx, om, opTime, data)
	default:
		// TODO: Return error msg
	}
}
