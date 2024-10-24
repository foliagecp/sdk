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

func GraphLinkCreateFromVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, data *easyjson.JSON, opTime int64) {
	opStack := getOpStackFromOptions(ctx.Options)

	var linkBody easyjson.JSON
	if data.GetByPath("body").IsObject() {
		linkBody = data.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	var toId string
	if s, ok := data.GetByPath("to").AsString(); ok && len(s) > 0 {
		toId = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("to is not defined")).Reply()
		return
	}
	toId = ctx.Domain.CreateObjectIDWithThisDomain(toId, false)

	var linkName string
	if s, ok := data.GetByPath("name").AsString(); ok && len(s) > 0 {
		linkName = s
		if !validLinkName.MatchString(linkName) {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
			return
		}
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("name is not defined")).Reply()
		return
	}
	var linkType string
	if s, ok := data.GetByPath("type").AsString(); ok && len(s) > 0 {
		linkType = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("type is not defined")).Reply()
		return
	}

	if !FixateOperationIdTime(ctx, fmt.Sprintf("link-%s", linkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	// Check if link with this name already exists --------------
	_, recordTime, err := ctx.Domain.Cache().GetValueWithRecordTime(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if opTime != recordTime && err == nil { // Only our time makes us go further (operation was not completed previously)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s already exists", ctx.Self.ID, linkName))).Reply()
		return
	}
	// ----------------------------------------------------------
	// Check if link with this type "type" to "to" already exists
	_, recordTime, err = ctx.Domain.Cache().GetValueWithRecordTime(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId))
	if opTime != recordTime && err == nil {
		om.AggregateOpMsg(
			sfMediators.OpMsgFailed(
				fmt.Sprintf("link from=%s with name=%s to=%s with type=%s already exists, two vertices can have a link with this type and direction only once", ctx.Self.ID, linkName, toId, linkType),
			),
		).Reply()
		return
	}
	// -----------------------------------------------------------

	// Create out link on this vertex -------------------------
	// Set link target ------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), []byte(fmt.Sprintf("%s.%s", linkType, toId)), opTime) // Store link body in KV
	// ----------------------------------
	// Set link body --------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), linkBody.ToBytes(), opTime) // Store link body in KV
	// ----------------------------------
	// Set link type --------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId), []byte(linkName), opTime) // Store link type
	// ----------------------------------
	// Index link type ------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "type", linkType), nil, opTime)
	// ----------------------------------
	// Index link tags ------------------
	if data.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := data.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", linkTag), nil, opTime)
			}
		}
	}
	// ----------------------------------
	// --------------------------------------------------------

	// Create this vertex if does not exist ----------------------
	_, err = ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil {
		createVertexPayload := easyjson.NewJSONObjectWithKeyValue("target", easyjson.NewJSON("vertex"))
		createVertexPayload.SetByPath("operation", easyjson.NewJSON("create"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Caller.Typename, ctx.Self.ID, &createVertexPayload, ctx.Options)
	}
	// -----------------------------------------------------------
	// Create in link on descendant vertex --------------------
	inLinkPayload := easyjson.NewJSONObjectWithKeyValue("target", easyjson.NewJSON("link"))
	inLinkPayload.SetByPath("operation", easyjson.NewJSON("create"))
	inLinkPayload.SetByPath("data.in_name", easyjson.NewJSON(linkName))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, toId, &inLinkPayload, ctx.Options)
	// --------------------------------------------------------

	addLinkOpToOpStack(opStack, "create", ctx.Self.ID, toId, linkName, linkType, nil, &linkBody)
	om.AddIntermediateResult(ctx, resultWithOpStack(nil, opStack).GetPtr())
}

func GraphLinkCreateToVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, linkFromVertexUUID, inLinkName string, data *easyjson.JSON, opTime int64) {
	if !FixateOperationIdTime(ctx, fmt.Sprintf("inlink-%s", inLinkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkFromVertexUUID, inLinkName), nil, opTime)

	// Create this vertex if does not exist ----------------------
	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil {
		createVertexPayload := easyjson.NewJSONObjectWithKeyValue("target", easyjson.NewJSON("vertex"))
		createVertexPayload.SetByPath("operation", easyjson.NewJSON("create"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.cud", ctx.Self.ID, &createVertexPayload, ctx.Options)
		return
	}
	// -----------------------------------------------------------

	om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
}

func getLinkNameTypeTargetFromVariousIdentifiers(ctx *sfPlugins.StatefunContextProcessor, payloadContainer string) (linkName string, linkType string, linkTargetId string, err error) {
	prefix := ""
	if len(payloadContainer) > 0 {
		prefix = payloadContainer + "."
	}

	linkName = ctx.Payload.GetByPath(prefix + "name").AsStringDefault("")
	linkType = ctx.Payload.GetByPath(prefix + "type").AsStringDefault("")
	linkTargetId = ctx.Domain.CreateObjectIDWithThisDomain(ctx.Payload.GetByPath(prefix+"to").AsStringDefault(""), false)

	if len(linkName) > 0 {
		linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
		if err != nil {
			return "", "", "", fmt.Errorf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName)
		}
		linkTargetStr := string(linkTargetBytes)
		linkTargetTokens := strings.Split(linkTargetStr, ".")
		if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
			return "", "", "", fmt.Errorf("link from=%s with name=%s, has invalid target: %s", ctx.Self.ID, linkName, linkTargetStr)
		}
		return linkName, linkTargetTokens[0], linkTargetTokens[1], nil
	} else {
		if len(linkTargetId) > 0 {
			if len(linkType) > 0 {
				linkNameBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, linkTargetId))
				if err != nil {
					return "", "", "", fmt.Errorf("link from=%s to=%s with type=%s does not exist", ctx.Self.ID, linkTargetId, linkType)
				}
				return string(linkNameBytes), linkType, linkTargetId, nil
			}
		}
	}
	return "", "", "", fmt.Errorf("not enough information about link, link name or link type with link target id are needed")
}

func GraphLinkUpdate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, data *easyjson.JSON, opTime int64) {
	opStack := getOpStackFromOptions(ctx.Options)

	upsert := data.GetByPath("upsert").AsBoolDefault(false)
	replace := data.GetByPath("replace").AsBoolDefault(false)

	var linkName string
	var linkTarget string
	var linkType string
	if lname, ltype, ltarget, err := getLinkNameTypeTargetFromVariousIdentifiers(ctx, "data"); err == nil {
		linkName = lname
		linkType = ltype
		linkTarget = ltarget
	} else {
		if upsert {
			createLinkPayload := easyjson.NewJSONObjectWithKeyValue("target", easyjson.NewJSON("link"))
			createLinkPayload.SetByPath("operation", easyjson.NewJSON("create"))
			createLinkPayload.SetByPath("data.body", data.GetByPath("body"))
			createLinkPayload.SetByPath("data.to", data.GetByPath("to"))
			createLinkPayload.SetByPath("data.name", data.GetByPath("name"))
			createLinkPayload.SetByPath("data.type", data.GetByPath("type"))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, ctx.Self.ID, &createLinkPayload, ctx.Options)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgIdle(err.Error())).Reply()
		}
		return
	}
	if !validLinkName.MatchString(linkName) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}
	if !FixateOperationIdTime(ctx, fmt.Sprintf("link-%s", linkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	oldLinkBody, err1 := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err1 != nil { // Link's body does not exist
		oldLinkBody = easyjson.NewJSONObject().GetPtr()
	}

	var linkBody easyjson.JSON
	if data.GetByPath("body").IsObject() {
		linkBody = data.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	if replace {
		// Remove all indices -----------------------------
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValueKVSync(indexKey, -1)
		}
		// ------------------------------------------------
	} else { // merge
		newBody := oldLinkBody.Clone().GetPtr()
		newBody.DeepMerge(linkBody)
		linkBody = *newBody
	}

	// Create out link on this vertex -------------------------
	// Set link body --------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), linkBody.ToBytes(), opTime) // Store link body in KV
	// ----------------------------------
	// Index link type ------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "type", linkType), nil, opTime)
	// ----------------------------------
	// Index link tags ------------------
	if data.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := data.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", linkTag), nil, opTime)
			}
		}
	}
	// ----------------------------------
	// --------------------------------------------------------
	addLinkOpToOpStack(opStack, "update", ctx.Self.ID, linkTarget, linkName, linkType, oldLinkBody, &linkBody)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

func GraphLinkDeleteFromVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, data *easyjson.JSON, opTime int64) {
	opStack := getOpStackFromOptions(ctx.Options)

	var linkName string
	var linkTarget string
	var linkType string
	if lname, ltype, ltarget, err := getLinkNameTypeTargetFromVariousIdentifiers(ctx, "data"); err == nil {
		linkName = lname
		linkType = ltype
		linkTarget = ltarget
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}
	if !validLinkName.MatchString(linkName) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}
	if !FixateOperationIdTime(ctx, fmt.Sprintf("link-%s", linkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	oldLinkBody, err1 := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err1 != nil { // Link's body does not exist
		oldLinkBody = easyjson.NewJSONObject().GetPtr()
	}

	// Remove all indices -----------------------------
	indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkName, ">"))
	for _, indexKey := range indexKeys {
		ctx.Domain.Cache().DeleteValueKVSync(indexKey, -1)
	}
	// ------------------------------------------------

	// Delete link type -----------------
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, linkTarget), -1)
	// ----------------------------------
	// Delete link body -----------------
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), -1)
	// ----------------------------------
	// Delete link target ---------------
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), -1)
	// ----------------------------------

	addLinkOpToOpStack(opStack, "delete", ctx.Self.ID, linkTarget, linkName, linkType, oldLinkBody, nil)

	// Delete in link on descendant vertex --------------------
	inLinkPayload := easyjson.NewJSONObjectWithKeyValue("target", easyjson.NewJSON("link"))
	inLinkPayload.SetByPath("operation", easyjson.NewJSON("delete"))
	inLinkPayload.SetByPath("data.in_name", easyjson.NewJSON(linkName))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, linkTarget, &inLinkPayload, ctx.Options)
	// --------------------------------------------------------

	om.AddIntermediateResult(ctx, resultWithOpStack(nil, opStack).GetPtr())
}

func GraphLinkDeleteToVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, linkFromVertexUUID, inLinkName string, data *easyjson.JSON, opTime int64) {
	if !FixateOperationIdTime(ctx, fmt.Sprintf("inlink-%s", inLinkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkFromVertexUUID, inLinkName), -1)
	om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
}

func GraphLinkCUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64) {
	data := ctx.Payload.GetByPath("data")

	switch strings.ToLower(operation) {
	case "create":
		inName := data.GetByPath("in_name").AsStringDefault("")
		if len(ctx.Caller.ID) > 0 && len(inName) > 0 {
			GraphLinkCreateToVertex(ctx, om, ctx.Caller.ID, inName, &data, opTime)
		} else {
			GraphLinkCreateFromVertex(ctx, om, &data, opTime)
		}
	case "update":
		GraphLinkUpdate(ctx, om, &data, opTime)
	case "delete":
		inName := data.GetByPath("in_name").AsStringDefault("")
		if len(ctx.Caller.ID) > 0 && len(inName) > 0 {
			GraphLinkDeleteToVertex(ctx, om, ctx.Caller.ID, inName, &data, opTime)
		} else {
			GraphLinkDeleteFromVertex(ctx, om, &data, opTime)
		}
	default:

	}
}

/*
Graph vertices link Create, Update, Delete function.
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
			target: string - required // "link"
			operation: string - required // "create", "update", "delete"
			op_stack: json array - optionall
*/
func GraphLinkCUD(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string) {
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
			GraphLinkCUD_Dispatcher(ctx, om, operation, system.Str2Int(optTimeStr))
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("GraphLinkCUD operation processor recevied no op_time")).Reply()
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
		var immediateAggregationResult easyjson.JSON = easyjson.NewJSONObject()
		if aggregatedOpStack.IsNonEmptyArray() {
			immediateAggregationResult = easyjson.NewJSONObjectWithKeyValue("op_stack", aggregatedOpStack)
		}
		immediateAggregationResult.SetByPath("target", easyjson.NewJSON("link"))
		immediateAggregationResult.SetByPath("operation", easyjson.NewJSON(operation))
		system.MsgOnErrorReturn(om.ReplyWithData(&immediateAggregationResult))
	}
}
