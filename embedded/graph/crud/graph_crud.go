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

func GraphAsyncLinkCreateFromVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, data *easyjson.JSON) {
	opStack := getOpStackFromOptions(ctx.Options)

	var linkBody easyjson.JSON
	if data.GetByPath("body").IsObject() {
		linkBody = data.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	var toId string
	if s, ok := data.GetByPath("to").AsString(); ok {
		toId = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("to is not defined")).Reply()
		return
	}
	toId = ctx.Domain.CreateObjectIDWithThisDomain(toId, false)

	var linkName string
	if s, ok := data.GetByPath("name").AsString(); ok {
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
	if s, ok := data.GetByPath("type").AsString(); ok {
		linkType = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("type is not defined")).Reply()
		return
	}

	// Check if link with this name already exists --------------
	_, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err == nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s already exists", ctx.Self.ID, linkName))).Reply()
		return
	}
	// ----------------------------------------------------------
	// Check if link with this type "type" to "to" already exists
	_, err = ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId))
	if err == nil {
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
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), []byte(fmt.Sprintf("%s.%s", linkType, toId)), true, -1, "") // Store link body in KV
	// ----------------------------------
	// Set link body --------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), linkBody.ToBytes(), true, -1, "") // Store link body in KV
	// ----------------------------------
	// Set link type --------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId), []byte(linkName), true, -1, "") // Store link type
	// ----------------------------------
	// Index link type ------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "type", linkType), nil, true, -1, "")
	// ----------------------------------
	// Index link tags ------------------
	if data.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := data.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", linkTag), nil, true, -1, "")
			}
		}
	}
	// ----------------------------------
	// --------------------------------------------------------

	addLinkOpToOpStack(opStack, "create", ctx.Self.ID, toId, linkName, linkType, nil, &linkBody)

	// Create in link on descendant vertex --------------------
	inLinkPayload := easyjson.NewJSONObjectWithKeyValue("operation", easyjson.NewJSON("create"))
	inLinkPayload.SetByPath("data.in_name", easyjson.NewJSON(linkName))
	linkMeErr := om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, toId, &inLinkPayload, ctx.Options)
	if linkMeErr != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}
	// --------------------------------------------------------

	om.AddIntermediateResult(ctx, resultWithOpStack(nil, opStack).GetPtr())
}

func GraphAsyncLinkCreateToVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, data *easyjson.JSON) {
	if inLinkName, ok := data.GetByPath("in_name").AsString(); ok && len(inLinkName) > 0 {
		if linkFromObjectUUID := ctx.Caller.ID; len(linkFromObjectUUID) > 0 {
			ctx.Domain.Cache().SetValue(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkFromObjectUUID, inLinkName), nil, true, -1, "")
			om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
			return
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("caller id is not defined, no source vertex id")).Reply()
			return
		}
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("in_name is not defined")).Reply()
		return
	}
}

func GraphAsyncLinkCUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator) {
	operation := ctx.Payload.GetByPath("operation").AsStringDefault("")

	switch strings.ToLower(operation) {
	case "create":
		data := ctx.Payload.GetByPath("data")
		if data.PathExists("in_name") {
			GraphAsyncLinkCreateToVertex(ctx, om, &data)
		} else {
			GraphAsyncLinkCreateFromVertex(ctx, om, &data)
		}
	default:

	}
}

/*
This function works only in async mode via pub calls.

Request:

	payload: json - optional
		operation: string - requred // supported values (case insensitive): "create", "update", "delete"
		data: json -

		// Initial request from caller:
		body: json - optional // Body for vertex to be created with.
			<key>: <type> - optional // Any additional key and value to be stored in objects's body.

	options: json - optional

		return_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			op_stack: json array - optional
*/
func GraphAsyncLinkCUD(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	switch om.GetOpType() {
	case mediator.MereOp: // Initial call from client
		fallthrough
	case mediator.WorkerIsTaskedByAggregatorOp:
		GraphAsyncLinkCUD_Dispatcher(ctx, om)
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
