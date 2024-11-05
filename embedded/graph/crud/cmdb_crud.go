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

type CMDB_CRUDDispatcher func(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON)

var (
	CMDB_CRUDDispatcherFromTarget = map[string]CMDB_CRUDDispatcher{
		"type":            CMDBTypeCRUD_Dispatcher,
		"type.relation":   CMDBTypeRelationCRUD_Dispatcher,
		"object":          CMDBObjectCRUD_Dispatcher,
		"object.relation": CMDBObjectRelationCRUD_Dispatcher,
	}
)

/*
CMDB_CRUDGateway. Garanties sequential order for all graph api calls
This function works via signals and request-reply.

Request:

	payload: json - optional
		operation: json
			type: string - requred // supported values (case insensitive): "create", "update", "delete", "read"
			target: string - requred // supported values (case insensitive): "type", "type.relation" "object", "object.relation"
		data: json - required // operation data

	options: json - optional
		retries: int - optional // retries on execution failure, default: 20

Reply:

	payload: json
		status: string
		details: string - optional, if any exists
		data: json - optional, id any exists
			operation: json
				type: string - required // "create", "update", "delete", "read"
				target: string - required // "type", "type.relation" "object", "object.relation"
*/
func CMDB_CRUDGateway(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", ctx.Self.ID, om.GetID(), ctx.Payload.ToString())

	meta := om.GetMeta(ctx)

	target := strings.ToLower(meta.GetByPath("operation.target").AsStringDefault(""))
	operation := strings.ToLower(meta.GetByPath("operation.type").AsStringDefault(""))

	if len(target) == 0 {
		if len(target) == 0 {
			target = ctx.Payload.GetByPath("operation.target").AsStringDefault("")
		}
		if len(operation) == 0 {
			operation = ctx.Payload.GetByPath("operation.type").AsStringDefault("")
		}
		meta := easyjson.NewJSONObject()
		meta.SetByPath("operation.target", easyjson.NewJSON(target))
		meta.SetByPath("operation.type", easyjson.NewJSON(operation))
		om.SetMeta(ctx, meta)
		fmt.Println("~~~~~", ctx.Self.ID, om.GetID(), meta.ToString())
	}

	if target == "object.relation" && operation == "create" {
		fmt.Println("............")
	}

	if strings.Split(target, ".")[0] == "type" && ctx.Domain.Name() != ctx.Domain.HubDomainName() { // Redirect to hub if needed
		idOnHub := ctx.Domain.CreateObjectIDWithHubDomain(ctx.Self.ID, true)
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, idOnHub, ctx.Payload, ctx.Options)
		return
	}

	CMDB_CRUDController(sfExec, ctx, om, target, operation)
}

func CMDB_CRUDController(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, target, operation string) {
	var dispatcher *CMDB_CRUDDispatcher
	if d, ok := CMDB_CRUDDispatcherFromTarget[target]; ok {
		dispatcher = &d
	} else {
		// TODO: Return error msg
		return
	}

	selfCallWithOpTime := func(payload, options *easyjson.JSON) {
		forwardOptions := options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, ctx.Self.ID, payload, &forwardOptions)
	}

	fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", ctx.Self.ID, om.GetID(), om.GetOpType())

	switch om.GetOpType() {
	case mediator.MereOp:
		if len(ctx.Options.GetByPath("op_time").AsStringDefault("")) == 0 {
			// Retries meta -------------------------------
			meta := om.GetMeta(ctx)
			meta.SetByPath("retries", easyjson.NewJSON(ctx.Options.GetByPath("retries").AsNumericDefault(20)))
			meta.SetByPath("retry_payload", *ctx.Payload.Clone().GetPtr())
			meta.SetByPath("retry_options", *ctx.Options.Clone().GetPtr())
			om.SetMeta(ctx, meta)
			// --------------------------------------------
			selfCallWithOpTime(ctx.Payload, ctx.Options)
			return
		}
		fallthrough
	case mediator.WorkerIsTaskedByAggregatorOp:
		data := ctx.Payload.GetByPath("data")
		opTimeStr := ctx.Options.GetByPath("op_time").AsStringDefault("")
		if len(opTimeStr) > 0 {
			(*dispatcher)(ctx, om, operation, system.Str2Int(opTimeStr), &data)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("CMDB_CRUDController for target %s detected no op_time", target))).Reply()
		}
	case mediator.AggregatedWorkersOp:
		if om.GetStatus() != mediator.SYNC_OP_STATUS_OK {
			meta := om.GetMeta(ctx)
			retries := int(meta.GetByPath("retries").AsNumericDefault(0))
			fmt.Println("^^^^^^^^^^", ctx.Self.ID, om.GetID(), retries)
			if retries > 0 {
				retryPayload := meta.GetByPath("retry_payload")
				retryOptions := meta.GetByPath("retry_options")

				retries--
				fmt.Println("--------- RETRYING", operation, target, "LEFT:", retries)
				meta.SetByPath("retries", easyjson.NewJSON(retries))
				om.SetMeta(ctx, meta)

				om.Reaggregate(ctx)
				selfCallWithOpTime(&retryPayload, &retryOptions)
				return
			}
		}

		opInfo := easyjson.NewJSONObject()
		opInfo.SetByPath("operation.type", easyjson.NewJSON(operation))
		opInfo.SetByPath("operation.target", easyjson.NewJSON(target))
		om.SetAdditionalReplyData(&opInfo)
		aggregatedData := unifiedCRUDDataAggregator(om)
		fmt.Println("          nnnnnone", aggregatedData.ToString(), om.GetDetails(), om.GetStatus())
		aggregatedData.RemoveByPath("op_stack")
		system.MsgOnErrorReturn(om.ReplyWithData(&aggregatedData))
	}
}

func CMDBDirtyTypeRead(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	if ctx.Domain.Name() != ctx.Domain.HubDomainName() { // Redirect to hub if needed
		idOnHub := ctx.Domain.CreateObjectIDWithHubDomain(ctx.Self.ID, true)
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, idOnHub, ctx.Payload, ctx.Options)
		return
	}
	CMDBTypeRead(ctx, om, system.GetCurrentTimeNs(), ctx.Payload)
}

func CMDBDirtyTypeRelationRead(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	if ctx.Domain.Name() != ctx.Domain.HubDomainName() { // Redirect to hub if needed
		idOnHub := ctx.Domain.CreateObjectIDWithHubDomain(ctx.Self.ID, true)
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, idOnHub, ctx.Payload, ctx.Options)
		return
	}
	CMDBTypeRelationRead(ctx, om, system.GetCurrentTimeNs(), ctx.Payload)
}

func CMDBDirtyObjectRead(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	CMDBObjectRead(ctx, om, system.GetCurrentTimeNs(), ctx.Payload)
}

func CMDBDirtyObjectRelationRead(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	CMDBObjectRelationRead(ctx, om, system.GetCurrentTimeNs(), ctx.Payload)
}
