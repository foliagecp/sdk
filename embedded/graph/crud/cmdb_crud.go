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
		return_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string - optional, if any exists
		data: json - optional, id any exists
			operation: json
				type: string - required // "create", "update", "delete", "read"
				target: string - required // "type", "type.relation" "object", "object.relation"
			op_stack: json array - optional
*/
func CMDB_CRUDGateway(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

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
		fmt.Println("~~~~~", om.GetID(), meta.ToString())
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
		data := ctx.Payload.GetByPath("data")
		opTimeStr := ctx.Options.GetByPath("op_time").AsStringDefault("")
		if len(opTimeStr) > 0 {
			(*dispatcher)(ctx, om, operation, system.Str2Int(opTimeStr), &data)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("CMDB_CRUDController for target %s detected no op_time", target))).Reply()
		}
	case mediator.AggregatedWorkersOp:
		opInfo := easyjson.NewJSONObject()
		opInfo.SetByPath("operation.type", easyjson.NewJSON(operation))
		opInfo.SetByPath("operation.target", easyjson.NewJSON(target))
		om.SetAdditionalReplyData(&opInfo)
		aggregatedData := unifiedCRUDDataAggregator(om)
		fmt.Println("          nnnnnone", aggregatedData.ToString())
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
