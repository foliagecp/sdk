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

type CMDB_CRUDDispatcher func(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON, begin bool)

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

	opTimeStr := strings.ToLower(meta.GetByPath("operation.op_time").AsStringDefault(""))
	target := strings.ToLower(meta.GetByPath("operation.target").AsStringDefault(""))
	operation := strings.ToLower(meta.GetByPath("operation.type").AsStringDefault(""))

	if len(opTimeStr)*len(target)*len(operation) == 0 {
		if len(opTimeStr) == 0 {
			opTimeStr = ctx.Options.GetByPath("op_time").AsStringDefault("")
			if len(opTimeStr) == 0 { // No op_time is declared
				opTimeStr = fmt.Sprintf("%d", system.GetCurrentTimeNs())
				ctx.Options.SetByPath("op_time", easyjson.NewJSON(opTimeStr))
			}
		}
		if len(target) == 0 {
			target = ctx.Payload.GetByPath("operation.target").AsStringDefault("")
		}
		if len(operation) == 0 {
			operation = ctx.Payload.GetByPath("operation.type").AsStringDefault("")
		}
		meta := easyjson.NewJSONObject()
		meta.SetByPath("operation.op_time", easyjson.NewJSON(opTimeStr))
		meta.SetByPath("operation.target", easyjson.NewJSON(target))
		meta.SetByPath("operation.type", easyjson.NewJSON(operation))
		om.SetMeta(ctx, meta)
		fmt.Println("~~~~~", om.GetID(), meta.ToString())
	}

	fmt.Println("")
	fmt.Println("-----", ctx.Self.ID+" --------", om.GetID(), "'"+opTimeStr+"'", om.GetOpType(), meta.ToString(), ctx.Payload.ToString())

	if target == "object.relation" && operation == "create" {
		fmt.Println("............")
	}

	if strings.Split(target, ".")[0] == "type" && ctx.Domain.Name() != ctx.Domain.HubDomainName() { // Redirect to hub if needed
		idOnHub := ctx.Domain.CreateObjectIDWithHubDomain(ctx.Self.ID, true)
		forwardOptions := ctx.Options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(opTimeStr))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, idOnHub, ctx.Payload, &forwardOptions)
		return
	}

	CMDB_CRUDController(sfExec, ctx, om, opTimeStr, target, operation)
}

func IsRunningOlderCMDBOpForThisID(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTimeStr string) bool {
	const (
		aggrPack      = "__mAggrPack"
		aggrPackTempl = aggrPack + ".%s"
	)
	funcContext := ctx.GetFunctionContext()
	mediatorIds := funcContext.GetByPath(aggrPack).ObjectKeys()
	for _, mediatorId := range mediatorIds {
		aggrPackPath := fmt.Sprintf(aggrPackTempl, mediatorId)
		opTimePath := fmt.Sprintf("%s.meta.operation.op_time", aggrPackPath)
		runningOptimeStr := funcContext.GetByPath(opTimePath).AsStringDefault("")
		if len(runningOptimeStr) > 0 {
			if system.Str2Int(runningOptimeStr) < system.Str2Int(opTimeStr) {
				return true
			}
		}
	}
	return false
}

func CMDB_CRUDController(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTimeStr, target, operation string) {
	var dispatcher *CMDB_CRUDDispatcher
	if d, ok := CMDB_CRUDDispatcherFromTarget[target]; ok {
		dispatcher = &d
	} else {
		// TODO: Return error msg
		return
	}

	switch om.GetOpType() {
	case mediator.MereOp:
		fallthrough
	case mediator.WorkerIsTaskedByAggregatorOp:
		// Extend execution if other operation is in progress for this id
		if IsRunningOlderCMDBOpForThisID(ctx, om, opTimeStr) {
			// Two danger situations:
			// 1. Parent with the same functionType calls for extra data (no opTime, ctx.Caller.Typename == ctx.Self.Typename)
			// 2. After this call (no opTime, ctx.Caller.Typename == ctx.Self.Typename)
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, ctx.Self.ID, ctx.Payload, ctx.Options)
			return
		}

		data := ctx.Payload.GetByPath("data")

		if len(opTimeStr) > 0 {
			// Mark this state as opBegin for CRUD End --------------
			meta := om.GetMeta(ctx)
			meta.SetByPath("original_data", data)
			meta.SetByPath("original_op_time_str", easyjson.NewJSON(opTimeStr))
			om.SetMeta(ctx, meta)
			// ------------------------------------------------------
			(*dispatcher)(ctx, om, operation, system.Str2Int(opTimeStr), &data, true)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("CMDB_CRUDController for target %s detected no op_time", target))).Reply()
		}
	case mediator.AggregatedWorkersOp:
		opInfo := easyjson.NewJSONObject()
		opInfo.SetByPath("operation.type", easyjson.NewJSON(operation))
		opInfo.SetByPath("operation.target", easyjson.NewJSON(target))
		om.SetAdditionalReplyData(&opInfo)

		// Mark this state as opBegin = true for further CRUD End -------------
		meta := om.GetMeta(ctx)
		data := meta.GetByPath("original_data")
		opTimeStr := meta.GetByPath("original_op_time_str").AsStringDefault("")
		if len(opTimeStr) > 0 {
			fmt.Println("          (*dispatcher)")
			(*dispatcher)(ctx, om, operation, system.Str2Int(opTimeStr), &data, false)
			return
		}
		// --------------------------------------------------------------------

		aggregatedData := unifiedCRUDDataAggregator(om)
		fmt.Println("          nnnnnone", aggregatedData.ToString())
		system.MsgOnErrorReturn(om.ReplyWithData(&aggregatedData))
	}
}
