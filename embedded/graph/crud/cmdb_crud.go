package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/logger"
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

	meta := om.GetMeta(ctx)

	target := strings.ToLower(meta.GetByPath("operation.target").AsStringDefault(""))
	operation := strings.ToLower(meta.GetByPath("operation.type").AsStringDefault(""))

	if len(target) == 0 {
		if len(target) == 0 {
			target = strings.ToLower(ctx.Payload.GetByPath("operation.target").AsStringDefault(""))
		}
		if len(operation) == 0 {
			operation = strings.ToLower(ctx.Payload.GetByPath("operation.type").AsStringDefault(""))
		}
		meta := easyjson.NewJSONObject()
		meta.SetByPath("operation.target", easyjson.NewJSON(target))
		meta.SetByPath("operation.type", easyjson.NewJSON(operation))
		om.SetMeta(ctx, meta)
	}

	if _, ok := CMDB_CRUDDispatcherFromTarget[target]; !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid operation target='%s'", target))).Reply()
		return
	}
	if _, ok := CRUDValidTypes[operation]; !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid operation type='%s'", operation))).Reply()
		return
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

	selfCallWithOpTime := func(payload, options *easyjson.JSON, opTimeStr string) {
		forwardOptions := options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(opTimeStr))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, ctx.Self.ID, payload, &forwardOptions)
	}

	switch om.GetOpType() {
	case mediator.MereOp:
		if len(ctx.Options.GetByPath("op_time").AsStringDefault("")) == 0 {
			opTime := system.GetCurrentTimeNs()
			if t := ctx.Options.GetByPath("nats.timestamp_nano_str").AsStringDefault(""); len(t) > 0 {
				opTime = system.Str2Int(t)
			}
			opTimeStr := fmt.Sprintf("%d", opTime)
			crudRegisterOperation(ctx, target, operation, opTime)

			// Retries meta -------------------------------
			meta := om.GetMeta(ctx)
			meta.SetByPath("", easyjson.NewJSON(target))
			meta.SetByPath("operation.time", easyjson.NewJSON(opTimeStr))
			meta.SetByPath("operation.payload", *ctx.Payload.Clone().GetPtr())
			meta.SetByPath("operation.options", *ctx.Options.Clone().GetPtr())
			om.SetMeta(ctx, meta)
			// --------------------------------------------

			selfCallWithOpTime(ctx.Payload, ctx.Options, opTimeStr)
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
		meta := om.GetMeta(ctx)
		opTimeStr := meta.GetByPath("operation.time").AsStringDefault("")
		if len(opTimeStr) > 0 {
			opTime := system.Str2Int(opTimeStr)
			omDetails := om.GetDetails()
			if strings.Contains(omDetails, "not_finished_cud.older") {
				opPayload := meta.GetByPath("operation.payload")
				opOptions := meta.GetByPath("operation.options")

				om.Reaggregate(ctx)
				selfCallWithOpTime(&opPayload, &opOptions, opTimeStr)
				return
			} else {
				crudUnregisterOperation(ctx, target, operation, opTime)
			}
		} else {
			logger.Logln(logger.FatalLevel, "no op time found")
		}

		opInfo := easyjson.NewJSONObject()
		opInfo.SetByPath("operation.type", easyjson.NewJSON(operation))
		opInfo.SetByPath("operation.target", easyjson.NewJSON(target))
		om.SetAdditionalReplyData(ctx, &opInfo)
		aggregatedData := unifiedCRUDDataAggregator(om)
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

	opTime := system.GetCurrentTimeNs()
	if t := ctx.Payload.GetByPath("op_time").AsStringDefault(""); len(t) > 0 {
		opTime = system.Str2Int(t)
	}
	dirtyReadAppendMeta(ctx, om, "type", "read", opTime)

	CMDBTypeRead(ctx, om, opTime, ctx.Payload)
}

func CMDBDirtyTypeRelationRead(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	if ctx.Domain.Name() != ctx.Domain.HubDomainName() { // Redirect to hub if needed
		idOnHub := ctx.Domain.CreateObjectIDWithHubDomain(ctx.Self.ID, true)
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, idOnHub, ctx.Payload, ctx.Options)
		return
	}

	opTime := system.GetCurrentTimeNs()
	if t := ctx.Payload.GetByPath("op_time").AsStringDefault(""); len(t) > 0 {
		opTime = system.Str2Int(t)
	}
	dirtyReadAppendMeta(ctx, om, "type.relation", "read", opTime)

	CMDBTypeRelationRead(ctx, om, opTime, ctx.Payload)
}

func CMDBDirtyObjectRead(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	opTime := system.GetCurrentTimeNs()
	if t := ctx.Payload.GetByPath("op_time").AsStringDefault(""); len(t) > 0 {
		opTime = system.Str2Int(t)
	}
	dirtyReadAppendMeta(ctx, om, "object", "read", opTime)

	CMDBObjectRead(ctx, om, opTime, ctx.Payload)
}

func CMDBDirtyObjectRelationRead(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	opTime := system.GetCurrentTimeNs()
	if t := ctx.Payload.GetByPath("op_time").AsStringDefault(""); len(t) > 0 {
		opTime = system.Str2Int(t)
	}
	dirtyReadAppendMeta(ctx, om, "object.relation", "read", opTime)

	CMDBObjectRelationRead(ctx, om, opTime, ctx.Payload)
}
