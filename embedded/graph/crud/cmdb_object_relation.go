package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBObjectRelationRead_ReadTypesRelation(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, fromObject, toObject string, opTime int64) (string, error) {
	var sourceObjectType string
	var targetObjectType string
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		msg := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.read", fromObject, &payload, nil))
		om.AggregateOpMsg(msg)
		if msg.Status != sfMediators.SYNC_OP_STATUS_OK {
			return "", fmt.Errorf(msg.Details)
		}
		sourceObjectType = msg.Data.GetByPath("type").AsStringDefault("")
		if len(sourceObjectType) == 0 {
			return "", fmt.Errorf(msg.Details)
		}
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		msg := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.read", toObject, &payload, nil))
		om.AggregateOpMsg(msg)
		if msg.Status != sfMediators.SYNC_OP_STATUS_OK {
			return "", fmt.Errorf(msg.Details)
		}
		targetObjectType = msg.Data.GetByPath("type").AsStringDefault("")
		if len(targetObjectType) == 0 {
			return "", fmt.Errorf(msg.Details)
		}
	}
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
	payload.SetByPath("to", easyjson.NewJSON(targetObjectType))
	payload.SetByPath("details", easyjson.NewJSON(true))
	msg := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.relation.read", sourceObjectType, &payload, nil))
	om.AggregateOpMsg(msg)
	if msg.Status != sfMediators.SYNC_OP_STATUS_OK {
		return "", fmt.Errorf(msg.Details)
	}
	return msg.Data.GetByPath("object_relation_type").AsStringDefault(""), nil
}

func CMDBObjectRelationRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	to := data.GetByPath("to").AsStringDefault("")
	if len(to) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target object uuid")).Reply()
		return
	}

	var objectsLinkType string
	if lt, err := CMDBObjectRelationRead_ReadTypesRelation(ctx, om, ctx.Self.ID, to, opTime); err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("error while reading objects relation type: %s", objectsLinkType))).Reply()
		return
	} else {
		objectsLinkType = lt
	}

	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", data.GetByPath("to"))
	payload.SetByPath("type", easyjson.NewJSON(objectsLinkType))
	payload.SetByPath("details", easyjson.NewJSON(true))

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.dirty.vertex.link.read", ctx.Self.ID, &payload, &forwardOptions)))
	if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
		om.Reply()
		return
	}

	resData := om.GetLastSyncOp().Data

	result := easyjson.NewJSONObject()

	result.SetByPath("body", resData.GetByPath("body"))
	result.SetByPath("objects.to", resData.GetByPath("vertex.to"))
	result.SetByPath("objects.from", resData.GetByPath("vertex.from"))
	result.SetByPath("type", resData.GetByPath("type"))
	result.SetByPath("name", resData.GetByPath("name"))
	result.SetByPath("tags", resData.GetByPath("tags"))
	if data.GetByPath("details").AsBoolDefault(false) {
		result.DeepMerge(resData)
		result.RemoveByPath("op_stack")
	}

	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

func CMDBObjectRelationCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	to := data.GetByPath("to").AsStringDefault("")
	if len(to) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target object uuid")).Reply()
		return
	}

	var objectsLinkType string
	if lt, err := CMDBObjectRelationRead_ReadTypesRelation(ctx, om, ctx.Self.ID, to, opTime); err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("error while reading objects relation type: %s", err.Error()))).Reply()
		return
	} else {
		objectsLinkType = lt
	}

	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("data", data.Clone())
	payload.SetByPath("data.type", easyjson.NewJSON(objectsLinkType))

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
}

func CMDBObjectRelationUpdate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	to := data.GetByPath("to").AsStringDefault("")
	if len(to) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target object uuid")).Reply()
		return
	}

	upsert := data.GetByPath("upsert").AsBoolDefault(false)

	if upsert {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
		payload.SetByPath("data", data.Clone())
		forwardOptions := ctx.Options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", ctx.Self.ID, &payload, &forwardOptions)
		return
	}

	objRelationType := ""
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		payload.SetByPath("to", easyjson.NewJSON(to))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.relation.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot update link from %s to %s, not a link between objects", ctx.Self.ID, to))).Reply()
			return
		}
		objRelationType = om.GetLastSyncOp().Data.GetByPath("type").AsStringDefault("")
	}
	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("update"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data", data.Clone())
		payload.SetByPath("data.to", easyjson.NewJSON(to))
		payload.SetByPath("data.type", easyjson.NewJSON(objRelationType))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
}

func CMDBObjectRelationDelete(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	to := data.GetByPath("to").AsStringDefault("")
	if len(to) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target object uuid")).Reply()
		return
	}

	objRelationType := ""
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		payload.SetByPath("to", easyjson.NewJSON(to))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.relation.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot delete link from %s to %s, not a link between objects", ctx.Self.ID, to))).Reply()
			return
		}
		objRelationType = om.GetLastSyncOp().Data.GetByPath("type").AsStringDefault("")
	}
	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data", data.Clone())
		payload.SetByPath("data.to", easyjson.NewJSON(to))
		payload.SetByPath("data.type", easyjson.NewJSON(objRelationType))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
}

func CMDBObjectRelationCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch operation {
	case "create":
		CMDBObjectRelationCreate(ctx, om, opTime, data)
	case "update":
		CMDBObjectRelationUpdate(ctx, om, opTime, data)
	case "delete":
		CMDBObjectRelationDelete(ctx, om, opTime, data)
	case "read":
		CMDBObjectRelationRead(ctx, om, opTime, data)
	}
}
