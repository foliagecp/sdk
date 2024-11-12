package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBObjectRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("details", easyjson.NewJSON(true))

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.dirty.vertex.read", ctx.Self.ID, &payload, &forwardOptions)))
	if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
		om.Reply()
		return
	}

	resData := om.GetLastSyncOp().Data

	objectType := ""
	fromObjects := []string{}
	for i := 0; i < resData.GetByPath("links.in.types").ArraySize(); i++ {
		tp := resData.GetByPath("links.in.types").ArrayElement(i).AsStringDefault("")
		fromId := resData.GetByPath("links.in.uuids").ArrayElement(i).AsStringDefault("")
		if tp == TYPE_OBJECT_LINKTYPE {
			objectType = fromId
		} else {
			if tp != OBJECTS_OBJECT_TYPELINK {
				fromObjects = append(fromObjects, fromId)
			}
		}
	}

	if len(objectType) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not an object", ctx.Self.ID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}
	toObjects := []string{}
	for i := 0; i < resData.GetByPath("links.out.types").ArraySize(); i++ {
		//tp := resData.GetByPath("links.out.types").ArrayElement(i).AsStringDefault("")
		toId := resData.GetByPath("links.out.uuids").ArrayElement(i).AsStringDefault("")
		toObjects = append(toObjects, toId)
	}

	result := easyjson.NewJSONObject()
	if resData.PathExists("body") {
		result.SetByPath("body", resData.GetByPath("body"))

	}
	result.SetByPath("type", easyjson.NewJSON(objectType))
	result.SetByPath("objects.to", easyjson.JSONFromArray(toObjects))
	result.SetByPath("objects.from", easyjson.JSONFromArray(fromObjects))
	if data.GetByPath("details").AsBoolDefault(false) {
		result.DeepMerge(resData)
	}
	if resData.PathExists("op_stack") {
		result.SetByPath("op_stack", resData.GetByPath("op_stack"))
	}

	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

func CMDBObjectCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	tp := data.GetByPath("type").AsStringDefault("")
	if len(tp) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing object's type")).Reply()
		return
	}
	tp = ctx.Domain.CreateObjectIDWithHubDomain(tp, true)

	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", tp, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("type %s does not exist", tp))).Reply()
			return
		}
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_OK {
			existingObjectType := om.GetLastSyncOp().Data.GetByPathPtr("type").AsStringDefault("")
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("object %s with type %s already exists", ctx.Self.ID, existingObjectType))).Reply()
			return
		}
	}

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))

	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		payload.SetByPath("data", data.Clone())
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data.to", easyjson.NewJSON(ctx.Self.ID))
		payload.SetByPath("data.type", easyjson.NewJSON(TYPE_OBJECT_LINKTYPE))
		payload.SetByPath("data.name", easyjson.NewJSON(ctx.Self.ID))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", tp, &payload, &forwardOptions)
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data.to", easyjson.NewJSON(ctx.Self.ID))
		payload.SetByPath("data.type", easyjson.NewJSON(OBJECTS_OBJECT_TYPELINK))
		payload.SetByPath("data.name", easyjson.NewJSON(ctx.Self.ID))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, true), &payload, &forwardOptions)
	}
}

func CMDBObjectUpdate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	upsert := data.GetByPath("upsert").AsBoolDefault(false)

	if upsert {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("object"))
		payload.SetByPath("data", data.Clone())
		forwardOptions := ctx.Options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", ctx.Self.ID, &payload, &forwardOptions)
		return
	}

	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot update %s, not an object", ctx.Self.ID))).Reply()
			return
		}
	}
	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("update"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		payload.SetByPath("data", data.Clone())
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
}

func CMDBObjectDelete(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot delete %s, not an object", ctx.Self.ID))).Reply()
			return
		}
	}

	deleteObjectMeta := easyjson.NewJSONObject()
	deleteObjectMeta.SetByPath("__deleted_object.uuid", easyjson.NewJSON(ctx.Self.ID))
	deleteObjectMeta.SetByPath("__deleted_object.type", easyjson.NewJSON(om.GetLastSyncOp().Data.GetByPath("type").AsStringDefault("")))
	om.AddIntermediateResult(ctx, &deleteObjectMeta)

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
}

func CMDBObjectCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch operation {
	case "create":
		CMDBObjectCreate(ctx, om, opTime, data)
	case "update":
		CMDBObjectUpdate(ctx, om, opTime, data)
	case "delete":
		CMDBObjectDelete(ctx, om, opTime, data)
	case "read":
		CMDBObjectRead(ctx, om, opTime, data)
	}
}
