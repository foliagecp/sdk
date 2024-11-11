package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBTypeRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
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

	vertexIsType := false
	fromTypes := []string{}
	for i := 0; i < resData.GetByPath("links.in.names").ArraySize(); i++ {
		tp := resData.GetByPath("links.in.types").ArrayElement(i).AsStringDefault("")
		fromId := resData.GetByPath("links.in.uuids").ArrayElement(i).AsStringDefault("")
		if tp == TYPES_TYPE_LINKTYPE {
			vertexIsType = true
		} else {
			if tp == TYPE_TYPE_LINKTYPE {
				fromTypes = append(fromTypes, fromId)
			}
		}
	}
	if !vertexIsType {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not a type", ctx.Self.ID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}
	toTypes := []string{}
	objects := []string{}
	for i := 0; i < resData.GetByPath("links.out.names").ArraySize(); i++ {
		tp := resData.GetByPath("links.out.types").ArrayElement(i).AsStringDefault("")
		toId := resData.GetByPath("links.out.uuids").ArrayElement(i).AsStringDefault("")
		if tp == TYPE_TYPE_LINKTYPE {
			toTypes = append(toTypes, toId)
		}
		if tp == TYPE_OBJECT_LINKTYPE {
			objects = append(objects, toId)
		}
	}

	result := easyjson.NewJSONObject()

	result.SetByPath("types.to", easyjson.JSONFromArray(toTypes))
	result.SetByPath("types.from", easyjson.JSONFromArray(fromTypes))
	result.SetByPath("object_uuids", easyjson.JSONFromArray(objects))
	if data.GetByPath("details").AsBoolDefault(false) {
		result.DeepMerge(resData)
		result.RemoveByPath("op_stack")
	}
	if resData.PathExists("body") {
		resultBody := resData.GetByPath("body").Clone()
		triggers := resultBody.GetByPath("triggers")
		resultBody.RemoveByPath("triggers")

		if triggers.IsNonEmptyObject() {
			result.SetByPath("triggers", triggers)
		}

		result.SetByPath("body", resultBody)
	}

	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

func CMDBTypeCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("type %s already exists", ctx.Self.ID))).Reply()
			return
		}
	}

	triggers := easyjson.NewJSONObject()
	if tr := data.GetByPath("triggers"); tr.IsNonEmptyObject() {
		triggers = tr
	}

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		if data.PathExists("body") {
			payload.SetByPath("data.body", data.GetByPath("body"))
		}
		payload.SetByPath("data.body.triggers", triggers)
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data.to", easyjson.NewJSON(ctx.Self.ID))
		payload.SetByPath("data.type", easyjson.NewJSON(TYPES_TYPE_LINKTYPE))
		payload.SetByPath("data.name", easyjson.NewJSON(ctx.Self.ID))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, true), &payload, &forwardOptions)
	}
}

func CMDBTypeUpdate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	upsert := data.GetByPath("upsert").AsBoolDefault(false)

	if upsert {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("type"))
		payload.SetByPath("data", data.Clone())
		forwardOptions := ctx.Options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", ctx.Self.ID, &payload, &forwardOptions)
		return
	}

	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot update %s, not a type", ctx.Self.ID))).Reply()
			return
		}
	}

	triggers := easyjson.NewJSONObject()
	if tr := data.GetByPath("triggers"); tr.IsNonEmptyObject() {
		triggers = tr
	}

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("update"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		payload.SetByPath("data", data.Clone())
		payload.SetByPath("data.body.triggers", triggers)
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
}

func CMDBTypeDelete(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	objectUUIDs := []string{}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot delete %s, not a type", ctx.Self.ID))).Reply()
			return
		}
		if a, ok := om.GetLastSyncOp().Data.GetByPath("object_uuids").AsArrayString(); ok {
			objectUUIDs = a
		}
	}
	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
	for _, objectUUID := range objectUUIDs {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", objectUUID, &payload, &forwardOptions)
	}
}

func CMDBTypeCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch operation {
	case "create":
		CMDBTypeCreate(ctx, om, opTime, data)
	case "update":
		CMDBTypeUpdate(ctx, om, opTime, data)
	case "delete":
		CMDBTypeDelete(ctx, om, opTime, data)
	case "read":
		CMDBTypeRead(ctx, om, opTime, data)
	}
}
