package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type LinkId struct {
	from string
	to   string
	tp   string
}

func CMDBTypeRelationRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	to := data.GetByPath("to").AsStringDefault("")
	if len(to) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target type name")).Reply()
		return
	}

	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", data.GetByPath("to"))
	payload.SetByPath("type", easyjson.NewJSON(TYPE_TYPE_LINKTYPE))
	payload.SetByPath("details", easyjson.NewJSON(true))

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.dirty.vertex.link.read", ctx.Self.ID, &payload, &forwardOptions)))
	if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
		om.Reply()
		return
	}

	resData := om.GetLastSyncOp().Data

	fromType := resData.GetByPath("vertex.from").AsStringDefault(ctx.Self.ID)
	toType := resData.GetByPath("vertex.to").AsStringDefault("")
	lt := resData.GetByPath("type").AsStringDefault("")
	if lt != TYPE_TYPE_LINKTYPE {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from vertex with uuid=%s to vertex with uuid=%s with type=%s is not a types relation", fromType, toType, lt)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}

	result := easyjson.NewJSONObject()

	resultBody := resData.GetByPath("body").Clone()
	objectsRelationType := resultBody.GetByPath("type").AsStringDefault("")
	triggers := resultBody.GetByPath("triggers")

	resultBody.RemoveByPath("type")
	resultBody.RemoveByPath("triggers")

	result.SetByPath("types.to", easyjson.NewJSON(toType))
	result.SetByPath("types.from", easyjson.NewJSON(fromType))
	result.SetByPath("object_relation_type", easyjson.NewJSON(objectsRelationType))
	if triggers.IsNonEmptyObject() {
		result.SetByPath("triggers", triggers)
	}
	if data.GetByPath("details").AsBoolDefault(false) {
		result.DeepMerge(resData)
	}
	result.SetByPath("body", resultBody)
	if resData.PathExists("op_stack") {
		result.SetByPath("op_stack", resData.GetByPath("op_stack"))
	}

	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

func CMDBTypeRelationCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	to := data.GetByPath("to").AsStringDefault("")
	if len(to) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target type name")).Reply()
		return
	}
	objRelationType := data.GetByPath("object_relation_type").AsStringDefault("")
	if len(objRelationType) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing object_relation_type")).Reply()
		return
	}

	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.Reply()
			return
		}
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", to, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.Reply()
			return
		}
	}

	triggers := easyjson.NewJSONObject()
	if tr := data.GetByPath("triggers"); tr.IsNonEmptyObject() {
		triggers = tr
	}

	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("data", data.Clone())
	payload.SetByPath("data.body.type", easyjson.NewJSON(objRelationType))
	payload.SetByPath("data.body.triggers", triggers)
	payload.SetByPath("data.type", easyjson.NewJSON(TYPE_TYPE_LINKTYPE))
	payload.SetByPath("data.name", easyjson.NewJSON(to))

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
}

func CMDBTypeRelationUpdate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	to := data.GetByPath("to").AsStringDefault("")
	if len(to) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target type name")).Reply()
		return
	}

	upsert := data.GetByPath("upsert").AsBoolDefault(false)

	if upsert {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("type.relation"))
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
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.relation.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot update link from %s to %s, not a link between types", ctx.Self.ID, to))).Reply()
			return
		}
		objRelationType = om.GetLastSyncOp().Data.GetByPath("object_relation_type").AsStringDefault("")
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
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data", data.Clone())
		payload.SetByPath("data.to", easyjson.NewJSON(to))
		payload.SetByPath("data.type", easyjson.NewJSON(TYPE_TYPE_LINKTYPE))
		payload.SetByPath("data.body.type", easyjson.NewJSON(objRelationType))
		payload.SetByPath("data.body.triggers", triggers)
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
}

func CMDBTypeRelationDelete(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	to := data.GetByPath("to").AsStringDefault("")
	if len(to) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target type name")).Reply()
		return
	}

	objRelationType := ""
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		payload.SetByPath("to", easyjson.NewJSON(to))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.relation.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot delete link from %s to %s, not a link between types", ctx.Self.ID, to))).Reply()
			return
		}
		objRelationType = om.GetLastSyncOp().Data.GetByPath("object_relation_type").AsStringDefault("")
	}
	fromTypeObjectUUIDs := []string{}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", ctx.Self.ID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot update %s, not a type", ctx.Self.ID))).Reply()
			return
		}
		if a, ok := om.GetLastSyncOp().Data.GetByPath("object_uuids").AsArrayString(); ok {
			fromTypeObjectUUIDs = a
		}
	}

	links2Delete := []LinkId{}
	for _, objectUUID := range fromTypeObjectUUIDs {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(opTime)))
		payload.SetByPath("details", easyjson.NewJSON(true))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.dirty.vertex.read", objectUUID, &payload, nil)))
		if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("cannot delete link from %s to %s, cannot read object %s of type %s", ctx.Self.ID, to, objectUUID, ctx.Self.ID))).Reply()
			return
		}
		for i := 0; i < om.GetLastSyncOp().Data.GetByPath("links.out.types").ArraySize(); i++ {
			outLinkType := om.GetLastSyncOp().Data.GetByPath("links.out.types").ArrayElement(i).AsStringDefault("")
			if outLinkType == objRelationType {
				toUUID := om.GetLastSyncOp().Data.GetByPath("links.out.uuids").ArrayElement(i).AsStringDefault("")
				links2Delete = append(links2Delete, LinkId{from: objectUUID, to: toUUID, tp: outLinkType})
			}
		}
	}

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	// Delete types link ----------------------------------
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data", data.Clone())
		payload.SetByPath("data.to", easyjson.NewJSON(to))
		payload.SetByPath("data.type", easyjson.NewJSON(TYPE_TYPE_LINKTYPE))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
	// ----------------------------------------------------
	// Delete corresponding object links ------------------
	for _, link2Delete := range links2Delete {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data", data.Clone())
		payload.SetByPath("data.to", easyjson.NewJSON(link2Delete.to))
		payload.SetByPath("data.type", easyjson.NewJSON(link2Delete.tp))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", link2Delete.from, &payload, &forwardOptions)
	}
	// ----------------------------------------------------
}

func CMDBTypeRelationCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch operation {
	case "create":
		CMDBTypeRelationCreate(ctx, om, opTime, data)
	case "update":
		CMDBTypeRelationUpdate(ctx, om, opTime, data)
	case "delete":
		CMDBTypeRelationDelete(ctx, om, opTime, data)
	case "read":
		CMDBTypeRelationRead(ctx, om, opTime, data)
	}
}
