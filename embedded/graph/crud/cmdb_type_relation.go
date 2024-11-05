package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

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

	typesRelationBody := resData.GetByPath("body").Clone()
	objectsRelationType := typesRelationBody.GetByPath("type").AsStringDefault("")
	typesRelationBody.RemoveByPath("type")

	result.SetByPath("body", typesRelationBody)
	result.SetByPath("types.to", easyjson.NewJSON(toType))
	result.SetByPath("types.from", easyjson.NewJSON(fromType))
	result.SetByPath("object_relation_type", easyjson.NewJSON(objectsRelationType))
	if data.GetByPath("details").AsBoolDefault(false) {
		result.DeepMerge(resData)
		result.RemoveByPath("op_stack")
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

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", ctx.Self.ID, easyjson.NewJSONObject().GetPtr(), nil)))
	if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
		om.Reply()
		return
	}
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", to, easyjson.NewJSONObject().GetPtr(), nil)))
	if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
		om.Reply()
		return
	}

	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("data", data.Clone())
	payload.SetByPath("data.body.type", easyjson.NewJSON(objRelationType))
	payload.SetByPath("data.type", easyjson.NewJSON(TYPE_TYPE_LINKTYPE))
	payload.SetByPath("data.name", easyjson.NewJSON(to))

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
}

func CMDBTypeRelationCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch operation {
	case "create":
		CMDBTypeRelationCreate(ctx, om, opTime, data)
	case "update":
		//GraphVertexUpdate(ctx, om, &data, opTime)
	case "delete":
		//GraphVertexDelete(ctx, om, &data, opTime)
	case "read":
		CMDBTypeRelationRead(ctx, om, opTime, data)
	default:
		// TODO: Return error msg
	}
}
