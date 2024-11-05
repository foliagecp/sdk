package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBObjectRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	fmt.Println("1 CMDBObjectRead", om.GetID(), ctx.Self.ID)
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
		result.RemoveByPath("op_stack")
	}

	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

func CMDBObjectCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	fmt.Println("CMDBObjectCreate 1", ctx.Self.ID)
	tp := data.GetByPath("type").AsStringDefault("")
	if len(tp) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("missing object's type")).Reply()
		return
	}
	tp = ctx.Domain.CreateObjectIDWithHubDomain(tp, true)

	fmt.Println("CMDBObjectCreate 2", ctx.Self.ID)
	typeMsg := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", tp, easyjson.NewJSONObject().GetPtr(), nil))
	if typeMsg.Status != sfMediators.SYNC_OP_STATUS_OK {
		fmt.Println("CMDBObjectCreate 2.1", ctx.Self.ID, typeMsg.ToJson().ToString())
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("type %s does not exist", tp))).Reply()
		return
	}
	fmt.Println("CMDBObjectCreate 3", ctx.Self.ID)
	objectMsg := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.read", ctx.Self.ID, easyjson.NewJSONObject().GetPtr(), nil))
	if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_OK {
		existingObjectType := objectMsg.Data.GetByPathPtr("type").AsStringDefault("")
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("object %s with type %s already exists", ctx.Self.ID, existingObjectType))).Reply()
		return
	}

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))

	fmt.Println("CMDBObjectCreate 4", ctx.Self.ID)
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		payload.SetByPath("data", data.Clone())
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	}
	fmt.Println("CMDBObjectCreate 5", ctx.Self.ID)
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data.to", easyjson.NewJSON(ctx.Self.ID))
		payload.SetByPath("data.type", easyjson.NewJSON(TYPE_OBJECT_LINKTYPE))
		payload.SetByPath("data.name", easyjson.NewJSON(ctx.Self.ID))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", tp, &payload, &forwardOptions)
	}
	fmt.Println("CMDBObjectCreate 6", ctx.Self.ID)
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

func CMDBObjectCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch operation {
	case "create":
		CMDBObjectCreate(ctx, om, opTime, data)
	case "update":
		//GraphVertexUpdate(ctx, om, &data, opTime)
	case "delete":
		//GraphVertexDelete(ctx, om, &data, opTime)
	case "read":
		CMDBObjectRead(ctx, om, opTime, data)
	default:
		// TODO: Return error msg
	}
}
