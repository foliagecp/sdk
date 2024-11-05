package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBTypeRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	fmt.Println("1 CMDBTypeRead", om.GetID(), ctx.Self.ID)
	payload := easyjson.NewJSONObject()
	payload.SetByPath("details", easyjson.NewJSON(true))

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))

	fmt.Println("2 CMDBTypeRead", om.GetID(), ctx.Self.ID)
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.dirty.vertex.read", ctx.Self.ID, &payload, &forwardOptions)))
	if om.GetLastSyncOp().Status != sfMediators.SYNC_OP_STATUS_OK {
		fmt.Println("2.1 CMDBTypeRead", om.GetID(), ctx.Self.ID, om.GetLastSyncOp().ToJson().ToString())
		om.Reply()
		return
	}

	resData := om.GetLastSyncOp().Data

	fmt.Println("3 CMDBTypeRead", om.GetID(), ctx.Self.ID)
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
		fmt.Println("3.1 CMDBTypeRead", om.GetID(), ctx.Self.ID)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not a type", ctx.Self.ID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}
	fmt.Println("4 CMDBTypeRead", om.GetID(), ctx.Self.ID)
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
	if resData.PathExists("body") {
		result.SetByPath("body", resData.GetByPath("body"))
	}
	result.SetByPath("types.to", easyjson.JSONFromArray(toTypes))
	result.SetByPath("types.from", easyjson.JSONFromArray(fromTypes))
	result.SetByPath("object_uuids", easyjson.JSONFromArray(objects))
	if data.GetByPath("details").AsBoolDefault(false) {
		result.DeepMerge(resData)
		result.RemoveByPath("op_stack")
	}

	fmt.Println("5 CMDBTypeRead", om.GetID(), ctx.Self.ID)
	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

func CMDBTypeCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	payload1 := easyjson.NewJSONObject()
	payload1.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload1.SetByPath("operation.target", easyjson.NewJSON("vertex"))

	forwardOptions := ctx.Options.Clone()
	forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
	forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload1, &forwardOptions)

	payload2 := easyjson.NewJSONObject()
	payload2.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload2.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload2.SetByPath("data.to", easyjson.NewJSON(ctx.Self.ID))
	payload2.SetByPath("data.type", easyjson.NewJSON(TYPES_TYPE_LINKTYPE))
	payload2.SetByPath("data.name", easyjson.NewJSON(ctx.Self.ID))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, true), &payload2, &forwardOptions)
}

func CMDBTypeCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch operation {
	case "create":
		CMDBTypeCreate(ctx, om, opTime, data)
	case "update":
		//GraphVertexUpdate(ctx, om, &data, opTime)
	case "delete":
		//GraphVertexDelete(ctx, om, &data, opTime)
	case "read":
		CMDBTypeRead(ctx, om, opTime, data)
	default:
		// TODO: Return error msg
	}
}
