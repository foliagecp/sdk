package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBObjectRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON, begin bool) {
	// read:vertex -> result
	if begin {
		fmt.Println("1 CMDBObjectRead", om.GetID(), ctx.Self.ID)
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("read"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		payload.SetByPath("data.details", easyjson.NewJSON(true))

		forwardOptions := ctx.Options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
		forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	} else {
		msgs := om.GetAggregatedOpMsgs()
		if len(msgs) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no data when tried to read type %s", ctx.Self.ID))).Reply()
			return
		}
		fmt.Println("2 CMDBObjectRead", om.GetID(), ctx.Self.ID)

		resData := msgs[0].Data

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
}

func CMDBObjectCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON, begin bool) {
	// read:type, read:object(read:vertex -> result) -> result
	if begin {
		tp := data.GetByPath("type").AsStringDefault("")
		if len(tp) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("missing object's type")).Reply()
			return
		}
		tp = ctx.Domain.CreateObjectIDWithHubDomain(tp, true)

		{
			fmt.Println("1 CMDBObjectCreate", om.GetID(), ctx.Self.ID)
			payload := easyjson.NewJSONObject()
			payload.SetByPath("operation.type", easyjson.NewJSON("read"))
			payload.SetByPath("operation.target", easyjson.NewJSON("type"))
			options := ctx.Options.Clone()
			options.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(1)))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", tp, &payload, &options)
		}
		{
			payload := easyjson.NewJSONObject()
			payload.SetByPath("operation.type", easyjson.NewJSON("read"))
			payload.SetByPath("operation.target", easyjson.NewJSON("object"))
			options := ctx.Options.Clone()
			options.SetByPath("op_time", easyjson.NewJSON(system.IntToStr(1)))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", ctx.Self.ID, &payload, &options)
		}
	} else {
		tp := data.GetByPath("type").AsStringDefault("")
		if len(tp) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("missing object's type")).Reply()
			return
		}
		tp = ctx.Domain.CreateObjectIDWithHubDomain(tp, true)

		msgs := om.GetAggregatedOpMsgs()
		if len(msgs) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no response data when tried to create object %s", ctx.Self.ID))).Reply()
			return
		}
		if len(msgs) == 1 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid response pattern when tried to create object %s", ctx.Self.ID))).Reply()
			return
		} else if len(msgs) == 2 { // Received info about type and this object (if already exists)
			fmt.Println("2 CMDBObjectCreate", om.GetID(), ctx.Self.ID)
			var thisObjectMsg *sfMediators.OpMsg
			var targetTypeMsg *sfMediators.OpMsg

			if strings.Split(msgs[0].Meta, ":")[1] == ctx.Self.ID {
				thisObjectMsg = &msgs[0]
				targetTypeMsg = &msgs[1]
			} else {
				thisObjectMsg = &msgs[1]
				targetTypeMsg = &msgs[0]
			}

			if thisObjectMsg.Status == sfMediators.SYNC_OP_STATUS_OK { // Object already exists
				existingObjectType := thisObjectMsg.Data.GetByPathPtr("type").AsStringDefault("")
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("object %s with type %s already exists", ctx.Self.ID, existingObjectType))).Reply()
				return
			}
			if targetTypeMsg.Status != sfMediators.SYNC_OP_STATUS_OK { // Object's type does not exist
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("type %s does not exist", tp))).Reply()
				return
			}

			om.Reaggregate(ctx)

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

			return
		}
		aggregatedData := unifiedCRUDDataAggregator(om)
		aggregatedData.RemoveByPath("op_stack")
		system.MsgOnErrorReturn(om.ReplyWithData(&aggregatedData))
	}
}

func CMDBObjectCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON, begin bool) {
	switch operation {
	case "create":
		CMDBObjectCreate(ctx, om, opTime, data, begin)
	case "update":
		//GraphVertexUpdate(ctx, om, &data, opTime)
	case "delete":
		//GraphVertexDelete(ctx, om, &data, opTime)
	case "read":
		CMDBObjectRead(ctx, om, opTime, data, begin)
	default:
		// TODO: Return error msg
	}
}
