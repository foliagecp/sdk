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
	if begin {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("read"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		payload.SetByPath("data.details", easyjson.NewJSON(true))

		forwardOptions := ctx.Options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	} else {
		msgs := om.GetAggregatedOpMsgs()
		if len(msgs) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no data when tried to read type %s", ctx.Self.ID))).Reply()
			return
		}

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

func CMDBObjectCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON, begin bool) {
	switch strings.ToLower(operation) {
	case "create":
		//GraphVertexCreate(ctx, om, &data, opTime)
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
