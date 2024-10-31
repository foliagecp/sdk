package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBTypeRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON, begin bool) {
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

		system.MsgOnErrorReturn(om.ReplyWithData(&result))
	}

}

func CMDBTypeCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON, begin bool) {
	switch strings.ToLower(operation) {
	case "create":
		//GraphVertexCreate(ctx, om, &data, opTime)
	case "update":
		//GraphVertexUpdate(ctx, om, &data, opTime)
	case "delete":
		//GraphVertexDelete(ctx, om, &data, opTime)
	case "read":
		CMDBTypeRead(ctx, om, opTime, data, begin)
	default:
		// TODO: Return error msg
	}
}
