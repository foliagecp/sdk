package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBTypeRelationRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON, begin bool) {
	if begin {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("read"))
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("data.to", data.GetByPath("to"))
		payload.SetByPath("data.type", easyjson.NewJSON(TYPE_TYPE_LINKTYPE))
		payload.SetByPath("data.details", easyjson.NewJSON(true))

		forwardOptions := ctx.Options.Clone()
		forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	} else {
		msgs := om.GetAggregatedOpMsgs()
		if len(msgs) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no data when tried to read type %s relation", ctx.Self.ID))).Reply()
			return
		}

		resData := msgs[0].Data

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
}

func CMDBTypeRelationCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON, begin bool) {
	switch strings.ToLower(operation) {
	case "create":
		//GraphVertexCreate(ctx, om, &data, opTime)
	case "update":
		//GraphVertexUpdate(ctx, om, &data, opTime)
	case "delete":
		//GraphVertexDelete(ctx, om, &data, opTime)
	case "read":
		CMDBTypeRelationRead(ctx, om, opTime, data, begin)
	default:
		// TODO: Return error msg
	}
}
