package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func CMDBObjectRelationRead_ReadTypesRelation(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON, begin bool) {
	// read:object(read:vertex -> result), read:object(read:vertex -> result) -> read:type.relation(read:vertex.link -> result) -> result
	if begin {
		fmt.Println("1 CMDBObjectRelationRead_ReadTypesRelation", ctx.Self.ID)
		to := data.GetByPath("to").AsStringDefault("")
		if len(to) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("missing target object uuid")).Reply()
			return
		}

		forwardOptions := ctx.Options.Clone()
		forwardOptions.RemoveByPath("op_time")

		payload1 := easyjson.NewJSONObject()
		payload1.SetByPath("operation.type", easyjson.NewJSON("read"))
		payload1.SetByPath("operation.target", easyjson.NewJSON("object"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", ctx.Self.ID, &payload1, &forwardOptions)

		payload2 := easyjson.NewJSONObject()
		payload2.SetByPath("operation.type", easyjson.NewJSON("read"))
		payload2.SetByPath("operation.target", easyjson.NewJSON("object"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", to, &payload2, &forwardOptions)
	} else {
		msgs := om.GetAggregatedOpMsgs()
		if len(msgs) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no data when tried to read object %s' type relation", ctx.Self.ID))).Reply()
			return
		}
		if len(msgs) == 1 { // Received data about types link
			fmt.Println("3 CMDBObjectRelationRead_ReadTypesRelation", ctx.Self.ID)
			resData := msgs[0].Data
			system.MsgOnErrorReturn(om.ReplyWithData(&resData))
		} else if len(msgs) == 2 { // Received data about objects
			fmt.Println("2 CMDBObjectRelationRead_ReadTypesRelation", ctx.Self.ID)
			om.Reaggregate(ctx)

			var sourceObjectType string
			var targetObjectType string
			if strings.Split(msgs[0].Meta, ":")[1] == ctx.Self.ID {
				sourceObjectType = msgs[0].Data.GetByPath("type").AsStringDefault("")
				targetObjectType = msgs[1].Data.GetByPath("type").AsStringDefault("")
			} else {
				sourceObjectType = msgs[1].Data.GetByPath("type").AsStringDefault("")
				targetObjectType = msgs[0].Data.GetByPath("type").AsStringDefault("")
			}

			if len(sourceObjectType) == 0 {
				om.AggregateOpMsg(sfMediators.OpMsgFailed("could not read source object type")).Reply()
				return
			}
			if len(targetObjectType) == 0 {
				om.AggregateOpMsg(sfMediators.OpMsgFailed("could not read source object type")).Reply()
				return
			}

			payload := easyjson.NewJSONObject()
			payload.SetByPath("operation.type", easyjson.NewJSON("read"))
			payload.SetByPath("operation.target", easyjson.NewJSON("type.relation"))
			payload.SetByPath("data.to", easyjson.NewJSON(targetObjectType))
			if data.GetByPath("details").AsBoolDefault(false) {
				payload.SetByPath("data.details", easyjson.NewJSON(true))
			}
			forwardOptions := ctx.Options.Clone()
			forwardOptions.RemoveByPath("op_time")
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", sourceObjectType, &payload, &forwardOptions)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid data received when tried to read object %s' type relation", ctx.Self.ID))).Reply()
		}
	}
}

func CMDBObjectRelationRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON, begin bool) {
	// read:object.relation[types_relation_only] -> read:vertex.link -> result
	if data.GetByPath("types_relation_only").AsBoolDefault(false) {
		CMDBObjectRelationRead_ReadTypesRelation(ctx, om, opTime, data, begin)
		return
	}

	if begin {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("read"))
		payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
		payload.SetByPath("data.to", data.GetByPath("to"))
		payload.SetByPath("data.types_relation_only", easyjson.NewJSON(true))

		forwardOptions := ctx.Options.Clone()
		forwardOptions.RemoveByPath("op_time")
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	} else {
		msgs := om.GetAggregatedOpMsgs()
		if len(msgs) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no data when tried to read object %s relation", ctx.Self.ID))).Reply()
			return
		}
		if strings.Split(msgs[0].Meta, ":")[0] == ctx.Self.Typename {
			om.Reaggregate(ctx)

			payload := easyjson.NewJSONObject()
			payload.SetByPath("operation.type", easyjson.NewJSON("read"))
			payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
			payload.SetByPath("data.to", data.GetByPath("to"))
			payload.SetByPath("data.type", msgs[0].Data.GetByPath("object_relation_type"))
			payload.SetByPath("data.details", easyjson.NewJSON(true))

			forwardOptions := ctx.Options.Clone()
			forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
			forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
		} else {
			resData := msgs[0].Data

			result := easyjson.NewJSONObject()

			result.SetByPath("body", resData.GetByPath("body"))
			result.SetByPath("objects.to", resData.GetByPath("vertex.to"))
			result.SetByPath("objects.from", resData.GetByPath("vertex.from"))
			result.SetByPath("type", resData.GetByPath("type"))
			result.SetByPath("name", resData.GetByPath("name"))
			result.SetByPath("tags", resData.GetByPath("tags"))
			if data.GetByPath("details").AsBoolDefault(false) {
				result.DeepMerge(resData)
				result.RemoveByPath("op_stack")
			}

			system.MsgOnErrorReturn(om.ReplyWithData(&result))
		}
	}
}

func CMDBObjectRelationCreate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON, begin bool) {
	// read:object.relation[types_relation_only] -> create:vertex.link -> result
	if begin {
		fmt.Println("1 CMDBObjectRelationCreate", ctx.Self.ID)
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.type", easyjson.NewJSON("read"))
		payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
		payload.SetByPath("data.to", data.GetByPath("to"))
		payload.SetByPath("data.types_relation_only", easyjson.NewJSON(true))

		forwardOptions := ctx.Options.Clone()
		forwardOptions.RemoveByPath("op_time")
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.cmdb.api.crud", ctx.Self.ID, &payload, &forwardOptions)
	} else {
		msgs := om.GetAggregatedOpMsgs()
		if len(msgs) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no data when tried to create object %s relation", ctx.Self.ID))).Reply()
			return
		}
		if strings.Split(msgs[0].Meta, ":")[0] == ctx.Self.Typename {
			fmt.Println("2 CMDBObjectRelationCreate", ctx.Self.ID)
			om.Reaggregate(ctx)

			payload := easyjson.NewJSONObject()
			payload.SetByPath("operation.type", easyjson.NewJSON("create"))
			payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
			payload.SetByPath("data", data.Clone())
			payload.SetByPath("data.type", msgs[0].Data.GetByPath("object_relation_type"))

			forwardOptions := ctx.Options.Clone()
			forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
			forwardOptions.SetByPath("op_stack", easyjson.NewJSON(true))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &payload, &forwardOptions)
		} else {
			fmt.Println("3 CMDBObjectRelationCreate", ctx.Self.ID)
			result := msgs[0].Data.Clone()
			result.RemoveByPath("op_stack")

			system.MsgOnErrorReturn(om.ReplyWithData(&result))
		}
	}
}

func CMDBObjectRelationCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON, begin bool) {
	switch operation {
	case "create":
		CMDBObjectRelationCreate(ctx, om, opTime, data, begin)
	case "update":
		//GraphVertexUpdate(ctx, om, &data, opTime)
	case "delete":
		//GraphVertexDelete(ctx, om, &data, opTime)
	case "read":
		CMDBObjectRelationRead(ctx, om, opTime, data, begin)
	default:
		// TODO: Return error msg
	}
}
