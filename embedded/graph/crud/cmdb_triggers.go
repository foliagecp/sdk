package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func reaObjectType(ctx *sfPlugins.StatefunContextProcessor, object string) string {
	msg := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.object.read", object, nil, nil))
	if msg.Status == sfMediators.SYNC_OP_STATUS_OK {
		return msg.Data.GetByPathPtr("type").AsStringDefault("")
	}
	return ""
}

func readObjectRelationType(ctx *sfPlugins.StatefunContextProcessor, fromObject, toObject string) string {
	om := sfMediators.NewOpMediator(ctx)
	if objectRelationType, err := CMDBObjectRelationRead_ReadTypesRelation(ctx, om, fromObject, toObject, system.GetCurrentTimeNs()); err == nil {
		return objectRelationType
	}
	return ""
}

func readTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, typeName string) *easyjson.JSON {
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.read", typeName, nil, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		return som.Data.GetByPath("triggers").GetPtr()
	}
	return easyjson.NewJSONObject().GetPtr()
}

func readTypeRelationTriggers(ctx *sfPlugins.StatefunContextProcessor, fromTypeName, toTypeName string) *easyjson.JSON {
	payload := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON(toTypeName))
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.dirty.type.relation.read", fromTypeName, &payload, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		return som.Data.GetByPath("triggers").GetPtr()
	}
	return easyjson.NewJSONObject().GetPtr()
}

func execTriggers(ctx *sfPlugins.StatefunContextProcessor, aggregatedData *easyjson.JSON) {
	deletedObjectUUID := aggregatedData.GetByPath("__deleted_object.uuid").AsStringDefault("")
	deletedObjectType := aggregatedData.GetByPath("__deleted_object.type").AsStringDefault("")
	opStack := aggregatedData.GetByPath("op_stack").GetPtr()
	if opStack != nil && opStack.IsArray() {
		for i := 0; i < opStack.ArraySize(); i++ {
			opData := opStack.ArrayElement(i)
			graphOperation := opData.GetByPath("operation.type").AsStringDefault("")
			graphTarget := opData.GetByPath("operation.target").AsStringDefault("")

			switch graphTarget {
			case "vertex":
				vertexId := opData.GetByPath("id").AsStringDefault("")
				objectType := ""
				if graphOperation == "delete" && deletedObjectUUID == vertexId {
					objectType = deletedObjectType
				} else {
					objectType = reaObjectType(ctx, vertexId)
				}
				if len(objectType) > 0 {
					var bodyDiff *easyjson.JSON = nil
					if opData.PathExists("body") {
						bodyDiff = opData.GetByPath("body").GetPtr()
					}
					execObjectTriggers(ctx, vertexId, objectType, bodyDiff, graphOperation)
				}
			case "vertex.link":
				fromVertex := opData.GetByPath("link.from").AsStringDefault("")
				toVertex := opData.GetByPath("link.to").AsStringDefault("")
				linkType := opData.GetByPath("link.type").AsStringDefault("")

				fromObjectType := reaObjectType(ctx, fromVertex)
				toObjectType := reaObjectType(ctx, toVertex)
				if len(linkType) > 0 && len(fromObjectType) > 0 && len(toObjectType) > 0 {
					var bodyDiff *easyjson.JSON = nil
					if opData.PathExists("body") {
						bodyDiff = opData.GetByPath("body").GetPtr()
					}
					execObjectRelationTriggers(ctx, fromVertex, toVertex, fromObjectType, toObjectType, linkType, bodyDiff, graphOperation)
				}
			}
		}
	}
}

func execObjectTriggers(ctx *sfPlugins.StatefunContextProcessor, objectID string, objectType string, bodyDiff *easyjson.JSON, graphOperation string) {
	triggers := readTypeTriggers(ctx, objectType)

	if triggers.IsNonEmptyObject() {
		var functions []string
		if arr, ok := triggers.GetByPath(graphOperation).AsArrayString(); ok {
			functions = arr
		} else {
			return
		}

		triggerData := easyjson.NewJSONObjectWithKeyValue("body", *bodyDiff)
		payload := easyjson.NewJSONObject()
		payload.SetByPath(fmt.Sprintf("trigger.object.%s", graphOperation), triggerData)

		for _, f := range functions {
			ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, objectID, &payload, nil)
		}
	}
}

func execObjectRelationTriggers(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId, fromObjectType, toObjectType, linkType string, bodyDiff *easyjson.JSON, graphOperation string) {
	triggers := readTypeRelationTriggers(ctx, fromObjectType, toObjectType)

	if triggers.IsNonEmptyObject() {
		var functions []string
		if arr, ok := triggers.GetByPath(graphOperation).AsArrayString(); ok {
			functions = arr
		} else {
			return
		}

		triggerData := easyjson.NewJSONObjectWithKeyValue("body", *bodyDiff)
		triggerData.SetByPath("to", easyjson.NewJSON(toObjectId))
		triggerData.SetByPath("type", easyjson.NewJSON(linkType))

		payload := easyjson.NewJSONObject()
		payload.SetByPath(fmt.Sprintf("trigger.object.relation.%s", graphOperation), triggerData)

		for _, f := range functions {
			ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, fromObjectId, &payload, nil)
		}
	}
}
