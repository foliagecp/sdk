package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func executeTriggersFromLLOpStack(ctx *sfPlugins.StatefunContextProcessor, opStack *easyjson.JSON, deletedObjectId, deletedObjectType string) {
	llAPIVertexCUDNames := []string{"functions.graph.api.vertex.create", "functions.graph.api.vertex.update", "functions.graph.api.vertex.delete", "functions.graph.api.vertex.read"}
	llAPILinkCUDNames := []string{"functions.graph.api.link.create", "functions.graph.api.link.update", "functions.graph.api.link.delete", "functions.graph.api.link.read"}

	if opStack != nil && opStack.IsArray() {
		for i := 0; i < opStack.ArraySize(); i++ {
			opData := opStack.ArrayElement(i)
			opStr := opData.GetByPath("op").AsStringDefault("")
			if len(opStr) > 0 {
				for j := 0; j < 4; j++ {
					if opStr == llAPIVertexCUDNames[j] {
						vId := opData.GetByPath("id").AsStringDefault("")
						if len(vId) > 0 {
							objectType := ""
							if j == 2 && vId == deletedObjectId {
								objectType = deletedObjectType
							} else {
								objectType, _ = findObjectType(ctx, vId)
							}
							if len(objectType) > 0 {
								var oldBody *easyjson.JSON = nil
								if opData.PathExists("old_body") {
									oldBody = opData.GetByPath("old_body").GetPtr()
								}
								var newBody *easyjson.JSON = nil
								if opData.PathExists("new_body") {
									newBody = opData.GetByPath("new_body").GetPtr()
								}
								executeObjectTriggers(ctx, vId, objectType, oldBody, newBody, j)
							}

						}
					}
					if opStr == llAPILinkCUDNames[j] {
						fromVId := opData.GetByPath("from").AsStringDefault("")
						toVId := opData.GetByPath("to").AsStringDefault("")
						lType := opData.GetByPath("type").AsStringDefault("")

						fromObjectType := ""
						toObjectType := ""
						if j == 2 && fromVId == deletedObjectId {
							fromObjectType = deletedObjectType
						} else {
							fromObjectType, _ = findObjectType(ctx, fromVId)
						}
						if j == 2 && toVId == deletedObjectId {
							toObjectType = deletedObjectType
						} else {
							toObjectType, _ = findObjectType(ctx, toVId)
						}

						if len(lType) > 0 && len(fromVId) > 0 && len(toVId) > 0 && len(fromObjectType) > 0 && len(toObjectType) > 0 {
							var oldBody *easyjson.JSON = nil
							if opData.PathExists("old_body") {
								oldBody = opData.GetByPath("old_body").GetPtr()
							}
							var newBody *easyjson.JSON = nil
							if opData.PathExists("new_body") {
								newBody = opData.GetByPath("new_body").GetPtr()
							}
							executeLinkTriggers(ctx, fromVId, toVId, fromObjectType, toObjectType, lType, oldBody, newBody, j)
						}
					}
				}
			}
		}
	}
}

func executeObjectTriggers(ctx *sfPlugins.StatefunContextProcessor, objectID string, objectType string, oldObjectBody, newObjectBody *easyjson.JSON, tt int /*0 - create, 1 - update, 2 - delete, 3 - read*/) {
	triggers := getTypeTriggers(ctx, objectType)
	if triggers.IsNonEmptyObject() && tt >= 0 && tt < 4 {
		elems := []string{"create", "update", "delete", "read"}
		var functions []string
		if arr, ok := triggers.GetByPath(elems[tt]).AsArrayString(); ok {
			functions = arr
		}

		triggerData := easyjson.NewJSONObject()
		if oldObjectBody != nil {
			triggerData.SetByPath("old_body", *oldObjectBody)
		}
		if newObjectBody != nil {
			triggerData.SetByPath("new_body", *newObjectBody)
		}
		payload := easyjson.NewJSONObject()
		payload.SetByPath(fmt.Sprintf("trigger.object.%s", elems[tt]), triggerData)

		for _, f := range functions {
			system.MsgOnErrorReturn(ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, objectID, &payload, nil, ctx.TraceContext()))
		}
	}
}

func executeLinkTriggers(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId, fromObjectType, toObjectType, linkType string, oldLinkBody, newLinkBody *easyjson.JSON, tt int /*0 - create, 1 - update, 2 - delete, 3 - read*/) {
	typesLinkBody, err := getLinkBody(ctx, fromObjectType, toObjectType)
	if err != nil || typesLinkBody == nil {
		return
	}
	triggers := typesLinkBody.GetByPath("triggers")
	referenceLinkType := typesLinkBody.GetByPath("type").AsStringDefault("")

	if err == nil && triggers.IsNonEmptyObject() && len(referenceLinkType) > 0 && tt >= 0 && tt < 4 {
		elems := []string{"create", "update", "delete", "read"}
		var functions []string
		if arr, ok := triggers.GetByPath(elems[tt]).AsArrayString(); ok {
			functions = arr
		}

		if err != nil || referenceLinkType != linkType {
			return
		}

		triggerData := easyjson.NewJSONObject()
		triggerData.SetByPath("to", easyjson.NewJSON(toObjectId))
		triggerData.SetByPath("type", easyjson.NewJSON(linkType))
		if oldLinkBody != nil {
			triggerData.SetByPath("old_body", *oldLinkBody)
		}
		if newLinkBody != nil {
			triggerData.SetByPath("new_body", *newLinkBody)
		}
		payload := easyjson.NewJSONObject()
		payload.SetByPath(fmt.Sprintf("trigger.link.%s", elems[tt]), triggerData)

		for _, f := range functions {
			system.MsgOnErrorReturn(ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, fromObjectId, &payload, nil, ctx.TraceContext()))
		}
	}
}
