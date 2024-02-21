package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func isVertexAnObject(ctx *sfPlugins.StatefunContextProcessor, id string) bool {
	return len(findObjectType(ctx, id)) > 0
}

func executeTriggersFromLLOpStack(ctx *sfPlugins.StatefunContextProcessor, opStack *easyjson.JSON) {
	if opStack != nil && opStack.IsArray() {
		for i := 0; i < opStack.ArraySize(); i++ {
			opData := opStack.ArrayElement(i)
			opStr := opData.GetByPath("op").AsStringDefault("")
			if len(opStr) > 0 {
				for j := 0; j < 3; j++ {
					if opStr == llAPIVertexCUDNames[j] {
						vId := opData.GetByPath("id").AsStringDefault("")
						if len(vId) > 0 && isVertexAnObject(ctx, vId) {
							var oldBody *easyjson.JSON = nil
							if opData.PathExists("old_body") {
								oldBody = opData.GetByPath("old_body").GetPtr()
							}
							var newBody *easyjson.JSON = nil
							if opData.PathExists("new_body") {
								newBody = opData.GetByPath("new_body").GetPtr()
							}
							executeObjectTriggers(ctx, vId, oldBody, newBody, j)
						}
					}
					if opStr == llAPILinkCUDNames[j] {
						fromVId := opData.GetByPath("from").AsStringDefault("")
						toVId := opData.GetByPath("to").AsStringDefault("")
						lType := opData.GetByPath("type").AsStringDefault("")
						if len(lType) > 0 && len(fromVId) > 0 && len(toVId) > 0 && isVertexAnObject(ctx, fromVId) && isVertexAnObject(ctx, toVId) {
							var oldBody *easyjson.JSON = nil
							if opData.PathExists("old_body") {
								oldBody = opData.GetByPath("old_body").GetPtr()
							}
							var newBody *easyjson.JSON = nil
							if opData.PathExists("new_body") {
								newBody = opData.GetByPath("new_body").GetPtr()
							}
							executeLinkTriggers(ctx, fromVId, toVId, lType, oldBody, newBody, j)
						}
					}
				}
			}
		}
	}
}

func executeObjectTriggers(ctx *sfPlugins.StatefunContextProcessor, objectID string, oldObjectBody, newObjectBody *easyjson.JSON, tt int /*0 - create, 1 - update, 2 - delete*/) {
	triggers := getObjectTypeTriggers(ctx, objectID)
	if triggers.IsNonEmptyObject() && tt >= 0 && tt < 3 {
		elems := []string{"create", "update", "delete"}
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
			ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, objectID, &payload, nil)
		}
	}
}

func getObjectsLinkTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId string) easyjson.JSON {
	fromTypeName := findObjectType(ctx, fromObjectId)
	toTypeName := findObjectType(ctx, toObjectId)
	typesLinkBody, err := getLinkBody(ctx, fromTypeName, toTypeName)
	if err != nil || !typesLinkBody.PathExists("triggers") {
		return easyjson.NewJSONObject()
	}
	return typesLinkBody.GetByPath("triggers")
}

func executeLinkTriggers(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId, linkType string, oldLinkBody, newLinkBody *easyjson.JSON, tt int /*0 - create, 1 - update, 2 - delete*/) {
	triggers := getObjectsLinkTypeTriggers(ctx, fromObjectId, toObjectId)
	if triggers.IsNonEmptyObject() && tt >= 0 && tt < 3 {
		elems := []string{"create", "update", "delete"}
		var functions []string
		if arr, ok := triggers.GetByPath(elems[tt]).AsArrayString(); ok {
			functions = arr
		}

		referenceLinkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, fromObjectId, toObjectId)
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
			ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, fromObjectId, &payload, nil)
		}
	}
}
