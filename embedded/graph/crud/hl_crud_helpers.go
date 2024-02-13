package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func GetObjectTypeTriggersStatefun(_ sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	result := easyjson.NewJSONObject().GetPtr()
	result.SetByPath("status", easyjson.NewJSON("failed"))

	typeName := FindObjectType(contextProcessor, contextProcessor.Self.ID)
	if len(typeName) > 0 {
		resp2, err2 := contextProcessor.Request(sfPlugins.AutoSelect, "functions.graph.api.vertex.read", typeName, nil, nil)
		if err2 == nil {
			if resp2.PathExists("result") {
				result.SetByPath("status", easyjson.NewJSON("ok"))
				typeBody := resp2.GetByPath("result")
				if typeBody.PathExists("triggers") {
					result.SetByPath("result", typeBody.GetByPath("triggers"))
				} else {
					result.SetByPath("result", easyjson.NewJSONObject())
				}
			} else {
				result.SetByPath("result", easyjson.NewJSON("invalid type's body"))
			}
		} else {
			result.SetByPath("result", easyjson.NewJSON(err2.Error()))
		}
	} else {
		result.SetByPath("result", easyjson.NewJSON("cannot get object's type"))
	}

	contextProcessor.Reply.With(result)
}

func FindObjectTypeStatefun(_ sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	result := easyjson.NewJSONObject().GetPtr()
	result.SetByPath("status", easyjson.NewJSON("failed"))

	pattern := fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, contextProcessor.Self.ID, TypeLink, ">")
	keys := contextProcessor.GlobalCache.GetKeysByPattern(pattern)
	if len(keys) > 0 {
		split := strings.Split(keys[0], ".")
		result.SetByPath("status", easyjson.NewJSON("ok"))
		result.SetByPath("result", easyjson.NewJSON(split[len(split)-1]))
	} else {
		result.SetByPath("result", easyjson.NewJSON("type not found"))
	}

	contextProcessor.Reply.With(result)
}

func FindTypeObjectsStatefun(_ sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	result := easyjson.NewJSONObject().GetPtr()
	result.SetByPath("status", easyjson.NewJSON("ok"))

	pattern := fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, contextProcessor.Self.ID, ObjectLink, ">")

	keys := contextProcessor.GlobalCache.GetKeysByPattern(pattern)
	if len(keys) > 0 {
		out := make([]string, 0, len(keys))
		for _, v := range keys {
			split := strings.Split(v, ".")
			out = append(out, split[len(split)-1])
		}
		result.SetByPath("result", easyjson.JSONFromArray(out))
	} else {
		result.SetByPath("result", easyjson.NewJSONArray())
	}

	contextProcessor.Reply.With(result)
}

// ------------------------------------------------------------------------------------------------

func GetObjectsLinkTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId string) easyjson.JSON {
	fromTypeName := FindObjectType(ctx, fromObjectId)
	toTypeName := FindObjectType(ctx, toObjectId)
	typesLinkBody, err := GetTypesLinkBody(ctx, fromTypeName, toTypeName)
	if err != nil || !typesLinkBody.PathExists("triggers") {
		return easyjson.NewJSONObject()
	}
	return typesLinkBody.GetByPath("triggers")
}

func GetObjectTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, objectID string) *easyjson.JSON {
	resp, err := ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.api.get_object_type_triggers", objectID, nil, nil)
	if err == nil && resp.GetByPath("status").AsStringDefault("failed") == "ok" {
		return resp.GetByPath("result").GetPtr()
	}
	return easyjson.NewJSONObject().GetPtr()
}

func FindObjectType(ctx *sfPlugins.StatefunContextProcessor, objectID string) string {
	resp, err := ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.api.find_object_type", objectID, nil, nil)
	if err == nil && resp.GetByPath("status").AsStringDefault("failed") == "ok" {
		return resp.GetByPath("result").AsStringDefault("")
	}
	return ""
}

func FindTypeObjects(ctx *sfPlugins.StatefunContextProcessor, objectID string) []string {
	resp, err := ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.api.find_type_objects", objectID, nil, nil)
	if err == nil && resp.GetByPath("status").AsStringDefault("failed") == "ok" && resp.GetByPath("status").IsArray() {
		if arr, ok := resp.GetByPath("status").AsArrayString(); ok {
			return arr
		}
	}
	return []string{}
}

func GetTypesLinkBody(ctx *sfPlugins.StatefunContextProcessor, from, to string) (*easyjson.JSON, error) {
	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON(TypeLink))

	resp, err := ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.read", from, &link, nil)
	if err != nil {
		return nil, err
	}

	if resp.GetByPath("status").AsStringDefault("failed") != "ok" {
		return nil, fmt.Errorf(resp.GetByPath("result").AsStringDefault("failed"))
	}

	return resp.GetByPath("result").GetPtr(), nil
}
