package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

/*
payload: json - required

	type: string - required
	to_object_type: string - required

options: json - optional

	return_op_stack: bool - optional
*/
func DeleteObjectFilteredOutLinksStatefun(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	opStack := getOpStackFromOptions(ctx.Options)

	linkType, ok := ctx.Payload.GetByPath("type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("type is not defined"))).Reply()
		return
	}

	toObjectType, ok := ctx.Payload.GetByPath("to_object_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("to_object_type is not defined"))).Reply()
		return
	}

	pattern := fmt.Sprintf(OutLinkTypeKeyPrefPatternNEW+LinkKeySuff2Pattern, ctx.Self.ID, linkType, ">")
	keys := ctx.Domain.Cache().GetKeysByPattern(pattern)
	if len(keys) > 0 {
		for _, v := range keys {
			split := strings.Split(v, ".")
			to := split[len(split)-1]

			if findObjectType(ctx, to) == toObjectType {
				objectLink := easyjson.NewJSONObject()
				objectLink.SetByPath("to", easyjson.NewJSON(to))
				objectLink.SetByPath("type", easyjson.NewJSON(linkType))

				om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.delete", ctx.Self.ID, &objectLink, ctx.Options)))
				mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
				if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
					om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
					return
				}
			}
		}
	}

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

func GetObjectTypeTriggersStatefun(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	typeName := findObjectType(ctx, ctx.Self.ID)
	if len(typeName) > 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.vertex.read", typeName, nil, nil)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_OK {
			om.AggregateOpMsg(sfMediators.OpMsgOk(om.GetLastSyncOp().Data.GetByPath("body.triggers")))
		}
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid object's typename"))
	}

	om.Reply()
}

func FindObjectTypeStatefun(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	pattern := fmt.Sprintf(OutLinkTypeKeyPrefPatternNEW+LinkKeySuff2Pattern, ctx.Self.ID, TYPE_TYPELINK, ">")
	keys := ctx.Domain.Cache().GetKeysByPattern(pattern)
	if len(keys) > 0 {
		split := strings.Split(keys[0], ".")
		t := split[len(split)-1]
		om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONObjectWithKeyValue("type", easyjson.NewJSON(t))))
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("cannot find object's type"))
	}

	om.Reply()
}

func FindTypeObjectsStatefun(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	keys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkTypeKeyPrefPatternNEW+LinkKeySuff2Pattern, ctx.Self.ID, OBJECT_TYPELINK, ">"))
	if len(keys) > 0 {
		out := make([]string, 0, len(keys))
		for _, v := range keys {
			split := strings.Split(v, ".")
			out = append(out, split[len(split)-1])
		}
		om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.JSONFromArray(out)))
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONArray()))
	}

	om.Reply()
}

// ------------------------------------------------------------------------------------------------

func getObjectTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, objectID string) *easyjson.JSON {
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.api.get_object_type_triggers", objectID, nil, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		return &som.Data
	}
	return easyjson.NewJSONObject().GetPtr()
}

func findObjectType(ctx *sfPlugins.StatefunContextProcessor, objectID string) string {
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.api.find_object_type", objectID, nil, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		return som.Data.GetByPath("type").AsStringDefault("")
	}
	return ""
}

func findTypeObjects(ctx *sfPlugins.StatefunContextProcessor, objectID string) ([]string, error) {
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.api.find_type_objects", objectID, nil, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if arr, ok := som.Data.AsArrayString(); ok {
			return arr, nil
		}
	}
	return nil, fmt.Errorf(som.Details)
}

func getLinkBody(ctx *sfPlugins.StatefunContextProcessor, from, to, linkType string) (*easyjson.JSON, error) {
	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(to))
	link.SetByPath("type", easyjson.NewJSON(linkType))

	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.read", from, &link, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if som.Data.PathExists("body") {
			return som.Data.GetByPathPtr("body"), nil
		}
		return nil, fmt.Errorf("'body' is not find")
	}
	return nil, fmt.Errorf(som.Details)
}

func getReferenceLinkTypeBetweenTwoObjects(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId string) (string, error) {
	fromType := findObjectType(ctx, fromObjectId)
	toType := findObjectType(ctx, toObjectId)

	return getObjectsLinkTypeFromTypesLink(ctx, fromType, toType)
}

func getObjectsLinkTypeFromTypesLink(ctx *sfPlugins.StatefunContextProcessor, fromType, toType string) (string, error) {
	linkBody, err := getLinkBody(ctx, fromType, toType, TYPE_TYPELINK)
	if err != nil {
		return "", err
	}

	linkType, ok := linkBody.GetByPath("type").AsString()
	if !ok {
		return "", fmt.Errorf("type of a link was not defined in link type")
	}
	return linkType, nil
}
