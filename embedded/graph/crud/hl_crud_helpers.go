package crud

import (
	"context"
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const (
	OBJECTS_TYPELINK = "__objects"
	TYPES_TYPELINK   = "__types"
	TO_TYPELINK      = "__type"
	OBJECT_TYPELINK  = "__object"

	GROUP_TYPELINK = "group"

	BUILT_IN_TYPES      = "types"
	BUILT_IN_OBJECTS    = "objects"
	BUILT_IN_ROOT       = "root"
	BUILT_IN_TYPE_GROUP = "group"
	BUILT_IN_OBJECT_NAV = "nav"
)

func typeOperationRedirectedToHub(ctx *sfPlugins.StatefunContextProcessor) bool {
	if ctx.Domain.Name() != ctx.Domain.HubDomainName() {
		om := sfMediators.NewOpMediator(ctx)
		selfID := getOriginalID(ctx.Self.ID)
		idOnHub := ctx.Domain.CreateObjectIDWithHubDomain(selfID, true)
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, ctx.Self.Typename, idOnHub, ctx.Payload, ctx.Options))).Reply()
		return true
	}
	return false
}

/*
payload: json - required

	link_type: string - required
	to_object_type: string - required

options: json - optional

	op_stack: bool - optional
*/
func DeleteObjectFilteredOutLinksStatefun(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	operationKeysMutexLock(ctx, []string{selfID})

	om := sfMediators.NewOpMediator(ctx)

	opStack := getOpStackFromOptions(ctx.Options)

	linkType, ok := ctx.Payload.GetByPath("link_type").AsString()
	if !ok {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed("type is not defined")).Reply()
		return
	}

	toObjectType, ok := ctx.Payload.GetByPath("to_object_type").AsString()
	if !ok {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed("to_object_type is not defined")).Reply()
		return
	}

	pattern := fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, linkType, ">")
	keys := ctx.Domain.Cache().GetKeysByPattern(pattern)
	if len(keys) > 0 {
		for _, v := range keys {
			split := strings.Split(v, ".")
			to := split[len(split)-1]

			if tp, _ := findObjectType(ctx, to, false); tp == toObjectType {
				objectLink := easyjson.NewJSONObject()
				objectLink.SetByPath("to", easyjson.NewJSON(to))
				objectLink.SetByPath("type", easyjson.NewJSON(linkType))

				om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeID(selfID), injectParentHoldsLocks(ctx, selfID, &objectLink), ctx.Options)))
				mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
				if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
					operationKeysMutexUnlock(ctx)
					system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
					return
				}
			}
		}
	}

	operationKeysMutexUnlock(ctx)
	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

// ------------------------------------------------------------------------------------------------

func getTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, typeName string, crudAPIChildOperation bool) *easyjson.JSON {
	/*options := easyjson.NewJSONObject()
	if ctx.Options != nil {
		options = ctx.Options.Clone()
	}*/
	indTypeName := typeName
	if crudAPIChildOperation {
		indTypeName = makeSequenceFreeID(indTypeName)
	}
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", indTypeName, injectParentHoldsLocks(ctx, typeName, nil), nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		return som.Data.GetByPath("body.triggers").GetPtr()
	}
	return easyjson.NewJSONObject().GetPtr()
}

func findObjectType(ctx *sfPlugins.StatefunContextProcessor, objectID string, crudAPIChildOperation bool) (string, error) {
	options := easyjson.NewJSONObject()
	if ctx.Options != nil {
		options = ctx.Options.Clone()
		options.RemoveByPath("op_stack") // Not to execute triggers in functions.cmdb.api.object.read
	}
	indObjectID := objectID
	if crudAPIChildOperation {
		indObjectID = makeSequenceFreeID(objectID)
	}
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.read", indObjectID, injectParentHoldsLocks(ctx, objectID, nil), &options))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		return som.Data.GetByPath("type").AsStringDefault(""), nil
	}
	return "", fmt.Errorf(som.Details)
}

func findTypeObjects(ctx *sfPlugins.StatefunContextProcessor, typeName string, crudAPIChildOperation bool) ([]string, error) {
	/*options := easyjson.NewJSONObject()
	if ctx.Options != nil {
		options = ctx.Options.Clone()
	}*/
	p := easyjson.NewJSONObject()

	indTypeName := typeName
	if crudAPIChildOperation {
		indTypeName = makeSequenceFreeID(indTypeName)
	}
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.read", indTypeName, injectParentHoldsLocks(ctx, typeName, &p), ctx.Options))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if arr, ok := som.Data.GetByPath("object_ids").AsArrayString(); ok {
			return arr, nil
		}
	}
	return nil, fmt.Errorf(som.Details)
}

func getLinkBody(ctx *sfPlugins.StatefunContextProcessor, from, linkName string, crudAPIChildOperation bool) (*easyjson.JSON, error) {
	link := easyjson.NewJSONObject()
	link.SetByPath("name", easyjson.NewJSON(linkName))

	/*options := easyjson.NewJSONObject()
	if ctx.Options != nil {
		options = ctx.Options.Clone()
	}*/
	indFrom := from
	if crudAPIChildOperation {
		indFrom = makeSequenceFreeID(indFrom)
	}
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", indFrom, injectParentHoldsLocks(ctx, from, &link), ctx.Options))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if som.Data.PathExists("body") {
			return som.Data.GetByPathPtr("body"), nil
		}
		return nil, fmt.Errorf("'body' is not find")
	}
	return nil, fmt.Errorf(som.Details)
}

// fromObjectId and toObjectId must be locked by key mutex for thread safety
func getReferenceLinkTypeBetweenTwoObjects(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId string, crudAPIChildOperation bool) (string, string, string, error) {
	fromType, err := findObjectType(ctx, fromObjectId, crudAPIChildOperation)
	if err != nil {
		return "", "", "", err
	}
	toType, err := findObjectType(ctx, toObjectId, crudAPIChildOperation)
	if len(toType) == 0 {
		return "", "", "", err
	}
	s, e := getObjectsLinkTypeFromTypesLink(ctx, fromType, toType, crudAPIChildOperation)
	return fromType, toType, s, e
}

func getObjectsLinkTypeFromTypesLink(ctx *sfPlugins.StatefunContextProcessor, fromType, toType string, crudAPIChildOperation bool) (string, error) {
	linkBody, err := getLinkBody(ctx, fromType, toType, crudAPIChildOperation)
	if err != nil {
		return "", err
	}

	linkType, ok := linkBody.GetByPath("type").AsString()
	if !ok {
		return "", fmt.Errorf("type of a link was not defined in link type")
	}
	return linkType, nil
}

func cmdbSchemaPrepare(ctx context.Context, runtime *statefun.Runtime) error {
	// ----------------------------------------------------
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", BUILT_IN_ROOT, easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", BUILT_IN_TYPES, easyjson.NewJSONObject().GetPtr(), nil))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", BUILT_IN_OBJECTS, easyjson.NewJSONObject().GetPtr(), nil))

	v := easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON(BUILT_IN_TYPES))
	v.SetByPath("type", easyjson.NewJSON(TYPES_TYPELINK))
	v.SetByPath("name", easyjson.NewJSON(runtime.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", BUILT_IN_ROOT, &v, nil))

	v = easyjson.NewJSONObject()
	v.SetByPath("to", easyjson.NewJSON(BUILT_IN_OBJECTS))
	v.SetByPath("type", easyjson.NewJSON(OBJECTS_TYPELINK))
	v.SetByPath("name", easyjson.NewJSON(runtime.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", BUILT_IN_ROOT, &v, nil))
	// ----------------------------------------------------

	// ----------------------------------------------------
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", BUILT_IN_TYPE_GROUP, nil, nil))

	v = easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON(BUILT_IN_TYPE_GROUP))
	v.SetByPath("object_type", easyjson.NewJSON(GROUP_TYPELINK))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.create", BUILT_IN_TYPE_GROUP, &v, nil))

	v = easyjson.NewJSONObjectWithKeyValue("origin_type", easyjson.NewJSON(BUILT_IN_TYPE_GROUP))
	system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", BUILT_IN_OBJECT_NAV, &v, nil))
	// ----------------------------------------------------
	return nil
}
