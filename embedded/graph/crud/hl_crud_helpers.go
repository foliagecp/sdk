package crud

import (
	"context"
	"fmt"
	"strings"
	"sync"

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

// ----------------------------
var (
	// key: objectID -> typeID
	objectTypeCache sync.Map
	// key: fromType -> *sync.Map(toType -> objectLinkType)
	type2TypeObjectLinkTypeCache sync.Map
)

func cacheGetObjectType(objectID string) (string, bool) {
	if v, ok := objectTypeCache.Load(objectID); ok {
		if s, ok := v.(string); ok && s != "" {
			return s, true
		}
	}
	return "", false
}
func cacheSetObjectType(objectID, typeID string) { objectTypeCache.Store(objectID, typeID) }
func cacheDeleteObjectType(objectID string)      { objectTypeCache.Delete(objectID) }

func cacheGetTypeEdge(fromType, toType string) (string, bool) {
	if v, ok := type2TypeObjectLinkTypeCache.Load(fromType); ok {
		if m, ok := v.(*sync.Map); ok {
			if lt, ok := m.Load(toType); ok {
				if s, ok := lt.(string); ok && s != "" {
					return s, true
				}
			}
		}
	}
	return "", false
}
func cacheSetTypeEdge(fromType, toType, linkType string) {
	var m *sync.Map
	if v, ok := type2TypeObjectLinkTypeCache.Load(fromType); ok {
		m, _ = v.(*sync.Map)
	}
	if m == nil {
		m = &sync.Map{}
		type2TypeObjectLinkTypeCache.Store(fromType, m)
	}
	m.Store(toType, linkType)
}
func cacheDeleteTypeEdge(fromType, toType string) {
	if v, ok := type2TypeObjectLinkTypeCache.Load(fromType); ok {
		if m, ok := v.(*sync.Map); ok {
			m.Delete(toType)
		}
	}
}
func cachePurgeTypeEdgesForType(typeID string) {
	// outcome
	type2TypeObjectLinkTypeCache.Delete(typeID)
	// income
	type2TypeObjectLinkTypeCache.Range(func(k, v any) bool {
		if m, ok := v.(*sync.Map); ok {
			m.Delete(typeID)
		}
		return true
	})
}

// -----------------------------------------------------------------------------

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

	om := sfMediators.NewOpMediator(ctx)

	opStack := getOpStackFromOptions(ctx.Options)

	linkType, ok := ctx.Payload.GetByPath("link_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("type is not defined")).Reply()
		return
	}

	toObjectType, ok := ctx.Payload.GetByPath("to_object_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("to_object_type is not defined")).Reply()
		return
	}

	pattern := fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, linkType, ">")

	operationKeysMutexLock(ctx, []string{selfID}, true)
	keys := ctx.Domain.Cache().GetKeysByPattern(pattern)
	if len(keys) > 0 {
		for _, v := range keys {
			split := strings.Split(v, ".")
			to := split[len(split)-1]

			if tp, _ := findObjectType(ctx, to); tp == toObjectType {
				objectLink := easyjson.NewJSONObject()
				objectLink.SetByPath("to", easyjson.NewJSON(to))
				objectLink.SetByPath("type", easyjson.NewJSON(linkType))

				om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), ctx.Options)))
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

func getTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, typeName string) *easyjson.JSON {
	/*options := easyjson.NewJSONObject()
	if ctx.Options != nil {
		options = ctx.Options.Clone()
	}*/
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, typeName), injectParentHoldsLocks(ctx, nil), nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		return som.Data.GetByPath("body.triggers").GetPtr()
	}
	return easyjson.NewJSONObject().GetPtr()
}

func FindObjectType(ctx *sfPlugins.StatefunContextProcessor, objectID string) (string, error) {
	return findObjectType(ctx, objectID)
}

func findObjectType(ctx *sfPlugins.StatefunContextProcessor, objectID string) (string, error) {
	if t, ok := cacheGetObjectType(objectID); ok {
		return t, nil
	}

	// Fast path: read the object's __type out-link directly from KV.
	// Every CMDB object has an out-link named "type" of type TO_TYPELINK
	// pointing to its type vertex (see CreateObject in hl_crud.go). The
	// OutLinkTargetKeyPrefPattern value is "<linkType>.<toId>", so we can
	// derive the object's type without any object.read round-trip.
	if val, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, objectID, "type")); err == nil {
		parts := strings.SplitN(string(val), ".", 2)
		if len(parts) == 2 && parts[0] == TO_TYPELINK && parts[1] != "" {
			cacheSetObjectType(objectID, parts[1])
			return parts[1], nil
		}
	}

	// Slow path: fall back to the full object.read (used when the __type
	// link is missing locally — e.g. shadow objects or fresh failover).
	options := easyjson.NewJSONObject()
	if ctx.Options != nil {
		options = ctx.Options.Clone()
		options.RemoveByPath("op_stack") // Not to execute triggers in functions.cmdb.api.object.read
	}
	id := makeSequenceFreeParentBasedID(ctx, objectID)

	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.read", makeSequenceFreeParentBasedID(ctx, id), injectParentHoldsLocks(ctx, nil), &options))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		tp := som.Data.GetByPath("type").AsStringDefault("")
		cacheSetObjectType(objectID, tp)
		return tp, nil
	}

	return "", fmt.Errorf(som.Details)
}

func findTypeObjects(ctx *sfPlugins.StatefunContextProcessor, typeName string) ([]string, error) {
	/*options := easyjson.NewJSONObject()
	if ctx.Options != nil {
		options = ctx.Options.Clone()
	}*/
	p := easyjson.NewJSONObject()

	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.read", makeSequenceFreeParentBasedID(ctx, typeName), injectParentHoldsLocks(ctx, &p), ctx.Options))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if arr, ok := som.Data.GetByPath("object_ids").AsArrayString(); ok {
			return arr, nil
		}
	}
	return nil, fmt.Errorf(som.Details)
}

func getLinkBody(ctx *sfPlugins.StatefunContextProcessor, from, linkName string) (*easyjson.JSON, error) {
	link := easyjson.NewJSONObject()
	link.SetByPath("name", easyjson.NewJSON(linkName))

	/*options := easyjson.NewJSONObject()
	if ctx.Options != nil {
		options = ctx.Options.Clone()
	}*/
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", makeSequenceFreeParentBasedID(ctx, from), injectParentHoldsLocks(ctx, &link), ctx.Options))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if som.Data.PathExists("body") {
			return som.Data.GetByPathPtr("body"), nil
		}
		return nil, fmt.Errorf("'body' is not find")
	}
	return nil, fmt.Errorf(som.Details)
}

// fromObjectId and toObjectId must be locked by key mutex for thread safety
func getReferenceLinkTypeBetweenTwoObjects(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId string) (string, string, string, error) {
	fromType, err := findObjectType(ctx, fromObjectId)
	if err != nil {
		return "", "", "", err
	}
	toType, err := findObjectType(ctx, toObjectId)
	if len(toType) == 0 {
		return "", "", "", err
	}
	s, e := getObjectsLinkTypeFromTypesLink(ctx, fromType, toType)
	return fromType, toType, s, e
}

// resolveLinkBetweenTwoObjects resolves (linkName, linkType) for the existing
// (fromObjectId -> toObjectId) edge using only KV indices. Performs NO
// object.read calls — the link type is parsed straight out of the
// OutLinkTypeKeyPrefPattern entries that LLAPILinkCreate writes at edge
// creation time.
//
// Returns (linkName, linkType, true) for the first matching edge, or
// ("", "", false) if no such edge exists. Both fromObjectId and toObjectId
// must already include their domain prefix (the same form used to construct
// KV keys in ll_crud.go).
func resolveLinkBetweenTwoObjects(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId string) (string, string, bool) {
	return resolveLinkBetweenTwoObjectsByTypePrefix(ctx, fromObjectId, toObjectId, "")
}

// resolveLinkBetweenTwoObjectsByTypePrefix is the claim-aware variant of
// resolveLinkBetweenTwoObjects. It returns the first edge whose linkType
// starts with linkTypePrefix; with an empty prefix it behaves exactly like
// resolveLinkBetweenTwoObjects.
//
// This MUST be used by the SuperType-flavoured HL APIs: between the same
// (from, to) object pair there can legitimately be multiple cross-pack
// edges with different compound types ("<fromClaim>#<toClaim>#<rel>"), and
// the order returned by GetKeysByPattern over a sharded map is not stable.
// Picking the "first" key and then checking the claim post-hoc is a
// non-deterministic bug: the targeted edge can stay in the graph while
// the call returns idle.
func resolveLinkBetweenTwoObjectsByTypePrefix(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId, linkTypePrefix string) (string, string, bool) {
	prefix := fmt.Sprintf(OutLinkTypeKeyPrefPattern, fromObjectId) // "<from>.ltype."
	suffix := "." + toObjectId
	keys := ctx.Domain.Cache().GetKeysByPattern(prefix + ">")
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) || !strings.HasSuffix(k, suffix) {
			continue
		}
		linkType := k[len(prefix) : len(k)-len(suffix)]
		if linkType == "" {
			continue
		}
		if linkTypePrefix != "" && !strings.HasPrefix(linkType, linkTypePrefix) {
			continue
		}
		nameBytes, err := ctx.Domain.Cache().GetValue(k)
		if err != nil {
			continue
		}
		linkName := string(nameBytes)
		if linkName == "" {
			continue
		}
		return linkName, linkType, true
	}
	return "", "", false
}

func getObjectsLinkTypeFromTypesLink(ctx *sfPlugins.StatefunContextProcessor, fromType, toType string) (string, error) {
	if lt, ok := cacheGetTypeEdge(fromType, toType); ok {
		return lt, nil
	}

	linkBody, err := getLinkBody(ctx, fromType, toType)
	if err != nil {
		return "", err
	}

	linkType, ok := linkBody.GetByPath("type").AsString()
	if !ok {
		return "", fmt.Errorf("type of a link was not defined in link type")
	}

	cacheSetTypeEdge(fromType, toType, linkType)
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
