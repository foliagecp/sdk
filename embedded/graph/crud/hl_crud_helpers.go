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

	// Trigger caches for the executeTriggersFromLLOpStack hot path.
	// Without these, every op in the LL op-stack costs a full
	// vertex.read (object triggers) or link.read (link triggers) round
	// trip just to discover the type's triggers section — for HLMB-style
	// massive CRUD that is N×NATS round trips per op_stack walk.
	//
	// Both caches store the result of the last server-side load and are
	// invalidated when the underlying type / TypesLink body is mutated
	// (UpdateType / DeleteType / *TypesLink*). The cached payload is
	// already trigger-shaped (object: *easyjson.JSON of triggers section;
	// link: triggers + referenceLinkType) so the hot path needs zero
	// further KV reads to decide whether to fire a Signal.
	//
	// Value sentinel: nil *easyjson.JSON means "known to have no
	// triggers" — a positive negative cache that lets us skip without
	// re-hitting NATS.

	// key: typeName -> *easyjson.JSON (triggers section; nil = none)
	typeObjectTriggersCache sync.Map
	// key: fromType + "|" + toType -> *linkTriggersInfo
	typesLinkTriggersCache sync.Map

	// key: typeName -> string (the body path the type declares as its
	// objects' human-readable name source; "" = none/invalid). Loaded once
	// per type and reused so ReadObject never reads the type body on the hot
	// path. Invalidated by Create/Update/DeleteType. Note: an empty-string
	// value is a real (negative) cache entry — Load's ok flag distinguishes
	// "cached as none" from "not cached".
	typeHRNFieldCache sync.Map
)

// humanReadableNameFieldKey is the body field a type uses to declare which
// path inside its objects' bodies holds the human-readable name.
const humanReadableNameFieldKey = "human_readable_name_field"

// linkTriggersInfo is the trigger-relevant subset of a TypesLink body,
// pre-extracted at load time so the hot path needs no further JSON
// traversal beyond fetching the cached value.
//
// triggers == nil means the TypesLink either has no triggers section
// or it is empty — equivalent to "fire nothing".
type linkTriggersInfo struct {
	triggers          *easyjson.JSON
	referenceLinkType string
}

func cacheGetTypeObjectTriggers(typeName string) (*easyjson.JSON, bool) {
	if v, ok := typeObjectTriggersCache.Load(typeName); ok {
		j, _ := v.(*easyjson.JSON)
		return j, true
	}
	return nil, false
}

// cacheSetTypeObjectTriggers stores the triggers section for typeName.
// Pass nil to record "known to have no triggers" — the negative cache
// that lets the hot path skip the vertex.read for trigger-less types.
func cacheSetTypeObjectTriggers(typeName string, triggers *easyjson.JSON) {
	typeObjectTriggersCache.Store(typeName, triggers)
}

func cacheInvalidateTypeObjectTriggers(typeName string) {
	typeObjectTriggersCache.Delete(typeName)
}

func cacheGetLinkTriggers(fromType, toType string) (*linkTriggersInfo, bool) {
	if v, ok := typesLinkTriggersCache.Load(fromType + "|" + toType); ok {
		info, _ := v.(*linkTriggersInfo)
		return info, true
	}
	return nil, false
}

// cacheSetLinkTriggers stores trigger info for (fromType, toType).
// Pass info with triggers=nil to record "no link triggers here".
func cacheSetLinkTriggers(fromType, toType string, info *linkTriggersInfo) {
	typesLinkTriggersCache.Store(fromType+"|"+toType, info)
}

func cacheInvalidateLinkTriggers(fromType, toType string) {
	typesLinkTriggersCache.Delete(fromType + "|" + toType)
}

// cachePurgeLinkTriggersForType drops every cached link-triggers entry
// involving typeName as either endpoint. Used when an entire type is
// removed (DeleteType cascades through every TypesLink it participates
// in).
func cachePurgeLinkTriggersForType(typeName string) {
	prefix := typeName + "|"
	suffix := "|" + typeName
	typesLinkTriggersCache.Range(func(k, _ any) bool {
		if s, ok := k.(string); ok {
			if strings.HasPrefix(s, prefix) || strings.HasSuffix(s, suffix) {
				typesLinkTriggersCache.Delete(s)
			}
		}
		return true
	})
}

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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID}, true, opTime)
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

// getTypeTriggers returns the triggers section of typeName's CMDB type
// body. Result is cached per typeName in typeObjectTriggersCache; the
// cache is invalidated by UpdateType / DeleteType so a fresh write to
// the type's body is observable on the next call.
//
// Returns an empty JSON object (not nil) when the type has no triggers
// — callers use .IsNonEmptyObject() to decide whether to dispatch.
func getTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, typeName string) *easyjson.JSON {
	if cached, ok := cacheGetTypeObjectTriggers(typeName); ok {
		if cached == nil {
			return easyjson.NewJSONObject().GetPtr()
		}
		return cached
	}

	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, typeName), injectParentHoldsLocks(ctx, nil), nil))
	if som.Status != sfMediators.SYNC_OP_STATUS_OK {
		// Transient failure — do not poison the cache.
		return easyjson.NewJSONObject().GetPtr()
	}

	triggers := som.Data.GetByPath("body.triggers")
	if triggers.IsNonEmptyObject() {
		cloned := triggers.Clone()
		ptr := &cloned
		cacheSetTypeObjectTriggers(typeName, ptr)
		return ptr
	}
	// Negative caching: remember that this type has no triggers so the
	// next hot-path call short-circuits without another vertex.read.
	cacheSetTypeObjectTriggers(typeName, nil)
	return easyjson.NewJSONObject().GetPtr()
}

func cacheGetTypeHRNField(typeName string) (string, bool) {
	if v, ok := typeHRNFieldCache.Load(typeName); ok {
		s, _ := v.(string)
		return s, true
	}
	return "", false
}
func cacheSetTypeHRNField(typeName, path string)  { typeHRNFieldCache.Store(typeName, path) }
func cacheInvalidateTypeHRNField(typeName string) { typeHRNFieldCache.Delete(typeName) }

// getTypeHumanReadableNameField returns the body path the type declares as
// the source of its objects' human-readable names (the type's
// body.human_readable_name_field), or "" when the type declares none or it
// is not a non-empty string.
//
// The result is cached per type (typeHRNFieldCache, including the negative
// "" case) and invalidated by Create/Update/DeleteType, so ReadObject pays
// at most one vertex.read per type for this — every subsequent object read
// of the same type is a cache hit with no type access at all.
func getTypeHumanReadableNameField(ctx *sfPlugins.StatefunContextProcessor, typeName string) string {
	if cached, ok := cacheGetTypeHRNField(typeName); ok {
		return cached
	}

	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, typeName), injectParentHoldsLocks(ctx, nil), nil))
	if som.Status != sfMediators.SYNC_OP_STATUS_OK {
		// Transient failure — do not poison the cache with a false negative.
		return ""
	}

	path := ""
	if s, ok := som.Data.GetByPath("body." + humanReadableNameFieldKey).AsString(); ok {
		path = s
	}
	cacheSetTypeHRNField(typeName, path)
	return path
}

// computeObjectName derives the human-readable name of an object for
// ReadObject. It first honours the name path the object's type declares
// (see getTypeHumanReadableNameField) and reads it from the object's own
// body; if the type declares no path, or the object's body has no non-empty
// string at that path, it falls back to a stable auto-generated name.
//
// objectID is the domain-prefixed object id; objectType is the
// domain-prefixed type id; objectBody may be nil.
func computeObjectName(ctx *sfPlugins.StatefunContextProcessor, objectID, objectType string, objectBody *easyjson.JSON) string {
	if path := getTypeHumanReadableNameField(ctx, objectType); path != "" && objectBody != nil {
		if s, ok := objectBody.GetByPath(path).AsString(); ok && s != "" {
			return s
		}
	}
	return autoObjectName(ctx, objectID, objectType)
}

// autoObjectName builds the fallback name
// "<last token of the type name>_<first 6 chars of the object uuid>_auto",
// where the type name is split on '_' and '-' and both ids are taken
// without their domain prefix.
func autoObjectName(ctx *sfPlugins.StatefunContextProcessor, objectID, objectType string) string {
	typeName := ctx.Domain.GetObjectIDWithoutDomain(objectType)
	lastToken := typeName
	if fields := strings.FieldsFunc(typeName, func(r rune) bool { return r == '_' || r == '-' }); len(fields) > 0 {
		lastToken = fields[len(fields)-1]
	}

	uuid := ctx.Domain.GetObjectIDWithoutDomain(objectID)
	const shortLen = 6
	if len(uuid) > shortLen {
		uuid = uuid[:shortLen]
	}

	return fmt.Sprintf("%s_%s_auto", lastToken, uuid)
}

// getLinkTriggersInfo returns trigger metadata for the (fromType,
// toType) TypesLink. Result is cached per pair in
// typesLinkTriggersCache; invalidated by CreateTypesLink /
// UpdateTypesLink / DeleteTypesLink / DeleteType.
//
// Returns (nil, false) when the TypesLink does not exist (caller treats
// this as "no triggers, no validation possible") or when the load
// transiently fails. Returns (info, true) on success — info.triggers
// may itself be nil to denote "no triggers section".
func getLinkTriggersInfo(ctx *sfPlugins.StatefunContextProcessor, fromType, toType string) (*linkTriggersInfo, bool) {
	if info, ok := cacheGetLinkTriggers(fromType, toType); ok {
		return info, info != nil
	}

	body, err := getLinkBody(ctx, fromType, toType)
	if err != nil || body == nil {
		// Transient or missing — store negative entry so the next call
		// does not retry the NATS round-trip. The negative entry is
		// cleared on TypesLinkCreate, which is the only way it can
		// become positive again.
		cacheSetLinkTriggers(fromType, toType, nil)
		return nil, false
	}

	info := &linkTriggersInfo{
		referenceLinkType: body.GetByPath("type").AsStringDefault(""),
	}
	if triggers := body.GetByPath("triggers"); triggers.IsNonEmptyObject() {
		cloned := triggers.Clone()
		info.triggers = &cloned
	}
	cacheSetLinkTriggers(fromType, toType, info)
	return info, true
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
		// Never cache an empty type: cacheGetObjectType rejects "" so such an
		// entry can never be a hit — it would only sit in the process-global
		// map forever, re-stored on every miss.
		if tp != "" {
			cacheSetObjectType(objectID, tp)
		}
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
