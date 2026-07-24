package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

const (
	TYPES_SUBTYPELINK = "__sub"
)

func RegisterPolyTypeFunctions(runtime *statefun.Runtime) {
	// High-Level Type Inheritance
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.supertype.create", CreateObjectsLinkFromSuperTypes, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.supertype.update", UpdateObjectsLinkFromSuperTypes, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.supertype.delete", DeleteObjectsLinkFromSuperTypes, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.supertype.read", ReadObjectsLinkFromSuperTypes, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.subtype.set", TypeSetSubType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.subtype.remove", TypeRemoveSubType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
}

/*var (
	currentTypeGraphVersion int64 = 0
)

func increaseTypeGraphVersion() {
	atomic.AddInt64(&currentTypeGraphVersion, 1)
}*/

/*
	{
		"sub_type": string
	}
*/
func TypeSetSubType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	childType, ok := ctx.Payload.GetByPath("sub_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'sub_type' undefined")).Reply()
		return
	}
	childTypeWithDomain := ctx.Domain.CreateObjectIDWithHubDomain(childType, true)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	// Whole-vertex write lock (NOT edge-granular): this op's type-model cascade
	// (UpdateTypeModelVersion / PolyType goal finalize) mutates shared type state
	// under the lock and its nested ops need write access to these vertices, so a
	// read guard would self-deadlock. Low-traffic schema op — no parallelism need.
	operationKeysMutexLock(ctx, []string{selfID, childTypeWithDomain}, true, opTime)
	defer operationKeysMutexUnlock(ctx)

	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(childTypeWithDomain))
	link.SetByPath("name", easyjson.NewJSON("child_"+childType))
	link.SetByPath("type", easyjson.NewJSON(TYPES_SUBTYPELINK))
	link.SetByPath("upsert", easyjson.NewJSON(true))

	// link.update in upsert mode instead of bare link.create: this op is part
	// of every application's bootstrap schema setup and MUST be idempotent
	// across restarts. link.create fails a repeat call with "link ... already
	// exists" — noise in logs and a FAILED reply on every reboot. Upsert keeps
	// both outcomes clean:
	//   - edge absent  → link.update delegates to link.create with all its
	//     invariant checks intact → OK (same as before);
	//   - edge present → empty-body merge with no tags is a provable no-op →
	//     IDLE, nothing written, no indices touched.
	// All three of name/to/type are supplied above, which is exactly what the
	// upsert contract requires (see LLAPILinkUpdate docs).
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))

	UpdateTypeModelVersion(ctx)

	om.Reply()
}

/*
	{
		"sub_type": string
	}
*/
func TypeRemoveSubType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	childType, ok := ctx.Payload.GetByPath("sub_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'sub_type' undefined")).Reply()
		return
	}
	childTypeWithDomain := ctx.Domain.CreateObjectIDWithHubDomain(childType, true)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	// Whole-vertex write lock (NOT edge-granular): the subtype-removal cascade
	// (PolyType goal prepare/finalize) mutates shared type state under the lock and
	// its nested ops need write access to these vertices, so a read guard would
	// self-deadlock. Low-traffic schema op — no parallelism need.
	operationKeysMutexLock(ctx, []string{selfID, childTypeWithDomain}, true, opTime)
	defer operationKeysMutexUnlock(ctx)

	link := easyjson.NewJSONObject()
	link.SetByPath("name", easyjson.NewJSON("child_"+childType))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))

	goal := PolyTypeCascadeDeleteGoalType{
		reason: SuperTypeDeleteSubType,
		target: childTypeWithDomain,
	}
	data := PolyTypeGoalPrepare(ctx, goal)
	PolyTypeGoalFinalize(ctx, data)

	om.Reply()
}

/*
	{
		"to": string,
		"from_super_type": string,
		"to_super_type": string,
		"name": string, // optional, "to" will be used if not defined
		"body": json
		"tags": []string
	}

create object -> object link
*/
func CreateObjectsLinkFromSuperTypes(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	objectToID = ctx.Domain.CreateObjectIDWithThisDomain(objectToID, false)

	linkName, ok := ctx.Payload.GetByPath("name").AsString()
	if !ok {
		linkName = objectToID
	}

	fromObjectClaimType := ctx.Payload.GetByPath("from_super_type").AsStringDefault("")
	toObjectClaimType := ctx.Payload.GetByPath("to_super_type").AsStringDefault("")

	fromObjectClaimType = ctx.Domain.CreateObjectIDWithHubDomain(fromObjectClaimType, true)
	toObjectClaimType = ctx.Domain.CreateObjectIDWithHubDomain(toObjectClaimType, true)

	// Lock the EDGE (owner + name), read-guard the endpoints; supertype links to
	// DIFFERENT targets of the same `from` create in parallel.
	operationKeysMutexLockMixed(ctx, []string{edgeLockKey(selfID, linkName)}, []string{selfID, objectToID}, opTime)

	objectLinkType := isObjectLinkPermittedForClaimedTypes(ctx, selfID, objectToID, fromObjectClaimType, toObjectClaimType)
	if len(objectLinkType) == 0 {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no object link from type %s to type %s", fromObjectClaimType, toObjectClaimType))).Reply()
		return
	}

	finalLinkType := ctx.Domain.GetObjectIDWithoutDomain(fromObjectClaimType) + "#" + ctx.Domain.GetObjectIDWithoutDomain(toObjectClaimType) + "#" + objectLinkType

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("name", easyjson.NewJSON(linkName))
	objectLink.SetByPath("type", easyjson.NewJSON(finalLinkType))
	objectLink.SetByPath("body", ctx.Payload.GetByPath("body"))
	if ctx.Payload.PathExists("tags") {
		objectLink.SetByPath("tags", ctx.Payload.GetByPath("tags"))
	}
	objectLink.SetByPath("op_time", easyjson.NewJSON(opTime))

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), &options)))
	operationKeysMutexUnlock(ctx)

	replyWithoutOpStack(om, ctx)
}

/*
	{
		"to":              string,
		"from_super_type": string,
		"to_super_type":   string,
		"name":            string, // optional; defaults to "to" ID
		"body":            json,
		"tags":            []string,
		"replace":         bool,   // false (default) = DeepMerge; true = replace + re-index
	}

Always upserts: creates the link if it does not exist.
*/
func UpdateObjectsLinkFromSuperTypes(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	objectToID = ctx.Domain.CreateObjectIDWithThisDomain(objectToID, false)

	linkName := ctx.Payload.GetByPath("name").AsStringDefault(objectToID)

	fromObjectClaimType := ctx.Payload.GetByPath("from_super_type").AsStringDefault("")
	toObjectClaimType := ctx.Payload.GetByPath("to_super_type").AsStringDefault("")

	fromObjectClaimType = ctx.Domain.CreateObjectIDWithHubDomain(fromObjectClaimType, true)
	toObjectClaimType = ctx.Domain.CreateObjectIDWithHubDomain(toObjectClaimType, true)

	// Lock the EDGE (owner + name), read-guard the endpoints.
	operationKeysMutexLockMixed(ctx, []string{edgeLockKey(selfID, linkName)}, []string{selfID, objectToID}, opTime)

	objectLinkType := isObjectLinkPermittedForClaimedTypes(ctx, selfID, objectToID, fromObjectClaimType, toObjectClaimType)
	if len(objectLinkType) == 0 {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("no object link from type %s to type %s", fromObjectClaimType, toObjectClaimType))).Reply()
		return
	}

	finalLinkType := ctx.Domain.GetObjectIDWithoutDomain(fromObjectClaimType) + "#" + ctx.Domain.GetObjectIDWithoutDomain(toObjectClaimType) + "#" + objectLinkType

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("type", easyjson.NewJSON(finalLinkType))
	objectLink.SetByPath("name", easyjson.NewJSON(linkName))
	objectLink.SetByPath("body", ctx.Payload.GetByPath("body"))
	objectLink.SetByPath("upsert", easyjson.NewJSON(true))
	if ctx.Payload.PathExists("tags") {
		objectLink.SetByPath("tags", ctx.Payload.GetByPath("tags"))
	}
	if ctx.Payload.PathExists("replace") {
		objectLink.SetByPath("replace", ctx.Payload.GetByPath("replace"))
	}
	objectLink.SetByPath("op_time", easyjson.NewJSON(opTime))

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), &options)))
	operationKeysMutexUnlock(ctx)

	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, "", "")
	}

	replyWithoutOpStack(om, ctx)
}

/*
	{
		"to": string,
		"from_super_type": string,
		"to_super_type": string
	}
*/
func DeleteObjectsLinkFromSuperTypes(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	objectToID = ctx.Domain.CreateObjectIDWithThisDomain(objectToID, false)

	// Cross-pack edges store a compound type "<fromClaim>#<toClaim>#<rel>".
	// Between the same (from, to) object pair there can be multiple such
	// edges with DIFFERENT claim pairs (and therefore different compound
	// prefixes). We MUST pick the edge whose compound type starts with
	// "<fromClaim>#<toClaim>#" matching the caller's claim — anything else
	// is a different cross-pack edge and must be left alone.
	//
	// The previous implementation called resolveLinkBetweenTwoObjects (which
	// returns the first key from a sharded-map scan) and verified the claim
	// post-hoc; that was non-deterministic and could silently leave the
	// targeted edge in the graph while returning idle.
	fromClaimShort := ctx.Domain.GetObjectIDWithoutDomain(
		ctx.Domain.CreateObjectIDWithHubDomain(ctx.Payload.GetByPath("from_super_type").AsStringDefault(""), true))
	toClaimShort := ctx.Domain.GetObjectIDWithoutDomain(
		ctx.Domain.CreateObjectIDWithHubDomain(ctx.Payload.GetByPath("to_super_type").AsStringDefault(""), true))
	compoundPrefix := fromClaimShort + "#" + toClaimShort + "#"

	// Resolve the SPECIFIC edge (by KV) BEFORE locking so we lock the edge itself,
	// not the owner vertex — supertype deletes of DIFFERENT links run in parallel.
	linkName, linkType, edgeExists := resolveLinkBetweenTwoObjectsByTypePrefix(ctx, selfID, objectToID, compoundPrefix)
	if !edgeExists {
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("object link from=%s to=%s with claimed types (%s,%s) does not exist", selfID, objectToID, fromClaimShort, toClaimShort))).Reply()
		return
	}

	// Lock the EDGE (owner + name), read-guard the endpoints.
	operationKeysMutexLockMixed(ctx, []string{edgeLockKey(selfID, linkName)}, []string{selfID, objectToID}, opTime)

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("name", easyjson.NewJSON(linkName))
	objectLink.SetByPath("type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("op_time", easyjson.NewJSON(opTime))

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), &options)))
	operationKeysMutexUnlock(ctx)

	replyWithoutOpStack(om, ctx)
}

/*
	{
		"to": string,
		"from_super_type": string,
		"to_super_type": string
	}

Reads the SuperType/composition (cross-pack) edge between two objects — the
one stored under the compound type "<fromClaim>#<toClaim>#<rel>". This is the
counterpart of ReadObjectsLink, which reads only the base-type edge declared
by the TypesLink between the objects' direct types.
*/
func ReadObjectsLinkFromSuperTypes(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	objectToID = ctx.Domain.CreateObjectIDWithThisDomain(objectToID, false)

	// Address the SPECIFIC cross-pack edge by its claim pair. As in
	// DeleteObjectsLinkFromSuperTypes, multiple compound edges with different
	// claim pairs can exist between the same (from, to) — match the exact
	// "<fromClaim>#<toClaim>#" prefix rather than picking an arbitrary edge.
	fromClaimShort := ctx.Domain.GetObjectIDWithoutDomain(
		ctx.Domain.CreateObjectIDWithHubDomain(ctx.Payload.GetByPath("from_super_type").AsStringDefault(""), true))
	toClaimShort := ctx.Domain.GetObjectIDWithoutDomain(
		ctx.Domain.CreateObjectIDWithHubDomain(ctx.Payload.GetByPath("to_super_type").AsStringDefault(""), true))
	compoundPrefix := fromClaimShort + "#" + toClaimShort + "#"

	// Resolve the SPECIFIC edge (by KV) BEFORE locking so we read-lock the edge
	// itself, not the owner vertex.
	linkName, _, edgeExists := resolveLinkBetweenTwoObjectsByTypePrefix(ctx, selfID, objectToID, compoundPrefix)
	if !edgeExists {
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("object link from=%s to=%s with claimed types (%s,%s) does not exist", selfID, objectToID, fromClaimShort, toClaimShort))).Reply()
		return
	}

	// Read-lock the EDGE (owner + name).
	operationKeysMutexLock(ctx, []string{edgeLockKey(selfID, linkName)}, false, opTime)

	payload := easyjson.NewJSONObject()
	payload.SetByPath("name", easyjson.NewJSON(linkName))
	payload.SetByPath("details", easyjson.NewJSON(true))
	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &payload), ctx.Options))

	// Object types for the result are best-effort (parity with ReadObjectsLink).
	fromObjectType, _ := findObjectType(ctx, selfID)
	toObjectType, _ := findObjectType(ctx, objectToID)

	operationKeysMutexUnlock(ctx)
	om.AggregateOpMsg(m)

	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, "", "")
	}

	result := m.Data
	result.SetByPath("from_type", easyjson.NewJSON(fromObjectType))
	result.SetByPath("to_type", easyjson.NewJSON(toObjectType))

	replyWithoutOpStack(om, ctx, result)
}
