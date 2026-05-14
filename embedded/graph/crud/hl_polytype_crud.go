package crud

import (
	"fmt"
	"strings"

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

	operationKeysMutexLock(ctx, []string{selfID, childTypeWithDomain}, true)
	defer operationKeysMutexUnlock(ctx)

	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(childTypeWithDomain))
	link.SetByPath("name", easyjson.NewJSON("child_"+childType))
	link.SetByPath("type", easyjson.NewJSON(TYPES_SUBTYPELINK))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))

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

	operationKeysMutexLock(ctx, []string{selfID, childTypeWithDomain}, true)
	defer operationKeysMutexUnlock(ctx)

	link := easyjson.NewJSONObject()
	link.SetByPath("name", easyjson.NewJSON("child_"+childType))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))

	goal := PolyTypeCascadeDeleteGoalType{
		reason: SuperTypeDeleteSubType,
		target: childTypeWithDomain,
	}
	data := PolyTypeGoalPrepare(ctx, goal)
	fmt.Println("     TypeRemoveSubType:", data)
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

	operationKeysMutexLock(ctx, []string{selfID, objectToID}, true)

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

	operationKeysMutexLock(ctx, []string{selfID, objectToID}, true)

	// Resolve the existing edge by KV only — no object.read. For delete the
	// type-claim/schema check is intentionally NOT performed: if the physical
	// edge is gone (target deleted, partial cleanup, etc.) deletion is a
	// no-op and must return idle, matching the LL link.delete contract.
	linkName, linkType, edgeExists := resolveLinkBetweenTwoObjects(ctx, selfID, objectToID)
	if !edgeExists {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("object link from=%s to=%s does not exist", selfID, objectToID))).Reply()
		return
	}

	// Cross-pack edges store a compound type "<fromClaim>#<toClaim>#<rel>".
	// If the existing edge claims something different from what the caller
	// requested, treat it as "the requested edge does not exist" — idle.
	// This prevents an unrelated edge with the same (from, to) from being
	// deleted by a SuperType-delete that targets a different claim pair.
	fromClaimShort := ctx.Domain.GetObjectIDWithoutDomain(
		ctx.Domain.CreateObjectIDWithHubDomain(ctx.Payload.GetByPath("from_super_type").AsStringDefault(""), true))
	toClaimShort := ctx.Domain.GetObjectIDWithoutDomain(
		ctx.Domain.CreateObjectIDWithHubDomain(ctx.Payload.GetByPath("to_super_type").AsStringDefault(""), true))
	tokens := strings.Split(linkType, "#")
	if len(tokens) != 3 || tokens[0] != fromClaimShort || tokens[1] != toClaimShort {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("object link from=%s to=%s with claimed types (%s,%s) does not exist", selfID, objectToID, fromClaimShort, toClaimShort))).Reply()
		return
	}

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
