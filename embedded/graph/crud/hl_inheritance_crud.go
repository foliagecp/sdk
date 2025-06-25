package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

const (
	TYPES_CHILDLINK = "__child"
)

/*var (
	currentTypeGraphVersion int64 = 0
)

func increaseTypeGraphVersion() {
	atomic.AddInt64(&currentTypeGraphVersion, 1)
}*/

/*
	{
		"child_type": string
	}
*/
func TypeSetChild(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	childType, ok := ctx.Payload.GetByPath("child_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'child_type' undefined")).Reply()
		return
	}
	childTypeWithDomain := ctx.Domain.CreateObjectIDWithHubDomain(childType, true)

	operationKeysMutexLock(ctx, []string{selfID, childTypeWithDomain})
	defer operationKeysMutexUnlock(ctx)

	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(childTypeWithDomain))
	link.SetByPath("name", easyjson.NewJSON("child_"+childType))
	link.SetByPath("type", easyjson.NewJSON(TYPES_CHILDLINK))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))

	UpdateTypeModelVersion(ctx)

	om.Reply()
}

/*
	{
		"child_type": string
	}
*/
func TypeRemoveChild(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	childType, ok := ctx.Payload.GetByPath("child_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'child_type' undefined")).Reply()
		return
	}
	childTypeWithDomain := ctx.Domain.CreateObjectIDWithHubDomain(childType, true)

	operationKeysMutexLock(ctx, []string{selfID, childTypeWithDomain})
	defer operationKeysMutexUnlock(ctx)

	link := easyjson.NewJSONObject()
	link.SetByPath("name", easyjson.NewJSON("child_"+childType))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))

	goal := InheritanceCascadeDeleteGoalType{
		reason: ParentTypeDeleteChild,
		target: childTypeWithDomain,
	}
	data := InheritaceGoalPrepare(ctx, goal)
	fmt.Println("     TypeRemoveChild:", data)
	InheritaceGoalFinalize(ctx, data)

	om.Reply()
}

/*
	{
		"to": string,
		"from_claim_type": string,
		"to_claim_type": string,
		"name": string, // optional, "to" will be used if not defined
		"body": json
		"tags": []string
	}

create object -> object link
*/
func CreateObjectsLinkFromClaimedTypes(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
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

	fromObjectClaimType := ctx.Payload.GetByPath("from_claim_type").AsStringDefault("")
	toObjectClaimType := ctx.Payload.GetByPath("to_claim_type").AsStringDefault("")

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

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), &options)))
	operationKeysMutexUnlock(ctx)

	replyWithoutOpStack(om, ctx)
}

/*
	{
		"to": string,
		"from_claim_type": string,
		"to_claim_type": string
	}
*/
func DeleteObjectsLinkFromClaimedTypes(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	objectToID = ctx.Domain.CreateObjectIDWithThisDomain(objectToID, false)

	operationKeysMutexLock(ctx, []string{selfID, objectToID})

	fromObjectClaimType := ctx.Payload.GetByPath("from_claim_type").AsStringDefault("")
	toObjectClaimType := ctx.Payload.GetByPath("to_claim_type").AsStringDefault("")

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
	objectLink.SetByPath("type", easyjson.NewJSON(finalLinkType))

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), &options)))
	operationKeysMutexUnlock(ctx)

	replyWithoutOpStack(om, ctx)
}
