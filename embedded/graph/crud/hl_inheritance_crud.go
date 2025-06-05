package crud

import (
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

func TypeRemoveChild(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	// for each child_type
	//
	//   objects := get_objects(child_type)
}
