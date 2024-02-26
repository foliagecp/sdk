package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

/*
	{
		"body": json
	}
*/
func CreateType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	thisType := ctx.Domain.CreateObjectIDWithHubDomain(ctx.Self.ID, true)
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", thisType, ctx.Payload, nil)))
	if om.GetStatus() == sfMediators.SYNC_OP_STATUS_INCOMPLETE || om.GetStatus() == sfMediators.SYNC_OP_STATUS_FAILED {
		om.Reply()
		return
	}

	// LINK: types -> <type_name>
	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(thisType))
	link.SetByPath("name", easyjson.NewJSON(thisType))
	link.SetByPath("type", easyjson.NewJSON(TYPE_TYPELINK))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", typesVertexId, &link, nil))).Reply()
}

/*
	{
		"replace": bool - optional, default: false
		"body": json
	}
*/
func UpdateType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", ctx.Self.ID, ctx.Payload, nil))).Reply()
}

/*
 */
func DeleteType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	// Vertice's out links are stored in the same domain with the vertex
	pattern := fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, OBJECT_TYPELINK, ">")
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(pattern)
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		toObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", toObjectID, nil, nil)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			om.Reply()
			return
		}
	}

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", ctx.Self.ID, nil, nil))).Reply()
}

/*
	{
		"origin_type": string,
		"body": json
	}
*/
func CreateObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	originType, ok := ctx.Payload.GetByPath("origin_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("origin_type is not defined")).Reply()
		return
	}
	originType = ctx.Domain.CreateObjectIDWithHubDomain(originType, true)
	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", ctx.Self.ID, ctx.Payload, &options)))

	var opStack *easyjson.JSON
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		opStack = om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
	}

	if !(om.GetStatus() == sfMediators.SYNC_OP_STATUS_INCOMPLETE || om.GetStatus() == sfMediators.SYNC_OP_STATUS_FAILED) {
		type _link struct {
			from, to, name, lt string
		}

		needLinks := []_link{
			{from: ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false), to: ctx.Self.ID, name: ctx.Self.ID, lt: OBJECT_TYPELINK},
			{from: ctx.Self.ID, name: originType, to: originType, lt: TYPE_TYPELINK},
			{from: originType, name: ctx.Self.ID, to: ctx.Self.ID, lt: OBJECT_TYPELINK},
		}

		for _, l := range needLinks {
			link := easyjson.NewJSONObject()
			link.SetByPath("to", easyjson.NewJSON(l.to))
			link.SetByPath("name", easyjson.NewJSON(l.name))
			link.SetByPath("type", easyjson.NewJSON(l.lt))
			link.SetByPath("body", easyjson.NewJSONObject())

			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", l.from, &link, nil)))
			if om.GetStatus() == sfMediators.SYNC_OP_STATUS_INCOMPLETE || om.GetStatus() == sfMediators.SYNC_OP_STATUS_FAILED {
				break // Operation cannot be completed fully, interrupt where it is now and go to the end
			}
		}
	}

	if opStack != nil {
		executeTriggersFromLLOpStack(ctx, opStack, "")
	}

	om.Reply()
}

/*
	{
		"replace": bool - optional, default: false
		"body": json
	}
*/
func UpdateObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", ctx.Self.ID, ctx.Payload, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "")
	}

	om.Reply()
}

/*
 */
func DeleteObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	objectType := findObjectType(ctx, ctx.Self.ID)

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", ctx.Self.ID, nil, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), objectType)
	}

	om.Reply()
}

/*
	{
		"to": string
		"object_type": string
		"body": json
	}

create type -> type link
*/
func CreateTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	objectLinkType, ok := ctx.Payload.GetByPath("object_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("object_type is not defined")).Reply()
		return
	}

	fromType := ctx.Domain.CreateObjectIDWithHubDomain(ctx.Self.ID, true)

	toType, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	toType = ctx.Domain.CreateObjectIDWithHubDomain(toType, false)

	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(toType))
	link.SetByPath("name", easyjson.NewJSON(toType))
	link.SetByPath("type", easyjson.NewJSON(TYPE_TYPELINK))
	link.SetByPath("body.type", easyjson.NewJSON(objectLinkType))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", fromType, &link, nil))).Reply()
}

/*
	{
		"to": string,
		"body": json, optional
	}
*/
func UpdateTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	toType, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	toType = ctx.Domain.CreateObjectIDWithHubDomain(toType, false)

	link := ctx.Payload.Clone()
	link.SetByPath("to", easyjson.NewJSON(toType))
	link.SetByPath("type", easyjson.NewJSON(TYPE_TYPELINK))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", ctx.Self.ID, &link, nil)))

	om.Reply()
}

/*
	{
		"to": string
	}
*/
func DeleteTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	toType, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	toType = ctx.Domain.CreateObjectIDWithHubDomain(toType, true)

	originLinkType, err := getObjectsLinkTypeFromTypesLink(ctx, ctx.Self.ID, toType)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	typeObjects, err := findTypeObjects(ctx, ctx.Self.ID)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	payload := easyjson.NewJSONObjectWithKeyValue("link_type", easyjson.NewJSON(originLinkType))
	payload.SetByPath("to_object_type", easyjson.NewJSON(toType))
	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	for _, objectId := range typeObjects {
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.delete_object_filtered_out_links", objectId, &payload, &options)))
		if om.GetLastSyncOp().Data.PathExists("op_stack") {
			executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "")
		}
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(toType))
	objectLink.SetByPath("type", easyjson.NewJSON(TYPE_TYPELINK))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", ctx.Self.ID, &objectLink, nil)))

	om.Reply()
}

/*
	{
		"to": string,
		"name": string, // optional, "to" will be used if not defined
		"body": json
	}

create object -> object link
*/
func CreateObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
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

	linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("name", easyjson.NewJSON(linkName))
	objectLink.SetByPath("type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("body", ctx.Payload.GetByPath("body"))

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", ctx.Self.ID, &objectLink, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "")
	}

	om.Reply()
}

/*
	{
		"to": string,
		"body": json
	}
*/
func UpdateObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	objectToID = ctx.Domain.CreateObjectIDWithThisDomain(objectToID, false)

	linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("body", ctx.Payload.GetByPath("body"))

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", ctx.Self.ID, &objectLink, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "")
	}

	om.Reply()
}

/*
	{
		"to": string,
	}
*/
func DeleteObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	objectToID = ctx.Domain.CreateObjectIDWithThisDomain(objectToID, false)

	linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("type", easyjson.NewJSON(linkType))

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", ctx.Self.ID, &objectLink, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "")
	}

	om.Reply()
}
