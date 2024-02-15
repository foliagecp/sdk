package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

/*
	{
		"body": json
	}
*/
func CreateType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	thisType := ctx.Domain.CreateObjectIDWithHubDomainIfndef(ctx.Domain.GetObjectIDWithoutDomain(ctx.Self.ID))
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomainIfndef(BUILT_IN_TYPES)

	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.vertex.create", thisType, ctx.Payload, nil)))
	if sosc.GetStatus() == common.SYNC_OP_STATUS_INCOMPLETE || sosc.GetStatus() == common.SYNC_OP_STATUS_FAILED {
		sosc.Reply()
		return
	}

	// LINK: types -> <type_name>
	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(thisType))
	link.SetByPath("link_type", easyjson.NewJSON(TYPE_TYPELINK))
	link.SetByPath("body.name", easyjson.JSONFromArray([]string{thisType}))

	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.create", typesVertexId, &link, nil)))
	sosc.Reply()
}

/*
	{
		"mode": string, optional, default: DeepMerge
		"body": json
	}
*/
func UpdateType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.vertex.update", ctx.Self.ID, ctx.Payload, nil)))
	sosc.Reply()
}

/*
 */
func DeleteType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	// Vertice's out links are stored in the same domain with the vertex
	pattern := fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, OBJECT_TYPELINK, ">")
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(pattern)
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		toObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.api.object.delete", toObjectID, nil, nil)))
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.Reply()
			return
		}
	}

	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.vertex.delete", ctx.Self.ID, nil, nil)))
	sosc.Reply()
}

/*
	{
		"origin_type": string,
		"body": json
	}
*/
func CreateObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	originType, ok := ctx.Payload.GetByPath("origin_type").AsString()
	if !ok {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("origin_type is not defined"))).Reply()
		return
	}
	originType = ctx.Domain.CreateObjectIDWithHubDomainIfndef(ctx.Domain.GetObjectIDWithoutDomain(originType))
	options := easyjson.NewJSONObjectWithKeyValue("return_op_stack", easyjson.NewJSON(true))
	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.vertex.create", ctx.Self.ID, ctx.Payload, &options)))

	var opStack *easyjson.JSON
	if sosc.GetLastSyncOp().Data.PathExists("op_stack") {
		opStack = sosc.GetLastSyncOp().Data.GetByPathPtr("op_stack")
	}

	if !(sosc.GetStatus() == common.SYNC_OP_STATUS_INCOMPLETE || sosc.GetStatus() == common.SYNC_OP_STATUS_FAILED) {
		type _link struct {
			from, to, lt string
		}

		needLinks := []_link{
			{from: ctx.Domain.CreateObjectIDWithHubDomainIfndef(BUILT_IN_OBJECTS), to: ctx.Self.ID, lt: OBJECT_TYPELINK},
			{from: ctx.Self.ID, to: originType, lt: TYPE_TYPELINK},
			{from: originType, to: ctx.Self.ID, lt: OBJECT_TYPELINK},
		}

		for _, l := range needLinks {
			link := easyjson.NewJSONObject()
			link.SetByPath("to", easyjson.NewJSON(l.to))
			link.SetByPath("link_type", easyjson.NewJSON(l.lt))
			link.SetByPath("body", easyjson.NewJSONObject())

			switch l.lt {
			case TYPE_TYPELINK:
				link.SetByPath("body.name", easyjson.NewJSON(l.to))
			}

			sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.create", l.from, &link, nil)))
			if sosc.GetStatus() == common.SYNC_OP_STATUS_INCOMPLETE || sosc.GetStatus() == common.SYNC_OP_STATUS_FAILED {
				break // Operation cannot be completed fully, interrupt where it is now and go to the end
			}
		}
	}

	if opStack != nil {
		executeTriggersFromLLOpStack(ctx, opStack)
	}

	sosc.Reply()
}

/*
	{
		"mode": string, optional, default: merge
		"body": json
	}
*/
func UpdateObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	options := easyjson.NewJSONObjectWithKeyValue("return_op_stack", easyjson.NewJSON(true))
	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.vertex.update", ctx.Self.ID, ctx.Payload, &options)))
	if sosc.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, sosc.GetLastSyncOp().Data.GetByPathPtr("op_stack"))
	}

	sosc.Reply()
}

/*
 */
func DeleteObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	options := easyjson.NewJSONObjectWithKeyValue("return_op_stack", easyjson.NewJSON(true))
	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.vertex.delete", ctx.Self.ID, nil, &options)))
	if sosc.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, sosc.GetLastSyncOp().Data.GetByPathPtr("op_stack"))
	}

	sosc.Reply()
}

/*
	{
		"to": string
		"object_link_type": string
		"body": json
	}

create type -> type link
*/
func CreateTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	objectLinkType, ok := ctx.Payload.GetByPath("object_link_type").AsString()
	if !ok {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("object_link_type is not defined"))).Reply()
		return
	}

	fromType := ctx.Domain.CreateObjectIDWithHubDomainIfndef(ctx.Domain.GetObjectIDWithoutDomain(ctx.Self.ID))

	toType := ctx.Payload.GetByPath("to").AsStringDefault("")
	toType = ctx.Domain.CreateObjectIDWithHubDomainIfndef(ctx.Domain.GetObjectIDWithoutDomain(toType))

	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(toType))
	link.SetByPath("link_type", easyjson.NewJSON(TYPE_TYPELINK))
	link.SetByPath("body.link_type", easyjson.NewJSON(objectLinkType))
	link.SetByPath("body.name", easyjson.JSONFromArray([]string{toType}))

	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.create", fromType, &link, nil)))
	sosc.Reply()
}

/*
	{
		"to": string,
		"body": json, optional
	}
*/
func UpdateTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	link := ctx.Payload.Clone()
	link.SetByPath("link_type", easyjson.NewJSON(TYPE_TYPELINK))
	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.update", ctx.Self.ID, &link, nil)))

	sosc.Reply()
}

/*
	{
		"to": string
	}
*/
func DeleteTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	toType, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		sosc.Integreate(common.SyncOpFailed("'to' undefined")).Reply()
		return
	}
	toType = ctx.Domain.CreateObjectIDWithHubDomainIfndef(ctx.Domain.GetObjectIDWithoutDomain(toType))

	originLinkType, err := getObjectsLinkTypeFromTypesLink(ctx, ctx.Self.ID, toType)
	if err != nil {
		sosc.Integreate(common.SyncOpFailed(err.Error())).Reply()
		return
	}

	typeObjects, err := findTypeObjects(ctx, ctx.Self.ID)
	if err != nil {
		sosc.Integreate(common.SyncOpFailed(err.Error())).Reply()
		return
	}

	// Delete all links with LINK_TYPE leading to an objects of TYPE

	payload := easyjson.NewJSONObjectWithKeyValue("link_type", easyjson.NewJSON(originLinkType))
	payload.SetByPath("to_object_type", easyjson.NewJSON(toType))
	options := easyjson.NewJSONObjectWithKeyValue("return_op_stack", easyjson.NewJSON(true))
	for _, objectId := range typeObjects {
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.create", objectId, &payload, &options)))
		if sosc.GetLastSyncOp().Data.PathExists("op_stack") {
			executeTriggersFromLLOpStack(ctx, sosc.GetLastSyncOp().Data.GetByPathPtr("op_stack"))
		}
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(toType))
	objectLink.SetByPath("link_type", easyjson.NewJSON(TYPE_TYPELINK))
	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.delete", ctx.Self.ID, &objectLink, nil)))

	sosc.Reply()
}

/*
	{
		"to": string,
		"body": json
	}

create object -> object link
*/
func CreateObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	objectToID = ctx.Domain.CreateObjectIDWithThisDomainIfndef(objectToID)
	if !ok {
		sosc.Integreate(common.SyncOpFailed("'to' undefined")).Reply()
		return
	}

	linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
	if err != nil {
		sosc.Integreate(common.SyncOpFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("link_type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("body", ctx.Payload.GetByPath("body"))

	options := easyjson.NewJSONObjectWithKeyValue("return_op_stack", easyjson.NewJSON(true))
	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.create", ctx.Self.ID, &objectLink, &options)))
	if sosc.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, sosc.GetLastSyncOp().Data.GetByPathPtr("op_stack"))
	}

	sosc.Reply()
}

/*
	{
		"to": string,
		"body": json
	}
*/
func UpdateObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	objectToID = ctx.Domain.CreateObjectIDWithThisDomainIfndef(objectToID)
	if !ok {
		sosc.Integreate(common.SyncOpFailed("'to' undefined")).Reply()
		return
	}

	linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
	if err != nil {
		sosc.Integreate(common.SyncOpFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("link_type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("body", ctx.Payload.GetByPath("body"))

	options := easyjson.NewJSONObjectWithKeyValue("return_op_stack", easyjson.NewJSON(true))
	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.update", ctx.Self.ID, &objectLink, &options)))
	if sosc.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, sosc.GetLastSyncOp().Data.GetByPathPtr("op_stack"))
	}

	sosc.Reply()
}

/*
	{
		"to": string,
	}
*/
func DeleteObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	objectToID = ctx.Domain.CreateObjectIDWithThisDomainIfndef(objectToID)
	if !ok {
		sosc.Integreate(common.SyncOpFailed("'to' undefined")).Reply()
		return
	}

	linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
	if err != nil {
		sosc.Integreate(common.SyncOpFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("link_type", easyjson.NewJSON(linkType))

	options := easyjson.NewJSONObjectWithKeyValue("return_op_stack", easyjson.NewJSON(true))
	sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.delete", ctx.Self.ID, &objectLink, &options)))
	if sosc.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, sosc.GetLastSyncOp().Data.GetByPathPtr("op_stack"))
	}

	sosc.Reply()
}
