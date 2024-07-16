package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func replyWithoutOpStack(om *sfMediators.OpMediator, ctx *sfPlugins.StatefunContextProcessor, data ...easyjson.JSON) {
	var res easyjson.JSON
	if len(data) > 0 {
		res = data[0]

	} else {
		res = om.GetData()
	}

	returnOpStack := false
	if ctx.Options != nil {
		returnOpStack = ctx.Options.GetByPath("op_stack").AsBoolDefault(false)
	}
	if !returnOpStack {
		res.RemoveByPath("op_stack")
	}
	if !res.IsNonEmptyObject() {
		res = easyjson.NewJSONNull()
	}
	system.MsgOnErrorReturn(om.ReplyWithData(&res))
}

/*
	{
		"body": json
	}
*/
func CreateType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if typeOperationRedirectedToHub(ctx) {
		return
	}
	om := sfMediators.NewOpMediator(ctx)
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", ctx.Self.ID, ctx.Payload, nil)))
	if om.GetStatus() == sfMediators.SYNC_OP_STATUS_INCOMPLETE || om.GetStatus() == sfMediators.SYNC_OP_STATUS_FAILED {
		om.Reply()
		return
	}

	// LINK: types -> <type_name>
	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(ctx.Self.ID))
	link.SetByPath("name", easyjson.NewJSON(ctx.Self.ID))
	link.SetByPath("type", easyjson.NewJSON(TO_TYPELINK))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", typesVertexId, &link, nil))).Reply()
}

/*
	{
		"upsert": bool - optional, default: false
		"replace": bool - optional, default: false
		"body": json
	}
*/
func UpdateType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	// Handle upsert request ------------------------------
	upsert := ctx.Payload.GetByPath("upsert").AsBoolDefault(false)
	if upsert {
		ctx.Payload.RemoveByPath("upsert")
		som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", ctx.Self.ID, nil, nil))
		if som.Status != sfMediators.SYNC_OP_STATUS_OK { // Type does not exist
			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", ctx.Self.ID, ctx.Payload, ctx.Options))).Reply()
			return
		}
	}
	// ----------------------------------------------------

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", ctx.Self.ID, ctx.Payload, nil))).Reply()
}

/*
 */
func DeleteType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if typeOperationRedirectedToHub(ctx) {
		return
	}

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
 */
func ReadType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", ctx.Self.ID, &payload, nil))
	om.AggregateOpMsg(m)

	vertexIsType := false
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)
	for i := 0; i < m.Data.GetByPath("links.in").ArraySize(); i++ {
		fromId := m.Data.GetByPath("links.in").ArrayElement(i).GetByPath("from").AsStringDefault("")
		if fromId == typesVertexId {
			vertexIsType = true
		}
	}
	if !vertexIsType {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not a type", ctx.Self.ID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}

	toTypes := []string{}
	toObjects := []string{}
	for i := 0; i < m.Data.GetByPath("links.out.names").ArraySize(); i++ {
		tp := m.Data.GetByPath("links.out.types").ArrayElement(i).AsStringDefault("")
		toId := m.Data.GetByPath("links.out.ids").ArrayElement(i).AsStringDefault("")
		if tp == TO_TYPELINK {
			toTypes = append(toTypes, toId)
		}
		if tp == OBJECT_TYPELINK {
			toObjects = append(toObjects, toId)
		}
	}

	result := easyjson.NewJSONObject()
	if m.Data.PathExists("body") {
		result.SetByPath("body", m.Data.GetByPath("body"))

	}
	result.SetByPath("to_types", easyjson.JSONFromArray(toTypes))
	result.SetByPath("object_ids", easyjson.JSONFromArray(toObjects))

	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

/*
	{
		"origin_type": string
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

	targetReply := om.GetLastSyncOp().Data
	var opStack *easyjson.JSON
	if targetReply.PathExists("op_stack") {
		opStack = targetReply.GetByPathPtr("op_stack")
	}

	if !(om.GetStatus() == sfMediators.SYNC_OP_STATUS_INCOMPLETE) {
		type _link struct {
			from, to, name, lt string
		}

		needLinks := []_link{
			{from: ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false), to: ctx.Self.ID, name: ctx.Self.ID, lt: OBJECT_TYPELINK},
			{from: ctx.Self.ID, name: originType, to: originType, lt: TO_TYPELINK},
			{from: originType, name: ctx.Self.ID, to: ctx.Self.ID, lt: OBJECT_TYPELINK},
		}

		for _, l := range needLinks {
			link := easyjson.NewJSONObject()
			link.SetByPath("to", easyjson.NewJSON(l.to))
			link.SetByPath("name", easyjson.NewJSON(l.name))
			link.SetByPath("type", easyjson.NewJSON(l.lt))
			link.SetByPath("body", easyjson.NewJSONObject())

			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", l.from, &link, nil)))
			if om.GetStatus() == sfMediators.SYNC_OP_STATUS_INCOMPLETE {
				break // Operation cannot be completed fully, interrupt where it is now and go to the end
			}
		}
	}

	if opStack != nil {
		executeTriggersFromLLOpStack(ctx, opStack, "", "")
	}

	replyWithoutOpStack(om, ctx, targetReply)
}

/*
	{
		"origin_type": string, not requred! required only if "upsert"==true
		"upsert": bool - optional, default: false
		"replace": bool - optional, default: false
		"body": json
	}
*/
func UpdateObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	// Handle upsert request ------------------------------
	upsert := ctx.Payload.GetByPath("upsert").AsBoolDefault(false)
	if upsert {
		ctx.Payload.RemoveByPath("upsert")
		if findObjectType(ctx, ctx.Self.ID) == "" { // Object does not exist
			if ctx.Payload.GetByPath("origin_type").IsString() {
				om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", ctx.Self.ID, ctx.Payload, ctx.Options)))
				replyWithoutOpStack(om, ctx)
			} else {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("object with id=%s does exist, upsert=true but origin_type is not specified", ctx.Self.ID))).Reply()
			}
			return
		}
	}
	// ----------------------------------------------------

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", ctx.Self.ID, ctx.Payload, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "", "")
	}

	replyWithoutOpStack(om, ctx)
}

/*
 */
func DeleteObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	objectType := findObjectType(ctx, ctx.Self.ID)

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", ctx.Self.ID, nil, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), ctx.Self.ID, objectType)
	}

	replyWithoutOpStack(om, ctx)
}

/*
 */
func ReadObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", ctx.Self.ID, &payload, nil))
	om.AggregateOpMsg(m)
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "", "")
	}

	objectType := ""
	toObjects := []string{}
	for i := 0; i < m.Data.GetByPath("links.out.names").ArraySize(); i++ {
		tp := m.Data.GetByPath("links.out.types").ArrayElement(i).AsStringDefault("")
		toId := m.Data.GetByPath("links.out.ids").ArrayElement(i).AsStringDefault("")
		if tp == TO_TYPELINK {
			objectType = toId
		}
		if tp == OBJECT_TYPELINK {
			toObjects = append(toObjects, toId)
		}
	}
	if len(objectType) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("objects with id=%s has no type", ctx.Self.ID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}

	vertexIsObject := false
	typeBidirectionalLink := false
	objectsVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)
	for i := 0; i < m.Data.GetByPath("links.in").ArraySize(); i++ {
		fromId := m.Data.GetByPath("links.in").ArrayElement(i).GetByPath("from").AsStringDefault("")
		if fromId == objectsVertexId {
			vertexIsObject = true
		}
		if fromId == objectType {
			typeBidirectionalLink = true
		}
	}
	if !vertexIsObject {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not an object, not connected to objects topology", ctx.Self.ID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}
	if !typeBidirectionalLink {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not an object, inlink from type is broken", ctx.Self.ID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}

	result := easyjson.NewJSONObject()
	if m.Data.PathExists("body") {
		result.SetByPath("body", m.Data.GetByPath("body"))

	}
	result.SetByPath("type", easyjson.NewJSON(objectType))
	result.SetByPath("to_objects", easyjson.JSONFromArray(toObjects))

	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

/*
	{
		"to": string
		"object_type": string
		"body": json
		"tags": []string
	}

create type -> type link
*/
func CreateTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	objectLinkType, ok := ctx.Payload.GetByPath("object_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("object_type is not defined")).Reply()
		return
	}

	toType, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	toType = ctx.Domain.CreateObjectIDWithHubDomain(toType, true)

	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(toType))
	link.SetByPath("name", easyjson.NewJSON(toType))
	link.SetByPath("type", easyjson.NewJSON(TO_TYPELINK))
	if ctx.Payload.PathExists("tags") {
		link.SetByPath("tags", ctx.Payload.GetByPath("tags"))
	}
	link.SetByPath("body.type", easyjson.NewJSON(objectLinkType))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", ctx.Self.ID, &link, nil))).Reply()
}

/*
	{
		"to": string,
		"body": json, optional
		"tags": []string
		"upsert": bool
		"replace": bool
	}
*/
func UpdateTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	toType, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	toType = ctx.Domain.CreateObjectIDWithHubDomain(toType, true)

	link := ctx.Payload.Clone()
	link.SetByPath("to", easyjson.NewJSON(toType))
	link.SetByPath("type", easyjson.NewJSON(TO_TYPELINK))
	if ctx.Payload.PathExists("tags") {
		link.SetByPath("tags", ctx.Payload.GetByPath("tags"))
	}
	if ctx.Payload.PathExists("upsert") {
		link.SetByPath("name", easyjson.NewJSON(toType))
		link.SetByPath("upsert", ctx.Payload.GetByPath("upsert"))
	}
	if ctx.Payload.PathExists("replace") {
		link.SetByPath("replace", ctx.Payload.GetByPath("replace"))
	}
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", ctx.Self.ID, &link, nil)))

	om.Reply()
}

/*
	{
		"to": string
	}
*/
func DeleteTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if typeOperationRedirectedToHub(ctx) {
		return
	}

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
			executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "", "")
		}
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(toType))
	objectLink.SetByPath("type", easyjson.NewJSON(TO_TYPELINK))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", ctx.Self.ID, &objectLink, nil)))

	om.Reply()
}

/*
	{
		"to": string
	}
*/
func ReadTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	toType, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}

	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toType))
	payload.SetByPath("type", easyjson.NewJSON(TO_TYPELINK))
	payload.SetByPath("details", easyjson.NewJSON(true))
	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", ctx.Self.ID, &payload, nil))
	om.AggregateOpMsg(m)

	om.Reply()
}

/*
	{
		"to": string,
		"name": string, // optional, "to" will be used if not defined
		"body": json
		"tags": []string
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

	_, _, linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("name", easyjson.NewJSON(linkName))
	objectLink.SetByPath("type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("body", ctx.Payload.GetByPath("body"))
	if ctx.Payload.PathExists("tags") {
		objectLink.SetByPath("tags", ctx.Payload.GetByPath("tags"))
	}

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", ctx.Self.ID, &objectLink, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "", "")
	}

	replyWithoutOpStack(om, ctx)
}

/*
	{
		"to": string
		"name": string, // not needed, required if "upsert" is true
		"body": json
		"tags": []string
		"upsert": bool
		"replace": bool
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

	_, _, linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("body", ctx.Payload.GetByPath("body"))
	if ctx.Payload.PathExists("tags") {
		objectLink.SetByPath("tags", ctx.Payload.GetByPath("tags"))
	}
	if ctx.Payload.PathExists("name") {
		objectLink.SetByPath("name", ctx.Payload.GetByPath("name"))
	}
	if ctx.Payload.PathExists("upsert") {
		objectLink.SetByPath("upsert", ctx.Payload.GetByPath("upsert"))
	}
	if ctx.Payload.PathExists("replace") {
		objectLink.SetByPath("replace", ctx.Payload.GetByPath("replace"))
	}

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", ctx.Self.ID, &objectLink, &options)))
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "", "")
	}

	replyWithoutOpStack(om, ctx)
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

	_, _, linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, objectToID)
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
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "", "")
	}

	replyWithoutOpStack(om, ctx)
}

/*
	{
		"to": string
	}
*/
func ReadObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	toObject, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}

	fromObjectType, toObjectType, linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, ctx.Self.ID, toObject)
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toObject))
	payload.SetByPath("type", easyjson.NewJSON(linkType))
	payload.SetByPath("details", easyjson.NewJSON(true))
	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", ctx.Self.ID, &payload, nil))
	om.AggregateOpMsg(m)
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		executeTriggersFromLLOpStack(ctx, om.GetLastSyncOp().Data.GetByPathPtr("op_stack"), "", "")
	}

	result := m.Data
	result.SetByPath("from_type", easyjson.NewJSON(fromObjectType))
	result.SetByPath("to_type", easyjson.NewJSON(toObjectType))

	replyWithoutOpStack(om, ctx)
	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}
