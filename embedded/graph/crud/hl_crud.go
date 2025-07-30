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
	selfID := getOriginalID(ctx.Self.ID)
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)

	if typeOperationRedirectedToHub(ctx) {
		return
	}
	operationKeysMutexLock(ctx, []string{selfID, typesVertexId}, true)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	om := sfMediators.NewOpMediator(ctx)

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), nil)))
	if om.GetStatus() == sfMediators.SYNC_OP_STATUS_INCOMPLETE || om.GetStatus() == sfMediators.SYNC_OP_STATUS_FAILED {
		operationKeysMutexUnlock(ctx)
		om.Reply()
		return
	}

	// LINK: types -> <type_name>
	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(selfID))
	link.SetByPath("name", easyjson.NewJSON(selfID))
	link.SetByPath("type", easyjson.NewJSON(TO_TYPELINK))
	link.SetByPath("op_time", easyjson.NewJSON(opTime))

	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, typesVertexId), injectParentHoldsLocks(ctx, &link), nil))
	operationKeysMutexUnlock(ctx)
	om.AggregateOpMsg(m).Reply()
}

/*
	{
		"upsert": bool - optional, default: false
		"replace": bool - optional, default: false
		"body": json
	}
*/
func UpdateType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)

	if typeOperationRedirectedToHub(ctx) {
		return
	}

	operationKeysMutexLock(ctx, []string{selfID}, true)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	om := sfMediators.NewOpMediator(ctx)

	// Handle upsert request ------------------------------
	upsert := ctx.Payload.GetByPath("upsert").AsBoolDefault(false)
	if upsert {
		ctx.Payload.RemoveByPath("upsert")
		som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), nil))
		if som.Status != sfMediators.SYNC_OP_STATUS_OK { // Type does not exist
			m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), ctx.Options))

			operationKeysMutexUnlock(ctx)
			om.AggregateOpMsg(m).Reply()
			return
		}
	}
	// ----------------------------------------------------

	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), nil))
	operationKeysMutexUnlock(ctx)

	om.AggregateOpMsg(m)
	om.Reply()
}

/*
 */
func DeleteType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)

	if typeOperationRedirectedToHub(ctx) {
		return
	}

	operationKeysMutexLock(ctx, []string{selfID}, true)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	goal := PolyTypeCascadeDeleteGoalType{
		reason: SuperTypeDelete,
		target: "",
	}
	polyTypeData := PolyTypeGoalPrepare(ctx, goal)

	om := sfMediators.NewOpMediator(ctx)

	// Vertice's out links are stored in the same domain with the vertex
	pattern := fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, OBJECT_TYPELINK, ">")
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(pattern)
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		toObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", makeSequenceFreeParentBasedID(ctx, toObjectID), injectParentHoldsLocks(ctx, ctx.Payload), nil)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			operationKeysMutexUnlock(ctx)
			om.Reply()
			return
		}
	}

	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), nil))
	operationKeysMutexUnlock(ctx)

	om.AggregateOpMsg(m)

	PolyTypeGoalFinalize(ctx, polyTypeData)

	om.Reply()
}

/*
 */
func ReadType(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)

	if typeOperationRedirectedToHub(ctx) {
		return
	}

	om := sfMediators.NewOpMediator(ctx)

	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))

	RecalculateInheritanceCacheForTypeAtSelfIDIfNeeded(ctx) // Will try to do operationKeysMutexLock(ctx, []string{selfID}, true) that's why it is before operationKeysMutexLock(ctx, []string{selfID}, false), otherwise deadlock appears
	operationKeysMutexLock(ctx, []string{selfID}, false)

	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &payload), ctx.Options))
	operationKeysMutexUnlock(ctx)

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
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not a type", selfID)))
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
	result.SetByPath("to_types", easyjson.NewJSON(toTypes))
	result.SetByPath("object_ids", easyjson.NewJSON(toObjects))
	result.SetByPath("links", m.Data.GetByPath("links"))

	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}

/*
	{
		"origin_type": string
		"body": json
	}
*/
func CreateObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	originType, ok := ctx.Payload.GetByPath("origin_type").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("origin_type is not defined")).Reply()
		return
	}

	originType = ctx.Domain.CreateObjectIDWithHubDomain(originType, true)
	builtInObjectsVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)

	opTime := ctx.Payload.GetByPath("op_time").AsNumericDefault(-1)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))

	operationKeysMutexLock(ctx, []string{builtInObjectsVertexId, selfID, originType}, true)
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), &options)))

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
			{from: builtInObjectsVertexId, to: selfID, name: selfID, lt: OBJECT_TYPELINK},
			{from: selfID, name: "type", to: originType, lt: TO_TYPELINK},
			{from: originType, name: selfID, to: selfID, lt: OBJECT_TYPELINK},
		}

		for _, l := range needLinks {
			link := easyjson.NewJSONObject()
			link.SetByPath("to", easyjson.NewJSON(l.to))
			link.SetByPath("name", easyjson.NewJSON(l.name))
			link.SetByPath("type", easyjson.NewJSON(l.lt))
			link.SetByPath("body", easyjson.NewJSONObject())
			link.SetByPath("force", easyjson.NewJSON(true))
			link.SetByPath("op_time", easyjson.NewJSON(opTime))

			//fmt.Println("             Create object's link:", l.from, l.to, l.lt)
			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, l.from), injectParentHoldsLocks(ctx, &link), ctx.Options)))
			if om.GetStatus() == sfMediators.SYNC_OP_STATUS_INCOMPLETE {
				break // Operation cannot be completed fully, interrupt where it is now and go to the end
			}
		}
	}

	operationKeysMutexUnlock(ctx)

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
	selfID := getOriginalID(ctx.Self.ID)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	om := sfMediators.NewOpMediator(ctx)

	// Handle upsert request ------------------------------
	upsert := ctx.Payload.GetByPath("upsert").AsBoolDefault(false)

	operationKeysMutexLock(ctx, []string{selfID}, true)

	if upsert {
		ctx.Payload.RemoveByPath("upsert")
		if _, err := findObjectType(ctx, selfID); err != nil { // Object does not exist
			if ctx.Payload.GetByPath("origin_type").IsString() {
				om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), ctx.Options)))
				operationKeysMutexUnlock(ctx)
				replyWithoutOpStack(om, ctx)
			} else {
				operationKeysMutexUnlock(ctx)
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("object with id=%s does exist, upsert=true but origin_type is not specified", selfID))).Reply()
			}
			return
		}
	}
	// ----------------------------------------------------

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), &options)))
	operationKeysMutexUnlock(ctx)

	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, "", "")
	}

	replyWithoutOpStack(om, ctx)
}

/*
 */
func DeleteObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	om := sfMediators.NewOpMediator(ctx)

	operationKeysMutexLock(ctx, []string{selfID}, true)
	objectType, err := findObjectType(ctx, selfID)
	if err != nil {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), &options)))
	operationKeysMutexUnlock(ctx)
	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, selfID, objectType)
	}

	replyWithoutOpStack(om, ctx)
}

/*
 */
func ReadObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)

	om := sfMediators.NewOpMediator(ctx)
	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))

	operationKeysMutexLock(ctx, []string{selfID}, false)
	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &payload), ctx.Options))
	operationKeysMutexUnlock(ctx)

	om.AggregateOpMsg(m)

	if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_IDLE {
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("object with id=%s does not exist", selfID))).Reply()
		return
	}

	executeTriggersLater := om.GetLastSyncOp().Data.PathExists("op_stack")

	objectType := ""
	toObjects := []string{}
	for i := 0; i < m.Data.GetByPath("links.out.names").ArraySize(); i++ {
		tp := m.Data.GetByPath("links.out.types").ArrayElement(i).AsStringDefault("")
		toId := m.Data.GetByPath("links.out.ids").ArrayElement(i).AsStringDefault("")
		if tp == TO_TYPELINK {
			objectType = toId
		} else {
			toObjects = append(toObjects, toId)
		}
	}
	if len(objectType) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("object with id=%s has no type", selfID)))
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
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not an object, not connected to objects topology", selfID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}
	if !typeBidirectionalLink {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s is not an object, inlink from type is broken", selfID)))
		system.MsgOnErrorReturn(om.ReplyWithData(easyjson.NewJSONObject().GetPtr()))
		return
	}

	result := easyjson.NewJSONObject()
	if m.Data.PathExists("body") {
		result.SetByPath("body", m.Data.GetByPath("body"))

	}
	result.SetByPath("type", easyjson.NewJSON(objectType))
	result.SetByPath("to_objects", easyjson.NewJSON(toObjects))
	result.SetByPath("links", m.Data.GetByPath("links"))

	if executeTriggersLater {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, "", "")
	}

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
	selfID := getOriginalID(ctx.Self.ID)
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

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
	link.SetByPath("op_time", easyjson.NewJSON(opTime))

	operationKeysMutexLock(ctx, []string{selfID, toType}, true)
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))
	operationKeysMutexUnlock(ctx)

	om.Reply()
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
	selfID := getOriginalID(ctx.Self.ID)
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

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
	link.SetByPath("op_time", easyjson.NewJSON(opTime))

	operationKeysMutexLock(ctx, []string{selfID, toType}, true)
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))
	operationKeysMutexUnlock(ctx)

	om.Reply()
}

/*
	{
		"to": string
	}
*/
func DeleteTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	if typeOperationRedirectedToHub(ctx) {
		return
	}

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	goal := PolyTypeCascadeDeleteGoalType{
		reason: SuperTypeDeleteOutTypeObjectLink,
		target: "",
	}
	polyTypeData := PolyTypeGoalPrepare(ctx, goal)

	om := sfMediators.NewOpMediator(ctx)

	toType, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	toType = ctx.Domain.CreateObjectIDWithHubDomain(toType, true)

	operationKeysMutexLock(ctx, []string{selfID, toType}, true)

	originLinkType, err := getObjectsLinkTypeFromTypesLink(ctx, selfID, toType)
	if err != nil {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	typeObjects, err := findTypeObjects(ctx, selfID)
	if err != nil {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	payload := easyjson.NewJSONObjectWithKeyValue("link_type", easyjson.NewJSON(originLinkType))
	payload.SetByPath("to_object_type", easyjson.NewJSON(toType))
	payload.SetByPath("op_time", easyjson.NewJSON(opTime))
	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))

	lateTriggersArr := []*easyjson.JSON{}
	for _, objectId := range typeObjects {
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.delete_object_filtered_out_links", makeSequenceFreeParentBasedID(ctx, objectId), injectParentHoldsLocks(ctx, &payload), &options)))
		if om.GetLastSyncOp().Data.PathExists("op_stack") {
			lateTriggersArr = append(lateTriggersArr, om.GetLastSyncOp().Data.GetByPathPtr("op_stack").Clone().GetPtr())
		}
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(toType))
	objectLink.SetByPath("type", easyjson.NewJSON(TO_TYPELINK))
	objectLink.SetByPath("op_time", easyjson.NewJSON(opTime))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), ctx.Options)))
	operationKeysMutexUnlock(ctx)

	PolyTypeGoalFinalize(ctx, polyTypeData)

	for _, lateTrigger := range lateTriggersArr {
		executeTriggersFromLLOpStack(ctx, lateTrigger, "", "")
	}

	om.Reply()
}

/*
	{
		"to": string
	}
*/
func ReadTypesLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
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

	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toType))
	payload.SetByPath("type", easyjson.NewJSON(TO_TYPELINK))
	payload.SetByPath("details", easyjson.NewJSON(true))

	operationKeysMutexLock(ctx, []string{selfID, toType}, false)
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &payload), ctx.Options)))
	operationKeysMutexUnlock(ctx)

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

	operationKeysMutexLock(ctx, []string{selfID, objectToID}, true)
	_, _, linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, selfID, objectToID)
	if err != nil {
		operationKeysMutexUnlock(ctx)
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
	objectLink.SetByPath("op_time", easyjson.NewJSON(opTime))

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), &options)))
	operationKeysMutexUnlock(ctx)

	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, "", "")
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
	_, _, linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, selfID, objectToID)
	if err != nil {
		operationKeysMutexUnlock(ctx)
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
	}
*/
func DeleteObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
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
	_, _, linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, selfID, objectToID)
	if err != nil {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("op_time", easyjson.NewJSON(opTime))

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &objectLink), &options)))
	operationKeysMutexUnlock(ctx)

	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, "", "")
	}

	replyWithoutOpStack(om, ctx)
}

/*
	{
		"to": string
	}
*/
func ReadObjectsLink(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	objectToID, ok := ctx.Payload.GetByPath("to").AsString()
	if !ok {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("'to' undefined")).Reply()
		return
	}
	objectToID = ctx.Domain.CreateObjectIDWithThisDomain(objectToID, false)

	operationKeysMutexLock(ctx, []string{selfID, objectToID}, false)
	fromObjectType, toObjectType, linkType, err := getReferenceLinkTypeBetweenTwoObjects(ctx, selfID, objectToID)
	if err != nil {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(objectToID))
	payload.SetByPath("type", easyjson.NewJSON(linkType))
	payload.SetByPath("details", easyjson.NewJSON(true))
	m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &payload), ctx.Options))
	operationKeysMutexUnlock(ctx)
	om.AggregateOpMsg(m)

	if om.GetLastSyncOp().Data.PathExists("op_stack") {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, "", "")
	}

	result := m.Data
	result.SetByPath("from_type", easyjson.NewJSON(fromObjectType))
	result.SetByPath("to_type", easyjson.NewJSON(toObjectType))

	replyWithoutOpStack(om, ctx)
	system.MsgOnErrorReturn(om.ReplyWithData(&result))
}
