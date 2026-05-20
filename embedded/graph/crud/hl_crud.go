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
	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID, typesVertexId}, true, opTime)

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
	// Defense-in-depth: drop any cached object-triggers entry for this
	// type. Today getTypeTriggers does not cache transient failures so a
	// stale entry can only arise if the type was previously created,
	// observed, deleted (cache cleared) and now re-created — DeleteType
	// already handles the clear, so this is a no-op in well-behaved
	// flows. The call is kept symmetric with CreateTypesLink's
	// invalidation below, so that a future change to getTypeTriggers
	// that adds negative caching for "type does not yet exist" cannot
	// silently break this code path.
	cacheInvalidateTypeObjectTriggers(selfID)
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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID}, true, opTime)

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

	// Type body may carry the triggers section — invalidate the trigger
	// cache so the next CRUD hot path observes the latest registration.
	cacheInvalidateTypeObjectTriggers(selfID)

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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID}, true, opTime)

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

	cachePurgeTypeEdgesForType(selfID)
	// Drop any cached object-trigger entry for this type AND every
	// link-trigger entry that mentions it as either endpoint (those
	// TypesLinks are gone with the type).
	cacheInvalidateTypeObjectTriggers(selfID)
	cachePurgeLinkTriggersForType(selfID)

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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID}, false, opTime)

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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))

	operationKeysMutexLock(ctx, []string{builtInObjectsVertexId, selfID, originType}, true, opTime)

	// rollbackStack accumulates the op_stack of every step that succeeded.
	// If a later step fails we walk this stack in reverse and invoke the
	// matching LL inverse for each entry, leaving the graph close to its
	// pre-call state instead of in the half-built "object without __type
	// link" trap that breaks subsequent recovery.
	rollbackStack := easyjson.NewJSONArray()

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), &options)))

	targetReply := om.GetLastSyncOp().Data
	var opStack *easyjson.JSON
	if targetReply.PathExists("op_stack") {
		opStack = targetReply.GetByPathPtr("op_stack")
		mergeOpStack(&rollbackStack, opStack)
	}

	// Only proceed to the link-create pipeline if vertex.create actually
	// succeeded — earlier code accepted FAILED too because it only filtered
	// out INCOMPLETE.
	if om.GetStatus() == sfMediators.SYNC_OP_STATUS_OK {
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

			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, l.from), injectParentHoldsLocks(ctx, &link), ctx.Options)))

			// Record what actually got created so we can undo it if needed.
			lastReply := om.GetLastSyncOp().Data
			if lastReply.PathExists("op_stack") {
				mergeOpStack(&rollbackStack, lastReply.GetByPathPtr("op_stack"))
			}

			if om.GetStatus() != sfMediators.SYNC_OP_STATUS_OK {
				break // any non-OK terminates the pipeline; do NOT continue to next link
			}
		}
	}

	// If the multi-step pipeline did not fully succeed, roll back recorded ops.
	// Best-effort: any failure during rollback is logged but not propagated.
	if om.GetStatus() != sfMediators.SYNC_OP_STATUS_OK {
		rollbackOpStack(ctx, &rollbackStack)
		cacheDeleteObjectType(selfID) // type cache must not claim the partial state
		operationKeysMutexUnlock(ctx)
		replyWithoutOpStack(om, ctx, targetReply)
		return
	}

	operationKeysMutexUnlock(ctx)

	if opStack != nil {
		executeTriggersFromLLOpStack(ctx, opStack, "", "")
	}
	cacheSetObjectType(selfID, originType)

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

	operationKeysMutexLock(ctx, []string{selfID}, true, opTime)

	if upsert {
		ctx.Payload.RemoveByPath("upsert")
		if _, err := findObjectType(ctx, selfID); err != nil {
			// Object's __type cannot be resolved. Three sub-cases:
			//   (a) origin_type missing in payload → cannot upsert at all.
			//   (b) vertex truly absent → original CreateObject path.
			//   (c) vertex exists but the __type link is missing — orphan from
			//       a partial failure or a bare LL link.delete. Self-heal by
			//       restoring just the CMDB structural links with force=true,
			//       then fall through to the regular vertex.update for body.
			originTypeShort, hasOriginType := ctx.Payload.GetByPath("origin_type").AsString()
			if !hasOriginType {
				operationKeysMutexUnlock(ctx)
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("object with id=%s does exist, upsert=true but origin_type is not specified", selfID))).Reply()
				return
			}

			vertexID := ctx.Domain.CreateObjectIDWithThisDomain(selfID, false)
			vertexExists := ctx.Domain.Cache().ExistsJson(vertexID)

			if !vertexExists {
				// (b) Object truly absent — delegate to ObjectCreate.
				om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), ctx.Options)))
				operationKeysMutexUnlock(ctx)
				replyWithoutOpStack(om, ctx)
				return
			}

			// (c) Orphan repair — restore the three CMDB invariant links so
			// the object becomes resolvable again. force=true makes each
			// link.create idempotent: a missing link is created, an existing
			// one is silently overwritten with its (empty) body.
			originType := ctx.Domain.CreateObjectIDWithHubDomain(originTypeShort, true)
			builtInObjectsVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)
			type _link struct {
				from, to, name, lt string
			}
			repairLinks := []_link{
				{from: builtInObjectsVertexId, to: selfID, name: selfID, lt: OBJECT_TYPELINK},
				{from: selfID, name: "type", to: originType, lt: TO_TYPELINK},
				{from: originType, name: selfID, to: selfID, lt: OBJECT_TYPELINK},
			}
			for _, l := range repairLinks {
				link := easyjson.NewJSONObject()
				link.SetByPath("to", easyjson.NewJSON(l.to))
				link.SetByPath("name", easyjson.NewJSON(l.name))
				link.SetByPath("type", easyjson.NewJSON(l.lt))
				link.SetByPath("body", easyjson.NewJSONObject())
				link.SetByPath("force", easyjson.NewJSON(true))
				link.SetByPath("op_time", easyjson.NewJSON(opTime))
				_, _ = ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, l.from), injectParentHoldsLocks(ctx, &link), ctx.Options)
			}
			cacheSetObjectType(selfID, originType)
			// Fall through to the regular vertex.update path so the body is
			// also refreshed.
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
// DeleteObject drives the LL vertex.delete pipeline and dispatches CMDB
// triggers keyed by the object's type.
//
// Orphan tolerance (Option 1B from the D4 triage): if findObjectType
// cannot resolve the type, we check the cache directly for the vertex
// body. Two recoverable sub-cases:
//
//   (a) vertex truly absent — there is nothing to delete, reply IDLE
//       (idempotent no-op).
//   (b) vertex body is present but the type is unresolvable — this is
//       the orphan state left behind by a partial LL vertex.delete or
//       a bare LL link.delete on a __type link. We STILL drive
//       vertex.delete to clean up the body; triggers are skipped
//       because there is no type context to dispatch them under.
//
// Without this branch, an orphan would permanently reject every
// subsequent ObjectDelete with "has no type" — exactly the symptom from
// the production bug report.
func DeleteObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	om := sfMediators.NewOpMediator(ctx)

	operationKeysMutexLock(ctx, []string{selfID}, true, opTime)
	objectType, err := findObjectType(ctx, selfID)
	if err != nil {
		// Probe the cache directly: with Option 1A applied, ReadObject
		// returns IDLE for both "vertex missing" and "vertex is an orphan",
		// so findObjectType's error text alone cannot distinguish them.
		vertexID := ctx.Domain.CreateObjectIDWithThisDomain(selfID, false)
		if !ctx.Domain.Cache().ExistsJson(vertexID) {
			// (a) Vertex truly absent — idempotent IDLE.
			operationKeysMutexUnlock(ctx)
			om.AggregateOpMsg(sfMediators.OpMsgIdle(err.Error())).Reply()
			return
		}
		// (b) Orphan: clear stale cached type and fall through with empty
		// objectType. vertex.delete still runs; trigger dispatch is
		// skipped below because objectType == "".
		cacheDeleteObjectType(selfID)
		objectType = ""
	}

	options := ctx.Options.Clone()
	options.SetByPath("op_stack", easyjson.NewJSON(true))

	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), &options)))
	operationKeysMutexUnlock(ctx)
	// Triggers can only be dispatched when we know the type. For orphans
	// (objectType == "") there is no meaningful type context to fire
	// delete-triggers under, so skip the trigger pass entirely. Body
	// cleanup is still what matters.
	if objectType != "" && om.GetLastSyncOp().Data.PathExists("op_stack") {
		j := om.GetLastSyncOp().Data.GetByPathPtr("op_stack")
		executeTriggersFromLLOpStack(ctx, j, selfID, objectType)
	}

	cacheDeleteObjectType(selfID)
	replyWithoutOpStack(om, ctx)
}

/*
 */
func ReadObject(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	detailsV2 := ctx.Payload.GetByPath("details_v2").AsBoolDefault(false)

	om := sfMediators.NewOpMediator(ctx)
	payload := easyjson.NewJSONObject()
	if detailsV2 {
		payload.SetByPath("details_v2", easyjson.NewJSON(true))
	} else {
		payload.SetByPath("details", easyjson.NewJSON(true))
	}

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID}, false, opTime)
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
	if detailsV2 {
		for i := 0; i < m.Data.GetByPath("links.out").ArraySize(); i++ {
			link := m.Data.GetByPath("links.out").ArrayElement(i)
			tp := link.GetByPath("type").AsStringDefault("")
			toId := link.GetByPath("to").AsStringDefault("")
			if tp == TO_TYPELINK {
				objectType = toId
			} else {
				toObjects = append(toObjects, toId)
			}
		}
	} else {
		for i := 0; i < m.Data.GetByPath("links.out.names").ArraySize(); i++ {
			tp := m.Data.GetByPath("links.out.types").ArrayElement(i).AsStringDefault("")
			toId := m.Data.GetByPath("links.out.ids").ArrayElement(i).AsStringDefault("")
			if tp == TO_TYPELINK {
				objectType = toId
			} else {
				toObjects = append(toObjects, toId)
			}
		}
	}
	if len(objectType) == 0 {
		// Orphan vertex: body is present in KV but the __type out-link is
		// gone. This can happen after a partial vertex.delete (link cleanup
		// loop fails mid-flight or the runtime loses leadership between the
		// WAL flush that committed link.deletes and the WAL flush that was
		// supposed to commit the body delete — see Causes 1/2/3 in the D4
		// triage). Externally, an orphan is indistinguishable from a missing
		// object: it cannot be resolved as a CMDB object, has no triggers,
		// no type. Surface it as IDLE ("not found") rather than as a hard
		// FAILED with "has no type" — this:
		//   - keeps every read caller on the existing "missing object"
		//     code path (no special handling required);
		//   - makes the client-side IDLE→ErrNotFound mapping work for
		//     orphans too;
		//   - stops the noisy "has no type" error storms that show up in
		//     production whenever an orphan is touched repeatedly.
		// The body itself is cleaned up by Option 1B (DeleteObject)
		// when something — HLMB rebuild, user code, manual operator
		// action — issues a delete against this id.
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("object with id=%s does not exist", selfID)))
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

	operationKeysMutexLock(ctx, []string{selfID, toType}, true, opTime)
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))
	operationKeysMutexUnlock(ctx)

	if om.GetStatus() == sfMediators.SYNC_OP_STATUS_OK {
		cacheSetTypeEdge(selfID, toType, objectLinkType)
	}
	// A new TypesLink may carry a triggers section in its body —
	// invalidate any negative entry produced by a prior failed lookup.
	cacheInvalidateLinkTriggers(selfID, toType)

	om.Reply()
}

/*
	{
		"to": string,
		"body": json, optional
		"tags": []string
		"upsert": bool
		"object_type": string // use when upsert=true; will be overrided if already presented on link
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

		objectLinkType := ""
		if olt, e := getObjectsLinkTypeFromTypesLink(ctx, ctx.Self.ID, toType); e == nil {
			objectLinkType = olt
		} else {
			objectLinkType = toType
		}
		objectLinkType = ctx.Payload.GetByPath("object_type").AsStringDefault(objectLinkType)

		if len(objectLinkType) > 0 {
			link.SetByPath("body.type", easyjson.NewJSON(objectLinkType))
		}
	}
	if ctx.Payload.PathExists("replace") {
		link.SetByPath("replace", ctx.Payload.GetByPath("replace"))
	}
	link.SetByPath("op_time", easyjson.NewJSON(opTime))

	operationKeysMutexLock(ctx, []string{selfID, toType}, true, opTime)
	om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &link), ctx.Options)))
	operationKeysMutexUnlock(ctx)

	if om.GetStatus() == sfMediators.SYNC_OP_STATUS_OK {
		if ctx.Payload.PathExists("body.type") {
			if newLT, ok := ctx.Payload.GetByPath("body.type").AsString(); ok && newLT != "" {
				cacheSetTypeEdge(selfID, toType, newLT)
			}
		}
	}
	// Body may carry a triggers section change — drop cached entry so
	// the next dispatch reloads.
	cacheInvalidateLinkTriggers(selfID, toType)

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

	operationKeysMutexLock(ctx, []string{selfID, toType}, true, opTime)

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

	cacheDeleteTypeEdge(selfID, toType)
	// TypesLink is gone — drop its cached trigger metadata so a future
	// re-create starts from a clean slate.
	cacheInvalidateLinkTriggers(selfID, toType)

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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID, toType}, false, opTime)
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

	operationKeysMutexLock(ctx, []string{selfID, objectToID}, true, opTime)
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

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("to", easyjson.NewJSON(objectToID))
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

	operationKeysMutexLock(ctx, []string{selfID, objectToID}, true, opTime)

	// Hot path: cheap KV-only resolution. Falls back to the schema-based
	// resolver only when the edge does not yet exist (upsert / cold path).
	linkName, linkType, edgeExists := resolveLinkBetweenTwoObjects(ctx, selfID, objectToID)
	if !edgeExists {
		_, _, lt, err := getReferenceLinkTypeBetweenTwoObjects(ctx, selfID, objectToID)
		if err != nil {
			operationKeysMutexUnlock(ctx)
			om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
			return
		}
		linkType = lt
	} else if !objectLink.PathExists("name") && linkName != "" {
		// Prefer the resolved link name when the caller did not provide one,
		// so LL link.update can target the edge unambiguously.
		objectLink.SetByPath("name", easyjson.NewJSON(linkName))
	}

	objectLink.SetByPath("type", easyjson.NewJSON(linkType))

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

	operationKeysMutexLock(ctx, []string{selfID, objectToID}, true, opTime)

	// Cheap KV-only resolution of (linkName, linkType) for the existing edge.
	// No object.read calls; idempotent if the edge is already gone.
	linkName, linkType, edgeExists := resolveLinkBetweenTwoObjects(ctx, selfID, objectToID)
	if !edgeExists {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("object link from=%s to=%s does not exist", selfID, objectToID))).Reply()
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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID, objectToID}, false, opTime)
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
