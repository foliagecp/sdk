package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func executeTriggersFromLLOpStack(ctx *sfPlugins.StatefunContextProcessor, opStack *easyjson.JSON, deletedObjectId, deletedObjectType string) {
	llAPIVertexCUDNames := []string{"functions.graph.api.vertex.create", "functions.graph.api.vertex.update", "functions.graph.api.vertex.delete", "functions.graph.api.vertex.read"}
	llAPILinkCUDNames := []string{"functions.graph.api.link.create", "functions.graph.api.link.update", "functions.graph.api.link.delete", "functions.graph.api.link.read"}

	if opStack != nil && opStack.IsArray() {
		for i := 0; i < opStack.ArraySize(); i++ {
			opData := opStack.ArrayElement(i)
			opStr := opData.GetByPath("op").AsStringDefault("")
			if len(opStr) > 0 {
				for j := 0; j < 4; j++ {
					if opStr == llAPIVertexCUDNames[j] {
						vId := opData.GetByPath("id").AsStringDefault("")
						if len(vId) > 0 {
							objectType := ""
							if j == 2 && vId == deletedObjectId {
								objectType = deletedObjectType
							} else {
								objectType, _ = findObjectType(ctx, vId)
							}
							if len(objectType) > 0 {
								var oldBody *easyjson.JSON = nil
								if opData.PathExists("old_body") {
									oldBody = opData.GetByPath("old_body").GetPtr()
								}
								var newBody *easyjson.JSON = nil
								if opData.PathExists("new_body") {
									newBody = opData.GetByPath("new_body").GetPtr()
								}
								executeObjectTriggers(ctx, vId, objectType, oldBody, newBody, j)
							}

						}
					}
					if opStr == llAPILinkCUDNames[j] {
						fromVId := opData.GetByPath("from").AsStringDefault("")
						toVId := opData.GetByPath("to").AsStringDefault("")
						lType := opData.GetByPath("type").AsStringDefault("")

						fromObjectType := ""
						toObjectType := ""
						if j == 2 && fromVId == deletedObjectId {
							fromObjectType = deletedObjectType
						} else {
							fromObjectType, _ = findObjectType(ctx, fromVId)
						}
						if j == 2 && toVId == deletedObjectId {
							toObjectType = deletedObjectType
						} else {
							toObjectType, _ = findObjectType(ctx, toVId)
						}

						if len(lType) > 0 && len(fromVId) > 0 && len(toVId) > 0 && len(fromObjectType) > 0 && len(toObjectType) > 0 {
							var oldBody *easyjson.JSON = nil
							if opData.PathExists("old_body") {
								oldBody = opData.GetByPath("old_body").GetPtr()
							}
							var newBody *easyjson.JSON = nil
							if opData.PathExists("new_body") {
								newBody = opData.GetByPath("new_body").GetPtr()
							}
							executeLinkTriggers(ctx, fromVId, toVId, fromObjectType, toObjectType, lType, oldBody, newBody, j)
						}
					}
				}
			}
		}
	}
}

func executeObjectTriggers(ctx *sfPlugins.StatefunContextProcessor, objectID string, objectType string, oldObjectBody, newObjectBody *easyjson.JSON, tt int /*0 - create, 1 - update, 2 - delete, 3 - read*/) {
	triggers := getTypeTriggers(ctx, objectType)
	if triggers.IsNonEmptyObject() && tt >= 0 && tt < 4 {
		elems := []string{"create", "update", "delete", "read"}
		var functions []string
		if arr, ok := triggers.GetByPath(elems[tt]).AsArrayString(); ok {
			functions = arr
		}

		triggerData := easyjson.NewJSONObject()
		if oldObjectBody != nil {
			triggerData.SetByPath("old_body", *oldObjectBody)
		}
		if newObjectBody != nil {
			triggerData.SetByPath("new_body", *newObjectBody)
		}
		payload := easyjson.NewJSONObject()
		payload.SetByPath(fmt.Sprintf("trigger.object.%s", elems[tt]), triggerData)

		for _, f := range functions {
			system.MsgOnErrorReturn(ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, objectID, &payload, nil))
		}
	}
}

func executeLinkTriggers(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId, fromObjectType, toObjectType, linkType string, oldLinkBody, newLinkBody *easyjson.JSON, tt int /*0 - create, 1 - update, 2 - delete, 3 - read*/) {
	info, ok := getLinkTriggersInfo(ctx, fromObjectType, toObjectType)
	if !ok || info == nil {
		// TypesLink does not exist or could not be loaded — nothing to dispatch.
		return
	}
	if info.triggers == nil || !info.triggers.IsNonEmptyObject() {
		// Cached negative — no triggers configured on this TypesLink.
		return
	}
	if info.referenceLinkType != linkType {
		// Trigger metadata refers to a different object-link-type than
		// the one actually crossed — skip (this is how the original
		// uncached code behaved).
		return
	}
	if tt < 0 || tt >= 4 {
		return
	}

	elems := []string{"create", "update", "delete", "read"}
	var functions []string
	if arr, ok := info.triggers.GetByPath(elems[tt]).AsArrayString(); ok {
		functions = arr
	}
	if len(functions) == 0 {
		return
	}

	triggerData := easyjson.NewJSONObject()
	triggerData.SetByPath("to", easyjson.NewJSON(toObjectId))
	triggerData.SetByPath("type", easyjson.NewJSON(linkType))
	if oldLinkBody != nil {
		triggerData.SetByPath("old_body", *oldLinkBody)
	}
	if newLinkBody != nil {
		triggerData.SetByPath("new_body", *newLinkBody)
	}
	payload := easyjson.NewJSONObject()
	payload.SetByPath(fmt.Sprintf("trigger.link.%s", elems[tt]), triggerData)

	for _, f := range functions {
		system.MsgOnErrorReturn(ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, fromObjectId, &payload, nil))
	}
}

// ----------------------------------------------------------------------------
// Meta lifecycle triggers
//
// User statefuns registered in the body of a system ROOT vertex
// (`types` / `objects`) that fire on lifecycle events of the types and
// type-to-type links (registered in the `types` root) or — globally — of ALL
// objects and object-to-object links (registered in the `objects` root). They
// are stored under `meta_triggers.<section>.<event>` so they never collide with
// the per-type `body.triggers` consumed by executeObjectTriggers /
// executeLinkTriggers. This whole layer is additive: it is dispatched only from
// the explicit hooks below, never re-uses the per-type op_stack path.
//
// section ∈ {type, types_link, object, object_link}; event index tt:
// 0 create, 1 update, 2 delete, 3 read.
// ----------------------------------------------------------------------------

var metaTriggerEvents = []string{"create", "update", "delete", "read"}

// readVertexBodyFromCache returns a vertex body straight from the in-memory
// cache (vertexID must be domain-prefixed), or nil if absent. Used to snapshot
// old/new bodies for meta lifecycle triggers with no extra NATS round-trip.
func readVertexBodyFromCache(ctx *sfPlugins.StatefunContextProcessor, vertexID string) *easyjson.JSON {
	if b, err := ctx.Domain.Cache().GetValueJSON(vertexID); err == nil {
		return b
	}
	return nil
}

// readLinkBodyFromCache returns an out-link body straight from the in-memory
// cache (fromVertexID domain-prefixed; linkName as stored, i.e. domain stripped),
// or nil if absent. Used to snapshot TypesLink bodies for meta triggers without
// the NATS link.read that getLinkBody performs.
func readLinkBodyFromCache(ctx *sfPlugins.StatefunContextProcessor, fromVertexID, linkName string) *easyjson.JSON {
	key := fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, fromVertexID, ctx.Domain.GetObjectIDWithoutDomain(linkName))
	if b, err := ctx.Domain.Cache().GetValueJSON(key); err == nil {
		return b
	}
	return nil
}

// rootHasAnyMetaTriggers reports, via one cheap in-memory read, whether a root
// vertex body carries any meta_triggers at all — used to skip per-op work
// entirely on the common path where nothing is registered.
func rootHasAnyMetaTriggers(ctx *sfPlugins.StatefunContextProcessor, rootVertexID string) bool {
	body, err := ctx.Domain.Cache().GetValueJSON(rootVertexID)
	if err != nil || body == nil {
		return false
	}
	return body.GetByPath("meta_triggers").IsNonEmptyObject()
}

// metaTriggerFunctions returns the statefuns registered for (section, event) in
// a root vertex body, read straight from the in-memory cache — no NATS, no
// locks. It returns nil fast when nothing is registered (the common case), so a
// no-subscriber lifecycle event costs one local read plus a null check, with no
// payload built and no signal sent.
func metaTriggerFunctions(ctx *sfPlugins.StatefunContextProcessor, rootVertexID, section string, tt int) []string {
	if tt < 0 || tt >= 4 {
		return nil
	}
	body, err := ctx.Domain.Cache().GetValueJSON(rootVertexID)
	if err != nil || body == nil {
		return nil
	}
	meta := body.GetByPath("meta_triggers")
	if !meta.IsNonEmptyObject() {
		return nil
	}
	if arr, ok := meta.GetByPath(section + "." + metaTriggerEvents[tt]).AsArrayString(); ok {
		return arr
	}
	return nil
}

func metaSignal(ctx *sfPlugins.StatefunContextProcessor, functions []string, subjectID string, payload *easyjson.JSON) {
	for _, f := range functions {
		system.MsgOnErrorReturn(ctx.Signal(sfPlugins.JetstreamGlobalSignal, f, subjectID, payload, nil))
	}
}

func metaBodyData(oldBody, newBody *easyjson.JSON) easyjson.JSON {
	d := easyjson.NewJSONObject()
	if oldBody != nil {
		d.SetByPath("old_body", *oldBody)
	}
	if newBody != nil {
		d.SetByPath("new_body", *newBody)
	}
	return d
}

// executeMetaTypeTriggers fires the `types` root `type` lifecycle triggers for
// the type vertex typeID. Payload path: trigger.type.<event> {old_body,new_body}.
func executeMetaTypeTriggers(ctx *sfPlugins.StatefunContextProcessor, typeID string, oldBody, newBody *easyjson.JSON, tt int) {
	functions := metaTriggerFunctions(ctx, ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false), "type", tt)
	if len(functions) == 0 {
		return
	}
	data := metaBodyData(oldBody, newBody)
	payload := easyjson.NewJSONObject()
	payload.SetByPath(fmt.Sprintf("trigger.type.%s", metaTriggerEvents[tt]), data)
	metaSignal(ctx, functions, typeID, &payload)
}

// executeMetaTypesLinkTriggers fires the `types` root `types_link` lifecycle
// triggers for the type→type relation fromType→toType. olt is the
// object-link-type the relation carries (its body.type), surfaced in the
// payload. Payload path: trigger.types_link.<event> {to,type,old_body,new_body}.
func executeMetaTypesLinkTriggers(ctx *sfPlugins.StatefunContextProcessor, fromType, toType, olt string, oldBody, newBody *easyjson.JSON, tt int) {
	functions := metaTriggerFunctions(ctx, ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false), "types_link", tt)
	if len(functions) == 0 {
		return
	}
	data := metaBodyData(oldBody, newBody)
	data.SetByPath("to", easyjson.NewJSON(toType))
	data.SetByPath("type", easyjson.NewJSON(olt))
	payload := easyjson.NewJSONObject()
	payload.SetByPath(fmt.Sprintf("trigger.types_link.%s", metaTriggerEvents[tt]), data)
	metaSignal(ctx, functions, fromType, &payload)
}

// executeMetaObjectTriggers fires the `objects` root `object` GLOBAL lifecycle
// triggers for any object objectID (its objectType surfaced in the payload).
// Payload path: trigger.object_meta.<event> {type,old_body,new_body} — a path
// distinct from the per-type trigger.object.<event>, so the two never collide.
func executeMetaObjectTriggers(ctx *sfPlugins.StatefunContextProcessor, objectID, objectType string, oldBody, newBody *easyjson.JSON, tt int) {
	functions := metaTriggerFunctions(ctx, ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false), "object", tt)
	if len(functions) == 0 {
		return
	}
	data := metaBodyData(oldBody, newBody)
	data.SetByPath("type", easyjson.NewJSON(objectType))
	payload := easyjson.NewJSONObject()
	payload.SetByPath(fmt.Sprintf("trigger.object_meta.%s", metaTriggerEvents[tt]), data)
	metaSignal(ctx, functions, objectID, &payload)
}

// executeMetaObjectLinkTriggers fires the `objects` root `object_link` GLOBAL
// lifecycle triggers for an object→object link. Structural link types are
// ignored defensively (these helpers are only invoked from the user
// *ObjectsLink ops, which never pass a structural type). Payload path:
// trigger.object_link.<event> {to,type,old_body,new_body}.
func executeMetaObjectLinkTriggers(ctx *sfPlugins.StatefunContextProcessor, fromObjID, toObjID, linkType string, oldBody, newBody *easyjson.JSON, tt int) {
	switch linkType {
	case TO_TYPELINK, OBJECT_TYPELINK, OBJECTS_TYPELINK, TYPES_TYPELINK:
		return
	}
	functions := metaTriggerFunctions(ctx, ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false), "object_link", tt)
	if len(functions) == 0 {
		return
	}
	data := metaBodyData(oldBody, newBody)
	data.SetByPath("to", easyjson.NewJSON(toObjID))
	data.SetByPath("type", easyjson.NewJSON(linkType))
	payload := easyjson.NewJSONObject()
	payload.SetByPath(fmt.Sprintf("trigger.object_link.%s", metaTriggerEvents[tt]), data)
	metaSignal(ctx, functions, fromObjID, &payload)
}

// executeMetaTriggersFromLLOpStack fires the GLOBAL object / object-link meta
// triggers for every op in an op_stack (mirrors executeTriggersFromLLOpStack but
// targets the `objects` root sections). It is called from the object C/U/D HL
// ops right after the existing per-type dispatch, reusing the op_stack those ops
// already produce. Structural vertices resolve to no type via findObjectType and
// are skipped; structural link types are skipped inside
// executeMetaObjectLinkTriggers — so the internal `__type`/`__object` links
// emitted by CreateObject/DeleteObject never raise a user meta event. Read meta
// is NOT dispatched here (reads carry no op_stack); the read HL ops fire it
// directly.
func executeMetaTriggersFromLLOpStack(ctx *sfPlugins.StatefunContextProcessor, opStack *easyjson.JSON, deletedObjectId, deletedObjectType string) {
	if opStack == nil || !opStack.IsArray() {
		return
	}
	// Fast path: skip the whole walk (incl. per-op findObjectType) when the
	// objects root carries no meta triggers — the common case.
	if !rootHasAnyMetaTriggers(ctx, ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)) {
		return
	}
	vertexOps := []string{"functions.graph.api.vertex.create", "functions.graph.api.vertex.update", "functions.graph.api.vertex.delete", "functions.graph.api.vertex.read"}
	linkOps := []string{"functions.graph.api.link.create", "functions.graph.api.link.update", "functions.graph.api.link.delete", "functions.graph.api.link.read"}
	for i := 0; i < opStack.ArraySize(); i++ {
		opData := opStack.ArrayElement(i)
		opStr := opData.GetByPath("op").AsStringDefault("")
		if len(opStr) == 0 {
			continue
		}
		for j := 0; j < 4; j++ {
			if opStr == vertexOps[j] {
				vId := opData.GetByPath("id").AsStringDefault("")
				if len(vId) == 0 {
					continue
				}
				objectType := ""
				if j == 2 && vId == deletedObjectId {
					objectType = deletedObjectType
				} else {
					objectType, _ = findObjectType(ctx, vId)
				}
				if len(objectType) == 0 {
					continue
				}
				executeMetaObjectTriggers(ctx, vId, objectType, opStackBody(opData, "old_body"), opStackBody(opData, "new_body"), j)
			}
			if opStr == linkOps[j] {
				fromVId := opData.GetByPath("from").AsStringDefault("")
				toVId := opData.GetByPath("to").AsStringDefault("")
				lType := opData.GetByPath("type").AsStringDefault("")
				if len(fromVId) == 0 || len(toVId) == 0 || len(lType) == 0 {
					continue
				}
				executeMetaObjectLinkTriggers(ctx, fromVId, toVId, lType, opStackBody(opData, "old_body"), opStackBody(opData, "new_body"), j)
			}
		}
	}
}

func opStackBody(opData easyjson.JSON, key string) *easyjson.JSON {
	if opData.PathExists(key) {
		return opData.GetByPath(key).GetPtr()
	}
	return nil
}
