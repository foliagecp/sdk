package crud

// Putting an object back together, or removing what is left of it.
//
// An object is a vertex plus THREE edges, and every edge is four cache keys —
// the owner's target, body and type-index, and the mirror in-key on the other
// vertex:
//
//	objects --<id>--> obj     the object is in the model
//	obj     --type--> type    the object knows its type
//	type    --<id>--> obj     the type knows its object
//
// Twelve keys. Lose any one of them and the object is no longer an object: a
// read answers "not connected to objects topology", or "inlink from type is
// broken", or the type resolves to nothing — and until now nothing put it
// back. The damage outlives every operation, because each operation asks
// whether the object is readable, not whether it is whole.
//
// THE RULE. If the type can be learned from ANY source, all twelve keys are
// rewritten and the object is whole again. The type survives in more places
// than one would think:
//
//	the object's own type link, by value       obj.out.to.type = __type.<id>
//	the index of that same link, by key        obj.ltype.__type.<id>
//	the in-key the type wrote on the object    obj.in.<type>.<id>
//	what this process already resolved         the object-type cache
//	what the trash can parked it under         the trash-can edge body
//	the halves that live on the type itself    a walk over the types
//
// If NO source has it, the vertex is not repairable and not admissible: an
// object whose type nobody knows cannot be read, cannot be enumerated, and
// cannot be restored later — parking it in the trash can would only record a
// type we do not have. It is erased, along with every half-edge left pointing
// at it.
//
// Cost. A healthy object is checked once per process and then remembered, so
// the steady state pays one twelve-key probe per object and nothing after
// that. The probe runs where an operation is already touching the object.

import (
	"fmt"
	"strings"
	"sync"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

// objectIntegrityVerified holds the objects this process has already found
// whole or made whole. It is a pure cache — dropping it costs one more probe
// per object, never correctness — and PurgeSchemaCaches drops it with the
// rest when the graph is replaced underneath (import, restore).
var objectIntegrityVerified sync.Map

type integrityOutcome int

const (
	integrityIntact   integrityOutcome = iota // nothing to do
	integrityRepaired                         // the skeleton was rewritten
	integrityErased                           // no type anywhere: the vertex is gone
	integrityAbsent                           // there is no vertex to speak of
)

// skeletonEdge is one of the three edges an object rests on.
type skeletonEdge struct {
	from, to, name, linkType string
}

func objectSkeletonEdges(ctx *sfPlugins.StatefunContextProcessor, objID, typeID string) []skeletonEdge {
	objectsID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)
	name := ctx.Domain.GetObjectIDWithoutDomain(objID)
	return []skeletonEdge{
		{from: objectsID, to: objID, name: name, linkType: OBJECT_TYPELINK},
		{from: objID, to: typeID, name: "type", linkType: TO_TYPELINK},
		{from: typeID, to: objID, name: name, linkType: OBJECT_TYPELINK},
	}
}

// broken reports whether any of this edge's four keys is missing or holds
// something other than what the edge says it should.
func (e skeletonEdge) broken(ctx *sfPlugins.StatefunContextProcessor) bool {
	c := ctx.Domain.Cache()
	target, err := c.GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, e.from, e.name))
	if err != nil || string(target) != e.linkType+"."+e.to {
		return true
	}
	if !c.ExistsJson(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, e.from, e.name)) {
		return true
	}
	if idx, err := c.GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, e.from, e.linkType, e.to)); err != nil || string(idx) != e.name {
		return true
	}
	if lt, err := c.GetValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, e.to, e.from, e.name)); err != nil || string(lt) != e.linkType {
		return true
	}
	return false
}

// resolveObjectTypeForRepair asks every source that can still name the
// object's type, cheapest first. The second return value is for the log: when
// an object is repaired it matters which fragment saved it.
func resolveObjectTypeForRepair(ctx *sfPlugins.StatefunContextProcessor, objID string) (typeID, source string, ok bool) {
	c := ctx.Domain.Cache()
	objectsID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)
	name := ctx.Domain.GetObjectIDWithoutDomain(objID)

	// The object's own type link, whose VALUE is the type.
	if v, err := c.GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, objID, "type")); err == nil {
		if parts := strings.SplitN(string(v), ".", 2); len(parts) == 2 && parts[0] == TO_TYPELINK && parts[1] != "" {
			return parts[1], "the object's type link", true
		}
	}

	// The index of that same link, whose KEY is the type — so it answers even
	// when the value above is gone. The index entry's value is the link's
	// NAME, and only the link named "type" is an object's type link: a link
	// between two types is an __type link as well, named after the type it
	// leads to.
	for _, k := range c.GetKeysByPattern(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, objID, TO_TYPELINK, ">")) {
		linkName, err := c.GetValue(k)
		if err != nil || string(linkName) != "type" {
			continue
		}
		tokens := strings.Split(k, ".")
		if id := tokens[len(tokens)-1]; id != "" {
			return id, "the type-link index on the object", true
		}
	}

	// The in-key the type wrote on the object. Its key names the type, and the
	// link name is the object's own id — which is what tells it apart from the
	// in-key the objects vertex writes.
	for _, k := range c.GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff1Pattern, objID, ">")) {
		tokens := strings.Split(k, ".")
		if len(tokens) < 2 {
			continue
		}
		from, linkName := tokens[len(tokens)-2], tokens[len(tokens)-1]
		if from == objectsID || linkName != name {
			continue
		}
		if v, err := c.GetValue(k); err == nil && string(v) == OBJECT_TYPELINK {
			return from, "the in-key from the type", true
		}
	}

	// What this process already knows. Kept after the graph, not before it: a
	// stale entry must never outvote what the graph still holds.
	if t, cached := cacheGetObjectType(objID); cached {
		return t, "the object-type cache", true
	}

	// The trash can records the type it parked the object under.
	if t, _ := trashCanEdgeInfo(ctx, objID); t != "" {
		return ctx.Domain.CreateObjectIDWithHubDomain(t, false), "the trash can", true
	}

	// Nothing on the object names the type any more. The other two halves of
	// its type edges live ON the type, so walk the types and ask them.
	if t, found := findTypeStillHoldingObject(ctx, objID); found {
		return t, "a type still holding half of the edge", true
	}

	return "", "", false
}

// mayBeAnObject excludes the vertices that are structure rather than data.
//
// A declared type must never be treated as an object, and it CAN look like one
// from the outside: a link between two types is an __type link as well, so a
// type that participates in a types-link appears to "have a type". Reading
// that as an object's type link and completing the skeleton around it would
// hang the type off the objects vertex and invent a membership that never
// existed — schema damage done in the name of repair.
//
// The graph's own roots are excluded by name for the same reason: they are the
// topology every object hangs from, not objects themselves. Everything else —
// including a bare vertex somebody wrote through the low-level API — is judged
// by what it actually holds.
func mayBeAnObject(ctx *sfPlugins.StatefunContextProcessor, objID string) bool {
	typesID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)
	if ctx.Domain.Cache().Exists(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, typesID, TO_TYPELINK, objID)) {
		return false // a declared type
	}
	switch ctx.Domain.GetObjectIDWithoutDomain(objID) {
	case BUILT_IN_ROOT, BUILT_IN_TYPES, BUILT_IN_OBJECTS:
		return false
	}
	return true
}

// objectEvidence answers the only question that matters before touching
// anything: does the graph itself assert that this vertex is an object?
//
// What makes a vertex an object is an __object link leading TO it — the
// objects vertex writes one to say "this is in the model", and its type writes
// another to say "this is mine". Either half of either link is the assertion,
// and nothing else in the graph carries __object links, so nothing else can be
// mistaken for an object.
//
// One more thing counts: the object's own link to its type, which is named
// exactly "type". That is the only trace left when both membership halves and
// both halves on the type side are gone, and it is unambiguous — a link
// between two types is named after the type it leads to, never "type", and a
// declared type is excluded before this is ever asked.
//
// The walk over the types comes last and only runs when nothing local answers,
// which is also when a repair is about to erase something — the one moment
// worth paying for certainty.
func objectEvidence(ctx *sfPlugins.StatefunContextProcessor, objID string) (string, bool) {
	c := ctx.Domain.Cache()
	objectsID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)
	name := ctx.Domain.GetObjectIDWithoutDomain(objID)

	// The objects vertex says so, from its side.
	if v, err := c.GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, objectsID, name)); err == nil &&
		string(v) == OBJECT_TYPELINK+"."+objID {
		return "the objects vertex links to it", true
	}
	if c.Exists(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, objectsID, OBJECT_TYPELINK, objID)) {
		return "the objects vertex indexes it", true
	}

	// Or an __object link leading to it, mirrored on the vertex itself: from
	// the objects vertex, or from the type that owns it.
	for _, k := range c.GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff1Pattern, objID, ">")) {
		tokens := strings.Split(k, ".")
		if len(tokens) < 2 || tokens[len(tokens)-1] != name {
			continue
		}
		if v, err := c.GetValue(k); err == nil && string(v) == OBJECT_TYPELINK {
			if tokens[len(tokens)-2] == objectsID {
				return "it holds the in-key from the objects vertex", true
			}
			return "it holds the in-key from its type", true
		}
	}

	// Or its own link to a type, which only an object has.
	if v, err := c.GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, objID, "type")); err == nil {
		if parts := strings.SplitN(string(v), ".", 2); len(parts) == 2 && parts[0] == TO_TYPELINK && parts[1] != "" {
			return "it holds a link named \"type\" to a type", true
		}
	}
	// Or the trash can parked it, which it only ever does to objects.
	if t, _ := trashCanEdgeInfo(ctx, objID); t != "" {
		return "the trash can holds it", true
	}

	// Or this runtime resolved it as an object itself and still holds the
	// answer. That is an assertion about THIS id made from the graph as it was
	// moments ago, and it is dropped the moment the object leaves the model —
	// so it speaks for an object whose every trace was just lost, and for
	// nothing else. A type never reaches this cache: resolving a type would
	// need a link named "type" leading out of it, and a declared type is
	// excluded before any of this is asked.
	if t, cached := cacheGetObjectType(objID); cached && t != "" {
		return "this runtime resolved it as an object of type " + t, true
	}

	// Or a type still holds its half of the edge to it.
	if typeID, found := findTypeStillHoldingObject(ctx, objID); found {
		return "type " + typeID + " links to it", true
	}
	return "", false
}

// findTypeStillHoldingObject walks the declared types looking for one that
// still holds either half of an edge to this object. Only reached when
// everything on the object itself is gone, and bounded by the number of types
// — a repair path, not a hot one.
func findTypeStillHoldingObject(ctx *sfPlugins.StatefunContextProcessor, objID string) (string, bool) {
	c := ctx.Domain.Cache()
	typesID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)
	name := ctx.Domain.GetObjectIDWithoutDomain(objID)

	// The types vertex holds every declared type under TO_TYPELINK — the same
	// link type an object uses to name its own type (see CreateType).
	for _, k := range c.GetKeysByPattern(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, typesID, TO_TYPELINK, ">")) {
		tokens := strings.Split(k, ".")
		typeID := tokens[len(tokens)-1]
		if typeID == "" {
			continue
		}
		// The type's link to the object: by index, or by name with the target
		// checked — a type may well own another link under the same name.
		if c.Exists(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, typeID, OBJECT_TYPELINK, objID)) {
			return typeID, true
		}
		if v, err := c.GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, typeID, name)); err == nil &&
			string(v) == OBJECT_TYPELINK+"."+objID {
			return typeID, true
		}
		// The object's type link, mirrored on the type.
		if c.Exists(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, typeID, objID, "type")) {
			return typeID, true
		}
	}
	return "", false
}

// ensureObjectIntegrity makes the object whole, or removes it when no source
// can name its type. It is safe to call on anything: a vertex that is not an
// object, one that does not exist, and one that is already whole all return
// without writing.
//
// parentHoldsLocks says the caller already holds the operation's locks, so the
// repair must not try to take them again.
func ensureObjectIntegrity(ctx *sfPlugins.StatefunContextProcessor, objID string, parentHoldsLocks bool, opTime int64) integrityOutcome {
	if _, seen := objectIntegrityVerified.Load(objID); seen {
		return integrityIntact
	}
	if !ctx.Domain.Cache().ExistsJson(objID) {
		return integrityAbsent
	}
	if !mayBeAnObject(ctx, objID) {
		return integrityAbsent
	}
	// Nothing is repaired, and nothing is erased, on a vertex the graph does
	// not call an object. A type, a root, a vertex somebody wrote through the
	// low-level API — none of them may be given a skeleton, and none of them
	// may lose anything for not having one.
	if _, isObject := objectEvidence(ctx, objID); !isObject {
		return integrityAbsent
	}

	typeID, source, found := resolveObjectTypeForRepair(ctx, objID)
	if !found {
		lg.Logf(lg.WarnLevel,
			"object %s has lost every trace of its type and cannot be restored; erasing what is left of it", objID)
		eraseUnrecoverableObject(ctx, objID, opTime)
		return integrityErased
	}
	// A parked object rests on the trash can's links, not on these — it is
	// outside the model on purpose.
	if isTrashCanType(ctx, typeID) {
		objectIntegrityVerified.Store(objID, struct{}{})
		return integrityIntact
	}

	var broken []skeletonEdge
	for _, e := range objectSkeletonEdges(ctx, objID, typeID) {
		if e.broken(ctx) {
			broken = append(broken, e)
		}
	}
	if len(broken) == 0 {
		objectIntegrityVerified.Store(objID, struct{}{})
		return integrityIntact
	}

	for _, e := range broken {
		rewriteSkeletonEdge(ctx, e, parentHoldsLocks, opTime)
	}
	cacheSetObjectType(objID, typeID)
	objectIntegrityVerified.Store(objID, struct{}{})
	lg.Logf(lg.WarnLevel, "object %s was missing %d of its three structural edges; rebuilt from %s (type=%s)",
		objID, len(broken), source, typeID)
	return integrityRepaired
}

// rewriteSkeletonEdge writes all four keys of one edge. force makes the write
// idempotent: a missing link is created, an existing one is overwritten in
// place, and the mirror in-key is written on the target whichever domain it
// lives in.
func rewriteSkeletonEdge(ctx *sfPlugins.StatefunContextProcessor, e skeletonEdge, parentHoldsLocks bool, opTime int64) {
	link := easyjson.NewJSONObject()
	link.SetByPath("to", easyjson.NewJSON(e.to))
	link.SetByPath("name", easyjson.NewJSON(e.name))
	link.SetByPath("type", easyjson.NewJSON(e.linkType))
	link.SetByPath("body", easyjson.NewJSONObject())
	link.SetByPath("force", easyjson.NewJSON(true))
	link.SetByPath("op_time", easyjson.NewJSON(opTime))

	payload := &link
	if parentHoldsLocks {
		payload = injectParentHoldsLocks(ctx, &link)
	}
	_, _ = ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create",
		makeSequenceFreeParentBasedID(ctx, e.from), payload, ctx.Options)
}

// eraseUnrecoverableObject removes the vertex and every half-edge left
// pointing at it.
//
// The vertex delete takes care of everything reachable from the object: its
// own links, and the owner-side keys of every in-key it still carries. What it
// cannot see is an owner side whose mirror in-key is already gone — nothing
// leads from the object back to it. One such edge is always addressable
// anyway: the objects vertex names its links after the object itself.
func eraseUnrecoverableObject(ctx *sfPlugins.StatefunContextProcessor, objID string, opTime int64) {
	objectsID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)
	name := ctx.Domain.GetObjectIDWithoutDomain(objID)

	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(opTime))
	_, _ = ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete",
		makeSequenceFreeParentBasedID(ctx, objID), &payload, ctx.Options)

	if ctx.Domain.GetDomainFromObjectID(objectsID) == ctx.Domain.Name() {
		deleteOutLinkKeysByName(ctx, objectsID, name, opTime)
		ctx.Domain.Cache().DeleteValue(
			fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, objectsID, OBJECT_TYPELINK, objID), true, opTime)
	} else {
		unlink := easyjson.NewJSONObject()
		unlink.SetByPath("name", easyjson.NewJSON(name))
		unlink.SetByPath("op_time", easyjson.NewJSON(opTime))
		_, _ = ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete",
			makeSequenceFreeParentBasedID(ctx, objectsID), &unlink, ctx.Options)
	}

	cacheDeleteObjectType(objID)
	objectIntegrityVerified.Delete(objID)
}

// forgetObjectIntegrity drops what this process remembers about an object's
// skeleton. Called wherever the object leaves the model, so the next one to
// carry that id is checked afresh.
func forgetObjectIntegrity(objID string) { objectIntegrityVerified.Delete(objID) }
