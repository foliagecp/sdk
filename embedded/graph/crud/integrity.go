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
// back. The damage outlived every operation, because each operation asked
// whether the object was readable, not whether it was whole.
//
// FIRST QUESTION: is this an object at all? Only one thing in the graph says
// so — an __object link leading TO the vertex. The objects vertex writes one
// to put an object in the model, its type writes another to own it, and the
// trash can (itself a type) writes one over what it parks. Nothing else
// creates __object links, and nothing points at a type or a root with one, so
// nothing else can be mistaken for an object. Either half of either link is
// the assertion, since both halves say the same thing.
//
// Everything weaker was tried and dropped: a link that happens to be named
// "type" is a naming convention anyone can write; a blacklist of "not a type,
// not a root" goes stale the moment the schema grows a kind nobody listed. A
// vertex with none of those halves left is indistinguishable from a bare
// vertex written through the low-level API, and is therefore neither rebuilt
// nor erased — only its half-edges are closed, which says nothing about what
// it is.
//
// SECOND QUESTION: what type is it? The graph is asked first, in this order:
//
//	the object's own type link, by value       obj.out.to.type = __type.<id>
//	the index of that same link, by key        obj.ltype.__type.<id>
//	the in-key the type wrote on the object    obj.in.<type>.<id>
//	what this process already resolved         the object-type cache
//	the trash can holding it — it is parked    the trash-can edge
//	the halves that live on the type itself    a walk over the types
//
// The operation itself may name a type too, and it is asked LAST. What the
// graph still says outranks it: an object created under one type and then
// updated, by mistake, under another must come out of that update as what it
// was. Only where the graph has gone silent is the caller believed — and then
// restoring the object under the type they name keeps every other link it
// holds, which erasing it would not.
//
// If nothing at all names the type, the vertex cannot be read, enumerated or
// restored later, and parking it in the trash can would record a type nobody
// has. It is erased, along with the half-edge left on the objects vertex.
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
	// Exists, not ExistsJson: what matters here is that the body key is there,
	// and asking the JSON-typed question about a LINK body makes the cache log
	// a nudge on every call — three per object, on a path that runs for every
	// object this process meets.
	if !c.Exists(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, e.from, e.name)) {
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

	// The trash can holding it means it is PARKED, and a parked object's type
	// is the trash can — not the type it was parked from. Answering with the
	// original type here would rebuild a deleted object as a live one.
	if t, _ := trashCanEdgeInfo(ctx, objID); t != "" {
		return trashCanTypeID(ctx), "the trash can holding it", true
	}

	// Nothing on the object names the type any more. The other two halves of
	// its type edges live ON the type, so walk the types and ask them.
	if t, _, found := findTypeStillHoldingObject(ctx, objID); found {
		return t, "a type still holding half of the edge", true
	}

	return "", "", false
}

// isDeclaredType reports whether the types vertex holds this vertex as a type.
// Both halves of that link are asked: a type whose registration lost one side
// is still a type, and must not become an object because of it.
func isDeclaredType(ctx *sfPlugins.StatefunContextProcessor, id string) bool {
	c := ctx.Domain.Cache()
	typesID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)
	if c.Exists(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, typesID, TO_TYPELINK, id)) {
		return true
	}
	if v, err := c.GetValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern,
		id, typesID, ctx.Domain.GetObjectIDWithoutDomain(id))); err == nil && string(v) == TO_TYPELINK {
		return true
	}
	return false
}

// objectIsAsserted answers the only question that may be answered from the
// graph: does anything in it say this vertex is an object?
//
// One thing does, and only one: an __object link leading TO the vertex. The
// objects vertex writes one to put it in the model, and its type writes
// another to own it. Nothing else in the graph creates __object links, so
// nothing else can be mistaken for an object — and everything weaker (a link
// that happens to be named "type", a trash-can record, what this process
// resolved a moment ago) rests on convention or on memory, not on the graph,
// and is not used here.
//
// Either half of either link is the assertion, since both halves say the same
// thing. The incoming halves come first: they sit on the vertex itself, one
// scan answers for both possible sources at once, and a vertex that has them
// needs nothing further looked up.
func objectIsAsserted(ctx *sfPlugins.StatefunContextProcessor, objID string) (string, bool) {
	c := ctx.Domain.Cache()
	objectsID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)
	name := ctx.Domain.GetObjectIDWithoutDomain(objID)

	// The objects edge, both of its halves, addressed directly. This is the
	// answer for every healthy object, and it costs two lookups — no scan, no
	// allocation. Everything below it is only reached by an object that has
	// already lost something.
	if lt, err := c.GetValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, objID, objectsID, name)); err == nil &&
		string(lt) == OBJECT_TYPELINK {
		return "the objects vertex links to it", true
	}
	if v, err := c.GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, objectsID, name)); err == nil &&
		string(v) == OBJECT_TYPELINK+"."+objID {
		return "the objects vertex links to it", true
	}
	if c.Exists(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, objectsID, OBJECT_TYPELINK, objID)) {
		return "the objects vertex indexes it", true
	}

	// The type's half, mirrored on the vertex. The type is not known here, so
	// this one is a scan — of the vertex's own in-keys, which are few.
	for _, k := range c.GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff1Pattern, objID, ">")) {
		tokens := strings.Split(k, ".")
		if len(tokens) < 2 || tokens[len(tokens)-1] != name {
			continue
		}
		if v, err := c.GetValue(k); err != nil || string(v) != OBJECT_TYPELINK {
			continue
		}
		if from := tokens[len(tokens)-2]; isDeclaredType(ctx, from) {
			return "type " + from + " links to it", true
		}
	}

	// The type's half on the type itself. Last, because it is the only one
	// that costs a walk — and it is only reached for a vertex nothing nearer
	// accounts for.
	if typeID, viaObjectLink, found := findTypeStillHoldingObject(ctx, objID); found && viaObjectLink {
		return "type " + typeID + " links to it", true
	}
	return "", false
}

// findTypeStillHoldingObject walks the declared types looking for one that
// still holds either half of an edge to this object. Only reached when
// everything on the object itself is gone, and bounded by the number of types
// — a repair path, not a hot one.
// The second return value says the type was found through an __object link it
// owns — the one finding that also proves the vertex is an object.
func findTypeStillHoldingObject(ctx *sfPlugins.StatefunContextProcessor, objID string) (string, bool, bool) {
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
			return typeID, true, true
		}
		if v, err := c.GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, typeID, name)); err == nil &&
			string(v) == OBJECT_TYPELINK+"."+objID {
			return typeID, true, true
		}
		// The object's type link, mirrored on the type. It names the type, but
		// it says nothing about the vertex being an object — an __type link
		// leads out of types as well as out of objects.
		if c.Exists(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, typeID, objID, "type")) {
			return typeID, false, true
		}
	}
	return "", false, false
}

// closeHalfEdges makes a vertex's edges whole again without deciding what the
// vertex is. Two shapes, and the direction decides which way each is closed:
//
//	an out-link with no mirror   the owner side holds the whole edge — target,
//	                             type, name, body — so the mirror is written
//	                             and nothing is lost;
//	an in-key with no out-link   the owner side is gone and cannot be
//	                             reconstructed from a key that holds only the
//	                             link type, so the orphan goes.
//
// Only the vertex the operation is about is walked, plus the one type that may
// still hold an in-key from it — the half that lives out of this vertex's
// reach.
func closeHalfEdges(ctx *sfPlugins.StatefunContextProcessor, vertexID string, opTime int64) {
	c := ctx.Domain.Cache()

	for _, k := range c.GetKeysByPattern(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, vertexID, ">")) {
		tokens := strings.Split(k, ".")
		linkName := tokens[len(tokens)-1]
		raw, err := c.GetValue(k)
		if err != nil {
			continue
		}
		parts := strings.SplitN(string(raw), ".", 2)
		if len(parts) != 2 || parts[1] == "" {
			continue
		}
		linkType, toID := parts[0], parts[1]
		inKey := fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, toID, vertexID, linkName)
		if c.Exists(inKey) {
			continue
		}
		if ctx.Domain.GetDomainFromObjectID(toID) == ctx.Domain.Name() {
			c.SetValue(inKey, []byte(linkType), true, opTime)
			continue
		}
		inlink := easyjson.NewJSONObject()
		inlink.SetByPath("in_name", easyjson.NewJSON(linkName))
		inlink.SetByPath("in_type", easyjson.NewJSON(linkType))
		inlink.SetByPath("op_time", easyjson.NewJSON(opTime))
		_, _ = ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create",
			makeSequenceFreeParentBasedID(ctx, toID, "inlink"), &inlink, ctx.Options)
	}

	for _, k := range c.GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff1Pattern, vertexID, ">")) {
		tokens := strings.Split(k, ".")
		if len(tokens) < 2 {
			continue
		}
		from, linkName := tokens[len(tokens)-2], tokens[len(tokens)-1]
		if !c.Exists(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, from, linkName)) {
			c.DeleteValue(k, true, opTime)
		}
	}

	// The type's own in-key from this vertex, when the vertex no longer owns
	// the link that wrote it. It is out of reach from here — the walk is what
	// finds it.
	if c.Exists(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, vertexID, "type")) {
		return
	}
	if typeID, _, found := findTypeStillHoldingObject(ctx, vertexID); found {
		orphan := fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, typeID, vertexID, "type")
		if c.Exists(orphan) {
			c.DeleteValue(orphan, true, opTime)
		}
	}
}

// ensureObjectIntegrity makes the object whole, or removes it when nothing can
// name its type. It is safe to call on anything: a vertex that is not an
// object, one that does not exist, and one that is already whole all return
// without writing.
//
// callerType is the type the operation itself was given — object.create and an
// upsert carry one, a read does not. It is the LAST source consulted and it
// can never overrule the graph. An object created under one type and then
// updated, by mistake, under another must not have its type rewritten by that
// mistake: what the graph still says about an object outranks what a caller
// says about it, and the caller is believed only where the graph has gone
// silent. Even then it is a judgement rather than a fact — but the caller of
// an upsert is usually the system that owns the object, and restoring it under
// the type they name keeps everything else the object holds, which erasing it
// would not.
//
// parentHoldsLocks says the caller already holds the operation's locks, so the
// repair must not try to take them again.
func ensureObjectIntegrity(ctx *sfPlugins.StatefunContextProcessor, objID, callerType string, parentHoldsLocks bool, opTime int64) integrityOutcome {
	if _, seen := objectIntegrityVerified.Load(objID); seen {
		return integrityIntact
	}
	if !ctx.Domain.Cache().ExistsJson(objID) {
		return integrityAbsent
	}
	// Nothing is repaired, and nothing is erased, on a vertex the graph does
	// not call an object. A type, a root, a vertex somebody wrote through the
	// low-level API — none of them may be given a skeleton, and none of them
	// may lose anything for not having one. What is still put right is a half
	// of an edge left hanging: that is broken whoever the vertex turns out to
	// be, and saying so asserts nothing about it.
	if _, asserted := objectIsAsserted(ctx, objID); !asserted {
		closeHalfEdges(ctx, objID, opTime)
		return integrityAbsent
	}

	typeID, source, found := resolveObjectTypeForRepair(ctx, objID)
	switch {
	case found && callerType != "" && callerType != typeID:
		// The two disagree, and the graph wins. Rebuilding the skeleton under
		// the type the caller named would change what the object IS, and no
		// repair may do that on its own.
		lg.Logf(lg.WarnLevel,
			"object %s is of type %s (from %s) but this operation names type %s; repairing as %s and leaving the type alone",
			objID, typeID, source, callerType, typeID)
	case !found && callerType != "":
		// The graph knows this is an object and no longer knows of what. The
		// caller does, so the object is restored instead of erased — and
		// everything else it holds is kept.
		typeID, source = callerType, "the operation that named it"
		lg.Logf(lg.WarnLevel,
			"object %s has lost every trace of its type; restoring it as %s, the type this operation names", objID, typeID)
	case !found:
		lg.Logf(lg.WarnLevel,
			"object %s has lost every trace of its type and nothing names it; erasing what is left of it", objID)
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
