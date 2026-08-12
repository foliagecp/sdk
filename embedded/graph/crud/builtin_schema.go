package crud

// The built-in CMDB skeleton — the vertices, types, type-links and objects
// every graph must have for CRUD to work at all.
//
// It is DECLARED ONCE, in builtInSchema below, and everything else is derived
// from that declaration:
//
//   - EnsureBuiltInSchema builds (and repairs) exactly what is declared;
//   - BuiltInSchemaIDs lists what a bulk graph load must never delete.
//
// This matters because a graph IMPORT rewrites the graph from a file: an export
// walks from `root`, so every dump carries the skeleton, and deleting a skeleton
// vertex cascades away registrations that only the dump's own edges restore — a
// dump taken before a built-in type existed leaves that type unregistered.
// Keeping the "do not delete" list separate from the declaration would mean a
// new built-in silently missing from it and the same breakage returning: add a
// built-in in ONE place below and both behaviours follow.
//
// EnsureBuiltInSchema is written as a REPAIR rather than a create, so a graph
// an earlier import already mangled heals on the next runtime start. Note that
// `functions.cmdb.api.type.create` cannot do that by itself — it bails as soon
// as the type vertex exists, without restoring the missing `types -> <type>`
// link, which is exactly the leftover state an import produces.

import (
	"sync"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type (
	// builtInLink is a structural link of the skeleton.
	builtInLink struct {
		from, to, name, linkType string
	}
	// builtInTypesLink declares the object-link type allowed between two types.
	builtInTypesLink struct {
		from, to, objectLinkType string
	}
	// builtInObject is a CMDB object of the skeleton; its three invariant links
	// (objects->obj, obj->type, type->obj) are derived, not spelled out.
	builtInObject struct {
		id, objectType string
	}
)

// builtInSchema is the single source of truth for the skeleton: what exists,
// what it is linked to, and — by derivation — what an import must not delete.
var builtInSchema = struct {
	vertices   []string
	links      []builtInLink
	types      []string
	typesLinks []builtInTypesLink
	objects    []builtInObject
}{
	// Plain vertices and the links hanging them off `root`.
	vertices: []string{BUILT_IN_ROOT, BUILT_IN_TYPES, BUILT_IN_OBJECTS},
	links: []builtInLink{
		{from: BUILT_IN_ROOT, to: BUILT_IN_TYPES, name: BUILT_IN_TYPES, linkType: TYPES_TYPELINK},
		{from: BUILT_IN_ROOT, to: BUILT_IN_OBJECTS, name: BUILT_IN_OBJECTS, linkType: OBJECTS_TYPELINK},
	},
	// CMDB types; each gets its vertex plus its registration under `types`.
	types: []string{BUILT_IN_TYPE_GROUP, BUILT_IN_TRASH_CAN},
	// Object-link types permitted between types.
	typesLinks: []builtInTypesLink{
		{from: BUILT_IN_TYPE_GROUP, to: BUILT_IN_TYPE_GROUP, objectLinkType: GROUP_TYPELINK},
	},
	// CMDB objects.
	objects: []builtInObject{
		{id: BUILT_IN_OBJECT_NAV, objectType: BUILT_IN_TYPE_GROUP},
	},
}

// BuiltInSchemaIDs lists every vertex of the skeleton — derived from the
// declaration, so it cannot fall behind it. A bulk graph load (import, restore)
// must not delete these: they carry registrations a dump cannot restore.
func BuiltInSchemaIDs() []string {
	ids := make([]string, 0, len(builtInSchema.vertices)+len(builtInSchema.types)+len(builtInSchema.objects))
	ids = append(ids, builtInSchema.vertices...)
	ids = append(ids, builtInSchema.types...)
	for _, o := range builtInSchema.objects {
		ids = append(ids, o.id)
	}
	return ids
}

// IsBuiltInSchemaID reports whether id (domain-qualified or not) belongs to the
// built-in skeleton.
func IsBuiltInSchemaID(domain sfPlugins.Domain, id string) bool {
	short := domain.GetObjectIDWithoutDomain(id)
	for _, builtIn := range BuiltInSchemaIDs() {
		if short == builtIn {
			return true
		}
	}
	return false
}

// EnsureBuiltInSchema creates or repairs the declared skeleton. Safe to call at
// any time and as often as needed: what is already in place stays untouched,
// what is missing is restored. Runtime startup uses it, and so should anything
// that loads a graph in bulk.
//
// All ids are hub-qualified — the schema lives in the hub domain, so the call
// works from any domain, and from a plain after-start hook as much as from a
// statefun handler (both provide a request function and a domain).
func EnsureBuiltInSchema(request sfPlugins.SFRequestFunc, domain sfPlugins.Domain) {
	// Whatever produced the state we are repairing (an import, a restore, a
	// half-finished startup) may have bypassed CRUD, leaving the schema caches
	// describing the previous graph. Decide on facts, not on that memory.
	PurgeSchemaCaches()

	hub := func(id string) string { return domain.CreateObjectIDWithHubDomain(id, false) }

	// An existing vertex answers "already exists" — expected, not an error: all
	// that matters is that it is there afterwards.
	ensureVertex := func(id string) {
		_, _ = request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", hub(id), easyjson.NewJSONObject().GetPtr(), nil)
	}
	// force=true makes the write a genuine repair: a missing link is created, an
	// existing one is rewritten instead of failing the uniqueness checks.
	ensureLink := func(l builtInLink) {
		link := easyjson.NewJSONObject()
		link.SetByPath("to", easyjson.NewJSON(hub(l.to)))
		link.SetByPath("name", easyjson.NewJSON(l.name))
		link.SetByPath("type", easyjson.NewJSON(l.linkType))
		link.SetByPath("force", easyjson.NewJSON(true))
		_, _ = request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", hub(l.from), &link, nil)
	}

	for _, v := range builtInSchema.vertices {
		ensureVertex(v)
	}
	for _, l := range builtInSchema.links {
		ensureLink(l)
	}

	// Types: type.create builds vertex + registration when the type is fully
	// absent; when only the registration is missing it stops at the existing
	// vertex, so the link is restored explicitly.
	for _, t := range builtInSchema.types {
		_, _ = request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", hub(t), nil, nil)
		ensureVertex(t)
		ensureLink(builtInLink{from: BUILT_IN_TYPES, to: t, name: t, linkType: TO_TYPELINK})
	}

	// TypesLinks. CreateTypesLink creates the link when missing and fails
	// harmlessly when it is already there.
	for _, tl := range builtInSchema.typesLinks {
		payload := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON(tl.to))
		payload.SetByPath("object_type", easyjson.NewJSON(tl.objectLinkType))
		_, _ = request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.create", hub(tl.from), &payload, nil)
	}

	// Objects, rebuilt from their three CMDB invariants. Deliberately NOT via
	// object.update+upsert: that path asks findObjectType first, which answers
	// from the in-process type cache — and after a bulk load the cache still
	// describes the PREVIOUS graph, so a missing object would look healthy and
	// never be restored.
	for _, o := range builtInSchema.objects {
		ensureVertex(o.id)
		ensureLink(builtInLink{from: BUILT_IN_OBJECTS, to: o.id, name: o.id, linkType: OBJECT_TYPELINK})
		ensureLink(builtInLink{from: o.id, to: o.objectType, name: "type", linkType: TO_TYPELINK})
		ensureLink(builtInLink{from: o.objectType, to: o.id, name: o.id, linkType: OBJECT_TYPELINK})
		cacheSetObjectType(hub(o.id), hub(o.objectType))
	}
}

// PurgeSchemaCaches drops every in-process cache derived from the graph's
// schema — object types, type-to-type link types, trigger sections, name
// fields. They are pure caches, refilled on demand, but they describe the graph
// as it WAS: after a bulk load that wrote vertices behind CRUD's back (import,
// snapshot restore) a stale entry makes CRUD reason about a graph that no
// longer exists — e.g. resolving an object's type to one the import replaced.
func PurgeSchemaCaches() {
	for _, m := range []*sync.Map{
		&objectTypeCache,
		&type2TypeObjectLinkTypeCache,
		&typeObjectTriggersCache,
		&typesLinkTriggersCache,
		&typeHRNFieldCache,
	} {
		m.Range(func(k, _ any) bool {
			m.Delete(k)
			return true
		})
	}
}

/*
EnsureBuiltInSchemaFunction exposes EnsureBuiltInSchema as a statefun, so any
importer — including ones living outside this process (snapshot restore in
pregel-backend, operator tooling) — can repair the skeleton after a bulk load
without linking this package.

Request: no payload.

Reply:

	payload: json
		status: string
		details: string
*/
func EnsureBuiltInSchemaFunction(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	EnsureBuiltInSchema(ctx.Request, ctx.Domain)
	om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
}
