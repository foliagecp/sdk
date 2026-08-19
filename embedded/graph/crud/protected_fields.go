package crud

// Protected body fields — top-level keys of ANY vertex body that a writer which
// does not carry them cannot destroy.
//
// The graph is written by many independent writers. Inventory builders
// (HLMB rebuild, osm reconcile, connectors) rewrite whole bodies with
// replace=true on every cycle and know nothing about data other parties keep
// on the same object (e.g. the user's tags and attributes under "usr"), so
// without this guarantee such data would be wiped every cycle. Rather than
// teaching every writer a rule it would eventually break, the invariant lives
// in the core, at the single vertex-body write path (LLAPIVertexUpdate).
//
// Contract (replace=true — the destructive mode):
//
//	field NOT in the request → grafted from the current body untouched;
//	field IS in the request  → the writer owns the write and the value is stored
//	                           as sent, exactly like any other body field.
//
// The second half is what keeps the field WRITABLE — including removals: the
// owner of the data (the platform API) reads the whole body, edits the
// protected field (adds an attribute, drops a tag) and writes it back with
// replace=true. Protection is against writers that know nothing about the
// field, not against the ones that manage it.
//
// replace=false (merge) needs no special handling: the whole body is
// deep-merged into the current one, so an absent field is preserved anyway.
//
// WHERE THE LIST COMES FROM. Not from this process: it is published in the
// graph itself, in the built-in `root` vertex — the policy is enforced on the
// single vertex-body write path and so holds for every vertex, which is why it
// is declared at the root of the graph and not in one of its branches. Every
// runtime pulls it from there when it starts, holding it in its Domain for
// stateful functions to read as ctx.Domain.ProtectedBodyFields().
//
// That is not a hub-versus-satellite matter. Several applications routinely
// work on ONE graph, most of them inside one domain: one provides the CRUD
// layer, creates the schema and declares what is protected; the others simply
// attach to that graph and write to it. An application's own environment says
// nothing about how the data it writes is protected, and an application that
// does not even register this package must not be the one that quietly wipes
// it — so the pull belongs to the runtime (statefun.Domain.PullProtectedBodyFields)
// rather than to CRUD.
//
// Publishing, on the other hand, is explicit and belongs to whoever sets up the
// built-in schema: the list is passed to RegisterAllFunctionTypes /
// EnsureBuiltInSchema (that caller is free to read its own env to obtain it).
// Callers that pass nothing publish nothing and simply follow what the graph
// says.
//
// The list is shared with the trash can, whose restore grafts exactly these
// fields back when a deleted object returns (see trash_can.go).

import (
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

// ProtectedBodyFieldsBodyPath is where the effective protected-field list is
// published inside the built-in `root` vertex body.
//
// A vertex every consumer can already reach — the root of the graph — turns
// "which fields must I not clobber?" from a guess into a lookup.
//
// Owned by the runtime (statefun), not by this package: the runtime is what
// reads the key, so an application that never registers the CRUD layer still
// learns the policy. Re-exported for callers that speak CMDB.
const ProtectedBodyFieldsBodyPath = statefun.ProtectedBodyFieldsBodyPath

// protectedBodyFieldsPuller is a real runtime Domain, which can go and read the
// list. The statefun-facing Domain interface exposes only the getter — a
// stateful function must not be able to redefine what is protected — so the
// pull is reached through a local assertion.
type protectedBodyFieldsPuller interface {
	PullProtectedBodyFields(request sfPlugins.SFRequestFunc, timeout ...time.Duration) ([]string, bool)
}

// publishProtectedBodyFields writes the effective list into the `root` vertex.
// Read-modify-write with replace=true on purpose: a merge would union the
// arrays (easyjson DeepMerge semantics), so a field REMOVED from the
// configuration would linger in the published list forever.
func publishProtectedBodyFields(request sfPlugins.SFRequestFunc, domain sfPlugins.Domain, fields []string) {
	rootID := domain.CreateObjectIDWithHubDomain(BUILT_IN_ROOT, false)

	body := easyjson.NewJSONObject()
	if m := sfMediators.OpMsgFromSfReply(request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", rootID, easyjson.NewJSONObject().GetPtr(), nil)); m.Status == sfMediators.SYNC_OP_STATUS_OK {
		if current := m.Data.GetByPath("body"); current.IsObject() {
			body = current.Clone()
		}
	}
	body.SetByPath(ProtectedBodyFieldsBodyPath, easyjson.NewJSON(fields))

	payload := easyjson.NewJSONObjectWithKeyValue("replace", easyjson.NewJSON(true))
	payload.SetByPath("body", body)
	_, _ = request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", rootID, &payload, nil)
}

// LoadProtectedBodyFieldsFromGraph makes the domain hold what the graph
// declares protected, and returns it. Every runtime already does this for
// itself at startup (statefun.Domain.PullProtectedBodyFields, which is what
// this delegates to); it is called here right after publishing, so the
// publisher's own view is current without waiting for anything.
//
// A graph that declares no list yields an empty one: nothing is protected until
// somebody publishes, and no process quietly decides that on its own.
func LoadProtectedBodyFieldsFromGraph(request sfPlugins.SFRequestFunc, domain sfPlugins.Domain) []string {
	if puller, ok := domain.(protectedBodyFieldsPuller); ok {
		fields, _ := puller.PullProtectedBodyFields(request)
		return fields
	}
	return domain.ProtectedBodyFields()
}

// jsonNonEmpty reports whether v holds anything worth preserving: a non-empty
// object, a non-empty array, or any other existing non-null value. Empty
// containers are skipped so an empty protected space never materializes in a
// body (snapshot-diff semantics: "absent = empty").
func jsonNonEmpty(v easyjson.JSON) bool {
	if v.IsObject() {
		return v.IsNonEmptyObject()
	}
	if v.IsArray() {
		return v.IsNonEmptyArray()
	}
	return !v.IsNull()
}

// applyProtectedFieldsOnReplace returns the body to store for a replace=true
// write: the incoming body with every protected field reconciled against the
// current one per the contract documented above. It allocates only when a
// protected field is actually present in either body, so the ordinary write
// path (no protected data anywhere) pays a couple of lookups.
//
// MUST be called BEFORE the no-op comparison of LLAPIVertexUpdate: an inventory
// rebuild that re-sends unchanged discovery fields has to stay a no-op even
// though its request lacks the protected fields — otherwise every cycle would
// produce a fake write, WAL traffic and a trigger fan-out.
func applyProtectedFieldsOnReplace(fields []string, oldBody *easyjson.JSON, incoming easyjson.JSON) easyjson.JSON {
	if oldBody == nil || len(fields) == 0 {
		return incoming
	}

	result := incoming
	cloned := false
	for _, field := range fields {
		oldValue := oldBody.GetByPath(field)
		if !jsonNonEmpty(oldValue) {
			continue // nothing protected to preserve for this field
		}
		if !cloned {
			result = incoming.Clone()
			cloned = true
		}
		if !result.PathExists(field) {
			result.SetByPath(field, oldValue) // writer does not carry it — keep as is
		}
		// The writer carries the field: it owns the write, exactly like any
		// other body field under replace=true — the incoming value is stored as
		// sent. That is what makes REMOVAL expressible: a caller reads the whole
		// body, drops a key or a tag, and writes it back.
	}
	return result
}
