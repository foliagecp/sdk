package crud

// Protected body fields — top-level keys of a vertex body that a writer which
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
// The list is configured via PROTECTED_BODY_FIELDS (comma-separated,
// default "usr") and is shared with the trash can, whose restore grafts exactly
// these fields back when a deleted object returns (see trash_can.go).

import (
	"strings"
	"sync/atomic"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/system"
)

// protectedBodyFieldsV holds the ordered list of protected top-level body keys.
// Stored atomically: CRUD handler goroutines read it on every replace-write
// concurrently with the test setter below, so a plain package var would be a
// data race.
var protectedBodyFieldsV atomic.Pointer[[]string]

func init() {
	fields := parseProtectedBodyFields(system.GetEnvMustProceed[string]("PROTECTED_BODY_FIELDS", "usr"))
	protectedBodyFieldsV.Store(&fields)
}

// getProtectedBodyFields returns the configured protected top-level body keys.
func getProtectedBodyFields() []string {
	if p := protectedBodyFieldsV.Load(); p != nil {
		return *p
	}
	return nil
}

func parseProtectedBodyFields(raw string) []string {
	var out []string
	for _, tok := range strings.Split(raw, ",") {
		if f := strings.TrimSpace(tok); len(f) > 0 {
			out = append(out, f)
		}
	}
	return out
}

// SetProtectedBodyFieldsForTest overrides the protected fields list; the list is
// resolved once from the environment at package init, so a test cannot change it
// via os.Setenv. Not for non-test use.
func SetProtectedBodyFieldsForTest(fields []string) { protectedBodyFieldsV.Store(&fields) }

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
func applyProtectedFieldsOnReplace(oldBody *easyjson.JSON, incoming easyjson.JSON) easyjson.JSON {
	fields := getProtectedBodyFields()
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
