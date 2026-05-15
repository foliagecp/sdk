// Package crud — atomicity, consistency and idempotency tests for HL/LL CRUD.
//
// These tests are written from the FIXED-behavior perspective: each assertion
// describes how the system SHOULD behave. Therefore on the current (buggy)
// code base every test FAILS — that is the desired signal. Once the
// corresponding defect is repaired, the matching test will PASS without any
// changes to the test itself.
//
//   D1 — multi-step HL CRUD operations must roll back on partial failure.
//        CreateObject must not leave a vertex without its __type link.
//        UpdateObject(upsert=true) must not silently mask a broken invariant.
//
//   D2 — HL DeleteObjectsLink must be idempotent the way LL link.delete is:
//        deleting an already-missing edge should return ok/idle, not failed.
//
//   D3 — HL link operations must not amplify reads. The link type is
//        physically stored in OutLinkTypeKeyPrefPattern; resolving it must
//        not cost two functions.cmdb.api.object.read calls per link op.
package crud

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type CrudAtomicityTestSuite struct {
	test.StatefunTestSuite
}

func TestCrudAtomicityTestSuite(t *testing.T) {
	suite.Run(t, new(CrudAtomicityTestSuite))
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// bootstrap registers all CRUD functions and starts the runtime. After
// startup it waits for cmdbSchemaPrepare to populate __root/__types/__objects.
func (s *CrudAtomicityTestSuite) bootstrap() {
	RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	// Give cmdbSchemaPrepare a moment to seed built-in vertices.
	s.waitForVertex(BUILT_IN_TYPES, 5*time.Second)
	s.waitForVertex(BUILT_IN_OBJECTS, 5*time.Second)
}

// waitForVertex polls the cache until the given vertex appears or timeout.
func (s *CrudAtomicityTestSuite) waitForVertex(id string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear within %s", id, timeout)
}

// cmdbTypeCreate creates a CMDB type.
func (s *CrudAtomicityTestSuite) cmdbTypeCreate(name string) {
	payload := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", name, &payload, nil)
	s.NoError(err)
	s.Equalf("ok", res.GetByPath("status").AsStringDefault(""),
		"type.create for %q must succeed, got: %s", name, res.ToString())
}

// cmdbTypesLink links two CMDB types so objects of those types can be linked.
// CreateTypesLink expects payload {to, object_type, ...}, where object_type is
// the type label that will be used on object-to-object links of this kind.
func (s *CrudAtomicityTestSuite) cmdbTypesLink(from, to, objectLinkType string) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("object_type", easyjson.NewJSON(objectLinkType))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.create", from, &payload, nil)
	s.NoError(err)
	s.Equalf("ok", res.GetByPath("status").AsStringDefault(""),
		"types.link.create %q→%q must succeed, got: %s", from, to, res.ToString())
}

// cmdbObjectCreate creates a CMDB object of the given type.
func (s *CrudAtomicityTestSuite) cmdbObjectCreate(id, originType string) *easyjson.JSON {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("origin_type", easyjson.NewJSON(originType))
	payload.SetByPath("body", easyjson.NewJSONObject())
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", id, &payload, nil)
	s.NoError(err)
	return res
}

// cmdbObjectsLinkCreate creates a link between two CMDB objects.
func (s *CrudAtomicityTestSuite) cmdbObjectsLinkCreate(from, to, name string) *easyjson.JSON {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("name", easyjson.NewJSON(name))
	payload.SetByPath("body", easyjson.NewJSONObject())
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.create", from, &payload, nil)
	s.NoError(err)
	return res
}

// cmdbTypeSetSubtype declares `child` as a sub-type of `parent`, enabling
// the SuperType machinery to treat objects of `child` as also being of
// `parent` for type-claim purposes.
func (s *CrudAtomicityTestSuite) cmdbTypeSetSubtype(parent, child string) {
	payload := easyjson.NewJSONObjectWithKeyValue("sub_type", easyjson.NewJSON(child))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.subtype.set", parent, &payload, nil)
	s.NoError(err)
	s.Equalf("ok", res.GetByPath("status").AsStringDefault(""),
		"type.subtype.set %q→%q must succeed, got: %s", parent, child, res.ToString())
}

// cmdbObjectsLinkSuperTypeCreate creates a cross-pack link via the
// SuperType-flavoured API (functions.cmdb.api.objects.link.supertype.create).
// The compound link type stored in KV becomes "<fromClaim>#<toClaim>#<rel>"
// where <rel> is the objectLinkType of the (fromClaim, toClaim) TypesLink.
func (s *CrudAtomicityTestSuite) cmdbObjectsLinkSuperTypeCreate(from, to, name, fromClaim, toClaim string) *easyjson.JSON {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("name", easyjson.NewJSON(name))
	payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaim))
	payload.SetByPath("to_super_type", easyjson.NewJSON(toClaim))
	payload.SetByPath("body", easyjson.NewJSONObject())
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.supertype.create", from, &payload, nil)
	s.NoError(err)
	return res
}

// cmdbObjectsLinkSuperTypeDelete deletes the cross-pack link via the
// SuperType-flavoured API.
func (s *CrudAtomicityTestSuite) cmdbObjectsLinkSuperTypeDelete(from, to, fromClaim, toClaim string) *easyjson.JSON {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaim))
	payload.SetByPath("to_super_type", easyjson.NewJSON(toClaim))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.supertype.delete", from, &payload, nil)
	s.NoError(err)
	return res
}

// vertexExists checks if a vertex body exists in the cache.
func (s *CrudAtomicityTestSuite) vertexExists(id string) bool {
	_, err := s.CacheValue(id)
	return err == nil
}

// hasOutLinkOfType reports true if the vertex `from` has any out-link of type `linkType`.
func (s *CrudAtomicityTestSuite) hasOutLinkOfType(from, linkType string) bool {
	domID := s.SetThisDomainPreffix(from)
	pattern := fmt.Sprintf(OutLinkTypeKeyPrefPattern+"%s.>", domID, linkType)
	keys := s.Runtime().Domain.Cache().GetKeysByPattern(pattern)
	return len(keys) > 0
}

// -----------------------------------------------------------------------------
// Defect 1 — CreateObject leaves orphan state on partial failure
// -----------------------------------------------------------------------------

// Test_D1_CreateObject_LeavesOrphanVertex_OnLinkCreateFailure forces the
// second link.create inside CreateObject to fail and verifies that the
// vertex remains created but without a __type link — the "typeless object"
// state that breaks subsequent operations.
//
// Mechanism: we wrap LLAPILinkCreate with a handler that returns FAILED on
// the second invocation per test. We register it AFTER RegisterAllFunctionTypes
// to overwrite the default registration.
//
// CURRENT behavior: vertex exists, no __type link → orphan.
// FIXED behavior (post-rollback): vertex must not exist OR __type link must exist.
func (s *CrudAtomicityTestSuite) Test_D1_CreateObject_LeavesOrphanVertex_OnLinkCreateFailure() {
	RegisterAllFunctionTypes(s.Runtime())

	var linkCreateCalls atomic.Int32
	failingLinkCreate := func(exec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		n := linkCreateCalls.Add(1)
		// Fail only the 2nd link.create that runs in CreateObject for "obj-d1".
		// CreateObject creates 3 links: __objects→obj, obj→type, type→obj.
		// We fail the obj→type link, which corresponds to the __type link.
		if n == 2 {
			om := sfMediators.NewOpMediator(ctx)
			om.AggregateOpMsg(sfMediators.OpMsgFailed("synthetic D1 failure on __type link.create")).Reply()
			return
		}
		LLAPILinkCreate(exec, ctx)
	}
	cfg := *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1)
	statefun.NewFunctionType(s.Runtime(), "functions.graph.api.link.create", failingLinkCreate, cfg)

	s.NoError(s.StartRuntime())
	s.waitForVertex(BUILT_IN_TYPES, 5*time.Second)
	s.waitForVertex(BUILT_IN_OBJECTS, 5*time.Second)

	s.cmdbTypeCreate("TypeD1")

	// Reset counter; from this point only CreateObject's own link.create calls count.
	linkCreateCalls.Store(0)

	res := s.cmdbObjectCreate("obj-d1", "TypeD1")
	status := res.GetByPath("status").AsStringDefault("")
	vertexLeft := s.vertexExists("obj-d1")
	hasTypeLink := s.hasOutLinkOfType("obj-d1", TO_TYPELINK)
	s.T().Logf("after partial failure: status=%q, vertex exists=%v, has __type out-link=%v",
		status, vertexLeft, hasTypeLink)

	// Correct behavior on partial failure (the FIX must satisfy this):
	//   either everything is rolled back  (vertex absent, no __type link), OR
	//   everything succeeded             (vertex present, __type link present).
	// The disallowed state is the orphan: vertex present, __type link absent.
	orphan := vertexLeft && !hasTypeLink
	s.Falsef(orphan,
		"orphan state after partial CreateObject failure: vertex exists without __type link — rollback missing (D1)")

	// Bonus invariant: if status is "ok", the graph MUST be fully consistent
	// (vertex present AND __type link present). An "ok" with missing __type
	// link is a silent lie from OpMediator.
	if status == "ok" {
		s.Truef(hasTypeLink,
			"OpMediator returned ok but __type link is missing — status aggregation bug")
	}
}

// Test_D1_UpdateObject_Upsert_SilentlyMissesBrokenInvariant simulates the
// orphan state directly via LL (delete the __type link of an otherwise valid
// object) and then asks UpdateObject(upsert=true, origin_type=...) to repair it.
//
// The interesting CURRENT behavior is worse than "upsert errors out":
//   - findObjectType() returns the cached value (cacheGetObjectType is not
//     invalidated by direct LL link.delete);
//   - the upsert path therefore skips its "object does not exist" branch;
//   - the regular UpdateObject path updates the body and returns "ok";
//   - the __type link is NEVER restored.
//
// Net effect: a SILENT consistency failure. The caller sees "ok" but the
// graph is still broken — and the cache continues to lie.
//
// FIXED behavior (any of):
//   (a) HL CRUD invariant check on update: verify __type link before returning ok.
//   (b) Cache invalidation when LL link.delete deletes a __type link.
//   (c) Repair branch in upsert: if invariant is broken, restore __type link.
func (s *CrudAtomicityTestSuite) Test_D1_UpdateObject_Upsert_SilentlyMissesBrokenInvariant() {
	s.bootstrap()
	s.cmdbTypeCreate("TypeD1b")

	// Create a healthy object.
	res := s.cmdbObjectCreate("obj-d1b", "TypeD1b")
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""),
		"healthy object creation must succeed: %s", res.ToString())

	// Break the invariant: delete the obj→TypeD1b __type link via LL.
	// We use LL directly so we don't trip any HL safeguards.
	delPayload := easyjson.NewJSONObject()
	delPayload.SetByPath("name", easyjson.NewJSON("type"))
	delRes, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", "obj-d1b", &delPayload, nil)
	s.NoError(err)
	s.Equal("ok", delRes.GetByPath("status").AsStringDefault(""),
		"LL link.delete of the __type link should succeed: %s", delRes.ToString())

	s.False(s.hasOutLinkOfType("obj-d1b", TO_TYPELINK),
		"after breaking, the object must have no __type link")
	s.True(s.vertexExists("obj-d1b"), "vertex still exists; that's the orphan state")

	// Attempt to repair via UpdateObject(upsert=true).
	upsertPayload := easyjson.NewJSONObject()
	upsertPayload.SetByPath("origin_type", easyjson.NewJSON("TypeD1b"))
	upsertPayload.SetByPath("upsert", easyjson.NewJSON(true))
	upsertPayload.SetByPath("body", easyjson.NewJSONObject())
	upsertRes, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.update", "obj-d1b", &upsertPayload, nil)
	s.NoError(err)

	status := upsertRes.GetByPath("status").AsStringDefault("")
	hasTypeLinkAfter := s.hasOutLinkOfType("obj-d1b", TO_TYPELINK)
	s.T().Logf("upsert repair attempt: status=%q, __type link present after=%v", status, hasTypeLinkAfter)

	// Correct behavior: upsert must NEVER claim success while leaving the
	// CMDB invariant violated. Either it repaired the __type link (status=ok
	// AND hasTypeLinkAfter), or it refused to lie (status != ok).
	if status == "ok" {
		s.Truef(hasTypeLinkAfter,
			"silent corruption (D1): upsert returned ok but did not restore the __type link")
	} else {
		s.T().Logf("upsert correctly declined to claim success: status=%q", status)
	}
}

// -----------------------------------------------------------------------------
// Defect 2 — DeleteObjectsLink fails when target vertex is gone,
// while LL link.delete is idempotent.
// -----------------------------------------------------------------------------

// Test_D2_DeleteObjectsLink_FailsWhenTargetDeleted shows the asymmetry:
// the LL primitive returns IDLE/OK when asked to delete a link whose source
// already has no record, but the HL wrapper performs an endpoint-type pre-read
// and refuses with FAILED if either endpoint object can't be resolved.
func (s *CrudAtomicityTestSuite) Test_D2_DeleteObjectsLink_FailsWhenTargetDeleted() {
	s.bootstrap()

	s.cmdbTypeCreate("TypeD2A")
	s.cmdbTypeCreate("TypeD2B")
	s.cmdbTypesLink("TypeD2A", "TypeD2B", "d2-link")

	s.Equal("ok", s.cmdbObjectCreate("obj-a-d2", "TypeD2A").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("obj-b-d2", "TypeD2B").GetByPath("status").AsStringDefault(""))

	linkName := "edge-d2"
	linkRes := s.cmdbObjectsLinkCreate("obj-a-d2", "obj-b-d2", linkName)
	s.Equal("ok", linkRes.GetByPath("status").AsStringDefault(""),
		"object link create must succeed in this happy path: %s", linkRes.ToString())

	// Now delete the target object obj-b-d2 directly.
	delObj := easyjson.NewJSONObject()
	delRes, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", "obj-b-d2", &delObj, nil)
	s.NoError(err)
	s.T().Logf("object.delete obj-b-d2 → status=%q", delRes.GetByPath("status").AsStringDefault(""))

	// 1) Sanity: LL link.delete is idempotent (returns ok or idle).
	llPayload := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(linkName))
	llRes, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", "obj-a-d2", &llPayload, nil)
	s.NoError(err)
	llStatus := llRes.GetByPath("status").AsStringDefault("")
	s.T().Logf("LL link.delete → status=%q details=%q",
		llStatus, llRes.GetByPath("details").AsStringDefault(""))
	s.Containsf([]string{"ok", "idle"}, llStatus,
		"LL link.delete must be idempotent (ok or idle), got %q", llStatus)

	// Re-create the edge so HL has something to delete in the next step.
	relinkRes := s.cmdbObjectsLinkCreate("obj-a-d2", "obj-b-d2", linkName)
	if relinkRes.GetByPath("status").AsStringDefault("") != "ok" {
		// If we cannot re-link (e.g. because obj-b-d2 vertex is gone), that's
		// fine for the next assertion — HL DeleteObjectsLink must still be
		// idempotent when called against an already-missing edge.
		s.T().Logf("re-link skipped (target object deleted): %s", relinkRes.ToString())
	}

	// 2) HL DeleteObjectsLink MUST be idempotent like LL.
	hlPayload := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("obj-b-d2"))
	hlRes, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", "obj-a-d2", &hlPayload, nil)
	s.NoError(err)
	hlStatus := hlRes.GetByPath("status").AsStringDefault("")
	s.T().Logf("HL DeleteObjectsLink → status=%q details=%q",
		hlStatus, hlRes.GetByPath("details").AsStringDefault(""))
	s.Containsf([]string{"ok", "idle"}, hlStatus,
		"HL DeleteObjectsLink must be idempotent like LL link.delete (D2): got %q", hlStatus)
}

// -----------------------------------------------------------------------------
// Defect 3 — read amplification: each HL link op costs 2× object.read
// -----------------------------------------------------------------------------

// Test_D3_ObjectsLinkOp_ReadAmplification wraps `functions.cmdb.api.object.read`
// with a counter and asserts that creating a single object link triggers
// at least 2 object.read calls (current behavior).
//
// Crucial setup detail: the SDK keeps an in-memory object-type cache
// (objectTypeCache in hl_crud_helpers.go). After ObjectCreate the type is
// already cached, so findObjectType is a cache hit and DOES NOT call
// object.read. To measure the actual cost on a cold path (which is what
// production hits whenever the cache is evicted, on a fresh runtime, or for
// an unknown endpoint), we explicitly invalidate the cache before the link op.
//
// FIXED behavior should bring this down to ≤1 (or 0) — the link type is
// already physically stored in KV under OutLinkTypeKeyPrefPattern and can be
// read directly without two object.read calls.
func (s *CrudAtomicityTestSuite) Test_D3_ObjectsLinkOp_ReadAmplification() {
	RegisterAllFunctionTypes(s.Runtime())

	var objectReadCalls atomic.Int64
	counted := func(exec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		objectReadCalls.Add(1)
		ReadObject(exec, ctx)
	}
	cfg := *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1)
	statefun.NewFunctionType(s.Runtime(), "functions.cmdb.api.object.read", counted, cfg)

	s.NoError(s.StartRuntime())
	s.waitForVertex(BUILT_IN_TYPES, 5*time.Second)
	s.waitForVertex(BUILT_IN_OBJECTS, 5*time.Second)

	s.cmdbTypeCreate("TypeD3A")
	s.cmdbTypeCreate("TypeD3B")
	s.cmdbTypesLink("TypeD3A", "TypeD3B", "d3-link")
	s.Equal("ok", s.cmdbObjectCreate("obj-a-d3", "TypeD3A").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("obj-b-d3", "TypeD3B").GetByPath("status").AsStringDefault(""))

	// Invalidate the object-type cache to simulate cold-path behavior
	// (production hits this whenever cache misses or on a fresh runtime).
	cacheDeleteObjectType(s.SetThisDomainPreffix("obj-a-d3"))
	cacheDeleteObjectType(s.SetThisDomainPreffix("obj-b-d3"))

	// Reset the counter — measure only what ObjectsLinkCreate itself triggers.
	objectReadCalls.Store(0)
	res := s.cmdbObjectsLinkCreate("obj-a-d3", "obj-b-d3", "edge-d3")
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""),
		"happy-path link.create must succeed: %s", res.ToString())

	got := objectReadCalls.Load()
	s.T().Logf("CreateObjectsLink (cold cache) → triggered %d object.read calls", got)

	// Correct behavior: the link type is already stored in the KV under
	// OutLinkTypeKeyPrefPattern at create-time; resolving it for a subsequent
	// link operation must NOT cost two full object.read calls per endpoint.
	// We allow at most 1 (e.g. one cache warm-up) — anything ≥2 is the
	// read-amplification bug (D3).
	s.LessOrEqualf(got, int64(1),
		"read amplification (D3): a single CreateObjectsLink triggered %d object.read calls (expected ≤1)", got)
}

// -----------------------------------------------------------------------------
// Defect 2 — SuperType (cross-pack) idempotency
//
// These tests mirror Test_D2 but exercise the SuperType-flavoured HL API
// (functions.cmdb.api.objects.link.supertype.{create,delete}) that osm-app
// actually uses for cross-pack edges. The plain DeleteObjectsLink was fixed
// in commit 838f7c7, but DeleteObjectsLinkFromSuperTypes still routes
// through isObjectLinkPermittedForClaimedTypes which requires BOTH endpoints
// to be type-resolvable and therefore fails when the target object has been
// deleted — even though the underlying LL link.delete is idempotent. This is
// Symptom A from the original bug report.
// -----------------------------------------------------------------------------

// Test_D2_super_DeleteObjectsLinkFromSuperTypes_IdempotentWhenTargetDeleted is
// the SuperType counterpart of Test_D2_DeleteObjectsLink_FailsWhenTargetDeleted.
// Workflow:
//   - set up CMDB schema TypeA → TypeB
//   - create obj-a (TypeA), obj-b (TypeB) and cross-pack link via SuperType create
//   - delete obj-b through ObjectDelete (its __type link goes away)
//   - call SuperType-delete from obj-a → obj-b
//
// Correct behaviour: ok / idle (no edge to delete). Currently the call returns
// failed with "no object link from type X to type Y" because
// isObjectLinkPermittedForClaimedTypes cannot resolve obj-b's type.
func (s *CrudAtomicityTestSuite) Test_D2_super_DeleteObjectsLinkFromSuperTypes_IdempotentWhenTargetDeleted() {
	s.bootstrap()

	s.cmdbTypeCreate("TypeD2sA")
	s.cmdbTypeCreate("TypeD2sB")
	s.cmdbTypesLink("TypeD2sA", "TypeD2sB", "d2s-link")

	s.Equal("ok", s.cmdbObjectCreate("obj-a-d2s", "TypeD2sA").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("obj-b-d2s", "TypeD2sB").GetByPath("status").AsStringDefault(""))

	createRes := s.cmdbObjectsLinkSuperTypeCreate("obj-a-d2s", "obj-b-d2s", "edge-d2s", "TypeD2sA", "TypeD2sB")
	s.Equal("ok", createRes.GetByPath("status").AsStringDefault(""),
		"supertype link create must succeed in happy path: %s", createRes.ToString())

	// Tear down the target. ObjectDelete removes obj-b-d2s and its CMDB
	// structural links; obj-a-d2s still has the dangling out-edge in KV.
	delObj := easyjson.NewJSONObject()
	delRes, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", "obj-b-d2s", &delObj, nil)
	s.NoError(err)
	s.T().Logf("object.delete obj-b-d2s → status=%q", delRes.GetByPath("status").AsStringDefault(""))

	// SuperType-delete on the half-gone edge must be idempotent.
	hlRes := s.cmdbObjectsLinkSuperTypeDelete("obj-a-d2s", "obj-b-d2s", "TypeD2sA", "TypeD2sB")
	hlStatus := hlRes.GetByPath("status").AsStringDefault("")
	s.T().Logf("HL DeleteObjectsLinkFromSuperTypes → status=%q details=%q",
		hlStatus, hlRes.GetByPath("details").AsStringDefault(""))

	s.Containsf([]string{"ok", "idle"}, hlStatus,
		"HL DeleteObjectsLinkFromSuperTypes must be idempotent like LL link.delete (D2 cross-pack): got %q", hlStatus)
}

// Test_D2_super_CreateObjectsLinkFromSuperTypes_FailsWhenSchemaMissing covers
// the configuration-error half of the distinct-errors story from the
// follow-up brief: a SuperType *create* against a TypesLink schema that does
// not exist must return failed (caller treats it as a config error, not a
// transient miss to retry). This is the corresponding asymmetry to the
// idempotency requirement on the delete path — a regression here would mean
// silently inventing edges between types the schema does not allow.
func (s *CrudAtomicityTestSuite) Test_D2_super_CreateObjectsLinkFromSuperTypes_FailsWhenSchemaMissing() {
	s.bootstrap()

	s.cmdbTypeCreate("TypeD2sX")
	s.cmdbTypeCreate("TypeD2sY")
	// NOTE: NO types.link.create between X and Y — schema is intentionally absent.

	s.Equal("ok", s.cmdbObjectCreate("obj-a-d2sx", "TypeD2sX").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("obj-b-d2sy", "TypeD2sY").GetByPath("status").AsStringDefault(""))

	res := s.cmdbObjectsLinkSuperTypeCreate("obj-a-d2sx", "obj-b-d2sy", "edge-d2sxy", "TypeD2sX", "TypeD2sY")
	status := res.GetByPath("status").AsStringDefault("")
	s.T().Logf("create with missing schema → status=%q details=%q",
		status, res.GetByPath("details").AsStringDefault(""))
	s.Equalf("failed", status,
		"SuperType-create against a missing TypesLink schema must fail (config error), got %q", status)
}

// Test_D2_super_DeleteObjectsLinkFromSuperTypes_SelectsCorrectEdge_WhenMultipleCompoundTypesExist
// pins down a non-determinism in the previous SuperType-delete fix.
//
// When two SuperType edges share the same (from, to) object pair but have
// different compound types (different fromClaim/toClaim pairs), the helper
// resolveLinkBetweenTwoObjects returned the FIRST matching key from the
// cache pattern scan — an order which is undefined for a sharded map.
// DeleteObjectsLinkFromSuperTypes then compared the compound type of that
// arbitrary edge with the caller's claim and:
//   - if it matched by luck → the targeted edge was deleted (correct);
//   - if it didn't → the call returned idle and the targeted edge stayed
//     in the graph (incorrect).
//
// The fix must look specifically for the edge whose compound type starts
// with "<fromClaim>#<toClaim>#" and ignore every other (from→to) edge.
func (s *CrudAtomicityTestSuite) Test_D2_super_DeleteObjectsLinkFromSuperTypes_SelectsCorrectEdge_WhenMultipleCompoundTypesExist() {
	s.bootstrap()

	// Two independent parent-type pairs, each with its own schema link.
	s.cmdbTypeCreate("D2MTypeX")
	s.cmdbTypeCreate("D2MTypeY")
	s.cmdbTypeCreate("D2MTypeP")
	s.cmdbTypeCreate("D2MTypeQ")
	s.cmdbTypeCreate("D2MChildA") // will inherit from X and Y
	s.cmdbTypeCreate("D2MChildB") // will inherit from P and Q

	s.cmdbTypeSetSubtype("D2MTypeX", "D2MChildA")
	s.cmdbTypeSetSubtype("D2MTypeY", "D2MChildA")
	s.cmdbTypeSetSubtype("D2MTypeP", "D2MChildB")
	s.cmdbTypeSetSubtype("D2MTypeQ", "D2MChildB")

	s.cmdbTypesLink("D2MTypeX", "D2MTypeP", "xp-link")
	s.cmdbTypesLink("D2MTypeY", "D2MTypeQ", "yq-link")

	s.Equal("ok", s.cmdbObjectCreate("obj-a-d2m", "D2MChildA").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("obj-b-d2m", "D2MChildB").GetByPath("status").AsStringDefault(""))

	// Create two SuperType edges between the same object pair.
	resXP := s.cmdbObjectsLinkSuperTypeCreate("obj-a-d2m", "obj-b-d2m", "edge-xp", "D2MTypeX", "D2MTypeP")
	s.Equalf("ok", resXP.GetByPath("status").AsStringDefault(""),
		"supertype create (X,P) must succeed: %s", resXP.ToString())

	resYQ := s.cmdbObjectsLinkSuperTypeCreate("obj-a-d2m", "obj-b-d2m", "edge-yq", "D2MTypeY", "D2MTypeQ")
	s.Equalf("ok", resYQ.GetByPath("status").AsStringDefault(""),
		"supertype create (Y,Q) must succeed: %s", resYQ.ToString())

	// Both edges must be present before the targeted delete.
	s.True(s.hasOutLinkOfType("obj-a-d2m", "D2MTypeX#D2MTypeP#xp-link"), "edge X#P must exist before delete")
	s.True(s.hasOutLinkOfType("obj-a-d2m", "D2MTypeY#D2MTypeQ#yq-link"), "edge Y#Q must exist before delete")

	// Delete only the (Y,Q) edge.
	delRes := s.cmdbObjectsLinkSuperTypeDelete("obj-a-d2m", "obj-b-d2m", "D2MTypeY", "D2MTypeQ")
	delStatus := delRes.GetByPath("status").AsStringDefault("")
	s.T().Logf("SuperType delete (Y,Q) → status=%q details=%q", delStatus, delRes.GetByPath("details").AsStringDefault(""))
	s.Containsf([]string{"ok", "idle"}, delStatus,
		"delete of (Y,Q) edge must succeed: got %q", delStatus)

	// Y#Q edge must be gone; X#P edge must survive untouched.
	s.False(s.hasOutLinkOfType("obj-a-d2m", "D2MTypeY#D2MTypeQ#yq-link"),
		"edge Y#Q must be removed after SuperType delete targeting (Y,Q)")
	s.True(s.hasOutLinkOfType("obj-a-d2m", "D2MTypeX#D2MTypeP#xp-link"),
		"edge X#P must survive: delete targeted (Y,Q) only")
}
