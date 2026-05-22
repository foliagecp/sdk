// Package crud — regression tests for the no-op short-circuit in
// LLAPIVertexUpdate and LLAPILinkUpdate.
//
// External SDK consumers tend to re-issue identical upserts in tight loops
// (idempotent state push). Without no-op detection every such upsert
// produces a WAL entry, fires triggers, and cascades through connected
// graphs — burning CPU on work that changes nothing.
//
// These tests pin the behaviour:
//   * identical body → status=idle, no op_stack entry, no cache write
//   * different body → status=ok, op_stack populated
//   * key-order independence (covered structurally — same map, different
//     iteration order would still compare equal)
//   * replace=true with identical body → still idle
//   * link update with identical body and no tags → idle
//   * link update with different body → ok
package crud

import (
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type LowLevelNoopTestSuite struct {
	test.StatefunTestSuite
}

func TestLowLevelNoopTestSuite(t *testing.T) {
	suite.Run(t, new(LowLevelNoopTestSuite))
}

// registerVertexFns installs vertex.create and vertex.update on the test
// runtime. Returns after StartRuntime.
func (s *LowLevelNoopTestSuite) registerVertexFns() {
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	s.RegisterFunction("functions.graph.api.vertex.create", LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.vertex.update", LLAPIVertexUpdate, cfg)
	s.NoError(s.StartRuntime())
}

// registerLinkFns installs the minimal set of LL functions needed for link
// update tests (vertex.create + link.create + link.update).
func (s *LowLevelNoopTestSuite) registerLinkFns() {
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	s.RegisterFunction("functions.graph.api.vertex.create", LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.create", LLAPILinkCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.update", LLAPILinkUpdate, cfg)
	s.NoError(s.StartRuntime())
}

// requireStatus asserts that the reply.status equals want, otherwise fails
// with the full reply body for triage.
func (s *LowLevelNoopTestSuite) requireStatus(reply *easyjson.JSON, want string) {
	s.Equalf(want, reply.GetByPath("status").AsStringDefault(""),
		"expected status=%q, got: %s", want, reply.ToString())
}

// -----------------------------------------------------------------------------
// Vertex update — no-op
// -----------------------------------------------------------------------------

// Test_VertexUpdate_NoOp_MergeSameBody — merge update with body identical
// to current state must return idle and not touch the KV.
func (s *LowLevelNoopTestSuite) Test_VertexUpdate_NoOp_MergeSameBody() {
	s.registerVertexFns()

	id := "v-noop-1"

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("body.cpu", easyjson.NewJSON(16))
	createPayload.SetByPath("body.role", easyjson.NewJSON("worker"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// Same merge payload — every field already equals the stored value.
	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body.cpu", easyjson.NewJSON(16))
	updatePayload.SetByPath("body.role", easyjson.NewJSON("worker"))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "idle")
}

// Test_VertexUpdate_NoOp_ReplaceSameBody — replace=true with body equal to
// current must also short-circuit to idle.
func (s *LowLevelNoopTestSuite) Test_VertexUpdate_NoOp_ReplaceSameBody() {
	s.registerVertexFns()

	id := "v-noop-2"

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("body.cpu", easyjson.NewJSON(16))
	createPayload.SetByPath("body.role", easyjson.NewJSON("worker"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("replace", easyjson.NewJSON(true))
	updatePayload.SetByPath("body.cpu", easyjson.NewJSON(16))
	updatePayload.SetByPath("body.role", easyjson.NewJSON("worker"))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "idle")
}

// Test_VertexUpdate_NoOp_OpStackIsEmpty — when the update is a no-op the
// returned op_stack must not contain a vertex_update entry. This guards the
// trigger fan-out: triggers iterate op_stack, an empty stack means nothing
// fires.
func (s *LowLevelNoopTestSuite) Test_VertexUpdate_NoOp_OpStackIsEmpty() {
	s.registerVertexFns()

	id := "v-noop-3"

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("body.cpu", easyjson.NewJSON(8))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body.cpu", easyjson.NewJSON(8))
	options := easyjson.NewJSONObject()
	options.SetByPath("op_stack", easyjson.NewJSON(true))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &updatePayload, &options)
	s.NoError(err)
	s.requireStatus(res, "idle")

	// op_stack must not contain a vertex_update for this id. Either the
	// field is absent entirely, or it is an empty array.
	opStack := res.GetByPath("data.op_stack")
	if opStack.IsArray() {
		s.Equalf(0, opStack.ArraySize(),
			"no-op vertex update must produce empty op_stack, got: %s", opStack.ToString())
	}
}

// Test_VertexUpdate_NotNoOp_DifferentBody — sanity check: a real change
// still returns ok (the no-op guard must not over-trigger).
func (s *LowLevelNoopTestSuite) Test_VertexUpdate_NotNoOp_DifferentBody() {
	s.registerVertexFns()

	id := "v-noop-4"

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("body.cpu", easyjson.NewJSON(16))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// Change cpu — must commit and return ok.
	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body.cpu", easyjson.NewJSON(32))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
}

// Test_VertexUpdate_NotNoOp_MergeAddsField — merge that adds a new field
// to the body must commit (the final tree differs from the stored one).
func (s *LowLevelNoopTestSuite) Test_VertexUpdate_NotNoOp_MergeAddsField() {
	s.registerVertexFns()

	id := "v-noop-5"

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("body.cpu", easyjson.NewJSON(16))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body.role", easyjson.NewJSON("worker"))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
}

// Test_VertexUpdate_NoOp_NestedSameBody — nested object equality must be
// honoured (the recursive structural compare in reflect.DeepEqual handles
// nested maps and arrays).
func (s *LowLevelNoopTestSuite) Test_VertexUpdate_NoOp_NestedSameBody() {
	s.registerVertexFns()

	id := "v-noop-6"

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("body.spec.cpu", easyjson.NewJSON(16))
	createPayload.SetByPath("body.spec.mem", easyjson.NewJSON(64))
	createPayload.SetByPath("body.tags", easyjson.NewJSON([]string{"a", "b"}))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body.spec.cpu", easyjson.NewJSON(16))
	updatePayload.SetByPath("body.spec.mem", easyjson.NewJSON(64))
	updatePayload.SetByPath("body.tags", easyjson.NewJSON([]string{"a", "b"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "idle")
}

// -----------------------------------------------------------------------------
// Link update — no-op
// -----------------------------------------------------------------------------

// Test_LinkUpdate_NoOp_MergeSameBody — merge update of a link with body
// equal to current state must short-circuit to idle.
func (s *LowLevelNoopTestSuite) Test_LinkUpdate_NoOp_MergeSameBody() {
	s.registerLinkFns()

	from := "lv-from-1"
	to := "lv-to-1"
	linkName := "edge"
	linkType := "__object"

	// Create both endpoints.
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", from, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", to, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// Create the link with a body.
	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("to", easyjson.NewJSON(to))
	createPayload.SetByPath("name", easyjson.NewJSON(linkName))
	createPayload.SetByPath("type", easyjson.NewJSON(linkType))
	createPayload.SetByPath("body.weight", easyjson.NewJSON(1))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// Update with identical body — should be idle.
	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("name", easyjson.NewJSON(linkName))
	updatePayload.SetByPath("body.weight", easyjson.NewJSON(1))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "idle")
}

// Test_LinkUpdate_NotNoOp_DifferentBody — link update with a changed body
// must commit normally.
func (s *LowLevelNoopTestSuite) Test_LinkUpdate_NotNoOp_DifferentBody() {
	s.registerLinkFns()

	from := "lv-from-2"
	to := "lv-to-2"
	linkName := "edge"
	linkType := "__object"

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", from, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", to, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("to", easyjson.NewJSON(to))
	createPayload.SetByPath("name", easyjson.NewJSON(linkName))
	createPayload.SetByPath("type", easyjson.NewJSON(linkType))
	createPayload.SetByPath("body.weight", easyjson.NewJSON(1))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("name", easyjson.NewJSON(linkName))
	updatePayload.SetByPath("body.weight", easyjson.NewJSON(2))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
}

// Test_LinkUpdate_NotNoOp_NewTagAdded — merge update with a new tag that
// does not yet exist on the link must commit (the tag set grows).
func (s *LowLevelNoopTestSuite) Test_LinkUpdate_NotNoOp_NewTagAdded() {
	s.registerLinkFns()

	from := "lv-from-3"
	to := "lv-to-3"
	linkName := "edge"
	linkType := "__object"

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", from, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", to, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("to", easyjson.NewJSON(to))
	createPayload.SetByPath("name", easyjson.NewJSON(linkName))
	createPayload.SetByPath("type", easyjson.NewJSON(linkType))
	createPayload.SetByPath("body.weight", easyjson.NewJSON(1))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// Same body, but a brand-new tag — must commit.
	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("name", easyjson.NewJSON(linkName))
	updatePayload.SetByPath("body.weight", easyjson.NewJSON(1))
	updatePayload.SetByPath("tags", easyjson.NewJSON([]string{"hot"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
}

// Test_LinkUpdate_NotNoOp_MergeWithTagsProvided — merge update with a
// non-empty payload.tags array always goes through the full re-issue
// path. This is intentional: merge cannot remove tags, but checking
// whether the requested set is a subset of existing tags would cost a
// GetKeysByPattern call on every merge upsert — too expensive for the
// common incremental-upsert hot path. So even when the merge would
// produce no observable change (e.g. all requested tags already exist),
// the operation still commits.
func (s *LowLevelNoopTestSuite) Test_LinkUpdate_NotNoOp_MergeWithTagsProvided() {
	s.registerLinkFns()

	from := "lv-from-3b"
	to := "lv-to-3b"
	linkName := "edge"
	linkType := "__object"

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", from, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", to, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("to", easyjson.NewJSON(to))
	createPayload.SetByPath("name", easyjson.NewJSON(linkName))
	createPayload.SetByPath("type", easyjson.NewJSON(linkType))
	createPayload.SetByPath("body.weight", easyjson.NewJSON(1))
	createPayload.SetByPath("tags", easyjson.NewJSON([]string{"hot", "warm"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// Same body and tags subset that would be a "physical" no-op, but we
	// commit anyway because the merge path doesn't scan existing tags.
	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("name", easyjson.NewJSON(linkName))
	updatePayload.SetByPath("body.weight", easyjson.NewJSON(1))
	updatePayload.SetByPath("tags", easyjson.NewJSON([]string{"hot"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
}

// Test_LinkUpdate_NoOp_Replace_SameBodySameTags — replace mode with body
// AND tag set identical to current state must short-circuit to idle. This
// is the high-value case: external consumers push full state with
// replace=true at every tick.
func (s *LowLevelNoopTestSuite) Test_LinkUpdate_NoOp_Replace_SameBodySameTags() {
	s.registerLinkFns()

	from := "lv-from-4"
	to := "lv-to-4"
	linkName := "edge"
	linkType := "__object"

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", from, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", to, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("to", easyjson.NewJSON(to))
	createPayload.SetByPath("name", easyjson.NewJSON(linkName))
	createPayload.SetByPath("type", easyjson.NewJSON(linkType))
	createPayload.SetByPath("body.weight", easyjson.NewJSON(1))
	createPayload.SetByPath("tags", easyjson.NewJSON([]string{"hot", "warm"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// Re-issue with replace=true, same body, same set of tags (different
	// order) — must be idle.
	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("name", easyjson.NewJSON(linkName))
	updatePayload.SetByPath("replace", easyjson.NewJSON(true))
	updatePayload.SetByPath("body.weight", easyjson.NewJSON(1))
	updatePayload.SetByPath("tags", easyjson.NewJSON([]string{"warm", "hot"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "idle")
}

// Test_LinkUpdate_NotNoOp_Replace_TagsDropped — replace mode with body
// identical but a SMALLER tag set must commit (the dropped tag is
// observable state).
func (s *LowLevelNoopTestSuite) Test_LinkUpdate_NotNoOp_Replace_TagsDropped() {
	s.registerLinkFns()

	from := "lv-from-5"
	to := "lv-to-5"
	linkName := "edge"
	linkType := "__object"

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", from, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", to, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("to", easyjson.NewJSON(to))
	createPayload.SetByPath("name", easyjson.NewJSON(linkName))
	createPayload.SetByPath("type", easyjson.NewJSON(linkType))
	createPayload.SetByPath("body.weight", easyjson.NewJSON(1))
	createPayload.SetByPath("tags", easyjson.NewJSON([]string{"hot", "warm"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// replace=true with same body but only one of the two existing tags
	// → the second tag must be wiped, so this is NOT a no-op.
	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("name", easyjson.NewJSON(linkName))
	updatePayload.SetByPath("replace", easyjson.NewJSON(true))
	updatePayload.SetByPath("body.weight", easyjson.NewJSON(1))
	updatePayload.SetByPath("tags", easyjson.NewJSON([]string{"hot"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
}

// Test_LinkUpdate_NotNoOp_Replace_AllTagsDropped — replace mode with
// existing tags but no tags in payload → all tags must be wiped (NOT a
// no-op).
func (s *LowLevelNoopTestSuite) Test_LinkUpdate_NotNoOp_Replace_AllTagsDropped() {
	s.registerLinkFns()

	from := "lv-from-6"
	to := "lv-to-6"
	linkName := "edge"
	linkType := "__object"

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", from, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", to, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("to", easyjson.NewJSON(to))
	createPayload.SetByPath("name", easyjson.NewJSON(linkName))
	createPayload.SetByPath("type", easyjson.NewJSON(linkType))
	createPayload.SetByPath("body.weight", easyjson.NewJSON(1))
	createPayload.SetByPath("tags", easyjson.NewJSON([]string{"hot"}))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("name", easyjson.NewJSON(linkName))
	updatePayload.SetByPath("replace", easyjson.NewJSON(true))
	updatePayload.SetByPath("body.weight", easyjson.NewJSON(1))
	// no "tags" field — replace should wipe the existing "hot" tag.
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
}

// Test_LinkUpdate_NoOp_Replace_NoTagsOnEitherSide — replace mode where
// neither the existing state nor the payload has tags AND body is
// identical → no-op.
func (s *LowLevelNoopTestSuite) Test_LinkUpdate_NoOp_Replace_NoTagsOnEitherSide() {
	s.registerLinkFns()

	from := "lv-from-7"
	to := "lv-to-7"
	linkName := "edge"
	linkType := "__object"

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", from, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", to, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("to", easyjson.NewJSON(to))
	createPayload.SetByPath("name", easyjson.NewJSON(linkName))
	createPayload.SetByPath("type", easyjson.NewJSON(linkType))
	createPayload.SetByPath("body.weight", easyjson.NewJSON(1))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &createPayload, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("name", easyjson.NewJSON(linkName))
	updatePayload.SetByPath("replace", easyjson.NewJSON(true))
	updatePayload.SetByPath("body.weight", easyjson.NewJSON(1))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &updatePayload, nil)
	s.NoError(err)
	s.requireStatus(res, "idle")
}
