package crud

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

// LinkConstraintsTestSuite pins the two structural invariants every CRUD link
// operation must preserve:
//
//	C1 — between two vertices, in one direction, there can be at most ONE link
//	     of a given type (the from.ltype.<type>.<to> index is unique).
//	C2 — a vertex can have at most ONE outgoing link with a given name (the
//	     from.out.to.<name> entry is unique).
//
// These run at the low-level graph API, where the invariants are enforced.
type LinkConstraintsTestSuite struct {
	test.StatefunTestSuite
}

func TestLinkConstraintsTestSuite(t *testing.T) {
	suite.Run(t, new(LinkConstraintsTestSuite))
}

func (s *LinkConstraintsTestSuite) setup() {
	cfg := *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1)
	s.RegisterFunction("functions.graph.api.vertex.create", LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.create", LLAPILinkCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.update", LLAPILinkUpdate, cfg)
	s.NoError(s.StartRuntime())
}

func status(r *easyjson.JSON) string { return r.GetByPath("status").AsStringDefault("failed") }

func (s *LinkConstraintsTestSuite) createVertex(id string) {
	p := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	r, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &p, nil)
	s.NoError(err)
	s.Equal("ok", status(r), "vertex %s create", id)
}

func (s *LinkConstraintsTestSuite) createLink(from, to, name, ltype string) *easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("to", easyjson.NewJSON(to))
	p.SetByPath("name", easyjson.NewJSON(name))
	p.SetByPath("type", easyjson.NewJSON(ltype))
	r, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &p, nil)
	s.NoError(err)
	return r
}

func (s *LinkConstraintsTestSuite) upsertLink(from, to, name, ltype string) *easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("to", easyjson.NewJSON(to))
	p.SetByPath("name", easyjson.NewJSON(name))
	p.SetByPath("type", easyjson.NewJSON(ltype))
	p.SetByPath("upsert", easyjson.NewJSON(true))
	r, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &p, nil)
	s.NoError(err)
	return r
}

// outLinkExists reports whether `from` has an out-link named `name`.
func (s *LinkConstraintsTestSuite) outLinkExists(from, name string) bool {
	key := fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, s.SetThisDomainPreffix(from), name)
	_, err := s.Runtime().Domain.Cache().GetValue(key)
	return err == nil
}

// linkNameForTypeTo returns the link name the ltype index records for the
// (from, type, to) triple, or "" if none.
func (s *LinkConstraintsTestSuite) linkNameForTypeTo(from, ltype, to string) string {
	key := fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, s.SetThisDomainPreffix(from), ltype, s.SetThisDomainPreffix(to))
	if v, err := s.Runtime().Domain.Cache().GetValue(key); err == nil {
		return string(v)
	}
	return ""
}

// --- C1: at most one link of a given type per (from -> to) ---

// Sanity: a plain create of a second same-type link to the same target is
// already rejected.
func (s *LinkConstraintsTestSuite) Test_C1_Create_DuplicateTypeToSameTarget_Rejected() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "e1", "rel")))
	s.Equal("failed", status(s.createLink("a", "b", "e2", "rel")),
		"a second rel-link a->b (different name) must be rejected")

	s.True(s.outLinkExists("a", "e1"))
	s.False(s.outLinkExists("a", "e2"))
	s.Equal("e1", s.linkNameForTypeTo("a", "rel", "b"))
}

// The bug: upsert-update with a NEW name but the SAME (type, to) as an existing
// link must NOT create a second link — it would put two rel-links a->b and
// corrupt the ltype index. This previously force-created the duplicate.
func (s *LinkConstraintsTestSuite) Test_C1_UpsertUpdate_DuplicateTypeToSameTarget_Rejected() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "e1", "rel")))

	s.Equal("failed", status(s.upsertLink("a", "b", "e2", "rel")),
		"upsert-update must not create a second rel-link a->b under a new name")

	// The original link is intact and no duplicate appeared.
	s.True(s.outLinkExists("a", "e1"), "original link e1 must survive")
	s.False(s.outLinkExists("a", "e2"), "duplicate link e2 must not exist")
	s.Equal("e1", s.linkNameForTypeTo("a", "rel", "b"), "ltype index must still point at e1")
}

// --- C2: at most one outgoing link per name ---

// Sanity: a plain create reusing an existing out-link name is rejected.
func (s *LinkConstraintsTestSuite) Test_C2_Create_DuplicateName_Rejected() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")
	s.createVertex("c")

	s.Equal("ok", status(s.createLink("a", "b", "edge", "rel")))
	s.Equal("failed", status(s.createLink("a", "c", "edge", "rel2")),
		"reusing out-link name 'edge' must be rejected")

	// The name still resolves to the original target/type.
	s.Equal("edge", s.linkNameForTypeTo("a", "rel", "b"))
	s.Equal("", s.linkNameForTypeTo("a", "rel2", "c"))
}

// An upsert that names an already-used out-link targets THAT link (the name is
// a vertex's out-link identity) — it never spawns a second link under the same
// name, so the C2 invariant holds. The caller-supplied to/type are ignored for
// identity; no new (rel2 -> c) edge is created.
func (s *LinkConstraintsTestSuite) Test_C2_UpsertUpdate_ExistingName_TargetsExistingLink() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")
	s.createVertex("c")

	s.Equal("ok", status(s.createLink("a", "b", "edge", "rel")))

	// name 'edge' already exists -> resolves to the a->b link; this is an
	// update of it, not a create. It must NOT mint a second 'edge'.
	s.upsertLink("a", "c", "edge", "rel2")

	s.True(s.outLinkExists("a", "edge"), "the single 'edge' out-link still exists")
	s.Equal("edge", s.linkNameForTypeTo("a", "rel", "b"), "'edge' still names the a->b rel link")
	s.Equal("", s.linkNameForTypeTo("a", "rel2", "c"), "no second link under name 'edge' was created")
}

// --- legit upsert-create must still work after the fix ---

func (s *LinkConstraintsTestSuite) Test_UpsertUpdate_NewLink_Created() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	// Nothing exists yet -> upsert creates the link.
	s.Equal("ok", status(s.upsertLink("a", "b", "e1", "rel")),
		"upsert of a brand-new link must create it")

	s.True(s.outLinkExists("a", "e1"))
	s.Equal("e1", s.linkNameForTypeTo("a", "rel", "b"))
}
