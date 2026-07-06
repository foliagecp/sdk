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

// LinkUpdateResolverBugTestSuite pins the fixes for two defects that lived in
// getFullLinkInfoFromSpecifiedIdentifier (ll_crud_helpers.go), the resolver
// LLAPILinkUpdate/Delete/Read use to locate the link a payload addresses.
//
// A link's semantic identity for the uniqueness invariant is (from, type, to)
// — two vertices can hold at most one link of a type per direction — yet the
// resolver used to address links by exactly ONE index and never cross-check
// the other:
//
//	BUG 1 — if the payload carried "name", the resolver looked up ONLY
//	  out.to.<name> and on a miss reported "does not exist", even when the
//	  payload's type+to identified an existing edge under another name. With
//	  upsert=true the update then fell into link.create, whose constraint
//	  check found that same edge via the ltype index and failed with "already
//	  exists" — upsert was a dead end: it could neither update nor create.
//	  Fixed by falling back to the type+to lookup on a name miss.
//
//	BUG 2 — the type+to branch built the ltype cache key from the RAW
//	  payload type, while LLAPILinkCreate normalizes the type via
//	  GetObjectIDWithoutDomain before writing that index. A domain-prefixed
//	  type therefore missed on lookup but matched on create — the same
//	  read/write asymmetry, one normalization step earlier. Fixed by
//	  normalizing the type in the resolver exactly like the write path.
//
// The Bug* tests assert the fixed behaviour and were RED before the resolver
// fix; the Control* tests passed all along and isolate each trigger.
//
// See also Test_C1_UpsertUpdate_DuplicateTypeToSameTarget_UpdatesExisting in
// link_constraints_test.go: the C1 invariant half (no duplicate edge appears)
// holds across the fix, while the status flipped from "failed" to "ok".
type LinkUpdateResolverBugTestSuite struct {
	test.StatefunTestSuite
}

func TestLinkUpdateResolverBugTestSuite(t *testing.T) {
	suite.Run(t, new(LinkUpdateResolverBugTestSuite))
}

func (s *LinkUpdateResolverBugTestSuite) setup() {
	cfg := *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1)
	s.RegisterFunction("functions.graph.api.vertex.create", LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.create", LLAPILinkCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.update", LLAPILinkUpdate, cfg)
	s.RegisterFunction("functions.graph.api.link.delete", LLAPILinkDelete, cfg)
	s.NoError(s.StartRuntime())
}

func (s *LinkUpdateResolverBugTestSuite) createVertex(id string) {
	p := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	r, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &p, nil)
	s.NoError(err)
	s.Equal("ok", status(r), "vertex %s create", id)
}

func (s *LinkUpdateResolverBugTestSuite) createLink(from, to, name, ltype string) *easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("to", easyjson.NewJSON(to))
	p.SetByPath("name", easyjson.NewJSON(name))
	p.SetByPath("type", easyjson.NewJSON(ltype))
	r, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &p, nil)
	s.NoError(err)
	return r
}

// updateLink issues functions.graph.api.link.update with exactly the given
// payload fields (empty string omits the field).
func (s *LinkUpdateResolverBugTestSuite) updateLink(from, to, name, ltype string, upsert bool, body easyjson.JSON) *easyjson.JSON {
	p := easyjson.NewJSONObject()
	if to != "" {
		p.SetByPath("to", easyjson.NewJSON(to))
	}
	if name != "" {
		p.SetByPath("name", easyjson.NewJSON(name))
	}
	if ltype != "" {
		p.SetByPath("type", easyjson.NewJSON(ltype))
	}
	if upsert {
		p.SetByPath("upsert", easyjson.NewJSON(true))
	}
	p.SetByPath("body", body)
	r, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", from, &p, nil)
	s.NoError(err)
	return r
}

func (s *LinkUpdateResolverBugTestSuite) outLinkExists(from, name string) bool {
	key := fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, s.SetThisDomainPreffix(from), name)
	_, err := s.Runtime().Domain.Cache().GetValue(key)
	return err == nil
}

// linkNameForTypeTo returns the link name the ltype index records for the
// (from, type, to) triple, or "" if none.
func (s *LinkUpdateResolverBugTestSuite) linkNameForTypeTo(from, ltype, to string) string {
	key := fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, s.SetThisDomainPreffix(from), ltype, s.SetThisDomainPreffix(to))
	if v, err := s.Runtime().Domain.Cache().GetValue(key); err == nil {
		return string(v)
	}
	return ""
}

func (s *LinkUpdateResolverBugTestSuite) linkBody(from, name string) *easyjson.JSON {
	key := fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, s.SetThisDomainPreffix(from), name)
	b, err := s.Runtime().Domain.Cache().GetValueJSON(key)
	if err != nil {
		return easyjson.NewJSONNull().GetPtr()
	}
	return b
}

// --- BUG 1: name-first lookup has no type+to fallback -----------------------

// Upsert-update addressing an existing (type, to) edge by a NEW name.
//
// Before the fix the resolver missed on out.to.bar and reported the link
// absent; upsert fell into link.create, which found the edge via the ltype
// index and failed "already exists ... only once" — the caller could neither
// update nor create. Now the resolver falls back to the payload's type+to,
// resolves the existing edge, and the update succeeds against it.
func (s *LinkUpdateResolverBugTestSuite) Test_Bug1_UpsertByNewName_ExistingTypeTo_MustUpdateNotFail() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "foo", "rel")))

	r := s.updateLink("a", "b", "bar", "rel", true, easyjson.NewJSONObjectWithKeyValue("upd", easyjson.NewJSON(true)))
	s.Equal("ok", status(r),
		"upsert-update addressing the existing (rel, a->b) edge by a new name must update it, not dead-end: %s",
		r.GetByPath("details").AsStringDefault(""))

	// Whatever name semantics the fix picks (keep "foo" or rename to "bar"),
	// exactly one edge must remain and carry the updated body.
	name := s.linkNameForTypeTo("a", "rel", "b")
	s.NotEqual("", name, "the single (rel, a->b) edge must still be indexed")
	s.NotEqual(s.outLinkExists("a", "foo"), s.outLinkExists("a", "bar"),
		"exactly one out-link must exist — no duplicate, no loss")
	s.True(s.linkBody("a", name).GetByPath("upd").AsBoolDefault(false),
		"the surviving edge must carry the updated body")
}

// Same asymmetry without upsert: the update addresses the existing edge by
// type+to but a new name, and used to be told the link "does not exist"
// (idle) even though (rel, a->b) exists under "foo". The same fallback
// resolves it via type+to and updates.
func (s *LinkUpdateResolverBugTestSuite) Test_Bug1_NonUpsertByNewName_ExistingTypeTo_MustResolve() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "foo", "rel")))

	r := s.updateLink("a", "b", "bar", "rel", false, easyjson.NewJSONObjectWithKeyValue("upd", easyjson.NewJSON(true)))
	s.Equal("ok", status(r),
		"update addressing the existing (rel, a->b) edge must resolve it via type+to fallback: %s",
		r.GetByPath("details").AsStringDefault(""))
	s.True(s.linkBody("a", s.linkNameForTypeTo("a", "rel", "b")).GetByPath("upd").AsBoolDefault(false))
}

// Control (passed before the fix too): addressing the link by its EXISTING
// name works, so the dead end above was caused purely by the missing fallback.
func (s *LinkUpdateResolverBugTestSuite) Test_Bug1_Control_UpsertByExistingName_Works() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "foo", "rel")))

	r := s.updateLink("a", "b", "foo", "rel", true, easyjson.NewJSONObjectWithKeyValue("upd", easyjson.NewJSON(true)))
	s.Equal("ok", status(r))
	s.True(s.linkBody("a", "foo").GetByPath("upd").AsBoolDefault(false))
}

// --- BUG 2: type+to lookup does not normalize a domain-prefixed type --------

// Update addressed by to + DOMAIN-PREFIXED type, no name.
//
// Before the fix the resolver built the ltype key from the raw "<domain>/rel"
// while the index was written under the normalized "rel", so the lookup
// missed and the update went idle ("does not exist") although the edge was
// right there — and LLAPILinkCreate would have accepted the very same
// prefixed type by normalizing it. Now the resolver normalizes the type
// exactly like the write path (GetObjectIDWithoutDomain) and finds the edge.
func (s *LinkUpdateResolverBugTestSuite) Test_Bug2_ByTypeTo_DomainPrefixedType_MustResolve() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "e1", "rel")))

	prefixedType := s.SetThisDomainPreffix("rel")
	r := s.updateLink("a", "b", "", prefixedType, false, easyjson.NewJSONObjectWithKeyValue("upd", easyjson.NewJSON(true)))
	s.Equal("ok", status(r),
		"update by to+type must resolve a domain-prefixed type the same way create normalizes it: %s",
		r.GetByPath("details").AsStringDefault(""))
	s.True(s.linkBody("a", "e1").GetByPath("upd").AsBoolDefault(false),
		"the existing e1 body must carry the update")
}

// Upsert flavour of the same miss: the resolver used to miss on the prefixed
// type, upsert fell into link.create, and create failed ("name is not
// defined" — the to+type addressing form has no name to create with).
func (s *LinkUpdateResolverBugTestSuite) Test_Bug2_ByTypeTo_DomainPrefixedType_UpsertMustUpdate() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "e1", "rel")))

	prefixedType := s.SetThisDomainPreffix("rel")
	r := s.updateLink("a", "b", "", prefixedType, true, easyjson.NewJSONObjectWithKeyValue("upd", easyjson.NewJSON(true)))
	s.Equal("ok", status(r),
		"upsert by to+prefixed-type must update the existing edge, not fall into a name-less create: %s",
		r.GetByPath("details").AsStringDefault(""))
	s.True(s.linkBody("a", "e1").GetByPath("upd").AsBoolDefault(false))
}

// Control: the same update with the plain type resolves fine — the prefix
// alone flipped the outcome before the fix.
func (s *LinkUpdateResolverBugTestSuite) Test_Bug2_Control_ByTypeTo_PlainType_Works() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "e1", "rel")))

	r := s.updateLink("a", "b", "", "rel", false, easyjson.NewJSONObjectWithKeyValue("upd", easyjson.NewJSON(true)))
	s.Equal("ok", status(r))
	s.True(s.linkBody("a", "e1").GetByPath("upd").AsBoolDefault(false))
}

// --- contract pin: the resolver is shared by delete/read too ----------------

// The fallback is a property of the shared resolver, so it deliberately
// extends to LLAPILinkDelete (and Read): a payload whose "name" misses but
// whose type+to identify an existing edge resolves to THAT edge and deletes
// it, instead of going idle. The docstrings already treat name and type+to as
// two equivalent addressing forms; this pins that the outcome no longer
// depends on which form happens to hit first.
func (s *LinkUpdateResolverBugTestSuite) Test_Contract_DeleteByStaleName_ResolvesViaTypeTo() {
	s.setup()
	s.createVertex("a")
	s.createVertex("b")

	s.Equal("ok", status(s.createLink("a", "b", "foo", "rel")))

	p := easyjson.NewJSONObject()
	p.SetByPath("to", easyjson.NewJSON("b"))
	p.SetByPath("name", easyjson.NewJSON("bar")) // stale/mismatched name
	p.SetByPath("type", easyjson.NewJSON("rel"))
	r, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", "a", &p, nil)
	s.NoError(err)
	s.Equal("ok", status(r),
		"delete addressing the existing (rel, a->b) edge by type+to must resolve it despite the stale name")

	s.False(s.outLinkExists("a", "foo"), "the resolved edge must be gone")
	s.Equal("", s.linkNameForTypeTo("a", "rel", "b"), "the ltype index entry must be gone")
}

// Control: LLAPILinkCreate NORMALIZES a domain-prefixed type before writing
// the ltype index — the write path and the resolver disagreeing on
// normalization was the root of Bug 2.
func (s *LinkUpdateResolverBugTestSuite) Test_Bug2_Control_Create_NormalizesPrefixedType() {
	s.setup()
	s.createVertex("a")
	s.createVertex("c")

	prefixedType := s.SetThisDomainPreffix("rel2")
	s.Equal("ok", status(s.createLink("a", "c", "e2", prefixedType)))

	s.Equal("e2", s.linkNameForTypeTo("a", "rel2", "c"),
		"create must have stored the ltype index under the normalized type")
}
