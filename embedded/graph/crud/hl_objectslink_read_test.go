package crud

// Regression tests for ReadObjectsLink (functions.cmdb.api.objects.link.read).
//
// Bug: ReadObjectsLink resolved the link TYPE from the type-schema
// (getObjectsLinkTypeFromTypesLink → the base "object_type" of the TypesLink)
// and then asked the LL read to find the link NAME via the per-edge index
// "<from>.ltype.<type>.<toId>". When the schema-derived type did not match the
// type the edge was actually stored under, that index lookup missed and the LL
// read replied "link from=… with name= does not exist" — an empty name.
//
// The deterministic trigger is a SuperType/composition edge: it is stored under
// a COMPOUND type "<fromClaim>#<toClaim>#<rel>", but the schema yields only the
// base "<rel>", so the lookup always missed.
//
// Fix: ReadObjectsLink now resolves the real (linkName, linkType) straight from
// the KV index (resolveLinkBetweenTwoObjects) and reads the link BY NAME.

import (
	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

// Test_ObjectsLinkRead_PlainLink_ReturnsBody is the happy path: a normal CMDB
// object link must read back with a non-empty name and its body.
func (s *CrudAtomicityTestSuite) Test_ObjectsLinkRead_PlainLink_ReturnsBody() {
	s.bootstrap()

	s.cmdbTypeCreate("OLRPlainA")
	s.cmdbTypeCreate("OLRPlainB")
	s.cmdbTypesLink("OLRPlainA", "OLRPlainB", "olr-plain-rel")
	s.Equal("ok", s.cmdbObjectCreate("olr-pa", "OLRPlainA").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("olr-pb", "OLRPlainB").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectsLinkCreate("olr-pa", "olr-pb", "olr-pedge").GetByPath("status").AsStringDefault(""))

	payload := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("olr-pb"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.read", "olr-pa", &payload, nil)
	s.NoError(err)
	s.Equalf("ok", res.GetByPath("status").AsStringDefault(""),
		"ObjectsLinkRead must succeed for a plain link, got: %s", res.ToString())
	s.NotEmptyf(res.GetByPath("data.name").AsStringDefault(""),
		"link name must not be empty, got: %s", res.ToString())
	s.Truef(res.GetByPath("data.body").IsObject(), "link body must be returned, got: %s", res.ToString())
}

// Test_ObjectsLinkRead_SuperTypeEdge_NotReadByBaseRead pins the intended split:
// ReadObjectsLink reads ONLY the base-type edge. A SuperType edge is stored
// under a compound "<fromClaim>#<toClaim>#<rel>" type, NOT the base "<rel>", so
// when only a supertype edge exists the base read must FAIL with a clear error
// (not the old empty-name "with name= does not exist") and must NOT silently
// pick the compound edge.
func (s *CrudAtomicityTestSuite) Test_ObjectsLinkRead_SuperTypeEdge_NotReadByBaseRead() {
	s.bootstrap()

	s.cmdbTypeCreate("OLRSuperA")
	s.cmdbTypeCreate("OLRSuperB")
	s.cmdbTypesLink("OLRSuperA", "OLRSuperB", "olr-super-rel")
	s.Equal("ok", s.cmdbObjectCreate("olr-sa", "OLRSuperA").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("olr-sb", "OLRSuperB").GetByPath("status").AsStringDefault(""))

	createRes := s.cmdbObjectsLinkSuperTypeCreate("olr-sa", "olr-sb", "olr-sedge", "OLRSuperA", "OLRSuperB")
	s.Equalf("ok", createRes.GetByPath("status").AsStringDefault(""),
		"supertype link create must succeed: %s", createRes.ToString())

	// Base read: no base-type edge exists (only the compound supertype one).
	payload := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("olr-sb"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.read", "olr-sa", &payload, nil)
	s.NoError(err)
	status := res.GetByPath("status").AsStringDefault("")
	s.Equalf("failed", status,
		"base ObjectsLinkRead must fail for a supertype-only edge, got: %s", res.ToString())
	s.NotContainsf(res.GetByPath("details").AsStringDefault(""), "with name=",
		"error message must not contain an empty name, got: %s", res.ToString())
}

// Test_ObjectsLinkSuperTypeRead_ReturnsBody verifies the dedicated supertype
// read resolves the compound-typed edge by its claim pair and returns its
// name+body.
func (s *CrudAtomicityTestSuite) Test_ObjectsLinkSuperTypeRead_ReturnsBody() {
	s.bootstrap()

	s.cmdbTypeCreate("OLRSuperRA")
	s.cmdbTypeCreate("OLRSuperRB")
	s.cmdbTypesLink("OLRSuperRA", "OLRSuperRB", "olr-superr-rel")
	s.Equal("ok", s.cmdbObjectCreate("olr-sra", "OLRSuperRA").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("olr-srb", "OLRSuperRB").GetByPath("status").AsStringDefault(""))

	createRes := s.cmdbObjectsLinkSuperTypeCreate("olr-sra", "olr-srb", "olr-sredge", "OLRSuperRA", "OLRSuperRB")
	s.Equalf("ok", createRes.GetByPath("status").AsStringDefault(""),
		"supertype link create must succeed: %s", createRes.ToString())

	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON("olr-srb"))
	payload.SetByPath("from_super_type", easyjson.NewJSON("OLRSuperRA"))
	payload.SetByPath("to_super_type", easyjson.NewJSON("OLRSuperRB"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.supertype.read", "olr-sra", &payload, nil)
	s.NoError(err)
	s.Equalf("ok", res.GetByPath("status").AsStringDefault(""),
		"supertype read must resolve the compound-typed edge, got: %s", res.ToString())
	s.NotEmptyf(res.GetByPath("data.name").AsStringDefault(""),
		"link name must not be empty, got: %s", res.ToString())
	s.Truef(res.GetByPath("data.body").IsObject(), "link body must be returned, got: %s", res.ToString())
}

// Test_ObjectsLinkRead_NoEdge_ReturnsError verifies that an absent base-type
// link returns a clear error (not the old empty-name "with name= does not
// exist").
func (s *CrudAtomicityTestSuite) Test_ObjectsLinkRead_NoEdge_ReturnsError() {
	s.bootstrap()

	s.cmdbTypeCreate("OLRNoneA")
	s.cmdbTypeCreate("OLRNoneB")
	s.cmdbTypesLink("OLRNoneA", "OLRNoneB", "olr-none-rel")
	s.Equal("ok", s.cmdbObjectCreate("olr-na", "OLRNoneA").GetByPath("status").AsStringDefault(""))
	s.Equal("ok", s.cmdbObjectCreate("olr-nb", "OLRNoneB").GetByPath("status").AsStringDefault(""))
	// No link created between them.

	payload := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("olr-nb"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.read", "olr-na", &payload, nil)
	s.NoError(err)
	s.Equalf("failed", res.GetByPath("status").AsStringDefault(""),
		"reading an absent link must fail, got: %s", res.ToString())
	s.NotContainsf(res.GetByPath("details").AsStringDefault(""), "with name=",
		"error message must not contain an empty name, got: %s", res.ToString())
}
