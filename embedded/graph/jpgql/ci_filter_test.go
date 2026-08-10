package jpgql

// Tests for the "string-ci" value_type modifier in has-filters: string
// comparisons become case-insensitive (both sides lower-cased) for ALL four
// operations, in the scalar and the array paths, and straight through the
// filter grammar. Plain "string" stays case-sensitive byte-for-byte.
//
// The seed mirrors the motivating osm-app case: two hosts whose hostnames
// differ only by case.
//
// NOTE: TestMain (Prometrics bootstrap) lives in body_filter_test.go in this
// same package — do not redeclare it here.

import (
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type CIFilterTestSuite struct {
	test.StatefunTestSuite
}

func TestCIFilterTestSuite(t *testing.T) {
	suite.Run(t, new(CIFilterTestSuite))
}

// build seeds root --e1--> h1, root --e2--> h2 with case-differing hostnames
// and returns the cache plus the domain-prefixed ids.
func (s *CIFilterTestSuite) build() (c *cache.Store, root, h1, h2 string) {
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	s.RegisterFunction("functions.graph.api.vertex.create", crud.LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.create", crud.LLAPILinkCreate, cfg)
	s.NoError(s.StartRuntime())

	mkVertex := func(id string, body easyjson.JSON) {
		p := easyjson.NewJSONObjectWithKeyValue("body", body)
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &p, nil)
		s.NoError(err)
		s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
	}
	h1Body := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("adb-master.corp.local"))
	h1Body.SetByPath("labels", easyjson.NewJSON([]string{"Hot", "SSD"}))
	h2Body := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("ADB-Master.Corp.Local"))
	mkVertex("root", *easyjson.NewJSONObject().GetPtr())
	mkVertex("strop-h1", h1Body)
	mkVertex("strop-h2", h2Body)

	mkLink := func(to, name string) {
		lp := easyjson.NewJSONObject()
		lp.SetByPath("to", easyjson.NewJSON(to))
		lp.SetByPath("name", easyjson.NewJSON(name))
		lp.SetByPath("type", easyjson.NewJSON("__object"))
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", "root", &lp, nil)
		s.NoError(err)
		s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
	}
	mkLink("strop-h1", "e1")
	mkLink("strop-h2", "e2")

	dom := func(id string) string { return s.Runtime().Domain.CreateObjectIDWithThisDomain(id, true) }
	return s.Runtime().Domain.Cache(), dom("root"), dom("strop-h1"), dom("strop-h2")
}

func (s *CIFilterTestSuite) Test_StringCI_Scalar() {
	c, _, h1, h2 := s.build()

	// Regression: plain "string" stays case-sensitive.
	s.True(IsVertexBodyHasIndexValue(c, h1, "hostname", "string", "==", "adb-master.corp.local"))
	s.False(IsVertexBodyHasIndexValue(c, h2, "hostname", "string", "==", "adb-master.corp.local"))
	s.True(IsVertexBodyHasIndexValue(c, h1, "hostname", "string", ">", "adb-master."))
	s.False(IsVertexBodyHasIndexValue(c, h2, "hostname", "string", ">", "adb-master."))

	// "==" ci: both case variants match.
	s.True(IsVertexBodyHasIndexValue(c, h1, "hostname", "string-ci", "==", "adb-master.corp.local"))
	s.True(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", "==", "adb-master.corp.local"))
	s.True(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", "==", "ADB-MASTER.CORP.LOCAL"))
	s.False(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", "==", "other-host"))

	// ">" ci (target is a substring of the field value).
	s.True(IsVertexBodyHasIndexValue(c, h1, "hostname", "string-ci", ">", "adb-master."))
	s.True(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", ">", "adb-master."))
	s.True(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", ">", "CORP.local"))
	s.False(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", ">", "nomatch"))

	// "<" ci (the field value is a substring of the target).
	s.True(IsVertexBodyHasIndexValue(c, h1, "hostname", "string-ci", "<", "THE ADB-MASTER.CORP.LOCAL HOST"))
	s.True(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", "<", "the adb-master.corp.local host"))
	s.False(IsVertexBodyHasIndexValue(c, h1, "hostname", "string", "<", "THE ADB-MASTER.CORP.LOCAL HOST"))

	// "!=" ci: a case variant of the same value is NOT "not equal".
	s.False(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", "!=", "adb-master.corp.local"))
	s.True(IsVertexBodyHasIndexValue(c, h2, "hostname", "string", "!=", "adb-master.corp.local"))
	s.True(IsVertexBodyHasIndexValue(c, h2, "hostname", "string-ci", "!=", "other-host"))

	// Short type spellings keep working; the suffix is recognized on them too.
	s.True(IsVertexBodyHasIndexValue(c, h2, "hostname", "s-ci", "==", "adb-master.corp.local"))

	// The suffix is meaningless for non-string types and is ignored there.
	s.False(IsVertexBodyHasIndexValue(c, h1, "hostname", "number-ci", "==", "1"))

	// Empty value_type matches nothing (and must not panic).
	s.False(IsVertexBodyHasIndexValue(c, h1, "hostname", "", "==", "adb-master.corp.local"))
}

func (s *CIFilterTestSuite) Test_StringCI_Array() {
	c, _, h1, _ := s.build()

	// Regression: plain "string" array membership stays case-sensitive.
	s.True(IsVertexBodyHasArrayValue(c, h1, "labels", "string", "==", "Hot"))
	s.False(IsVertexBodyHasArrayValue(c, h1, "labels", "string", "==", "hot"))

	s.True(IsVertexBodyHasArrayValue(c, h1, "labels", "string-ci", "==", "hot"))
	s.True(IsVertexBodyHasArrayValue(c, h1, "labels", "string-ci", "==", "ssd"))
	s.True(IsVertexBodyHasArrayValue(c, h1, "labels", "string-ci", ">", "SS"))
	s.False(IsVertexBodyHasArrayValue(c, h1, "labels", "string-ci", "==", "cold"))
}

// Test_StringCI_ThroughFilterGrammar drives the exact query shape from the
// osm-app request through ParseFilter and the fan-out evaluation, proving the
// "string-ci" literal survives the grammar (quote handling, colon rewriting).
func (s *CIFilterTestSuite) Test_StringCI_ThroughFilterGrammar() {
	c, root, h1, h2 := s.build()

	// The returned map's KEYS are the matched targets (the bool value is an
	// internal traversal flag, not "matched") — assert key presence.
	targets := func(filter string) map[string]bool {
		return GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(c, root, "*", filter)
	}

	// Case-sensitive today: only h1.
	got := targets("v:has('hostname','string','==','adb-master.corp.local')")
	s.Contains(got, h1)
	s.NotContains(got, h2)

	// ci equality: both.
	got = targets("v:has('hostname','string-ci','==','adb-master.corp.local')")
	s.Contains(got, h1)
	s.Contains(got, h2)

	// ci substring (">"): both.
	got = targets("v:has('hostname','string-ci','>','adb-master.')")
	s.Contains(got, h1)
	s.Contains(got, h2)

	// Disjunction with a ci feature keeps working.
	got = targets("v:has('hostname','string-ci','==','no-such-host') || v:has('hostname','string-ci','>','corp.LOCAL')")
	s.Contains(got, h1)
	s.Contains(got, h2)
}
