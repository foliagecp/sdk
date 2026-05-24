package jpgql

// Regression tests for the parse-once optimization in
// GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery and the refactored
// IsLinkSatifiesFilterCreteria. The optimized fan-out (parse the link filter
// once, skip the per-link index scan when the filter does not need it) must
// return EXACTLY the same target set as evaluating the per-link wrapper for
// every filter, including the empty-filter and unknown-index cases.
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

type FanoutFilterTestSuite struct {
	test.StatefunTestSuite
}

func TestFanoutFilterTestSuite(t *testing.T) {
	suite.Run(t, new(FanoutFilterTestSuite))
}

func (s *FanoutFilterTestSuite) build() (*cache.Store, string) {
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	s.RegisterFunction("functions.graph.api.vertex.create", crud.LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.create", crud.LLAPILinkCreate, cfg)
	s.NoError(s.StartRuntime())

	for _, id := range []string{"a", "b", "c"} {
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, easyjson.NewJSONObject().GetPtr(), nil)
		s.NoError(err)
		s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
	}
	mk := func(to, name, ltype string) {
		lp := easyjson.NewJSONObject()
		lp.SetByPath("to", easyjson.NewJSON(to))
		lp.SetByPath("name", easyjson.NewJSON(name))
		lp.SetByPath("type", easyjson.NewJSON(ltype))
		lp.SetByPath("body.w", easyjson.NewJSON(1))
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", "a", &lp, nil)
		s.NoError(err)
		s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
	}
	mk("b", "e1", "__object")
	mk("c", "e2", "rel")

	c := s.Runtime().Domain.Cache()
	from := s.Runtime().Domain.CreateObjectIDWithThisDomain("a", true)
	return c, from
}

// perLinkReference reproduces the pre-optimization behavior: enumerate links and
// evaluate the per-link wrapper IsLinkSatifiesFilterCreteria for each.
func (s *FanoutFilterTestSuite) perLinkReference(c *cache.Store, from, linkName, filter string) map[string]bool {
	out := map[string]bool{}
	for _, ln := range GetLinkNamesFromJPGQLLinkName(c, from, linkName) {
		tid := GetTargetIdFromSourceIdAndLinkName(c, from, ln)
		if IsLinkSatifiesFilterCreteria(c, from, tid, ln, filter) && len(tid) > 0 {
			out[tid] = false
		}
	}
	return out
}

func (s *FanoutFilterTestSuite) Test_ParseOnceFanout_MatchesPerLink() {
	c, from := s.build()

	filters := []string{
		"",                      // no filter → every link passes
		"l:type('__object')",    // index path → e1 only
		"l:type('rel')",         // index path → e2 only
		"l:type('nonexistent')", // index path → none
	}
	for _, f := range filters {
		got := GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(c, from, "*", f)
		want := s.perLinkReference(c, from, "*", f)
		s.Equalf(want, got, "parse-once fan-out must equal per-link evaluation for filter %q", f)
	}

	// Concrete cardinalities (independent of exact target-id formatting).
	s.Len(GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(c, from, "*", ""), 2)
	s.Len(GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(c, from, "*", "l:type('__object')"), 1)
	s.Len(GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(c, from, "*", "l:type('rel')"), 1)
	s.Len(GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(c, from, "*", "l:type('nonexistent')"), 0)

	// Literal (wildcard-free) link name resolves to a single edge target.
	s.Len(GetObjectIDsFromJPGQLLinkNameAndLinkFilterQuery(c, from, "e1", ""), 1)
}
