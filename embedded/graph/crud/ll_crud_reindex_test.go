package crud

// Tests for the incremental body-value reindex (reindexVertexBody /
// reindexVertexLinkBody). The secondary index MUST end up identical to what
// a full teardown+rebuild would produce — JPGQL index lookups fall back to
// the body only when a key is MISSING, so a STALE key (not deleted on field
// removal or type change) would silently return wrong query results.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type ReindexTestSuite struct {
	test.StatefunTestSuite
}

func TestReindexTestSuite(t *testing.T) {
	suite.Run(t, new(ReindexTestSuite))
}

func (s *ReindexTestSuite) registerVertexFns() {
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	s.RegisterFunction("functions.graph.api.vertex.create", LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.vertex.update", LLAPIVertexUpdate, cfg)
	s.NoError(s.StartRuntime())
}

// vertexIndex snapshots the body-value index of a vertex as a map of
// "<typeToken>.<bodyKey>" → raw stored bytes (as string).
func (s *ReindexTestSuite) vertexIndex(id string) map[string]string {
	c := s.Runtime().Domain.Cache()
	vid := s.Runtime().Domain.CreateObjectIDWithThisDomain(id, true)
	keys := c.GetKeysByPattern(fmt.Sprintf(VertexBodyValueIndexPrefPattern+KeySuff1Pattern, vid, ">"))
	out := map[string]string{}
	for _, k := range keys {
		toks := strings.Split(k, ".")
		typeStr := toks[len(toks)-2]
		bodyKey := toks[len(toks)-1]
		v, err := c.GetValue(k)
		s.NoError(err)
		out[typeStr+"."+bodyKey] = string(v)
	}
	return out
}

func (s *ReindexTestSuite) requireStatus(reply *easyjson.JSON, want string) {
	s.Equalf(want, reply.GetByPath("status").AsStringDefault(""),
		"expected status=%q, got: %s", want, reply.ToString())
}

// expectedIndex builds the index a full rebuild would produce for a body.
func expectedIndex(fields map[string]easyjson.JSON) map[string]string {
	out := map[string]string{}
	for k, v := range fields {
		typeStr, bytesVal, ok := indexableScalar(v)
		if !ok {
			continue
		}
		out[typeStr+"."+k] = string(bytesVal)
	}
	return out
}

// Test_Reindex_FullLifecycle exercises add / change-value / change-type /
// remove / non-scalar in a single update and asserts the resulting index
// exactly matches a from-scratch projection of the final body.
func (s *ReindexTestSuite) Test_Reindex_FullLifecycle() {
	s.registerVertexFns()

	id := "rv1"

	// Initial body: numeric, string, bool, empty-string (not indexed),
	// nested object (not indexed).
	create := easyjson.NewJSONObject()
	create.SetByPath("body.cpu", easyjson.NewJSON(16))      // n.cpu
	create.SetByPath("body.role", easyjson.NewJSON("worker")) // s.role
	create.SetByPath("body.up", easyjson.NewJSON(true))       // b.up
	create.SetByPath("body.note", easyjson.NewJSON(""))       // empty string → NOT indexed
	create.SetByPath("body.spec.gpu", easyjson.NewJSON(2))    // nested → NOT indexed
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &create, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	got := s.vertexIndex(id)
	want := expectedIndex(map[string]easyjson.JSON{
		"cpu":  easyjson.NewJSON(16),
		"role": easyjson.NewJSON("worker"),
		"up":   easyjson.NewJSON(true),
	})
	s.Equal(want, got, "index after create")

	// Update (merge): change cpu value, change role TYPE (string→number),
	// remove up (set via replace below won't work in merge; use replace),
	// add mem. Use replace=true to drop "up" and "note", keep explicit set.
	upd := easyjson.NewJSONObject()
	upd.SetByPath("replace", easyjson.NewJSON(true))
	upd.SetByPath("body.cpu", easyjson.NewJSON(32))        // n.cpu changed value
	upd.SetByPath("body.role", easyjson.NewJSON(7))        // role: s → n (type change)
	upd.SetByPath("body.mem", easyjson.NewJSON(64))        // new n.mem
	upd.SetByPath("body.spec.gpu", easyjson.NewJSON(4))    // still nested → not indexed
	// "up" and "note" dropped by replace.
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &upd, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	got = s.vertexIndex(id)
	want = expectedIndex(map[string]easyjson.JSON{
		"cpu":  easyjson.NewJSON(32),
		"role": easyjson.NewJSON(7),
		"mem":  easyjson.NewJSON(64),
	})
	s.Equal(want, got, "index after replace update")

	// Specifically assert NO stale keys remain for the type change / removal.
	s.NotContains(got, "s.role", "stale string index for role must be gone after type change")
	s.NotContains(got, "b.up", "index for removed field 'up' must be gone")
	s.Contains(got, "n.role", "role must now be indexed as numeric")
}

// Test_Reindex_MergeChangeOneField verifies a merge update that changes a
// single field leaves the rest of the index intact and updates only that key.
func (s *ReindexTestSuite) Test_Reindex_MergeChangeOneField() {
	s.registerVertexFns()

	id := "rv2"

	create := easyjson.NewJSONObject()
	create.SetByPath("body.a", easyjson.NewJSON(1))
	create.SetByPath("body.b", easyjson.NewJSON("x"))
	create.SetByPath("body.c", easyjson.NewJSON(true))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &create, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	// Merge update changing only "a".
	upd := easyjson.NewJSONObject()
	upd.SetByPath("body.a", easyjson.NewJSON(99))
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &upd, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	got := s.vertexIndex(id)
	want := expectedIndex(map[string]easyjson.JSON{
		"a": easyjson.NewJSON(99),
		"b": easyjson.NewJSON("x"),
		"c": easyjson.NewJSON(true),
	})
	s.Equal(want, got, "merge changing one field must keep others and update the one")
}

// Test_Reindex_StringToEmptyRemovesIndex verifies that updating a string
// field to empty (which is not indexable) removes its index key.
func (s *ReindexTestSuite) Test_Reindex_StringToEmptyRemovesIndex() {
	s.registerVertexFns()

	id := "rv3"

	create := easyjson.NewJSONObject()
	create.SetByPath("body.label", easyjson.NewJSON("hot"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &create, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")
	s.Contains(s.vertexIndex(id), "s.label")

	upd := easyjson.NewJSONObject()
	upd.SetByPath("body.label", easyjson.NewJSON("")) // empty → not indexable
	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", id, &upd, nil)
	s.NoError(err)
	s.requireStatus(res, "ok")

	got := s.vertexIndex(id)
	s.NotContains(got, "s.label", "empty string must drop the index key")
	s.Empty(got, "no index keys expected when the only field became empty")
}

// Test_indexableScalar_Matrix is a small unit check of the scalar predicate
// that both create and incremental paths share.
func Test_indexableScalar_Matrix(t *testing.T) {
	cases := []struct {
		name    string
		val     easyjson.JSON
		wantTok string
		wantOK  bool
	}{
		{"int", easyjson.NewJSON(5), "n", true},
		{"float", easyjson.NewJSON(3.14), "n", true},
		{"bool_true", easyjson.NewJSON(true), "b", true},
		{"bool_false", easyjson.NewJSON(false), "b", true},
		{"string", easyjson.NewJSON("x"), "s", true},
		{"empty_string", easyjson.NewJSON(""), "", false},
		{"object", easyjson.NewJSONObjectWithKeyValue("k", easyjson.NewJSON(1)), "", false},
		{"array", easyjson.NewJSON([]string{"a"}), "", false},
		{"null", easyjson.NewJSONNull(), "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tok, b, ok := indexableScalar(c.val)
			if ok != c.wantOK || tok != c.wantTok {
				t.Fatalf("indexableScalar(%s) = (%q,%v), want (%q,%v)", c.name, tok, ok, c.wantTok, c.wantOK)
			}
			if c.wantOK && len(b) == 0 {
				t.Fatalf("indexableScalar(%s) returned empty bytes for an indexable value", c.name)
			}
		})
	}
}
