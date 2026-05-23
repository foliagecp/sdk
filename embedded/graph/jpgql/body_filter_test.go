package jpgql

// End-to-end tests for body-value filtering after the per-vertex body index
// was removed. These exercise the index-free path: IsVertexBodyHasIndexValue /
// IsLinkBodyHasIndexValue / *HasArrayValue now read the field straight from
// the cached body via cache.GetValueJSONByPath. They must return the same
// results the indexed path used to.

import (
	"os"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

func TestMain(m *testing.M) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "127.0.0.1:0")
	}
	os.Exit(m.Run())
}

type BodyFilterTestSuite struct {
	test.StatefunTestSuite
}

func TestBodyFilterTestSuite(t *testing.T) {
	suite.Run(t, new(BodyFilterTestSuite))
}

func (s *BodyFilterTestSuite) registerLLFns() {
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	s.RegisterFunction("functions.graph.api.vertex.create", crud.LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.create", crud.LLAPILinkCreate, cfg)
	s.NoError(s.StartRuntime())
}

func (s *BodyFilterTestSuite) Test_VertexBodyFilter_IndexFree() {
	s.registerLLFns()

	create := easyjson.NewJSONObject()
	create.SetByPath("body.cpu", easyjson.NewJSON(16))
	create.SetByPath("body.role", easyjson.NewJSON("worker"))
	create.SetByPath("body.up", easyjson.NewJSON(true))
	create.SetByPath("body.tags", easyjson.NewJSON([]string{"hot", "ssd"}))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", "v1", &create, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	c := s.Runtime().Domain.Cache()
	vid := s.Runtime().Domain.CreateObjectIDWithThisDomain("v1", true)

	// numeric scalar
	s.True(IsVertexBodyHasIndexValue(c, vid, "cpu", "number", "==", "16"))
	s.True(IsVertexBodyHasIndexValue(c, vid, "cpu", "number", ">", "10"))
	s.False(IsVertexBodyHasIndexValue(c, vid, "cpu", "number", "<", "10"))
	s.True(IsVertexBodyHasIndexValue(c, vid, "cpu", "number", "!=", "8"))

	// string scalar
	s.True(IsVertexBodyHasIndexValue(c, vid, "role", "string", "==", "worker"))
	s.False(IsVertexBodyHasIndexValue(c, vid, "role", "string", "==", "master"))
	s.True(IsVertexBodyHasIndexValue(c, vid, "role", "string", "!=", "master"))

	// bool scalar
	s.True(IsVertexBodyHasIndexValue(c, vid, "up", "bool", "==", "true"))
	s.False(IsVertexBodyHasIndexValue(c, vid, "up", "bool", "==", "false"))

	// absent field → false (not an error/panic)
	s.False(IsVertexBodyHasIndexValue(c, vid, "missing", "number", "==", "1"))

	// non-existent vertex → false
	missVid := s.Runtime().Domain.CreateObjectIDWithThisDomain("nope", true)
	s.False(IsVertexBodyHasIndexValue(c, missVid, "cpu", "number", "==", "16"))

	// array membership
	s.True(IsVertexBodyHasArrayValue(c, vid, "tags", "string", "==", "hot"))
	s.True(IsVertexBodyHasArrayValue(c, vid, "tags", "string", "==", "ssd"))
	s.False(IsVertexBodyHasArrayValue(c, vid, "tags", "string", "==", "cold"))
}

func (s *BodyFilterTestSuite) Test_LinkBodyFilter_IndexFree() {
	s.registerLLFns()

	// Two vertices.
	for _, id := range []string{"a", "b"} {
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, easyjson.NewJSONObject().GetPtr(), nil)
		s.NoError(err)
		s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
	}

	// Link a→b with a body.
	lp := easyjson.NewJSONObject()
	lp.SetByPath("to", easyjson.NewJSON("b"))
	lp.SetByPath("name", easyjson.NewJSON("edge"))
	lp.SetByPath("type", easyjson.NewJSON("__object"))
	lp.SetByPath("body.weight", easyjson.NewJSON(5))
	lp.SetByPath("body.kind", easyjson.NewJSON("cable"))
	lp.SetByPath("body.labels", easyjson.NewJSON([]string{"primary"}))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", "a", &lp, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	c := s.Runtime().Domain.Cache()
	fromId := s.Runtime().Domain.CreateObjectIDWithThisDomain("a", true)

	s.True(IsLinkBodyHasIndexValue(c, fromId, "edge", "weight", "number", "==", "5"))
	s.True(IsLinkBodyHasIndexValue(c, fromId, "edge", "weight", "number", ">", "1"))
	s.True(IsLinkBodyHasIndexValue(c, fromId, "edge", "kind", "string", "==", "cable"))
	s.False(IsLinkBodyHasIndexValue(c, fromId, "edge", "kind", "string", "==", "fiber"))
	s.True(IsLinkBodyHasArrayValue(c, fromId, "edge", "labels", "string", "==", "primary"))
	s.False(IsLinkBodyHasArrayValue(c, fromId, "edge", "labels", "string", "==", "backup"))
}
