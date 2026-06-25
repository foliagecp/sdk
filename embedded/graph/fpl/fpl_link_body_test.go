package fpl_test

import (
	"os"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
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

// Verifies the links_in_body / links_out_body post-processor flags: when set, each
// link entry in links.in / links.out carries the link's body; without them vbody
// stays body-only (no link enumeration).
type FPLLinkBodySuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestFPLLinkBodySuite(t *testing.T) { suite.Run(t, new(FPLLinkBodySuite)) }

func (s *FPLLinkBodySuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	fpl.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_TYPES)
	s.waitForVertex(crud.BUILT_IN_OBJECTS)
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb = dbc.CMDB
}

func (s *FPLLinkBodySuite) waitForVertex(id string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear in time", id)
}

// pp calls a post-processor (vbody/obody) for a single uuid and returns arr[0].
func (s *FPLLinkBodySuite) pp(fn, uuid string, data easyjson.JSON) easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("uuids", easyjson.NewJSON([]string{uuid}))
	p.SetByPath("data", data)
	r, err := s.Request(sfPlugins.AutoRequestSelect, fn, "ppcall", &p, nil)
	s.NoError(err)
	s.Equal("ok", r.GetByPath("status").AsStringDefault(""), "%s status", fn)
	arr := r.GetByPath("data.arr")
	s.Require().True(arr.IsArray() && arr.ArraySize() == 1, "%s: arr must hold exactly one element", fn)
	return arr.ArrayElement(0)
}

// linkBodyW finds the entry named `name` in a links.in/out array and returns body.w.
func linkBodyW(arr easyjson.JSON, name string) (float64, bool) {
	if !arr.IsArray() {
		return 0, false
	}
	for i := 0; i < arr.ArraySize(); i++ {
		e := arr.ArrayElement(i)
		if e.GetByPath("name").AsStringDefault("") == name {
			if !e.PathExists("body") {
				return 0, false
			}
			return e.GetByPath("body.w").AsNumericDefault(-1), true
		}
	}
	return 0, false
}

func (s *FPLLinkBodySuite) Test_LinkBodiesAttached() {
	s.boot()

	body := easyjson.NewJSONObjectWithKeyValue("w", easyjson.NewJSON(7))
	s.NoError(s.cmdb.TypeCreate("owner_t"))
	s.NoError(s.cmdb.TypeCreate("item_t"))
	s.NoError(s.cmdb.TypesLinkCreate("owner_t", "item_t", "owns", []string{"rel"}))
	s.NoError(s.cmdb.ObjectCreate("src_obj", "owner_t"))
	s.NoError(s.cmdb.ObjectCreate("dst_obj", "item_t"))
	s.NoError(s.cmdb.ObjectsLinkCreate("src_obj", "dst_obj", "rel1", []string{"rel"}, body))

	src := s.SetThisDomainPreffix("src_obj")
	dst := s.SetThisDomainPreffix("dst_obj")

	withOut := easyjson.NewJSONObjectWithKeyValue("links_out_body", easyjson.NewJSON(true))
	withIn := easyjson.NewJSONObjectWithKeyValue("links_in_body", easyjson.NewJSON(true))

	for _, fn := range []string{"functions.graph.api.query.fpl.pp.vbody", "functions.graph.api.query.fpl.pp.obody"} {
		// out-link body (from src's perspective)
		out := s.pp(fn, src, withOut)
		w, ok := linkBodyW(out.GetByPath("links.out"), "rel1")
		s.True(ok, "%s: links.out must contain rel1 with a body", fn)
		s.Equal(float64(7), w, "%s: out-link body.w", fn)

		// in-link body (from dst's perspective)
		in := s.pp(fn, dst, withIn)
		w, ok = linkBodyW(in.GetByPath("links.in"), "rel1")
		s.True(ok, "%s: links.in must contain rel1 with a body", fn)
		s.Equal(float64(7), w, "%s: in-link body.w", fn)
	}

	// Default (no flags): vbody stays body-only — no link enumeration.
	v := s.pp("functions.graph.api.query.fpl.pp.vbody", src, easyjson.NewJSONObject())
	s.False(v.GetByPath("links.out").IsArray(), "vbody without flags must not enumerate links.out")
	s.True(v.PathExists("body"), "vbody without flags must still return the vertex body")
}
