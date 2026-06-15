package crud_test

// Every CRUD operation — high-level and low-level — applied to a NON-EXISTENT
// target (or a target with a missing dependency) must reply gracefully, never
// panic the worker (a panic sends no reply, so the client just times out).
//
// This generalises the ReadType nil-deref regression
// (RecalculateInheritanceCacheForTypeAtSelfIDIfNeeded dereferenced a nil body
// for a missing type) across the whole surface — vertex, link, object, type,
// types-link, objects-link and supertype objects-link, for create / update /
// delete / read — to close the class once and for all.
//
// Each operation runs against a realistic, initialised schema (global
// type-model version set; inheritance / link-type resolution paths real) and is
// asserted to REPLY within a bounded window. A panicking handler never
// completes, so the case fails loudly (and, using a non-fatal assert, every
// failing case is reported in a single run rather than only the first).

import (
	"strings"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type NonExistentTargetTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestNonExistentTargetTestSuite(t *testing.T) {
	suite.Run(t, new(NonExistentTargetTestSuite))
}

func (s *NonExistentTargetTestSuite) swallowExists(err error) {
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		s.NoError(err)
	}
}

// completes runs op and reports a (non-fatal) failure if it does not return
// within 8s — a handler that panics replies to nobody, so the in-process request
// never completes. Non-fatal so a single run surfaces every broken op.
func (s *NonExistentTargetTestSuite) completes(name string, op func()) {
	done := make(chan struct{})
	go func() { defer close(done); op() }()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		s.Failf("operation on a non-existent target did not reply", "%s: the handler likely panicked (no reply within 8s)", name)
	}
}

func (s *NonExistentTargetTestSuite) Test_AllCrudOps_OnNonExistentTargets_ReplyGracefully() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_TYPES); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc

	// Realistic, initialised schema: concrete c_src/c_dst, claim i_src/i_dst,
	// IS-A edges, a type-level link per pair, one root (c_src) linked to a real
	// target (c_dst). Sets the global type-model version (the condition that made
	// ReadType panic) and gives inheritance / link-type resolution real endpoints.
	for _, tp := range []string{"c_src", "c_dst", "i_src", "i_dst"} {
		s.NoError(dbc.CMDB.TypeUpdate(tp, easyjson.NewJSONObject(), false, true))
	}
	s.swallowExists(dbc.CMDB.TypeSetSubType("i_src", "c_src"))
	s.swallowExists(dbc.CMDB.TypeSetSubType("i_dst", "c_dst"))
	s.NoError(dbc.CMDB.TypesLinkUpdate("c_src", "c_dst", nil, easyjson.NewJSONObject(), false, "owns"))
	s.NoError(dbc.CMDB.TypesLinkUpdate("i_src", "i_dst", nil, easyjson.NewJSONObject(), false, "claims"))
	s.NoError(dbc.CMDB.ObjectUpdate("root", easyjson.NewJSONObject(), true, "c_src"))
	s.NoError(dbc.CMDB.ObjectUpdate("tgt", easyjson.NewJSONObject(), true, "c_dst"))
	s.NoError(dbc.CMDB.ObjectsLinkUpdate("root", "tgt", nil, easyjson.NewJSONObject(), true, "rel"))

	rt := s.Runtime()
	body := easyjson.NewJSONObject()
	req := func(fn, id string, set func(p *easyjson.JSON)) func() {
		return func() {
			p := easyjson.NewJSONObject()
			if set != nil {
				set(&p)
			}
			_, _ = rt.Request(sfPlugins.AutoRequestSelect, fn, id, &p, nil)
		}
	}

	// ===== LL vertex =====
	s.completes("vertex.read", req("functions.graph.api.vertex.read", "no_v", func(p *easyjson.JSON) { p.SetByPath("details", easyjson.NewJSON(true)) }))
	s.completes("vertex.update", req("functions.graph.api.vertex.update", "no_v", func(p *easyjson.JSON) { p.SetByPath("body", easyjson.NewJSONObject()) }))
	s.completes("vertex.delete", req("functions.graph.api.vertex.delete", "no_v", nil))

	// ===== LL link =====
	s.completes("link.read", req("functions.graph.api.link.read", "root", func(p *easyjson.JSON) { p.SetByPath("name", easyjson.NewJSON("no_link")) }))
	s.completes("link.create(from missing vertex)", req("functions.graph.api.link.create", "no_v1", func(p *easyjson.JSON) {
		p.SetByPath("to", easyjson.NewJSON("no_v2"))
		p.SetByPath("name", easyjson.NewJSON("l"))
		p.SetByPath("type", easyjson.NewJSON("tp"))
		p.SetByPath("body", easyjson.NewJSONObject())
	}))
	s.completes("link.update(missing link)", req("functions.graph.api.link.update", "root", func(p *easyjson.JSON) {
		p.SetByPath("name", easyjson.NewJSON("no_link"))
		p.SetByPath("body", easyjson.NewJSONObject())
	}))
	s.completes("link.delete(missing link)", req("functions.graph.api.link.delete", "root", func(p *easyjson.JSON) { p.SetByPath("name", easyjson.NewJSON("no_link")) }))

	// ===== HL object =====
	s.completes("object.read", func() { _, _ = dbc.CMDB.ObjectRead("no_obj") })
	s.completes("object.create(missing type)", func() { _ = dbc.CMDB.ObjectCreate("orphan_obj", "no_such_type") })
	s.completes("object.update(missing, no upsert)", func() { _ = dbc.CMDB.ObjectUpdate("no_obj", body, false) })
	s.completes("object.delete(missing)", func() { _ = dbc.CMDB.ObjectDelete("no_obj") })

	// ===== HL type =====
	s.completes("type.read", func() { _, _ = dbc.CMDB.TypeRead("no_type") })
	s.completes("type.update(missing, no upsert)", func() { _ = dbc.CMDB.TypeUpdate("no_type", body, false) })
	s.completes("type.delete(missing)", func() { _ = dbc.CMDB.TypeDelete("no_type") })
	s.completes("type.setSubType(missing)", func() { _ = dbc.CMDB.TypeSetSubType("no_base", "no_child") })
	s.completes("type.removeSubType(missing)", func() { _ = dbc.CMDB.TypeRemoveSubType("no_base", "no_child") })

	// ===== HL types-link =====
	s.completes("types-link.read", func() { _, _ = dbc.CMDB.TypesLinkRead("c_src", "no_type") })
	s.completes("types-link.create(missing types)", func() { _ = dbc.CMDB.TypesLinkCreate("no_t1", "no_t2", "rel", nil) })
	s.completes("types-link.update(missing, no upsert)", func() { _ = dbc.CMDB.TypesLinkUpdate("c_src", "no_type", nil, body, false) })
	s.completes("types-link.delete(missing)", func() { _ = dbc.CMDB.TypesLinkDelete("c_src", "no_type") })

	// ===== HL objects-link =====
	s.completes("objects-link.read", func() { _, _ = dbc.CMDB.ObjectsLinkRead("root", "no_obj") })
	s.completes("objects-link.create(missing objects)", func() { _ = dbc.CMDB.ObjectsLinkCreate("no_o1", "no_o2", "rel", nil) })
	s.completes("objects-link.update(missing, no upsert)", func() { _ = dbc.CMDB.ObjectsLinkUpdate("root", "no_obj", nil, body, false) })
	s.completes("objects-link.delete(missing)", func() { _ = dbc.CMDB.ObjectsLinkDelete("root", "no_obj") })

	// ===== HL supertype objects-link =====
	s.completes("supertype.read", req("functions.cmdb.api.objects.link.supertype.read", "root", func(p *easyjson.JSON) {
		p.SetByPath("to", easyjson.NewJSON("no_obj"))
		p.SetByPath("from_super_type", easyjson.NewJSON("i_src"))
		p.SetByPath("to_super_type", easyjson.NewJSON("i_dst"))
	}))
	s.completes("supertype.create(missing to)", func() { _ = dbc.CMDB.ObjectsLinkSuperTypeCreate("root", "no_obj", "i_src", "i_dst", "rel", nil) })
	s.completes("supertype.create(missing claim types)", func() { _ = dbc.CMDB.ObjectsLinkSuperTypeCreate("root", "tgt", "no_c1", "no_c2", "rel", nil) })
	s.completes("supertype.update(missing to)", func() { _ = dbc.CMDB.ObjectsLinkSuperTypeUpdate("root", "no_obj", "i_src", "i_dst", "rel", nil, body, false) })
	s.completes("supertype.delete(missing to)", func() { _ = dbc.CMDB.ObjectsLinkSuperTypeDelete("root", "no_obj", "i_src", "i_dst") })
}
