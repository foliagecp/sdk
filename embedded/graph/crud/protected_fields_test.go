package crud_test

// Functional tests of protected body fields: a writer that does not carry a
// protected field cannot destroy it, even with replace=true (the mode every
// inventory rebuild uses), while a writer that DOES carry it owns what it
// brings — additively, so omitted keys survive.

import (
	"context"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type ProtectedFieldsTestSuite struct {
	test.StatefunTestSuite
	cmdb  db.CMDBSyncClient
	graph db.GraphSyncClient
}

func TestProtectedFieldsTestSuite(t *testing.T) { suite.Run(t, new(ProtectedFieldsTestSuite)) }

func (s *ProtectedFieldsTestSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime(), "usr")
	// The built-in schema (and with it the protected-field list) is prepared by
	// an after-start hook. Hooks run in registration order, so a hook registered
	// AFTER crud's fires once that one is done — a deterministic signal, unlike
	// polling for a vertex the hook creates half-way through its work.
	schemaReady := make(chan struct{})
	s.Runtime().RegisterOnAfterStartFunction(func(context.Context, *statefun.Runtime) error {
		close(schemaReady)
		return nil
	}, false)
	s.NoError(s.StartRuntime())
	select {
	case <-schemaReady:
	case <-time.After(20 * time.Second):
		s.T().Fatal("built-in schema was not prepared in time")
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb, s.graph = dbc.CMDB, dbc.Graph
}

func (s *ProtectedFieldsTestSuite) body(id string) easyjson.JSON {
	res, err := s.cmdb.ObjectRead(id)
	s.Require().NoError(err)
	return res.GetByPath("body")
}

// objectUpdate issues functions.cmdb.api.object.update and returns its status,
// so tests can assert the no-op ("idle") outcome the rebuild path depends on.
func (s *ProtectedFieldsTestSuite) objectUpdate(id string, body easyjson.JSON, replace bool) string {
	p := easyjson.NewJSONObject()
	p.SetByPath("replace", easyjson.NewJSON(replace))
	p.SetByPath("body", body)
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.update", id, &p, nil)
	s.Require().NoError(err)
	return res.GetByPath("status").AsStringDefault("")
}

// usr builds a protected-space value: {attrs:{responsible}, tags:[...]}.
func usr(responsible string, tags ...string) easyjson.JSON {
	u := easyjson.NewJSONObject()
	if responsible != "" {
		u.SetByPath("attrs.responsible", easyjson.NewJSON(responsible))
	}
	u.SetByPath("tags", easyjson.NewJSON(tags))
	return u
}

func bodyWithUsr(hostname, responsible string, tags ...string) easyjson.JSON {
	b := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON(hostname))
	b.SetByPath("usr", usr(responsible, tags...))
	return b
}

// THE core FR-20 guarantee: an inventory rebuild rewrites the whole body with
// replace=true and no knowledge of "usr" — the protected space must survive.
func (s *ProtectedFieldsTestSuite) Test_ReplaceWithoutProtectedField_Survives() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("pf_t"))
	s.NoError(s.cmdb.ObjectCreate("pf1", "pf_t", bodyWithUsr("srv-1", "alice", "prod")))

	// Rebuild: fresh discovery fields, no usr, replace=true.
	s.Equal("ok", s.objectUpdate("pf1", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv-2")), true))

	b := s.body("pf1")
	s.Equal("srv-2", b.GetByPath("hostname").AsStringDefault(""), "discovery fields are the writer's own")
	s.Equal("alice", b.GetByPath("usr.attrs.responsible").AsStringDefault(""), "protected attrs must survive replace=true")
	tags, _ := b.GetByPath("usr.tags").AsArrayString()
	s.Equal([]string{"prod"}, tags, "protected tags must survive replace=true")
}

// The grafting must NOT break the no-op short-circuit: a rebuild re-sending
// unchanged discovery fields (still without usr) has to stay idle — otherwise
// every cycle writes to the KV, floods the WAL and fires triggers.
func (s *ProtectedFieldsTestSuite) Test_UnchangedRebuildStaysNoOp() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("pf_t"))
	s.NoError(s.cmdb.ObjectCreate("pf2", "pf_t", bodyWithUsr("srv", "bob", "dc1")))

	// Same discovery fields as created with, no usr: the grafted body equals the
	// stored one, so even the FIRST such rebuild is already a no-op.
	discovery := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv"))
	s.Equal("idle", s.objectUpdate("pf2", discovery, true), "an unchanged rebuild must be a no-op")
	s.Equal("idle", s.objectUpdate("pf2", discovery, true), "and stay a no-op on every further cycle")

	// A rebuild that really changes a discovery field writes once, then goes
	// quiet again — the protected graft must not make it churn.
	changed := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv-renamed"))
	s.Equal("ok", s.objectUpdate("pf2", changed, true), "a real discovery change must be written")
	s.Equal("idle", s.objectUpdate("pf2", changed, true), "and the next identical cycle is a no-op again")

	s.Equal("bob", s.body("pf2").GetByPath("usr.attrs.responsible").AsStringDefault(""), "usr still intact after repeated rebuilds")
}

// A writer that DOES carry the protected field owns the write: the value is
// stored exactly as sent, like any other body field under replace=true.
func (s *ProtectedFieldsTestSuite) Test_ReplaceWithProtectedField_WritesAsSent() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("pf_t"))
	s.NoError(s.cmdb.ObjectCreate("pf3", "pf_t", bodyWithUsr("srv", "alice", "prod")))

	incoming := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv"))
	incoming.SetByPath("usr", usr("carol", "dc2"))
	s.Equal("ok", s.objectUpdate("pf3", incoming, true))

	b := s.body("pf3")
	s.Equal("carol", b.GetByPath("usr.attrs.responsible").AsStringDefault(""), "the carried value wins")
	tags, _ := b.GetByPath("usr.tags").AsArrayString()
	s.Equal([]string{"dc2"}, tags, "the protected field is stored as sent, not merged into the old one")
}

// Removal works exactly as the platform API does it: read the whole body, drop
// a tag / an attribute, write it back with replace=true. Nothing resurrects.
func (s *ProtectedFieldsTestSuite) Test_ReadModifyWrite_RemovesTagAndAttribute() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("pf_t"))
	initial := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv"))
	u := easyjson.NewJSONObject()
	u.SetByPath("attrs.responsible", easyjson.NewJSON("alice"))
	u.SetByPath("attrs.comment", easyjson.NewJSON("drop me"))
	u.SetByPath("tags", easyjson.NewJSON([]string{"prod", "dc1"}))
	initial.SetByPath("usr", u)
	s.NoError(s.cmdb.ObjectCreate("pf7", "pf_t", initial))

	// Read-modify-write: drop the "dc1" tag and the "comment" attribute.
	edited := s.body("pf7")
	edited.SetByPath("usr.tags", easyjson.NewJSON([]string{"prod"}))
	edited.RemoveByPath("usr.attrs.comment")
	s.Equal("ok", s.objectUpdate("pf7", edited, true))

	b := s.body("pf7")
	tags, _ := b.GetByPath("usr.tags").AsArrayString()
	s.Equal([]string{"prod"}, tags, "a removed tag must not come back")
	s.False(b.PathExists("usr.attrs.comment"), "a removed attribute must not come back")
	s.Equal("alice", b.GetByPath("usr.attrs.responsible").AsStringDefault(""), "untouched data stays")

	// The inventory writer still cannot destroy what is left.
	s.Equal("ok", s.objectUpdate("pf7", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv-2")), true))
	s.Equal("alice", s.body("pf7").GetByPath("usr.attrs.responsible").AsStringDefault(""))
}

// Merge mode (replace=false) preserves the protected space by construction —
// pin it so a future refactor cannot regress it.
func (s *ProtectedFieldsTestSuite) Test_MergeWithoutProtectedField_Survives() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("pf_t"))
	s.NoError(s.cmdb.ObjectCreate("pf4", "pf_t", bodyWithUsr("srv", "dave", "prod")))

	s.Equal("ok", s.objectUpdate("pf4", easyjson.NewJSONObjectWithKeyValue("extra", easyjson.NewJSON(1)), false))

	b := s.body("pf4")
	s.Equal("dave", b.GetByPath("usr.attrs.responsible").AsStringDefault(""))
	s.Equal(int64(1), int64(b.GetByPath("extra").AsNumericDefault(0)))
}

// A protected field absent from the CURRENT body is simply written as the
// writer sent it — protection preserves, it does not invent.
func (s *ProtectedFieldsTestSuite) Test_FirstWriteOfProtectedField() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("pf_t"))
	s.NoError(s.cmdb.ObjectCreate("pf5", "pf_t", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv"))))
	s.False(s.body("pf5").PathExists("usr"), "sanity: no protected space yet")

	s.Equal("ok", s.objectUpdate("pf5", bodyWithUsr("srv", "erin", "prod"), true))
	s.Equal("erin", s.body("pf5").GetByPath("usr.attrs.responsible").AsStringDefault(""))
}

// Protection is a property of the vertex-body write path, not of objects: a
// type vertex and a plain vertex keep their protected fields through a
// destructive rewrite exactly like an object does. That is precisely why the
// list is declared on the graph root and not in one of its branches.
func (s *ProtectedFieldsTestSuite) Test_ProtectionAppliesToAnyVertex() {
	s.boot()

	// A type vertex, written through the CMDB type API.
	typeBody := easyjson.NewJSONObjectWithKeyValue("descr", easyjson.NewJSON("kept by the model"))
	typeBody.SetByPath("usr", usr("alice", "prod"))
	s.NoError(s.cmdb.TypeCreate("pf_any_t", typeBody))
	s.NoError(s.cmdb.TypeUpdate("pf_any_t", easyjson.NewJSONObjectWithKeyValue("descr", easyjson.NewJSON("rewritten")), true))

	typeRead, err := s.cmdb.TypeRead("pf_any_t")
	s.Require().NoError(err)
	s.Equal("rewritten", typeRead.GetByPath("body.descr").AsStringDefault(""), "the writer owns its own fields")
	s.Equal("alice", typeRead.GetByPath("body.usr.attrs.responsible").AsStringDefault(""),
		"a type vertex must keep its protected field through replace=true")

	// A plain vertex, outside the CMDB model entirely.
	plainBody := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv"))
	plainBody.SetByPath("usr", usr("bob", "dc1"))
	s.NoError(s.graph.VertexCreate("pf_any_v", plainBody))
	s.NoError(s.graph.VertexUpdate("pf_any_v", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv-2")), true))

	plainRead, err := s.graph.VertexRead("pf_any_v")
	s.Require().NoError(err)
	s.Equal("srv-2", plainRead.GetByPath("body.hostname").AsStringDefault(""))
	s.Equal("bob", plainRead.GetByPath("body.usr.attrs.responsible").AsStringDefault(""),
		"a plain vertex must keep its protected field through replace=true")
}

// The protected list is generic and configurable: every configured field is
// protected, and a field NOT on the list is replaced as usual.
func (s *ProtectedFieldsTestSuite) Test_ConfiguredFieldsOnly() {
	s.boot()
	// Publish a wider list: that is how the owner of the data declares policy.
	crud.EnsureBuiltInSchema(s.Runtime().Request, s.Runtime().Domain, "usr", "ops")
	defer crud.EnsureBuiltInSchema(s.Runtime().Request, s.Runtime().Domain, "usr")

	s.NoError(s.cmdb.TypeCreate("pf_t"))
	initial := bodyWithUsr("srv", "frank", "prod")
	initial.SetByPath("ops", easyjson.NewJSONObjectWithKeyValue("owner", easyjson.NewJSON("team-a")))
	initial.SetByPath("discovered", easyjson.NewJSONObjectWithKeyValue("cpu", easyjson.NewJSON(8)))
	s.NoError(s.cmdb.ObjectCreate("pf6", "pf_t", initial))

	s.Equal("ok", s.objectUpdate("pf6", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv")), true))

	b := s.body("pf6")
	s.Equal("frank", b.GetByPath("usr.attrs.responsible").AsStringDefault(""), "usr protected")
	s.Equal("team-a", b.GetByPath("ops.owner").AsStringDefault(""), "ops protected too")
	s.False(b.PathExists("discovered"), "an unprotected field is replaced away as usual")
}
