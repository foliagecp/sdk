package crud_test

// The protected-field list lives in the GRAPH, not in a process.
//
// It is published into the built-in `root` vertex by whoever sets up the schema
// — the policy holds for every vertex, so it is declared at the root of the
// graph — and every runtime loads it at startup into its domain, where stateful
// functions read it as ctx.Domain.ProtectedBodyFields(). That is what lets a
// second application — configured differently, or not configured at all — write
// to the same graph without either guessing which fields it must not clobber or
// imposing its own idea of them.

import (
	"context"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type ProtectedFieldsPublishedTestSuite struct {
	test.StatefunTestSuite
	cmdb  db.CMDBSyncClient
	graph db.GraphSyncClient
}

func TestProtectedFieldsPublishedTestSuite(t *testing.T) {
	suite.Run(t, new(ProtectedFieldsPublishedTestSuite))
}

// boot starts a runtime that DECLARES the given protected fields (the owner of
// the data); with none it starts a follower that only reads what the graph says.
func (s *ProtectedFieldsPublishedTestSuite) boot(protectedFields ...string) {
	crud.RegisterAllFunctionTypes(s.Runtime(), protectedFields...)
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

// publishedList reads the list the way any consumer would: an ordinary vertex
// read of `root`, no SDK internals involved.
func (s *ProtectedFieldsPublishedTestSuite) publishedList() []string {
	data, err := s.graph.VertexRead(crud.BUILT_IN_ROOT)
	s.Require().NoError(err)
	list, _ := data.GetByPath("body." + crud.ProtectedBodyFieldsBodyPath).AsArrayString()
	return list
}

// usrSurvivesRebuild reports whether protection is actually in force: it writes
// a body with `usr`, then rewrites it the way an inventory rebuild does.
func (s *ProtectedFieldsPublishedTestSuite) usrSurvivesRebuild(id string) bool {
	body := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv"))
	body.SetByPath("usr.attrs.responsible", easyjson.NewJSON("alice"))
	s.Require().NoError(s.cmdb.ObjectCreate(id, "pfp_t", body))

	p := easyjson.NewJSONObjectWithKeyValue("replace", easyjson.NewJSON(true))
	p.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv-2")))
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.update", id, &p, nil)
	s.Require().NoError(err)

	read, err := s.cmdb.ObjectRead(id)
	s.Require().NoError(err)
	return read.GetByPath("body.usr.attrs.responsible").AsStringDefault("") == "alice"
}

// The declaring runtime publishes the list, and a plain vertex read finds it —
// that is all a foreign application needs.
func (s *ProtectedFieldsPublishedTestSuite) Test_DeclaredListIsPublished() {
	s.boot("usr")
	s.Equal([]string{"usr"}, s.publishedList())
}

// Stateful functions get the list through the domain — the same value, without
// a graph read per write.
func (s *ProtectedFieldsPublishedTestSuite) Test_DomainServesTheGraphList() {
	s.boot("usr", "ops")
	s.ElementsMatch([]string{"usr", "ops"}, s.Runtime().Domain.ProtectedBodyFields())
}

// The published list follows what is declared, including SHRINKING: a removed
// field must disappear (a merge would have unioned the arrays and kept it).
func (s *ProtectedFieldsPublishedTestSuite) Test_RepublishingReplacesTheList() {
	s.boot("usr")

	crud.EnsureBuiltInSchema(s.Runtime().Request, s.Runtime().Domain, "usr", "ops")
	s.ElementsMatch([]string{"usr", "ops"}, s.publishedList(), "a widened declaration must be published")

	crud.EnsureBuiltInSchema(s.Runtime().Request, s.Runtime().Domain, "usr")
	s.Equal([]string{"usr"}, s.publishedList(), "a removed field must disappear from the published list")
	s.Equal([]string{"usr"}, s.Runtime().Domain.ProtectedBodyFields(), "and the domain must follow")
}

// A runtime that declares nothing publishes nothing: it must not impose its own
// view on a graph it does not own.
func (s *ProtectedFieldsPublishedTestSuite) Test_FollowerPublishesNothing() {
	s.boot()
	s.Empty(s.publishedList(), "a follower must not publish a policy of its own")
	s.Empty(s.Runtime().Domain.ProtectedBodyFields())
}

// What is enforced is what the GRAPH says — not what the process was started
// with: the same runtime protects nothing before the declaration exists and
// protects `usr` after it is published.
func (s *ProtectedFieldsPublishedTestSuite) Test_EnforcementFollowsTheGraph() {
	s.boot() // no declaration anywhere yet
	s.NoError(s.cmdb.TypeCreate("pfp_t"))

	s.False(s.usrSurvivesRebuild("pfp1"), "with no declaration in the graph nothing is protected")

	crud.EnsureBuiltInSchema(s.Runtime().Request, s.Runtime().Domain, "usr")

	s.True(s.usrSurvivesRebuild("pfp2"), "once the graph declares usr, the very same runtime protects it")
}

// Publishing must not disturb anything else living in the `root` body.
func (s *ProtectedFieldsPublishedTestSuite) Test_PublishingKeepsOtherBodyContent() {
	s.boot("usr")

	p := easyjson.NewJSONObjectWithKeyValue("replace", easyjson.NewJSON(false))
	p.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("custom", easyjson.NewJSON("keep me")))
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", crud.BUILT_IN_ROOT, &p, nil)
	s.NoError(err)

	crud.EnsureBuiltInSchema(s.Runtime().Request, s.Runtime().Domain, "usr")

	data, err := s.graph.VertexRead(crud.BUILT_IN_ROOT)
	s.NoError(err)
	s.Equal("keep me", data.GetByPath("body.custom").AsStringDefault(""), "unrelated body content must survive publishing")
	s.Equal([]string{"usr"}, s.publishedList())
}

// The download itself: a runtime that declared nothing picks up a list that is
// already in the graph. The list is put there WITHOUT crud's publish path — a
// plain body write, as if another application had declared it earlier — so what
// is exercised is purely the pull.
func (s *ProtectedFieldsPublishedTestSuite) Test_RuntimePullsListFromGraph() {
	s.boot() // declares nothing, so the graph carries no list yet
	s.Empty(s.Runtime().Domain.ProtectedBodyFields(), "sanity: nothing declared yet")

	p := easyjson.NewJSONObjectWithKeyValue("replace", easyjson.NewJSON(false))
	p.SetByPath("body."+crud.ProtectedBodyFieldsBodyPath, easyjson.NewJSON([]string{"usr", "ops"}))
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", crud.BUILT_IN_ROOT, &p, nil)
	s.NoError(err)

	// This is what a starting runtime does.
	pulled := crud.LoadProtectedBodyFieldsFromGraph(s.Runtime().Request, s.Runtime().Domain)

	s.ElementsMatch([]string{"usr", "ops"}, pulled, "the runtime must pull the list the graph carries")
	s.ElementsMatch([]string{"usr", "ops"}, s.Runtime().Domain.ProtectedBodyFields(),
		"and serve it to stateful functions through the domain")
}

// Out-of-process callers get the same control through the statefun.
func (s *ProtectedFieldsPublishedTestSuite) Test_DeclareViaStatefun() {
	s.boot()

	p := easyjson.NewJSONObjectWithKeyValue(crud.ProtectedBodyFieldsBodyPath, easyjson.NewJSON([]string{"usr", "ops"}))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.schema.ensure", crud.BUILT_IN_ROOT, &p, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	s.ElementsMatch([]string{"usr", "ops"}, s.publishedList())
	s.ElementsMatch([]string{"usr", "ops"}, s.Runtime().Domain.ProtectedBodyFields())
}

// startAttachedApplication starts a SECOND application next to this suite's
// own: its own runtime, its own store, in the same domain — the shape of a
// Foliage deployment where one application provides the CRUD layer and the
// others merely work on the same graph. Runtimes never share an in-memory
// store, so the graph is reachable for it exactly the way it is for any foreign
// application: over the API.
func (s *ProtectedFieldsPublishedTestSuite) startAttachedApplication(name string) *statefun.Runtime {
	cfg := statefun.NewRuntimeConfigSimple(s.NatsURL(), name).
		SetActivePassiveMode(false).
		SetNatsAPITimeoutSec(30)
	rt, err := statefun.NewRuntime(*cfg)
	s.Require().NoError(err)

	go func() { _ = rt.Start(context.TODO(), cache.NewCacheConfig(name+"_cache")) }()

	s.Require().Eventually(rt.IsReady, 30*time.Second, 10*time.Millisecond, "the attached application did not start")
	return rt
}

// THE point of publishing the list into the graph: an application that declares
// nothing, and does not even register the CRUD layer, still starts out knowing
// what the graph protects. It gets it from the graph, at runtime start, without
// anybody configuring it — which is the only thing that keeps it from wiping
// protected data written by the application that owns the schema.
func (s *ProtectedFieldsPublishedTestSuite) Test_AttachedApplicationPullsTheListAtStart() {
	s.boot("usr", "ops") // the application providing CRUD: it creates the schema and declares the policy

	other := s.startAttachedApplication("test_app_attached")
	defer other.Shutdown(true)

	s.Require().Eventually(func() bool { return len(other.Domain.ProtectedBodyFields()) > 0 },
		30*time.Second, 20*time.Millisecond,
		"an application attached to the graph must pull the protected-field list at start")
	s.ElementsMatch([]string{"usr", "ops"}, other.Domain.ProtectedBodyFields())
}

// A domain where nobody serves the graph is not an error: the application
// starts, protects nothing, and says so — rather than hanging or inventing a
// policy of its own.
func (s *ProtectedFieldsPublishedTestSuite) Test_NoGraphInTheDomainProtectsNothing() {
	s.NoError(s.StartRuntime()) // no crud registered anywhere in this domain

	other := s.startAttachedApplication("test_app_alone")
	defer other.Shutdown(true)

	s.Empty(other.Domain.ProtectedBodyFields())
}

// Deployments start their applications in whatever order they like, so one may
// well be up before the application that owns the graph. Its startup read then
// has nobody to answer it — and it must not spend the rest of its life
// unprotected for having started first, so it keeps asking until the graph
// answers once.
func (s *ProtectedFieldsPublishedTestSuite) Test_ApplicationStartedBeforeTheProviderStillGetsTheList() {
	other := s.startAttachedApplication("test_app_early") // up before any graph exists
	defer other.Shutdown(true)
	s.Empty(other.Domain.ProtectedBodyFields(), "sanity: nobody has declared anything yet")

	s.boot("usr") // the application that owns the graph comes up later

	s.Require().Eventually(func() bool { return len(other.Domain.ProtectedBodyFields()) > 0 },
		30*time.Second, 50*time.Millisecond,
		"an application that started first must still end up with the declared list")
	s.Equal([]string{"usr"}, other.Domain.ProtectedBodyFields())
}
