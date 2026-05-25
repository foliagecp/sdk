package crud_test

// Payload/options isolation under fan-out — run these with `-race`.
//
// ObjectCallRequest fans a single caller-provided payload out to many targets in
// concurrent goroutines (object_ops.go). The per-request defensive clone in
// io.go (goLangLocalRequest) is what keeps each target working on its own copy;
// without it the concurrent targets would read/write one shared JSON tree — a
// data race, and a correctness bug (one target's mutation visible to others and
// leaking back into the caller). These tests pin that contract: they must pass
// cleanly under -race, and a removed clone must make them fail.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

const fanoutEchoFn = "test.fanout.echo"

// fanoutEchoHandler mutates the received payload in place (so a shared payload
// across concurrent fan-out targets would be a data race) and echoes the marker.
func fanoutEchoHandler(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if ctx.Payload != nil {
		ctx.Payload.SetByPath("seen_by", easyjson.NewJSON(ctx.Self.ID))
	}
	reply := easyjson.NewJSONObject()
	reply.SetByPath("id", easyjson.NewJSON(ctx.Self.ID))
	if ctx.Payload != nil {
		reply.SetByPath("marker", ctx.Payload.GetByPath("marker"))
	}
	sfMediators.NewOpMediator(ctx).AggregateOpMsg(sfMediators.OpMsgOk(reply)).Reply()
}

type IOFanoutTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestIOFanoutTestSuite(t *testing.T) {
	suite.Run(t, new(IOFanoutTestSuite))
}

func (s *IOFanoutTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	jpgql.RegisterAllFunctionTypes(s.Runtime())
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
		SetMaxIdHandlers(-1)
	s.RegisterFunction(fanoutEchoFn, fanoutEchoHandler, cfg)

	s.NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_TYPES, 15*time.Second)
	s.waitForVertex(crud.BUILT_IN_OBJECTS, 15*time.Second)

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

func (s *IOFanoutTestSuite) waitForVertex(id string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear within %s", id, timeout)
}

// ObjectCallRequest fan-out: the real consumer API. One payload, many targets,
// concurrent goroutines. Asserts every target replies correctly and the caller's
// payload is not corrupted by any target's in-place mutation.
func (s *IOFanoutTestSuite) Test_ObjectCallRequest_FanoutPayloadIsolation() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("FoSrc"))
	s.NoError(s.dbc.CMDB.TypeCreate("FoTgt"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("FoSrc", "FoTgt", "fan", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("fo-src", easyjson.NewJSONObject(), false, "FoSrc"))

	const n = 8
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("fo-tgt-%d", i)
		s.NoError(s.dbc.CMDB.ObjectUpdate(id, easyjson.NewJSONObject(), false, "FoTgt"))
		s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("fo-src", id, nil, easyjson.NewJSONObject(), false, "edge-"+id))
	}

	payload := easyjson.NewJSONObjectWithKeyValue("marker", easyjson.NewJSON("M"))
	lq := sfPlugins.NewLinkQuery("fan")

	res, err := s.Runtime().ObjectCallRequest(sfPlugins.GolangLocalRequest, lq, fanoutEchoFn, "fo-src", &payload, nil)
	s.NoError(err)
	s.Lenf(res, n, "fan-out must reach all %d targets; got %d", n, len(res))
	for id, rep := range res {
		s.NoErrorf(rep.ReqError, "target %s replied with error", id)
		if rep.ReqReply != nil {
			s.Equalf("M", rep.ReqReply.GetByPath("data.marker").AsStringDefault(""), "target %s echoed wrong marker", id)
		}
	}
	s.Falsef(payload.PathExists("seen_by"),
		"fan-out target mutation leaked into the caller's payload — clone isolation broken")
}

// Direct concurrent local requests sharing one payload pointer — a broad race
// surface for the goLangLocalRequest clone (io.go:301), independent of the graph.
func (s *IOFanoutTestSuite) Test_LocalRequest_ConcurrentPayloadIsolation() {
	s.bootstrap()

	payload := easyjson.NewJSONObjectWithKeyValue("marker", easyjson.NewJSON("X"))

	const n = 16
	type rr struct {
		rep *easyjson.JSON
		err error
	}
	results := make([]rr, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rep, err := s.Runtime().Request(sfPlugins.GolangLocalRequest, fanoutEchoFn, fmt.Sprintf("obj-%d", i), &payload, nil)
			results[i] = rr{rep: rep, err: err} // distinct index per goroutine — no shared write
		}(i)
	}
	wg.Wait()

	for i := range results {
		s.NoErrorf(results[i].err, "request %d failed", i)
		if results[i].rep != nil {
			s.Equalf("X", results[i].rep.GetByPath("data.marker").AsStringDefault(""), "request %d echoed wrong marker", i)
		}
	}
	s.Falsef(payload.PathExists("seen_by"),
		"concurrent callee mutation leaked into the shared caller payload — clone isolation broken")
}
