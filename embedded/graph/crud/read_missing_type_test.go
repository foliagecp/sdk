package crud_test

// Reading a type that does not exist must be "not found", not a failure.
//
// ReadType aggregated the vertex.read reply (IDLE for a missing vertex) and
// then kept going: it inspected the in-links of a vertex that is not there,
// concluded "is not a type" and aggregated FAILED on top — and IDLE+FAILED is
// FAILED. Callers therefore could not tell a misspelled type from a broken
// graph: the DB client maps IDLE to ErrNotFound and anything else to a real
// error, so an API listing objects of an unknown type answered 500 instead of
// 404.

import (
	"errors"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type ReadMissingTypeTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestReadMissingTypeTestSuite(t *testing.T) { suite.Run(t, new(ReadMissingTypeTestSuite)) }

func (s *ReadMissingTypeTestSuite) boot() {
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
	s.cmdb = dbc.CMDB
}

func (s *ReadMissingTypeTestSuite) readTypeStatus(id string) string {
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.read", id, easyjson.NewJSONObject().GetPtr(), nil)
	s.Require().NoError(err)
	return res.GetByPath("status").AsStringDefault("")
}

// A type id nobody ever created: idle, not failed.
func (s *ReadMissingTypeTestSuite) Test_MissingType_IsIdleNotFailed() {
	s.boot()
	s.Equal("idle", s.readTypeStatus("no_such_type_at_all"))
}

// What the API actually depends on: the client turns that idle into
// ErrNotFound, which a REST layer maps to 404 — a hard error would become 500.
func (s *ReadMissingTypeTestSuite) Test_MissingType_ClientReportsNotFound() {
	s.boot()

	_, err := s.cmdb.TypeRead("no_such_type_at_all")
	s.Require().Error(err)
	s.Truef(errors.Is(err, db.ErrNotFound), "a missing type must be ErrNotFound, got %v", err)

	var opErr *db.OpError
	s.Falsef(errors.As(err, &opErr), "a missing type must not surface as a hard operation error: %v", err)
}

// An existing type still reads fine — the early exit must not swallow the
// healthy path.
func (s *ReadMissingTypeTestSuite) Test_ExistingType_StillReadsOk() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("rt_t"))

	s.Equal("ok", s.readTypeStatus("rt_t"))

	data, err := s.cmdb.TypeRead("rt_t")
	s.NoError(err)
	s.True(data.PathExists("body"), "type body must be returned")
}

// A vertex that exists but is not registered as a type keeps failing: that is a
// broken graph, not a missing type, and the two must stay distinguishable.
func (s *ReadMissingTypeTestSuite) Test_NonTypeVertex_StillFails() {
	s.boot()
	body := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", "plain_vertex", &body, nil)
	s.NoError(err)

	s.Equal("failed", s.readTypeStatus("plain_vertex"))
}
