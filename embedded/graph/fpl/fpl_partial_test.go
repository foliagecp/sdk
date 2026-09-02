package fpl_test

// A partial answer must say that it is partial.
//
// FPL merges only the sub-queries that SUCCEEDED into its intersection; one
// that failed was recorded in stats and the whole call still answered OK. A
// consumer that does not walk stats — and the SDK's own client wrapper drops
// stats entirely — took a narrowed answer for the whole truth. The same holds
// for a traversal truncated by its own timeout.
//
// The flag is additive and the status is unchanged, so existing consumers see
// exactly what they saw before; `strict` is for those who would rather be told
// by status.

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type FPLPartialSuite struct {
	test.StatefunTestSuite
	cmdb  db.CMDBSyncClient
	query db.QuerySyncClient
}

func TestFPLPartialSuite(t *testing.T) { suite.Run(t, new(FPLPartialSuite)) }

func (s *FPLPartialSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	jpgql.RegisterAllFunctionTypes(s.Runtime())
	fpl.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_OBJECTS); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb, s.query = dbc.CMDB, dbc.Query

	// A tiny graph: a → b, so a sub-query over it can succeed.
	s.NoError(s.cmdb.TypeCreate("fp_t"))
	s.NoError(s.cmdb.TypesLinkCreate("fp_t", "fp_t", "fp_rel", nil))
	s.NoError(s.cmdb.ObjectCreate("fp_a", "fp_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectCreate("fp_b", "fp_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectsLinkCreate("fp_a", "fp_b", "fp_b", nil, easyjson.NewJSONObject()))
}

// fplQuery runs one FPL request built from the given sub-queries.
func (s *FPLPartialSuite) fplQuery(strict bool, subQueries ...[2]string) easyjson.JSON {
	uoi := easyjson.NewJSONArray()
	group := easyjson.NewJSONArray()
	for _, q := range subQueries {
		item := easyjson.NewJSONObjectWithKeyValue("from_uuid", easyjson.NewJSON(q[0]))
		item.SetByPath("jpgql", easyjson.NewJSON(q[1]))
		group.AddToArray(item)
	}
	uoi.AddToArray(group)

	p := easyjson.NewJSONObjectWithKeyValue("jpgql_uoi", uoi)
	if strict {
		p.SetByPath("strict", easyjson.NewJSON(true))
	}
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.query.fpl", "fplcall", &p, nil)
	s.Require().NoError(err)
	return *res
}

// Every sub-query answered: nothing is missing, and the flag says so.
func (s *FPLPartialSuite) Test_CompleteAnswerIsNotPartial() {
	s.boot()

	res := s.fplQuery(false, [2]string{"fp_a", ".*[l:type('fp_rel')]"})

	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
	s.False(res.GetByPath("data.partial").AsBoolDefault(true),
		"a complete answer must report partial=false, not leave it to be guessed")
}

// A sub-query that did not succeed narrows the intersection — silently, before.
func (s *FPLPartialSuite) Test_FailedSubQueryMakesTheAnswerPartial() {
	s.boot()

	res := s.fplQuery(false,
		[2]string{"fp_a", ".*[l:type('fp_rel')]"},
		[2]string{"fp_a", "((((not a query"}, // rejected by the query parser
	)

	s.Equal("ok", res.GetByPath("status").AsStringDefault(""),
		"the default status stays what it was — existing consumers must not change")
	s.True(res.GetByPath("data.partial").AsBoolDefault(false),
		"a sub-query that did not succeed must be reported as a partial answer")

	// And the reason is still in the per-call diagnostics.
	calls := res.GetByPath("data.stats.jpgql_calls")
	s.Require().True(calls.IsArray())
	sawFailure := false
	for i := 0; i < calls.ArraySize(); i++ {
		if calls.ArrayElement(i).GetByPath("status").AsStringDefault("") != "ok" {
			sawFailure = true
		}
	}
	s.True(sawFailure, "the failing sub-call must remain visible in stats")
}

// Strict mode reports the same thing as a status, for callers who would rather
// have it that way.
func (s *FPLPartialSuite) Test_StrictReportsIncomplete() {
	s.boot()

	res := s.fplQuery(true,
		[2]string{"fp_a", ".*[l:type('fp_rel')]"},
		[2]string{"fp_a", "((((not a query"},
	)

	s.Equal("incomplete", res.GetByPath("status").AsStringDefault(""),
		"with strict the caller is told by status, not only by a field")
	s.True(res.GetByPath("data.partial").AsBoolDefault(false))
}

// Nothing came back at all: that is not a partial answer, it is no answer.
func (s *FPLPartialSuite) Test_StrictWithEverySubQueryFailedIsFailure() {
	s.boot()

	res := s.fplQuery(true,
		[2]string{"fp_a", "((((not a query"},
		[2]string{"fp_b", "))))also not a query"},
	)

	s.Equal("failed", res.GetByPath("status").AsStringDefault(""))
}

// The client says it too: the old wrapper returns the uuids and nothing else,
// so a caller could not tell a narrowed answer from a whole one. The new one
// hands back the flag — and, if asked, refuses to pretend at all.
func (s *FPLPartialSuite) Test_ClientReportsPartial() {
	s.boot()

	uoi := `{"jpgql_uoi":[[{"from_uuid":"fp_a","jpgql":".*[l:type('fp_rel')]"},{"from_uuid":"fp_a","jpgql":"((((not a query"}]]}`

	data, partial, err := s.query.FPLQueryEx("fplcall", uoi)
	s.NoError(err, "by default a partial answer is still an answer")
	s.True(partial, "and the caller is told that it is partial")
	s.True(data.PathExists("stats.jpgql_calls"), "with the diagnostics behind it")

	_, _, err = s.query.FPLQueryEx("fplcall", uoi, true)
	s.Error(err, "with strict the same call is an error, not a quiet half-answer")
}

// The whole point of the default: nothing changes for whoever is already
// calling FPL today.
func (s *FPLPartialSuite) Test_OldClientMethodUnchanged() {
	s.boot()

	uoi := `{"jpgql_uoi":[[{"from_uuid":"fp_a","jpgql":".*[l:type('fp_rel')]"},{"from_uuid":"fp_a","jpgql":"((((not a query"}]]}`
	data, err := s.query.FPLQuery("fplcall", uoi)

	s.NoError(err, "the existing method keeps returning success on a partial answer")
	s.True(data.PathExists("uuids"))
}
