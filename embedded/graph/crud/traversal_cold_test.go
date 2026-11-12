package crud_test

// Traversal.
//
// A JPGQL query that walks cold vertices — that is, ones nobody has touched
// since the last maintenance pass: their buckets are packed into bytes, and
// in the compressing mode compressed as well, and their bodies are not
// parsed. This is the worst read case for records and the best for the tree,
// which has no cold state at all — it always keeps everything parsed.
//
// The numbers are printed for the report; the test itself demands only what
// can be demanded of it regardless of the machine: the traversal must return
// the whole graph in every mode, and a cold traversal must leave warm records
// behind it, not bloated ones.

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type ColdTraversalTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestColdTraversalTestSuite(t *testing.T) {
	suite.Run(t, new(ColdTraversalTestSuite))
}

func (s *ColdTraversalTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	jpgql.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	for _, v := range []string{crud.BUILT_IN_TYPES, crud.BUILT_IN_OBJECTS} {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := s.CacheValue(v); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

// buildFan builds a hub with n out-links of one type and returns its id.
func (s *ColdTraversalTestSuite) buildFan(prefix string, n int) string {
	cmdb := s.dbc.CMDB
	typeName := prefix + "_t"
	s.NoError(cmdb.TypeCreate(typeName))
	s.NoError(cmdb.TypesLinkCreate(typeName, typeName, prefix+"_rel", nil))

	hub := prefix + "-hub"
	body := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(hub))
	s.NoError(cmdb.ObjectUpdate(hub, body, false, typeName))
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%05d", prefix, i)
		b := easyjson.NewJSONObject()
		b.SetByPath("name", easyjson.NewJSON(id))
		b.SetByPath("cpu", easyjson.NewJSON(int64(8+i%16)))
		b.SetByPath("rack", easyjson.NewJSON(fmt.Sprintf("R%02d", i%12)))
		s.NoError(cmdb.ObjectUpdate(id, b, false, typeName))
		s.NoError(cmdb.ObjectsLinkCreate(hub, id, fmt.Sprintf("l%05d", i), nil))
	}
	return hub
}

// coldTraversal cools the cache and measures the traversal, best of three —
// cooling it afresh each time.
func (s *ColdTraversalTestSuite) coldTraversal(hub, linkType string, want int) time.Duration {
	cs := s.Runtime().Domain.Cache()
	query := fmt.Sprintf(".*[l:type('%s')]", linkType)
	best := time.Duration(1<<62 - 1)
	for attempt := 0; attempt < 3; attempt++ {
		// two passes: the first clears the access marks, the second releases
		cs.RunMaintenanceForTest()
		cs.RunMaintenanceForTest()

		started := time.Now()
		ids, err := s.dbc.Query.JPGQLCtraQuery(hub, query)
		took := time.Since(started)
		s.Require().NoError(err)
		s.Require().Lenf(ids, want, "the traversal must return all %d vertices", want)
		if took < best {
			best = took
		}
	}
	return best
}

// cacheWalk — the cache's own share of a traversal: list the hub's links and
// read the target of each, cold. This is what we changed, without the query
// engine around it.
func (s *ColdTraversalTestSuite) cacheWalk(hub string, want int) time.Duration {
	cs := s.Runtime().Domain.Cache()
	domHub := s.SetThisDomainPreffix(hub)
	pattern := fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+">", domHub)

	best := time.Duration(1<<62 - 1)
	for attempt := 0; attempt < 3; attempt++ {
		cs.RunMaintenanceForTest()
		cs.RunMaintenanceForTest()

		started := time.Now()
		keys := cs.GetKeysByPattern(pattern)
		n := 0
		for _, k := range keys {
			if v, err := cs.GetValue(k); err == nil && len(v) > 0 {
				n++
			}
		}
		took := time.Since(started)
		// want + 1: every object also has a __type link to its type.
		s.Require().Equalf(want+1, n, "the cache must return all %d of the hub's links", want+1)
		if took < best {
			best = took
		}
	}
	return best
}

func (s *ColdTraversalTestSuite) Test_ColdTraversal() {
	s.bootstrap()
	cs := s.Runtime().Domain.Cache()

	sizes := []int{1000}
	if os.Getenv("TRAVERSAL_BIG") != "" {
		// The target is ten thousand vertices, but an end-to-end
		// query over them takes twenty seconds, and almost all of that is the
		// query engine: it makes about eight accesses per vertex, and as the
		// graph grows there are more of them, not the same number. Keeping
		// this in the regular run means paying twenty seconds to measure
		// someone else's work.
		sizes = append(sizes, 10000)
	}

	for _, n := range sizes {
		prefix := fmt.Sprintf("tr%d", n)
		hub := s.buildFan(prefix, n)

		walk := s.cacheWalk(hub, n)
		query := s.coldTraversal(hub, prefix+"_rel", n)

		vertices, bytes, buckets, compressed, _, parsed := cs.RecordStatsForTest()
		s.T().Logf("mode %s, %d vertices:", cache.CacheMode(), n)
		s.T().Logf("  cache walk:    %s (%s per link)",
			walk.Round(time.Microsecond), (walk / time.Duration(n)).Round(time.Nanosecond))
		s.T().Logf("  JPGQL query:   %s (%s per vertex)",
			query.Round(time.Millisecond), (query / time.Duration(n)).Round(time.Microsecond))
		s.T().Logf("  %d records, %d bytes, %d buckets (%d of them compressed), %d bodies parsed",
			vertices, bytes, buckets, compressed, parsed)

		// A cold traversal reads links, not bodies, so it has no reason to
		// parse bodies — and it must not leave them parsed behind it.
		s.Require().Zerof(parsed,
			"the traversal over links left %d bodies parsed — it does not read them", parsed)
	}
}
