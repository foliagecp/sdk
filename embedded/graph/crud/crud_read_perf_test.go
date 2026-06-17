//go:build perf

package crud_test

// EMBEDDED read bench (in-process, server-side cost — NO NATS round-trips).
// Seeds a pool of N objects, then ObjectRead's them across a concurrency sweep.
// Tunables: PERF_READ_N, PERF_CONCURRENCIES, PERF_EMBEDDED_CSV.

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type CRUDReadPerfTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestCRUDReadPerfTestSuite(t *testing.T) { suite.Run(t, new(CRUDReadPerfTestSuite)) }

func (s *CRUDReadPerfTestSuite) Test_ReadPerf() {
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
	body := easyjson.NewJSONObject()

	n := perfEnvInt("PERF_READ_N", 2000)
	s.NoError(s.cmdb.TypeUpdate("perf_read_t", body, false, true))
	for i := 0; i < n; i++ {
		s.NoError(s.cmdb.ObjectUpdate(fmt.Sprintf("r_%d", i), body, true, "perf_read_t"))
	}

	for _, conc := range perfConcurrencies() {
		r := measurePerf(s.T(), conc, n, func(i int) error {
			_, e := s.cmdb.ObjectRead(fmt.Sprintf("r_%d", i))
			return e
		})
		recordPerf(s.T(), "object-read", "ObjectRead", r)
	}

	drainPendingWAL(s.Runtime())
}
