//go:build perf

package crud_test

// EMBEDDED link-create parallelism probe (in-process, server-side cost).
//
// Question this answers: when many out-links are created FROM THE SAME vertex
// concurrently, do they run in parallel or serialize? Compares two shapes at a
// concurrency sweep:
//
//   same-from     : N links  src -> dst_i        (one shared from-vertex)
//   distinct-from : N links  src_i -> dst_i      (a distinct from-vertex each)
//
// If distinct-from throughput scales with concurrency but same-from does NOT,
// the bottleneck is per-from-id serialization (the runtime processes one message
// per id at a time — the function type's per-id worker on the from-vertex),
// which sits UPSTREAM of the graph key mutex. Edge-granular graph locking alone
// then cannot parallelize same-from fan-out.
//
// Run: go test -tags perf -run TestCRUDLinkParallelismPerfTestSuite
//        ./embedded/graph/crud/ -v -count=1
// Tunables: PERF_LINK_N (default 1000), PERF_CONCURRENCIES (default "1 4 16").

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type CRUDLinkParallelismPerfTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestCRUDLinkParallelismPerfTestSuite(t *testing.T) {
	suite.Run(t, new(CRUDLinkParallelismPerfTestSuite))
}

func (s *CRUDLinkParallelismPerfTestSuite) waitBuiltins() {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_TYPES); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatal("built-in types did not appear in time")
}

func (s *CRUDLinkParallelismPerfTestSuite) Test_LinkCreateParallelism() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	s.waitBuiltins()

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb = dbc.CMDB

	n := perfEnvInt("PERF_LINK_N", 1000)

	s.NoError(s.cmdb.TypeCreate("lp_t"))
	s.NoError(s.cmdb.TypesLinkCreate("lp_t", "lp_t", "lp_rel", nil))
	s.NoError(s.cmdb.ObjectCreate("lp_src", "lp_t")) // shared hub for same-from

	for _, conc := range perfConcurrencies() {
		// Fresh vertices PER conc so no op ever re-creates an existing edge
		// (one link per (type,direction) — targets must be unique per measured op).
		for i := 0; i < n; i++ {
			s.NoError(s.cmdb.ObjectCreate(fmt.Sprintf("lp_dst_%d_%d", conc, i), "lp_t")) // same-from targets
			s.NoError(s.cmdb.ObjectCreate(fmt.Sprintf("lp_sfr_%d_%d", conc, i), "lp_t")) // distinct froms
			s.NoError(s.cmdb.ObjectCreate(fmt.Sprintf("lp_dfr_%d_%d", conc, i), "lp_t")) // distinct-from targets
		}

		// same-from: every link goes out of the single hub lp_src.
		rSame := measurePerf(s.T(), conc, n, func(i int) error {
			t := fmt.Sprintf("lp_dst_%d_%d", conc, i)
			return s.cmdb.ObjectsLinkCreate("lp_src", t, t, nil)
		})
		recordPerf(s.T(), "link-create-same-from", "ObjectsLinkCreate", rSame)

		// distinct-from: each link goes out of its own vertex.
		rDist := measurePerf(s.T(), conc, n, func(i int) error {
			f := fmt.Sprintf("lp_sfr_%d_%d", conc, i)
			t := fmt.Sprintf("lp_dfr_%d_%d", conc, i)
			return s.cmdb.ObjectsLinkCreate(f, t, t, nil)
		})
		recordPerf(s.T(), "link-create-distinct-from", "ObjectsLinkCreate", rDist)

		s.T().Logf("PARALLELISM conc=%-3d  same-from=%8.0f ops/s   distinct-from=%8.0f ops/s   distinct/same=%.2fx",
			conc, rSame.throughputOpsPerSec, rDist.throughputOpsPerSec,
			rDist.throughputOpsPerSec/rSame.throughputOpsPerSec)
	}

	drainPendingWAL(s.Runtime())
}
