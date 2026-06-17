//go:build perf

package crud_test

// Focused profiling target (EMBEDDED): ONLY CMDB ObjectDelete, so a CPU profile
// + line listing of DeleteObject attributes the per-op cost to its sub-steps
// (findObjectType / vertex.delete request / op_stack / the trigger walk).
//
//   go test -tags perf -run TestCRUDDeleteProfileSuite -cpuprofile p.prof ./embedded/graph/crud/
//   go tool pprof -list 'DeleteObject$' p.prof
//
// Tunables: PERF_PROF_N.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type CRUDDeleteProfileSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestCRUDDeleteProfileSuite(t *testing.T) { suite.Run(t, new(CRUDDeleteProfileSuite)) }

func (s *CRUDDeleteProfileSuite) Test_ObjectDeleteOnly() {
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

	const poolSize = 32
	for k := 0; k < poolSize; k++ {
		s.NoError(s.cmdb.TypeUpdate(fmt.Sprintf("prof_t_%d", k), body, false, true))
	}

	n := perfEnvInt("PERF_PROF_N", 4000)
	conc := 16
	for i := 0; i < n; i++ {
		s.NoError(s.cmdb.ObjectUpdate(fmt.Sprintf("P_%d", i), body, true, fmt.Sprintf("prof_t_%d", i%poolSize)))
	}
	// Only this loop is the profiling hot path.
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	t0 := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			_ = s.cmdb.ObjectDelete(fmt.Sprintf("P_%d", i))
		}(i)
	}
	wg.Wait()
	s.T().Logf("deleted %d objects in %v (%.0f ops/s)", n, time.Since(t0), float64(n)/time.Since(t0).Seconds())

	drainPendingWAL(s.Runtime())
}
