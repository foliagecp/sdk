//go:build perf

package crud_test

// EMBEDDED delete-path bench (in-process, server-side cost — NO NATS
// round-trips). Three shapes across a concurrency sweep:
//   - ObjectsLinkDelete (single link)        — per-stale-link unlink cost.
//   - ObjectDelete on an orphan (link-less)   — GC baseline; only the object's
//                                               system type-links cascade.
//   - ObjectDelete on a connected object by   — the cascade: LLAPIVertexDelete
//     out-degree D                              issues one link.delete per link.
//
// Object-links resolve their type via the type-link between endpoint types, so
// each run pre-creates a pool of (src,dst) type pairs with a src->dst type-link.
// Objects spread across the pool: deleting a vertex cascades the kind->object
// link on its type vertex; one shared type would be an artificial hotspot. Each
// run gets a FRESH pool so a concurrent-delete race on the type vertices never
// leaks into the next run's seeding.
//
// Tunables: PERF_DELETE_N, PERF_DELETE_DEGREE_N, PERF_CONCURRENCIES,
// PERF_EMBEDDED_CSV.

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

const perfDeleteTypePoolSize = 32

type CRUDDeletePerfTestSuite struct {
	test.StatefunTestSuite
	cmdb    db.CMDBSyncClient
	typeSeq int
}

func TestCRUDDeletePerfTestSuite(t *testing.T) { suite.Run(t, new(CRUDDeletePerfTestSuite)) }

// freshTypePool creates `size` fresh (src,dst) type pairs with a src->dst
// type-link so an object-link between a src-typed and a dst-typed object
// resolves. A fresh pool per run isolates concurrent-delete races on the type
// vertices from the next run's seeding.
func (s *CRUDDeletePerfTestSuite) freshTypePool(size int) (srcs, dsts []string) {
	s.typeSeq++
	srcs = make([]string, size)
	dsts = make([]string, size)
	for k := 0; k < size; k++ {
		src := fmt.Sprintf("perf_del_%d_s%d", s.typeSeq, k)
		dst := fmt.Sprintf("perf_del_%d_d%d", s.typeSeq, k)
		s.NoError(s.cmdb.TypeUpdate(src, easyjson.NewJSONObject(), false, true))
		s.NoError(s.cmdb.TypeUpdate(dst, easyjson.NewJSONObject(), false, true))
		s.NoError(s.cmdb.TypesLinkUpdate(src, dst, nil, easyjson.NewJSONObject(), false, "perf_rel"))
		srcs[k], dsts[k] = src, dst
	}
	return srcs, dsts
}

func (s *CRUDDeletePerfTestSuite) boot() {
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

func (s *CRUDDeletePerfTestSuite) Test_DeleteOperationsPerf() {
	s.boot()
	body := easyjson.NewJSONObject()
	n := perfEnvInt("PERF_DELETE_N", 2000)
	degN := perfEnvInt("PERF_DELETE_DEGREE_N", 100)
	concs := perfConcurrencies()

	// Scenario 1: ObjectsLinkDelete — single link, distinct from-objects.
	for _, conc := range concs {
		srcs, dsts := s.freshTypePool(perfDeleteTypePoolSize)
		for i := 0; i < n; i++ {
			a := fmt.Sprintf("ld_%d_a_%d", conc, i)
			b := fmt.Sprintf("ld_%d_b_%d", conc, i)
			s.NoError(s.cmdb.ObjectUpdate(a, body, true, srcs[i%len(srcs)]))
			s.NoError(s.cmdb.ObjectUpdate(b, body, true, dsts[i%len(dsts)]))
			s.NoError(s.cmdb.ObjectsLinkUpdate(a, b, nil, body, true, fmt.Sprintf("ld_l_%d", i)))
		}
		r := measurePerf(s.T(), conc, n, func(i int) error {
			return s.cmdb.ObjectsLinkDelete(fmt.Sprintf("ld_%d_a_%d", conc, i), fmt.Sprintf("ld_%d_b_%d", conc, i))
		})
		recordPerf(s.T(), "link-delete", "ObjectsLinkDelete", r)
	}

	// Scenario 2: ObjectDelete on orphans — link-less GC baseline.
	for _, conc := range concs {
		srcs, _ := s.freshTypePool(perfDeleteTypePoolSize)
		for i := 0; i < n; i++ {
			s.NoError(s.cmdb.ObjectUpdate(fmt.Sprintf("od_%d_%d", conc, i), body, true, srcs[i%len(srcs)]))
		}
		r := measurePerf(s.T(), conc, n, func(i int) error {
			return s.cmdb.ObjectDelete(fmt.Sprintf("od_%d_%d", conc, i))
		})
		recordPerf(s.T(), "object-delete-orphan", "ObjectDelete", r)
	}

	// Scenario 3: ObjectDelete by out-degree — the cascade.
	cascadeConc := 16
	if len(concs) > 0 {
		cascadeConc = concs[len(concs)-1]
		if cascadeConc > 32 {
			cascadeConc = 32
		}
	}
	for _, degree := range []int{1, 10, 100} {
		srcs, dsts := s.freshTypePool(perfDeleteTypePoolSize)
		for i := 0; i < degN; i++ {
			root := fmt.Sprintf("cd_%d_r_%d", degree, i)
			s.NoError(s.cmdb.ObjectUpdate(root, body, true, srcs[i%len(srcs)]))
			for d := 0; d < degree; d++ {
				leaf := fmt.Sprintf("cd_%d_r_%d_l_%d", degree, i, d)
				s.NoError(s.cmdb.ObjectUpdate(leaf, body, true, dsts[i%len(dsts)]))
				s.NoError(s.cmdb.ObjectsLinkUpdate(root, leaf, nil, body, true, fmt.Sprintf("cd_l_%d", d)))
			}
		}
		r := measurePerf(s.T(), cascadeConc, degN, func(i int) error {
			return s.cmdb.ObjectDelete(fmt.Sprintf("cd_%d_r_%d", degree, i))
		})
		r.degree = degree
		recordPerf(s.T(), "object-delete-cascade", fmt.Sprintf("ObjectDelete(degree=%d)", degree), r)
	}

	drainPendingWAL(s.Runtime())
}
