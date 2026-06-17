//go:build perf

package crud_test

// Diagnostic (EMBEDDED): what dominates ObjectDelete latency/throughput — the
// per-op write path, or a single in-link from a SHARED vertex (the builtInObjects
// membership link every CMDB object carries)?
//
// Four delete profiles, each at conc=1 (uncontended) and conc=16 (contention):
//   A cmdb-object        — ObjectDelete; object has its service links.
//   B vertex-bare        — raw VertexDelete; ZERO links. Pure vertex delete.
//   C vertex-shared-in   — raw vertex with ONE in-link from a single SHARED
//                          vertex S (mimics builtInObjects -> obj).
//   D vertex-unique-in   — raw vertex with ONE in-link from its OWN unique
//                          vertex (same work as C, but no shared lock).
//   E raw-3link-profile  — raw vertices with a CMDB object's exact link profile,
//                          deleted via low-level VertexDelete (isolates the HL
//                          object.delete wrapper from the cascade: E vs A).

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type CRUDDeleteDiagSuite struct {
	test.StatefunTestSuite
	cmdb  db.CMDBSyncClient
	graph db.GraphSyncClient
}

func TestCRUDDeleteDiagSuite(t *testing.T) { suite.Run(t, new(CRUDDeleteDiagSuite)) }

func (s *CRUDDeleteDiagSuite) measureDiag(name string, conc, n int, op func(i int) error) {
	durs := make([]time.Duration, n)
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var failed int64
	wall0 := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			if err := op(i); err != nil && !strings.Contains(err.Error(), "does not exist") {
				atomic.AddInt64(&failed, 1)
			}
			durs[i] = time.Since(t0)
		}(i)
	}
	wg.Wait()
	wall := time.Since(wall0)
	sort.Slice(durs, func(a, b int) bool { return durs[a] < durs[b] })
	p := func(q float64) time.Duration {
		idx := int(q * float64(n))
		if idx >= n {
			idx = n - 1
		}
		return durs[idx]
	}
	s.T().Logf("[%-18s] conc=%-3d n=%-5d  p50=%-10v p99=%-10v  throughput=%.0f ops/s  failed=%d",
		name, conc, n, p(0.50).Round(time.Microsecond), p(0.99).Round(time.Microsecond), float64(n)/wall.Seconds(), failed)
}

func (s *CRUDDeleteDiagSuite) Test_WhatDominatesDelete() {
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
	s.graph = dbc.Graph
	body := easyjson.NewJSONObject()

	// CMDB object link profile (from createObjectInline): 1 out (->type) + 2 in
	// (<-type, <-builtInObjects). Two of three touch a SHARED vertex.
	s.T().Logf("CMDB object link profile (createObjectInline): out=1 (->type), in=2 (<-type, <-builtInObjects)")

	n := 1000
	const poolSize = 32
	for _, conc := range []int{1, 16} {
		// A: CMDB ObjectDelete (service links, types spread over a pool)
		types := make([]string, poolSize)
		for k := 0; k < poolSize; k++ {
			types[k] = fmt.Sprintf("diag_at_%d_%d", conc, k)
			s.NoError(s.cmdb.TypeUpdate(types[k], body, false, true))
		}
		for i := 0; i < n; i++ {
			s.NoError(s.cmdb.ObjectUpdate(fmt.Sprintf("A_%d_%d", conc, i), body, true, types[i%poolSize]))
		}
		s.measureDiag("A cmdb-object", conc, n, func(i int) error {
			return s.cmdb.ObjectDelete(fmt.Sprintf("A_%d_%d", conc, i))
		})

		// B: raw vertex, ZERO links
		for i := 0; i < n; i++ {
			s.NoError(s.graph.VertexCreate(fmt.Sprintf("B_%d_%d", conc, i), body))
		}
		s.measureDiag("B vertex-bare", conc, n, func(i int) error {
			return s.graph.VertexDelete(fmt.Sprintf("B_%d_%d", conc, i))
		})

		// C: raw vertex with ONE in-link from a single SHARED vertex S
		shared := fmt.Sprintf("C_shared_%d", conc)
		s.NoError(s.graph.VertexCreate(shared, body))
		for i := 0; i < n; i++ {
			v := fmt.Sprintf("C_%d_%d", conc, i)
			s.NoError(s.graph.VertexCreate(v, body))
			s.NoError(s.graph.VerticesLinkCreate(shared, v, fmt.Sprintf("cl_%d", i), "rel", nil))
		}
		s.measureDiag("C vertex-shared-in", conc, n, func(i int) error {
			return s.graph.VertexDelete(fmt.Sprintf("C_%d_%d", conc, i))
		})

		// D: raw vertex with ONE in-link from its OWN unique vertex
		for i := 0; i < n; i++ {
			u := fmt.Sprintf("D_u_%d_%d", conc, i)
			v := fmt.Sprintf("D_%d_%d", conc, i)
			s.NoError(s.graph.VertexCreate(u, body))
			s.NoError(s.graph.VertexCreate(v, body))
			s.NoError(s.graph.VerticesLinkCreate(u, v, "dl", "rel", nil))
		}
		s.measureDiag("D vertex-unique-in", conc, n, func(i int) error {
			return s.graph.VertexDelete(fmt.Sprintf("D_%d_%d", conc, i))
		})

		// E: raw vertices with a CMDB object's exact link profile, deleted via
		// LOW-LEVEL VertexDelete. E vs A = the cost of the HL object.delete wrapper.
		objectsV := fmt.Sprintf("E_objects_%d", conc)
		s.NoError(s.graph.VertexCreate(objectsV, body))
		etypes := make([]string, poolSize)
		for k := 0; k < poolSize; k++ {
			etypes[k] = fmt.Sprintf("E_type_%d_%d", conc, k)
			s.NoError(s.graph.VertexCreate(etypes[k], body))
		}
		for i := 0; i < n; i++ {
			v := fmt.Sprintf("E_%d_%d", conc, i)
			tp := etypes[i%poolSize]
			s.NoError(s.graph.VertexCreate(v, body))
			s.NoError(s.graph.VerticesLinkCreate(objectsV, v, fmt.Sprintf("el_o_%d", i), "rel", nil))
			s.NoError(s.graph.VerticesLinkCreate(v, tp, "el_type", "rel", nil))
			s.NoError(s.graph.VerticesLinkCreate(tp, v, fmt.Sprintf("el_t_%d", i), "rel", nil))
		}
		s.measureDiag("E raw-3link-profile", conc, n, func(i int) error {
			return s.graph.VertexDelete(fmt.Sprintf("E_%d_%d", conc, i))
		})
	}

	drainPendingWAL(s.Runtime())
}
