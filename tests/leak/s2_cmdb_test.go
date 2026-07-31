//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

// S2 — CMDB object churn (types, objects, object links) with fresh ids every
// cycle, plus a targeted probe for the object-type cache orphaned by a
// partial delete. Main churn expected: PASS. Probe expected: FAIL until the
// objectTypeCache lifecycle is fixed (finding L3).

type S2Suite struct{ leakSuite }

func TestS2CMDBObjectChurn(t *testing.T) { suite.Run(t, new(S2Suite)) }

func (s *S2Suite) Test_CMDBObjectChurn() {
	s.bootCRUD()
	s.Require().NoError(s.dbc.CMDB.TypeCreate("t_s2"))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate("t_s2", "t_s2", "rel", nil))
	n := scaled(60)
	body := leakBody(200)

	cycle := func(c int) error {
		id := func(i int) string { return fmt.Sprintf("s2o-%d-%d", c, i) }

		for i := 0; i < n; i++ {
			if err := s.dbc.CMDB.ObjectCreate(id(i), "t_s2", body); err != nil {
				return fmt.Errorf("object.create %s: %w", id(i), err)
			}
		}
		for i := 0; i < n-1; i++ {
			if err := s.dbc.CMDB.ObjectsLinkCreate(id(i), id(i+1), fmt.Sprintf("l%d", i), []string{"tag"}, leakBody(50)); err != nil {
				return fmt.Errorf("objects.link.create %d: %w", i, err)
			}
		}
		upd := leakBody(100)
		for i := 0; i < n; i += 2 {
			if err := s.dbc.CMDB.ObjectUpdate(id(i), upd, false); err != nil {
				return fmt.Errorf("object.update %s: %w", id(i), err)
			}
		}
		for i := 0; i < n-1; i++ {
			if err := s.dbc.CMDB.ObjectsLinkDelete(id(i), id(i+1)); err != nil {
				return fmt.Errorf("objects.link.delete %d: %w", i, err)
			}
		}
		for i := 0; i < n; i++ {
			if err := s.dbc.CMDB.ObjectDelete(id(i)); err != nil {
				return fmt.Errorf("object.delete %s: %w", id(i), err)
			}
		}
		return nil
	}

	rep := s.newRunner("s2_cmdb_churn", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}

// Test_ObjectTypeCacheOrphanedByPartialDelete — EXPECTED TO FAIL today (L3).
//
// A partially deleted object (its body key already gone, exactly the state an
// aborted cross-domain delete leaves behind, see LLAPILinkDelete/-VertexDelete
// abort paths) makes functions.graph.api.vertex.delete short-circuit IDLE
// without purging anything (ll_crud.go body-existence check). The object's
// entry in the process-global objectTypeCache then survives forever — one
// orphaned map entry per interrupted delete. The probe asserts the cache
// returns to baseline; it will not until the lifecycle is fixed.
func (s *S2Suite) Test_ObjectTypeCacheOrphanedByPartialDelete() {
	s.bootCRUD()
	s.Require().NoError(s.dbc.CMDB.TypeCreate("t_s2p"))
	k := scaled(20)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("s2p-%d-%d", c, i)
			if err := s.dbc.CMDB.ObjectCreate(id, "t_s2p"); err != nil {
				return fmt.Errorf("object.create %s: %w", id, err)
			}
			// Simulate the interrupted delete: drop the body key directly,
			// then delete the vertex — the API replies IDLE and purges
			// nothing (the reply status is intentionally ignored here).
			s.cacheStore().DeleteValue(s.domainID(id), true, -1)
			_ = s.dbc.Graph.VertexDelete(id)
		}
		return nil
	}

	rep := s.newRunner("s2_otc_orphan", cycle, s.collectCore).Run(s.T())
	rep.AssertStable(s.T(), "object_type_cache")
}
