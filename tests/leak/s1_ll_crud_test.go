//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

// S1 — low-level graph CRUD churn with FRESH ids every cycle. Fresh ids are
// the variant that exposes per-key structures which are populated but never
// cleaned (map entries, tree nodes, mutex entries); reusing the same ids
// every cycle would mask exactly that class. Expected: PASS.

type S1Suite struct{ leakSuite }

func TestS1LLCrudChurn(t *testing.T) { suite.Run(t, new(S1Suite)) }

func (s *S1Suite) Test_LLCrudChurn() {
	s.bootCRUD()
	n := scaled(100)
	body := leakBody(200)

	cycle := func(c int) error {
		id := func(i int) string { return fmt.Sprintf("s1v-%d-%d", c, i) }
		ln := func(i int) string { return fmt.Sprintf("l%d", i) }

		for i := 0; i < n; i++ {
			if err := s.dbc.Graph.VertexCreate(id(i), body); err != nil {
				return fmt.Errorf("vertex.create %s: %w", id(i), err)
			}
		}
		for i := 0; i < n-1; i++ {
			if err := s.dbc.Graph.VerticesLinkCreate(id(i), id(i+1), ln(i), "rel", []string{"tagA", "tagB"}, leakBody(50)); err != nil {
				return fmt.Errorf("link.create %s: %w", ln(i), err)
			}
		}
		upd := leakBody(100)
		for i := 0; i < n; i += 2 {
			if err := s.dbc.Graph.VertexUpdate(id(i), upd, false); err != nil {
				return fmt.Errorf("vertex.update %s: %w", id(i), err)
			}
		}
		for i := 0; i < n-1; i += 2 {
			if err := s.dbc.Graph.VerticesLinkUpdate(id(i), ln(i), []string{"tagC"}, upd, false); err != nil {
				return fmt.Errorf("link.update %s: %w", ln(i), err)
			}
		}
		for i := 0; i < n-1; i++ {
			if err := s.dbc.Graph.VerticesLinkDelete(id(i), ln(i)); err != nil {
				return fmt.Errorf("link.delete %s: %w", ln(i), err)
			}
		}
		for i := 0; i < n; i++ {
			if err := s.dbc.Graph.VertexDelete(id(i)); err != nil {
				return fmt.Errorf("vertex.delete %s: %w", id(i), err)
			}
		}
		return nil
	}

	rep := s.newRunner("s1_ll_crud", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}
