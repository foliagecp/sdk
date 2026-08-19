//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

// S3 — type-cascade churn: every cycle creates a FRESH type with linked
// objects and deletes the whole family via type.delete (cascading object
// deletes plus the per-type caches: type edges, triggers, HRN fields).
// Expected: PASS.

type S3Suite struct{ leakSuite }

func TestS3TypeCascadeChurn(t *testing.T) { suite.Run(t, new(S3Suite)) }

func (s *S3Suite) Test_TypeCascadeChurn() {
	s.bootCRUD()
	n := scaled(50)
	body := leakBody(200)

	cycle := func(c int) error {
		tp := fmt.Sprintf("t_s3_%d", c)
		id := func(i int) string { return fmt.Sprintf("s3o-%d-%d", c, i) }

		if err := s.dbc.CMDB.TypeCreate(tp); err != nil {
			return fmt.Errorf("type.create %s: %w", tp, err)
		}
		if err := s.dbc.CMDB.TypesLinkCreate(tp, tp, "rel", nil); err != nil {
			return fmt.Errorf("types.link.create %s: %w", tp, err)
		}
		for i := 0; i < n; i++ {
			if err := s.dbc.CMDB.ObjectCreate(id(i), tp, body); err != nil {
				return fmt.Errorf("object.create %s: %w", id(i), err)
			}
		}
		for i := 0; i < n-1; i += 2 {
			if err := s.dbc.CMDB.ObjectsLinkCreate(id(i), id(i+1), fmt.Sprintf("l%d", i), []string{"tag"}); err != nil {
				return fmt.Errorf("objects.link.create %d: %w", i, err)
			}
		}
		// Cascade: deletes every object of the type, the type vertex and the
		// per-type caches.
		if err := s.dbc.CMDB.TypeDelete(tp); err != nil {
			return fmt.Errorf("type.delete %s: %w", tp, err)
		}
		// The cascade PARKS every object of the type in the trash can (it goes
		// through object.delete); erase them through the vertex API, or each
		// cycle leaves a whole family behind and the scenario measures the bin
		// instead of the cascade.
		for i := 0; i < n; i++ {
			if err := s.dbc.Graph.VertexDelete(id(i)); err != nil {
				return fmt.Errorf("parked vertex.delete %s: %w", id(i), err)
			}
		}
		return nil
	}

	rep := s.newRunner("s3_type_cascade", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}
