//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/suite"
)

// S8 — the graph per-key mutex. Under normal churn every entry is refcounted
// away on unlock (PASS). The dotted-id probe pins the L6 fix: rejected at
// creation, and legacy dotted data neither leaks locks nor wedges the WAL.

type S8Suite struct{ leakSuite }

func TestS8KeyMutex(t *testing.T) { suite.Run(t, new(S8Suite)) }

func (s *S8Suite) Test_KeyMutexChurn() {
	s.bootCRUD()
	k := scaled(30)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("s8v-%d-%d", c, i)
			if err := s.dbc.Graph.VertexCreate(id, leakBody(50)); err != nil {
				return err
			}
		}
		for i := 0; i < k; i++ {
			if err := s.dbc.Graph.VertexDelete(fmt.Sprintf("s8v-%d-%d", c, i)); err != nil {
				return err
			}
		}
		return nil
	}

	rep := s.newRunner("s8_keymutex_churn", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}

// Test_DottedIdsAreSafe — regression guard for finding L6.
//
// Historically a dotted vertex id broke the payload lock bookkeeping: the
// unlock walk could not find the record, leaving a permanently write-locked
// KeyMutex entry AND skipping MarkOperationDone — the orphaned activeOps
// entry then wedged the WAL publisher for the runtime's lifetime. The fix is
// three-layered: dotted ids are rejected at creation, held-lock records are
// keyed by a hash (parse-proof for any key content), and completion marking
// is decoupled from the lock records. This probe pins all three:
//   1. creating a dotted id must be REJECTED (and must not leak a lock);
//   2. legacy dotted vertices (seeded directly into the cache, as if created
//      before validation existed) must be deletable without leaving a mutex
//      entry, an activeOps orphan or an undrainable WAL.
func (s *S8Suite) Test_DottedIdsAreSafe() {
	s.bootCRUD()
	k := scaled(10)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("leak.probe.%d.%d", c, i)
			if err := s.dbc.Graph.VertexCreate(id, leakBody(20)); err == nil {
				return fmt.Errorf("vertex.create accepted dotted id %s", id)
			}
		}
		for i := 0; i < k; i++ {
			plain := fmt.Sprintf("legacy.%d.%d", c, i)
			body := leakBody(20)
			if !s.cacheStore().SetValueJSON(s.domainID(plain), &body, true, system.GetCurrentTimeNs()) {
				return fmt.Errorf("cannot seed legacy dotted vertex %s", plain)
			}
			if err := s.dbc.Graph.VertexDelete(plain); err != nil {
				return fmt.Errorf("vertex.delete of legacy dotted id %s: %w", plain, err)
			}
		}
		return nil
	}

	rep := s.newRunner("s8_dotted_id", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	rep.AssertStable(s.T(), "graph_keymutex_entries")
	rep.AssertStable(s.T(), "cache_active_ops")
	rep.AssertStable(s.T(), "cache_pending_txs")
	rep.AssertStable(s.T(), "cache_live_values")
}
