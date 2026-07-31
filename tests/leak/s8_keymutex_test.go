//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

// S8 — the graph per-key mutex. Under normal churn every entry is refcounted
// away on unlock (PASS). The probe demonstrates finding L6: a vertex id
// containing dots breaks operationKeysMutexUnlock's payload parsing (it walks
// `__key_locks.<key>.{w,r}` two levels deep, a dotted key nests deeper), so
// the lock is never released — one permanently write-locked KeyMutex entry
// per dotted id, and every later operation on that id stalls for the full
// graph-key lock timeout. EXPECTED FAIL until ids are validated or the
// unlock parsing handles dotted keys.

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

// Test_DottedIdLeaksKeyMutexEntry — EXPECTED TO FAIL today (L6). One
// vertex.create per dotted id is enough: the create succeeds, the unlock is
// silently skipped, the entry (and its write lock) stays forever. Fresh ids
// every cycle keep the probe stall-free (nothing ever touches a poisoned id
// twice).
//
// The blast radius goes beyond the mutex entry: MarkOperationDone also lives
// in the skipped unlock path, so every dotted-id operation orphans an
// activeOps entry. hasActiveOperationsUpTo then holds true forever and the
// WAL publisher is WEDGED for the runtime's remaining lifetime — pendingTxs
// only grows and nothing reaches KV anymore. One unvalidated id is enough to
// silently kill persistence. The runner therefore deliberately skips the
// WAL-drain quiesce (it can never succeed here) and asserts all three
// symptoms; each stays red until ids are validated or the unlock parsing
// handles dotted keys.
func (s *S8Suite) Test_DottedIdLeaksKeyMutexEntry() {
	s.bootCRUD()
	k := scaled(10)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("leak.probe.%d.%d", c, i)
			if err := s.dbc.Graph.VertexCreate(id, leakBody(20)); err != nil {
				return fmt.Errorf("vertex.create %s: %w", id, err)
			}
		}
		return nil
	}

	r := &CycleRunner{
		Scenario:      "s8_dotted_id",
		Warmup:        warmupCycles(),
		Measure:       measureCycles(),
		Cycle:         cycle,
		Collect:       s.collectCore,
		SplitNatsHeap: true,
		// No Quiesce: the first dotted-id op wedges the WAL publisher, so a
		// drain wait would only ever time out — the wedge IS the finding.
	}
	rep := r.Run(s.T())
	rep.AssertStable(s.T(), "graph_keymutex_entries")
	rep.AssertStable(s.T(), "cache_active_ops")
	rep.AssertStable(s.T(), "cache_pending_txs")
}
