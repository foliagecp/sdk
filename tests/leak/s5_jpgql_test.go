//go:build leak

package leak

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	"github.com/stretchr/testify/suite"
)

// S5 — JPGQL query pressure over a static graph, with a churning subtree
// between queries. The live engine is an in-memory DFS that must externalize
// nothing; what CAN accumulate is the per-id statefun machinery of the query
// function and the mediator's global reply store — both must decay back to
// zero once the queries go idle. Expected: PASS (pins the decay behavior
// against regressions).

type S5Suite struct{ leakSuite }

func TestS5JPGQL(t *testing.T) { suite.Run(t, new(S5Suite)) }

const jpgqlTypename = "functions.graph.api.query.jpgql.ctra"

// waitIDHandlersDecay blocks until the given function types hold zero per-id
// handler entries (idle GC has reclaimed them all).
func (s *leakSuite) waitIDHandlersDecay(typenames []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		stats := s.Runtime().FunctionTypeIDStatsForTest()
		total := 0
		for _, tn := range typenames {
			total += stats[tn]
		}
		if total == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d id handlers still alive for %v after %v", total, typenames, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (s *leakSuite) collectQueryMachinery(typenames []string) func(*Sample) {
	return func(smp *Sample) {
		s.collectCore(smp)
		stats := s.Runtime().FunctionTypeIDStatsForTest()
		total := 0
		for _, tn := range typenames {
			total += stats[tn]
		}
		smp.Custom["query_id_handlers"] = float64(total)
		smp.Custom["mediator_reply_store"] = float64(sfMediators.ReplyStoreSizeForTest())
	}
}

// buildStaticQueryGraph creates root -> M mids -> M leaves each, all of one
// type linked to itself by "rel" object links.
func (s *leakSuite) buildStaticQueryGraph(prefix string, m int) string {
	tp := "t_" + prefix
	s.Require().NoError(s.dbc.CMDB.TypeCreate(tp))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(tp, tp, "rel", nil))
	root := prefix + "root"
	s.Require().NoError(s.dbc.CMDB.ObjectCreate(root, tp))
	for i := 0; i < m; i++ {
		mid := fmt.Sprintf("%sm%d", prefix, i)
		s.Require().NoError(s.dbc.CMDB.ObjectCreate(mid, tp))
		s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate(root, mid, mid, nil))
		for j := 0; j < m; j++ {
			leaf := fmt.Sprintf("%sl%d_%d", prefix, i, j)
			s.Require().NoError(s.dbc.CMDB.ObjectCreate(leaf, tp))
			s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate(mid, leaf, leaf, nil))
		}
	}
	return root
}

func (s *S5Suite) Test_JPGQLPressure() {
	s.bootCRUD(jpgql.RegisterAllFunctionTypes)
	root := s.buildStaticQueryGraph("s5", 6)
	k := scaled(50)

	cycle := func(c int) error {
		// Churn a temp subtree so queries run against a mutating graph.
		tmp := fmt.Sprintf("s5tmp-%d", c)
		if err := s.dbc.CMDB.ObjectCreate(tmp, "t_s5"); err != nil {
			return err
		}
		if err := s.dbc.CMDB.ObjectsLinkCreate(root, tmp, tmp, nil); err != nil {
			return err
		}

		for i := 0; i < k; i++ {
			var from, q string
			switch i % 3 {
			case 0:
				from, q = root, ".*[l:type('rel')]"
			case 1:
				from, q = root, ".*[l:type('rel')].*[l:type('rel')]"
			default:
				from, q = fmt.Sprintf("s5m%d", i%6), ".*[l:type('rel')]"
			}
			uuids, err := s.dbc.Query.JPGQLCtraQuery(from, q)
			if err != nil {
				return fmt.Errorf("jpgql %q from %s: %w", q, from, err)
			}
			if len(uuids) == 0 {
				return fmt.Errorf("jpgql %q from %s returned no uuids", q, from)
			}
		}

		if err := s.purgeObject(tmp); err != nil {
			return err
		}
		// Idle: the query function's per-id machinery must be reclaimed.
		return s.waitIDHandlersDecay([]string{jpgqlTypename}, 15*time.Second)
	}

	rep := s.newRunner("s5_jpgql", cycle, s.collectQueryMachinery([]string{jpgqlTypename})).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
	rep.AssertStable(s.T(), "query_id_handlers")
	rep.AssertStable(s.T(), "mediator_reply_store")
}
