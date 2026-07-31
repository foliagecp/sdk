//go:build leak

package leak

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/batch"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/stretchr/testify/suite"
)

// S10 — goroutine hygiene across subsystems in one runtime: graph churn,
// jpgql queries and batches interleaved in every cycle. The concentrated
// check is the exact goroutine-baseline settle (every scenario also checks
// it, this one exercises the cross-subsystem mix). Expected: PASS.

type S10Suite struct{ leakSuite }

func TestS10GoroutineHygiene(t *testing.T) { suite.Run(t, new(S10Suite)) }

func (s *S10Suite) Test_MixedWorkloadGoroutines() {
	s.bootCRUD(jpgql.RegisterAllFunctionTypes, batch.RegisterAllFunctionTypes)
	root := s.buildStaticQueryGraph("s10", 4)

	cycle := func(c int) error {
		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("s10v-%d-%d", c, i)
			if err := s.dbc.Graph.VertexCreate(id, leakBody(50)); err != nil {
				return err
			}
			if err := s.dbc.Graph.VertexDelete(id); err != nil {
				return err
			}
		}
		for i := 0; i < 5; i++ {
			if _, err := s.dbc.Query.JPGQLCtraQuery(root, ".*[l:type('rel')]"); err != nil {
				return err
			}
		}
		b := s.dbc.BatchCreate(fmt.Sprintf("s10b-%d", c)).Parallel()
		for i := 0; i < 10; i++ {
			b.Operation("functions.cmdb.api.object.read", fmt.Sprintf("s10m%d", i%4), easyjson.NewJSONObject())
		}
		if _, err := b.Commit(); err != nil {
			return err
		}
		return s.waitIDHandlersDecay([]string{jpgqlTypename}, 15*time.Second)
	}

	rep := s.newRunner("s10_goroutines", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}
