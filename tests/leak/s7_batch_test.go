//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/batch"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/suite"
)

// S7 — batch executor churn: sequential and parallel batches (with sub-batch
// splitting) carrying mixed CMDB operations. The executor must hold no state
// beyond the request and join every worker goroutine. Expected: PASS.

type S7Suite struct{ leakSuite }

func TestS7Batch(t *testing.T) { suite.Run(t, new(S7Suite)) }

// upsertPayload mirrors what CMDB.ObjectUpdate sends for a create-via-upsert.
func upsertPayload(originType string, body easyjson.JSON) easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	p.SetByPath("upsert", easyjson.NewJSON(true))
	p.SetByPath("origin_type", easyjson.NewJSON(originType))
	p.SetByPath("replace", easyjson.NewJSON(true))
	p.SetByPath("body", body)
	return p
}

func opTimePayload() easyjson.JSON {
	return easyjson.NewJSONObjectWithKeyValue("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
}

func (s *S7Suite) Test_BatchChurn() {
	s.bootCRUD(batch.RegisterAllFunctionTypes)
	s.Require().NoError(s.dbc.CMDB.TypeCreate("t_s7"))
	n := scaled(60)

	cycle := func(c int) error {
		id := func(i int) string { return fmt.Sprintf("s7o-%d-%d", c, i) }

		// Sequential batch: create every object via upsert, then read it back
		// in the same round-trip (order preserved by the executor).
		b1 := s.dbc.BatchCreate(fmt.Sprintf("s7b1-%d", c))
		for i := 0; i < n; i++ {
			b1.Operation("functions.cmdb.api.object.update", id(i), upsertPayload("t_s7", leakBody(100)))
		}
		for i := 0; i < n; i++ {
			b1.Operation("functions.cmdb.api.object.read", id(i), easyjson.NewJSONObject())
		}
		results, err := b1.Commit()
		if err != nil {
			return fmt.Errorf("sequential batch: %w", err)
		}
		for _, r := range results {
			if !r.OK() {
				return fmt.Errorf("sequential batch op %d (%s %s): status %s", r.Index, r.Typename, r.ID, r.Status)
			}
		}

		// Parallel batch with sub-batch splitting: delete everything.
		b2 := s.dbc.BatchCreate(fmt.Sprintf("s7b2-%d", c)).Parallel().SubBatchSize(16)
		for i := 0; i < n; i++ {
			b2.Operation("functions.cmdb.api.object.delete", id(i), opTimePayload())
		}
		results, err = b2.Commit()
		if err != nil {
			return fmt.Errorf("parallel batch: %w", err)
		}
		for _, r := range results {
			if !r.OK() {
				return fmt.Errorf("parallel delete op %d (%s): status %s", r.Index, r.ID, r.Status)
			}
		}
		return nil
	}

	rep := s.newRunner("s7_batch", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}
