//go:build leak

package leak

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/stretchr/testify/suite"
)

// S6 — FPL query pressure, including the vbody/obody post-processors with
// link bodies attached (links_in_body/links_out_body). FPL mints a FRESH
// unique statefun id per jpgql sub-query and per post-processor call
// (`<uuid>===<hash>`), so every cycle creates hundreds of one-shot ids whose
// handler machinery must decay to zero when idle. Expected: PASS.

type S6Suite struct{ leakSuite }

func TestS6FPL(t *testing.T) { suite.Run(t, new(S6Suite)) }

var fplTypenames = []string{
	"functions.graph.api.query.fpl",
	"functions.graph.api.query.fpl.pp.vbody",
	"functions.graph.api.query.fpl.pp.obody",
	jpgqlTypename,
}

func (s *S6Suite) fplQueryJSON(fromUUID, pp string) string {
	el := easyjson.NewJSONObject()
	el.SetByPath("jpgql", easyjson.NewJSON(".*[l:type('rel')]"))
	el.SetByPath("from_uuid", easyjson.NewJSON(fromUUID))
	el2 := easyjson.NewJSONObject()
	el2.SetByPath("jpgql", easyjson.NewJSON(".*[l:type('rel')].*[l:type('rel')]"))
	el2.SetByPath("from_uuid", easyjson.NewJSON(fromUUID))

	inter1 := easyjson.NewJSONArray()
	inter1.AddToArray(el)
	inter2 := easyjson.NewJSONArray()
	inter2.AddToArray(el2)
	uoi := easyjson.NewJSONArray()
	uoi.AddToArray(inter1)
	uoi.AddToArray(inter2)

	payload := easyjson.NewJSONObject()
	payload.SetByPath("jpgql_uoi", uoi)
	if pp != "" {
		ppData := easyjson.NewJSONObject()
		ppData.SetByPath("links_in_body", easyjson.NewJSON(true))
		ppData.SetByPath("links_out_body", easyjson.NewJSON(true))
		ppFunc := easyjson.NewJSONObject()
		ppFunc.SetByPath("name", easyjson.NewJSON(pp))
		ppFunc.SetByPath("data", ppData)
		payload.SetByPath("post_processor_func", ppFunc)
	}
	return payload.ToString()
}

func (s *S6Suite) Test_FPLPressure() {
	s.bootCRUD(jpgql.RegisterAllFunctionTypes, fpl.RegisterAllFunctionTypes)
	root := s.buildStaticQueryGraph("s6", 6)
	fromUUID := s.domainID(root)
	k := scaled(30)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			pp := ""
			switch i % 3 {
			case 0:
				pp = "functions.graph.api.query.fpl.pp.obody"
			case 1:
				pp = "functions.graph.api.query.fpl.pp.vbody"
			}
			res, err := s.dbc.Query.FPLQuery(fmt.Sprintf("s6q-%d-%d", c, i), s.fplQueryJSON(fromUUID, pp))
			if err != nil {
				return fmt.Errorf("fpl query %d (pp=%q): %w", i, pp, err)
			}
			if res.GetByPath("uuids").ArraySize() == 0 {
				return fmt.Errorf("fpl query %d returned no uuids", i)
			}
		}
		return s.waitIDHandlersDecay(fplTypenames, 20*time.Second)
	}

	rep := s.newRunner("s6_fpl", cycle, s.collectQueryMachinery(fplTypenames)).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
	rep.AssertStable(s.T(), "query_id_handlers")
	rep.AssertStable(s.T(), "mediator_reply_store")
}
