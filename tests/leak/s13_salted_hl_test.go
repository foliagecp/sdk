//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/stretchr/testify/suite"
)

// S13 — HL CRUD churn driven entirely through SALTED ids (`<id>===<hash>`,
// the sequence-free parallelization suffix): every operation reaches its
// handler with a salted ctx.Self.ID, exactly as parent-based sequence-free
// dispatch and parallelized callers produce. The outcome must be identical
// to clean invocations, and no per-key structure — cache tree, key mutex,
// any of the five process-global crud caches — may retain anything keyed by
// a salt. Fresh salts on every call keep the pressure on.
//
// This scenario exists because a real leak slipped past the suite: salted
// types.link.update planted one permanent type-edge cache entry per call.
// Its per-entry cost sat far below the statistical heap floors, and the
// affected cache had no counter — the exact combination S13 now covers.

type S13Suite struct{ leakSuite }

func TestS13SaltedHLChurn(t *testing.T) { suite.Run(t, new(S13Suite)) }

func (s *S13Suite) saltedRequest(typename, id, salt string, payload easyjson.JSON) error {
	reply, err := s.Request(sfPlugins.AutoRequestSelect, typename, id+"==="+salt, &payload, nil)
	om := sfMediators.OpMsgFromSfReply(reply, err)
	if om.Status != sfMediators.SYNC_OP_STATUS_OK {
		return fmt.Errorf("%s on %s===%s: %s (%s)", typename, id, salt, sfMediators.OpStatusNames[om.Status], om.Details)
	}
	return nil
}

func (s *S13Suite) Test_SaltedHLChurn() {
	s.bootCRUD()
	s.Require().NoError(s.dbc.CMDB.TypeCreate("t_s13a"))
	s.Require().NoError(s.dbc.CMDB.TypeCreate("t_s13b"))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate("t_s13a", "t_s13b", "rel", nil))
	k := scaled(15)

	cycle := func(c int) error {
		salt := func(i int) string { return fmt.Sprintf("%08x%08x", c, i) }

		// types.link.update upsert with a FRESH salt every call — the exact
		// shape of the type-edge cache leak this scenario was born from.
		for i := 0; i < k; i++ {
			p := easyjson.NewJSONObject()
			p.SetByPath("to", easyjson.NewJSON("t_s13b"))
			p.SetByPath("upsert", easyjson.NewJSON(true))
			p.SetByPath("body.weight", easyjson.NewJSON(i))
			if err := s.saltedRequest("functions.cmdb.api.types.link.update", "t_s13a", salt(i), p); err != nil {
				return err
			}
		}

		// Object create (via upsert) / read / delete — every call salted,
		// each with its own salt, fresh object ids per cycle.
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("s13o-%d-%d", c, i)
			if err := s.saltedRequest("functions.cmdb.api.object.update", id, salt(k+i), upsertPayload("t_s13a", leakBody(80))); err != nil {
				return err
			}
			if err := s.saltedRequest("functions.cmdb.api.object.read", id, salt(2*k+i), easyjson.NewJSONObject()); err != nil {
				return err
			}
			// object.delete parks the object in the trash can; the low-level
			// vertex.delete erases the parked one — still salted, which is the
			// point here.
			if err := s.saltedRequest("functions.cmdb.api.object.delete", id, salt(3*k+i), opTimePayload()); err != nil {
				return err
			}
			if err := s.saltedRequest("functions.graph.api.vertex.delete", id, salt(4*k+i), opTimePayload()); err != nil {
				return err
			}
		}
		return nil
	}

	rep := s.newRunner("s13_salted_hl", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}
