//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/suite"
)

// S14 covers the park -> restore transition omitted by other churn scenarios.
// Both CreateObject and the UpdateObject upsert diversion reach the restore.

type S14Suite struct{ leakSuite }

func TestS14TrashCanRestore(t *testing.T) { suite.Run(t, new(S14Suite)) }

func (s *S14Suite) Test_TrashCanRestoreChurn() {
	s.bootCRUD()
	s.Require().NoError(s.dbc.CMDB.TypeCreate("t_s14"))
	k := scaled(10)

	body := func(host string) easyjson.JSON {
		b := leakBody(50)
		b.SetByPath("hostname", easyjson.NewJSON(host))
		b.SetByPath("usr.attrs.responsible", easyjson.NewJSON("s14"))
		return b
	}

	cycle := func(c int) error {
		// Leg A — restore through plain re-creation (CreateObject).
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("s14a-%d-%d", c, i)
			if err := s.dbc.CMDB.ObjectCreate(id, "t_s14", body("h1")); err != nil {
				return err
			}
			if err := s.dbc.CMDB.ObjectDelete(id); err != nil { // park
				return err
			}
			if err := s.dbc.CMDB.ObjectCreate(id, "t_s14", body("h2")); err != nil { // restore
				return err
			}
			if err := s.purgeObject(id); err != nil { // park + erase: back to baseline
				return err
			}
		}

		// Leg B — restore through the upsert diversion (UpdateObject).
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("s14b-%d-%d", c, i)
			if err := s.dbc.CMDB.ObjectCreate(id, "t_s14", body("h1")); err != nil {
				return err
			}
			if err := s.dbc.CMDB.ObjectDelete(id); err != nil { // park
				return err
			}
			if err := s.dbc.CMDB.ObjectUpdate(id, body("h2"), true, "t_s14"); err != nil { // restore
				return err
			}
			if err := s.purgeObject(id); err != nil {
				return err
			}
		}
		return nil
	}

	rep := s.newRunner("s14_trashcan_restore", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}
