package crud_test

import (
	"time"

	"github.com/foliagecp/easyjson"
)

// An orphaned activeOps entry prevents pendingTxs from draining.
func (s *TrashCanTestSuite) requireCacheQuiesced(what string) {
	s.T().Helper()

	cs := s.Runtime().Domain.Cache()
	st := cs.StatsForTest()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		st = cs.StatsForTest()
		if st.ActiveOps == 0 && st.PendingTxs == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	s.T().Fatalf("cache did not quiesce after %s: activeOps=%d pendingTxs=%d",
		what, st.ActiveOps, st.PendingTxs)
}

func (s *TrashCanTestSuite) Test_Restore_LeavesNoOrphanedActiveOperation() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tcb_t"))
	s.requireCacheQuiesced("type create")

	s.NoError(s.cmdb.ObjectCreate("tcb1", "tcb_t", usrBody("h1", "alice", "prod")))
	s.requireCacheQuiesced("object create")

	s.NoError(s.cmdb.ObjectDelete("tcb1"))
	s.requireCacheQuiesced("object delete (park)")

	s.NoError(s.cmdb.ObjectCreate("tcb1", "tcb_t", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("h2"))))
	s.requireCacheQuiesced("object create (restore from trash can)")

	// An orphan from restore would block this later transaction.
	s.NoError(s.cmdb.ObjectCreate("tcb1-after", "tcb_t", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("h3"))))
	s.requireCacheQuiesced("write after restore")
}

func (s *TrashCanTestSuite) Test_UpsertRestore_LeavesNoOrphanedActiveOperation() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tcb_t"))
	s.NoError(s.cmdb.ObjectCreate("tcb2", "tcb_t", usrBody("h1", "bob")))
	s.NoError(s.cmdb.ObjectDelete("tcb2"))
	s.requireCacheQuiesced("object delete (park)")

	s.NoError(s.cmdb.ObjectUpdate("tcb2", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("h2")), true, "tcb_t"))
	s.requireCacheQuiesced("object upsert (restore from trash can)")

	s.NoError(s.cmdb.ObjectCreate("tcb2-after", "tcb_t", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("h3"))))
	s.requireCacheQuiesced("write after upsert-restore")
}
