package crud_test

// Functional tests of the graph-native trash can ("nothing dies instantly"):
// deleting an object keeps its body, cascades its links away and re-links it
// under the built-in trash-can type (original type + deletion moment on the
// edge); a true re-creation of the id restores it, grafting the protected
// body fields (PROTECTED_BODY_FIELDS, default "usr"); the bin is capped by
// object count with oldest-first eviction.

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type TrashCanTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestTrashCanTestSuite(t *testing.T) { suite.Run(t, new(TrashCanTestSuite)) }

func (s *TrashCanTestSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_TRASH_CAN); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb = dbc.CMDB
}

func usrBody(hostname, responsible string, tags ...string) easyjson.JSON {
	body := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON(hostname))
	usr := easyjson.NewJSONObject()
	usr.SetByPath("attrs.responsible", easyjson.NewJSON(responsible))
	usr.SetByPath("tags", easyjson.NewJSON(tags))
	body.SetByPath("usr", usr)
	return body
}

func (s *TrashCanTestSuite) vertexExists(id string) bool {
	return s.Runtime().Domain.Cache().ExistsJson(s.SetThisDomainPreffix(id))
}

// typeHoldsObject reports whether the ltype index records typeName→__object→id.
func (s *TrashCanTestSuite) typeHoldsObject(typeName, id string) bool {
	key := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern,
		s.SetThisDomainPreffix(typeName), crud.OBJECT_TYPELINK, s.SetThisDomainPreffix(id))
	return s.Runtime().Domain.Cache().Exists(key)
}

// trashEdgeBody returns the trash_can→object edge body (null JSON if absent).
func (s *TrashCanTestSuite) trashEdgeBody(id string) *easyjson.JSON {
	key := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern,
		s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN), id)
	if b, err := s.Runtime().Domain.Cache().GetValueJSON(key); err == nil {
		return b
	}
	return easyjson.NewJSONNull().GetPtr()
}

func (s *TrashCanTestSuite) readBody(id string) easyjson.JSON {
	res, err := s.cmdb.ObjectRead(id)
	s.Require().NoError(err)
	return res.GetByPath("body")
}

// Deleting a live object PARKS it: body kept, links gone, re-homed under the
// trash-can type with {original_type, deleted_at} on the edge.
func (s *TrashCanTestSuite) Test_Delete_ParksUnderTrashCan() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.ObjectCreate("tc1", "tc_t", usrBody("srv", "alice", "prod")))

	s.NoError(s.cmdb.ObjectDelete("tc1"))

	s.True(s.vertexExists("tc1"), "parked object keeps its body in the graph")
	s.False(s.typeHoldsObject("tc_t", "tc1"), "the original type must not enumerate the parked object")
	s.True(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tc1"), "the trash-can type must hold the parked object")

	edge := s.trashEdgeBody("tc1")
	s.Equal("tc_t", edge.GetByPath("original_type").AsStringDefault(""), "original type recorded on the trash edge")
	s.True(edge.GetByPath("deleted_at").AsNumericDefault(0) > 0, "deletion moment recorded on the trash edge")
}

// Re-creation of a parked id restores it: protected fields grafted, fresh
// discovery fields win, trash links gone, object typed back.
func (s *TrashCanTestSuite) Test_Recreate_RestoresProtectedOnly() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.ObjectCreate("tc2", "tc_t", usrBody("srv-old", "alice", "prod")))
	s.NoError(s.cmdb.ObjectDelete("tc2"))

	s.NoError(s.cmdb.ObjectCreate("tc2", "tc_t", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv-new"))))

	body := s.readBody("tc2")
	s.Equal("srv-new", body.GetByPath("hostname").AsStringDefault(""), "fresh discovery fields win")
	s.Equal("alice", body.GetByPath("usr.attrs.responsible").AsStringDefault(""), "usr attrs must be restored")
	tags, _ := body.GetByPath("usr.tags").AsArrayString()
	s.Equal([]string{"prod"}, tags, "usr tags must be restored")

	s.True(s.typeHoldsObject("tc_t", "tc2"), "restored object is typed back")
	s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tc2"), "trash link must be gone after restore")
}

// An id never deleted creates clean — no usr materializes.
func (s *TrashCanTestSuite) Test_CleanCreate_NoRestore() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.ObjectCreate("tc3", "tc_t", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("h"))))
	s.False(s.readBody("tc3").PathExists("usr"))
}

// Upsert (ObjectUpdate upsert=true) on a parked id is the object's RETURN —
// it restores exactly like plain create.
func (s *TrashCanTestSuite) Test_UpsertOnParked_Restores() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.ObjectCreate("tc4", "tc_t", usrBody("h1", "bob")))
	s.NoError(s.cmdb.ObjectDelete("tc4"))

	s.NoError(s.cmdb.ObjectUpdate("tc4", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("h2")), true, "tc_t"))

	body := s.readBody("tc4")
	s.Equal("h2", body.GetByPath("hostname").AsStringDefault(""))
	s.Equal("bob", body.GetByPath("usr.attrs.responsible").AsStringDefault(""), "upsert-restored object must carry usr")
	s.True(s.typeHoldsObject("tc_t", "tc4"))
	s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tc4"))
}

// A creation that PASSES a protected field owns it — the parked value must
// not override the caller's.
func (s *TrashCanTestSuite) Test_IncomingProtectedFieldWins() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.ObjectCreate("tc5", "tc_t", usrBody("h", "old-owner")))
	s.NoError(s.cmdb.ObjectDelete("tc5"))

	s.NoError(s.cmdb.ObjectCreate("tc5", "tc_t", usrBody("h", "new-owner")))
	s.Equal("new-owner", s.readBody("tc5").GetByPath("usr.attrs.responsible").AsStringDefault(""))
}

// Deleting an already-parked object is the PHYSICAL deletion.
func (s *TrashCanTestSuite) Test_DeleteParked_ErasesPhysically() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.ObjectCreate("tc6", "tc_t", usrBody("h", "x")))
	s.NoError(s.cmdb.ObjectDelete("tc6")) // park
	s.True(s.vertexExists("tc6"))

	s.NoError(s.cmdb.ObjectDelete("tc6")) // erase
	s.False(s.vertexExists("tc6"), "second delete must physically erase the parked object")
	s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tc6"))
}

// Restore under a DIFFERENT type succeeds (warned in logs) — the object comes
// back under the requested type with its protected fields.
func (s *TrashCanTestSuite) Test_RestoreUnderDifferentType_WarnsButRestores() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.TypeCreate("tc_other"))
	s.NoError(s.cmdb.ObjectCreate("tc7", "tc_t", usrBody("h", "keeper")))
	s.NoError(s.cmdb.ObjectDelete("tc7"))

	s.NoError(s.cmdb.ObjectCreate("tc7", "tc_other", easyjson.NewJSONObject()))

	s.Equal("keeper", s.readBody("tc7").GetByPath("usr.attrs.responsible").AsStringDefault(""))
	s.True(s.typeHoldsObject("tc_other", "tc7"), "restored under the requested (different) type")
	s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tc7"))
}

// Retention AGE: a parked object older than OBJECT_TRASH_CAN_MAX_AGE_SEC is
// physically evicted, while a freshly parked one survives — the count cap is
// left wide open so only the age dimension can be responsible.
func (s *TrashCanTestSuite) Test_RetentionAge_EvictsExpired() {
	s.boot()
	crud.SetTrashCanMaxAgeForTest(150 * time.Millisecond)
	defer crud.SetTrashCanMaxAgeForTest(7 * 24 * time.Hour)

	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.ObjectCreate("tage_old", "tc_t", usrBody("h", "x")))
	s.NoError(s.cmdb.ObjectCreate("tage_new", "tc_t", usrBody("h", "y")))
	s.NoError(s.cmdb.ObjectCreate("tage_trigger", "tc_t", easyjson.NewJSONObject()))

	s.NoError(s.cmdb.ObjectDelete("tage_old")) // will age out
	time.Sleep(250 * time.Millisecond)
	s.NoError(s.cmdb.ObjectDelete("tage_new")) // fresh

	// Any parking runs the retention pass; tage_old is now over the age.
	s.NoError(s.cmdb.ObjectDelete("tage_trigger"))

	s.False(s.vertexExists("tage_old"), "a parked object past the retention age must be evicted")
	s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tage_old"))
	s.True(s.vertexExists("tage_new"), "a freshly parked object must survive the age pass")
	s.True(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tage_new"))
}

// The periodic sweep enforces the age dimension WITHOUT any delete traffic:
// park one object, then simply wait — no further CRUD operations at all.
func (s *TrashCanTestSuite) Test_RetentionAge_PeriodicSweepWithoutTraffic() {
	crud.SetTrashCanSweepIntervalForTest(100 * time.Millisecond) // before StartRuntime
	defer crud.SetTrashCanSweepIntervalForTest(300 * time.Second)
	s.boot()
	crud.SetTrashCanMaxAgeForTest(100 * time.Millisecond)
	defer crud.SetTrashCanMaxAgeForTest(7 * 24 * time.Hour)

	s.NoError(s.cmdb.TypeCreate("tc_t"))
	s.NoError(s.cmdb.ObjectCreate("tsweep", "tc_t", usrBody("h", "z")))
	s.NoError(s.cmdb.ObjectDelete("tsweep"))
	s.True(s.vertexExists("tsweep"), "sanity: parked right after delete")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && s.vertexExists("tsweep") {
		time.Sleep(50 * time.Millisecond)
	}
	s.False(s.vertexExists("tsweep"), "the periodic sweep must evict an expired parked object with no delete traffic")
	s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tsweep"))
}

// The per-run eviction cap (OBJECT_TRASH_CAN_EVICT_BATCH_SIZE) bounds one
// retention pass: with 3 expired objects and a batch of 1, a single pass evicts
// exactly one, and the remainder is picked up by subsequent passes.
func (s *TrashCanTestSuite) Test_EvictBatchSize_BoundsOneRunAndDefersRest() {
	s.boot()
	crud.SetTrashCanMaxAgeForTest(100 * time.Millisecond)
	defer crud.SetTrashCanMaxAgeForTest(7 * 24 * time.Hour)
	crud.SetTrashCanEvictBatchSizeForTest(1)
	defer crud.SetTrashCanEvictBatchSizeForTest(64)

	s.NoError(s.cmdb.TypeCreate("tc_t"))
	expired := []string{"tbat1", "tbat2", "tbat3"}
	for _, id := range expired {
		s.NoError(s.cmdb.ObjectCreate(id, "tc_t", usrBody("h", "x")))
		s.NoError(s.cmdb.ObjectDelete(id))
	}
	for i := 1; i <= 3; i++ {
		s.NoError(s.cmdb.ObjectCreate(fmt.Sprintf("tbat_trig%d", i), "tc_t", easyjson.NewJSONObject()))
	}
	time.Sleep(200 * time.Millisecond) // all three are now past the retention age

	alive := func() int {
		n := 0
		for _, id := range expired {
			if s.vertexExists(id) {
				n++
			}
		}
		return n
	}

	// Each parking runs exactly one retention pass → at most one eviction.
	s.NoError(s.cmdb.ObjectDelete("tbat_trig1"))
	s.Equal(2, alive(), "a single pass with batch=1 must evict exactly one expired object")
	s.NoError(s.cmdb.ObjectDelete("tbat_trig2"))
	s.Equal(1, alive(), "the second pass evicts the next one")
	s.NoError(s.cmdb.ObjectDelete("tbat_trig3"))
	s.Equal(0, alive(), "the deferred remainder is picked up by later passes")
}

// Capacity: parking beyond OBJECT_TRASH_CAN_MAX_OBJECTS evicts the OLDEST
// parked object (physically) and logs it. The age dimension is disabled so only
// the count cap can be responsible.
func (s *TrashCanTestSuite) Test_Capacity_EvictsOldest() {
	s.boot()
	crud.SetTrashCanMaxObjectsForTest(2)
	defer crud.SetTrashCanMaxObjectsForTest(10000)
	crud.SetTrashCanMaxAgeForTest(0) // age dimension off
	defer crud.SetTrashCanMaxAgeForTest(7 * 24 * time.Hour)

	s.NoError(s.cmdb.TypeCreate("tc_t"))
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("tcap%d", i)
		s.NoError(s.cmdb.ObjectCreate(id, "tc_t", easyjson.NewJSONObject()))
	}

	// Park in a known order: tcap1 becomes the oldest parked object.
	s.NoError(s.cmdb.ObjectDelete("tcap1"))
	time.Sleep(5 * time.Millisecond)
	s.NoError(s.cmdb.ObjectDelete("tcap2"))
	time.Sleep(5 * time.Millisecond)
	s.NoError(s.cmdb.ObjectDelete("tcap3")) // pushes the bin to 3 > cap 2 → evict tcap1

	s.False(s.vertexExists("tcap1"), "the oldest parked object must be physically evicted")
	s.True(s.vertexExists("tcap2"), "newer parked objects survive")
	s.True(s.vertexExists("tcap3"))
	s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tcap1"))
	s.True(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tcap2"))
	s.True(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "tcap3"))
}
