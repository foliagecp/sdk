package crud_test

// A delete the time guard partly refuses must not leave a vertex without its
// body and with its links.
//
// cache.DeleteValue refuses a deletion older than the value it would remove
// and says so, but the vertex.delete cascade ignores the answer: every link key
// it fails to remove stays, the body after them goes, and the reply is OK.
//
// The times only need to differ in the right order. CreateObject writes the
// body and its type links under one op time, but a load from KV — a restart,
// or the passive→active rehydrate — stamps every key with the moment it was
// read, in stream order, so the body (written first) becomes OLDER than the
// links written right after it. A delete whose op_time falls between the two
// then removes the body and none of the links.

import (
	"context"
	"fmt"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

// reloadFromKV drains the WAL into KV and rebuilds the cache from KV alone.
func (s *TrashCanTestSuite) reloadFromKV() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s.Require().NoError(s.Runtime().Domain.WaitForKVCaughtUp(ctx, 60*time.Second))
	s.Require().NoError(s.Runtime().Domain.Cache().RehydrateFromKV(ctx))
}

// staleDeleteFixture creates an object with an application link, reloads the
// cache from KV and returns the body's update time after the reload, checking
// that the object's link keys came out newer than it.
func (s *TrashCanTestSuite) staleDeleteFixture(typeName, objName string) (objID string, vertices []string, bodyTime int64) {
	peerName := typeName + "-p" // an id that does not contain objName
	s.boot()
	s.NoError(s.cmdb.TypeCreate(typeName))
	s.NoError(s.cmdb.TypeCreate(typeName + "_peer"))
	s.NoError(s.cmdb.TypesLinkCreate(typeName, typeName+"_peer", "sd-rel", nil))
	s.NoError(s.cmdb.ObjectCreate(objName, typeName, easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectCreate(peerName, typeName+"_peer", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectsLinkCreate(objName, peerName, "sd-edge", nil))

	s.reloadFromKV()

	c := s.Runtime().Domain.Cache()
	objID = s.SetThisDomainPreffix(objName)
	vertices = []string{
		objID,
		s.SetThisDomainPreffix(typeName),
		s.SetThisDomainPreffix(crud.BUILT_IN_OBJECTS),
		s.SetThisDomainPreffix(peerName),
	}
	bodyTime = c.GetValueUpdateTime(objID)
	s.Require().Greater(bodyTime, int64(0), "the body must be there after the reload")

	newer, total := 0, 0
	for _, k := range s.residueOf(objID, vertices...) {
		if k == objID {
			continue
		}
		total++
		if c.GetValueUpdateTime(k) > bodyTime {
			newer++
		}
	}
	s.T().Logf("after the reload: body t=%d, %d of %d link keys are newer than the body", bodyTime, newer, total)
	s.Require().Positive(newer, "the reload must have stamped the link keys after the body — the precondition of this test")
	return objID, vertices, bodyTime
}

// The low-level delete with an op_time equal to the body's time: the body may
// be deleted, the links may not — and it must not be both.
func (s *TrashCanTestSuite) Test_StaleDelete_VertexDeleteLeavesNoBodylessVertex() {
	objID, vertices, bodyTime := s.staleDeleteFixture("sd_v", "sd1")
	c := s.Runtime().Domain.Cache()

	payload := easyjson.NewJSONObjectWithKeyValue("op_time", easyjson.NewJSON(bodyTime))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", "sd1", &payload, nil)
	s.Require().NoError(err)
	status := res.GetByPath("status").AsStringDefault("")

	bodyGone := !c.ExistsJson(objID)
	left := s.residueOf(objID, vertices...)
	s.T().Logf("vertex.delete status=%s body gone=%v, keys left=%d: %v", status, bodyGone, len(left), left)

	s.Falsef(bodyGone && len(left) > 0,
		"the body was deleted while %d link keys stayed (status=%s): %v", len(left), status, left)
	if len(left) > 0 {
		s.NotEqual("ok", status, "a delete that left link keys behind must not report OK")
	}
}

// The same stale delete through the object API, which parks: it never deletes
// the body, but it must not report OK for a cascade that removed nothing
// while hanging the trash links on top of the old ones.
func (s *TrashCanTestSuite) Test_StaleDelete_ObjectDeleteDoesNotReportOk() {
	objID, vertices, bodyTime := s.staleDeleteFixture("sd_o", "sd2")
	c := s.Runtime().Domain.Cache()

	payload := easyjson.NewJSONObjectWithKeyValue("op_time", easyjson.NewJSON(bodyTime))
	status := s.objectOp("object.delete", "sd2", payload)

	left := s.residueOf(objID, vertices[1:]...) // what the original type, objects and the peer still hold
	s.T().Logf("object.delete status=%s body kept=%v, parked=%v, keys on others left=%d: %v",
		status, c.ExistsJson(objID), s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "sd2"), len(left), left)

	if len(left) > 0 {
		s.NotEqual("ok", status, fmt.Sprintf("a delete that left %d link keys behind must not report OK", len(left)))
	}
}
