package crud_test

// Root-cause hunt, group A: concurrency at the CRUD level — the layer the
// production incident actually ran through.
//
// Every CMDB edge is written as two halves (the owner's out-side and the
// mirror in-key on the target) by separate calls. The cache layer is already
// proven not to lose them (rehydrate and the maintenance sweep are green in
// both representations), so if half-edges are born anywhere, it is here:
// concurrent object writes, concurrent link writes, and the create/delete
// race that mixes the two.
//
// Every assertion is the same one the integrity matrix uses: an object is
// either whole (all twelve skeleton keys) or entirely absent — never a half.

import (
	"fmt"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

// Concurrent upserts of the SAME object must converge on a whole skeleton:
// they all write the same three edges, and no interleaving may leave one half
// of one edge behind.
func (s *CMDBClientContractTestSuite) Test_RootCause_ConcurrentUpsertsSameObject() {
	s.bootstrap()
	const objType = "CcuType"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))

	const writers = 8
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			body := easyjson.NewJSONObjectWithKeyValue("w", easyjson.NewJSON(w))
			_ = s.dbc.CMDB.ObjectUpdate("ccu-1", body, false, objType)
		}(w)
	}
	wg.Wait()

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	assertObjectIntegrity(s.T(), c, dm, "ccu-1", objType, "concurrent upserts of one object")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false), "objects vertex")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain(objType, false), "type vertex")
}

// Many objects of one type created concurrently: the type vertex accumulates
// one edge per object, and none of them may end up half-written.
func (s *CMDBClientContractTestSuite) Test_RootCause_ConcurrentObjectCreationUnderOneType() {
	s.bootstrap()
	const objType = "CcbType"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))

	const n = 40
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.dbc.CMDB.ObjectUpdate(fmt.Sprintf("ccb-%03d", i), easyjson.NewJSONObject(), false, objType)
		}(i)
	}
	wg.Wait()

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	for i := 0; i < n; i++ {
		assertObjectIntegrity(s.T(), c, dm, fmt.Sprintf("ccb-%03d", i), objType, "concurrent creation under one type")
	}
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false), "objects vertex")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain(objType, false), "type vertex")
}

// Object links created and deleted concurrently between the same pair: the
// edge must end up either whole or gone, never half.
func (s *CMDBClientContractTestSuite) Test_RootCause_ConcurrentLinkCreateDelete() {
	s.bootstrap()
	const tA, tB = "CclA", "CclB"
	s.NoError(s.dbc.CMDB.TypeCreate(tA))
	s.NoError(s.dbc.CMDB.TypeCreate(tB))
	s.NoError(s.dbc.CMDB.TypesLinkCreate(tA, tB, "ccl-rel", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("ccl-a", easyjson.NewJSONObject(), false, tA))
	s.NoError(s.dbc.CMDB.ObjectUpdate("ccl-b", easyjson.NewJSONObject(), false, tB))

	var wg sync.WaitGroup
	for r := 0; r < 12; r++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.dbc.CMDB.ObjectsLinkUpdate("ccl-a", "ccl-b", nil, easyjson.NewJSONObject(), false, "ccl-edge") }()
		go func() { defer wg.Done(); _ = s.dbc.CMDB.ObjectsLinkDelete("ccl-a", "ccl-b") }()
	}
	wg.Wait()

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	// Both endpoints must be whole objects whatever the race decided, and the
	// contested edge must not survive as a half.
	assertObjectIntegrity(s.T(), c, dm, "ccl-a", tA, "after link create/delete race")
	assertObjectIntegrity(s.T(), c, dm, "ccl-b", tB, "after link create/delete race")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithThisDomain("ccl-a", false), "link owner")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithThisDomain("ccl-b", false), "link target")
}

// The sharpest race: an object is deleted while it is being upserted. The
// outcome may be either — alive or parked — but never a torn skeleton, and
// never a half-edge left on objects or on the type.
func (s *CMDBClientContractTestSuite) Test_RootCause_ConcurrentDeleteAndUpsert() {
	s.bootstrap()
	const objType = "CcdType"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))

	const rounds = 12
	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	objectsID := dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false)
	typeID := dm.CreateObjectIDWithHubDomain(objType, false)

	for r := 0; r < rounds; r++ {
		id := fmt.Sprintf("ccd-%02d", r)
		s.Require().NoError(s.dbc.CMDB.ObjectUpdate(id, easyjson.NewJSONObject(), false, objType))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.dbc.CMDB.ObjectDelete(id) }()
		go func() {
			defer wg.Done()
			_ = s.dbc.CMDB.ObjectUpdate(id, easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1)), false, objType)
		}()
		wg.Wait()

		ctx := fmt.Sprintf("delete/upsert race, round %d", r)
		// Whatever won, nothing may dangle.
		assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithThisDomain(id, false), ctx+" (object)")
		assertMirrorSymmetry(s.T(), c, objectsID, ctx+" (objects)")
		assertMirrorSymmetry(s.T(), c, typeID, ctx+" (type)")

		// And if the object is still enumerated by objects, it must be whole.
		if _, err := c.GetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, objectsID, id)); err == nil {
			assertObjectIntegrity(s.T(), c, dm, id, objType, ctx+": still enumerated, so it must be whole")
		}
	}
}

// The remaining candidate: op_time is not the graph's own clock — it arrives
// in the payload, so it is the CALLER's clock. A delete stamped from the
// future (a skewed client, a replayed tail) is applied as the newest write of
// those keys; a subsequent, honestly-timed create then loses the last-writer
// race for exactly those keys and silently writes nothing — while the keys the
// delete never touched are written normally.
//
// That is a half-written object produced without any cache defect, and it is
// the shape seen in production: some halves present, others missing, KV
// perfectly consistent with the (wrong) decision.
func (s *CMDBClientContractTestSuite) Test_RootCause_SkewedOpTimeDeleteThenRecreate() {
	s.bootstrap()
	const objType = "CskType"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))
	s.NoError(s.dbc.CMDB.ObjectUpdate("csk-1", easyjson.NewJSONObject(), false, objType))

	// Delete stamped an hour ahead — a client whose clock ran away.
	future := time.Now().Add(time.Hour).UnixNano()
	payload := easyjson.NewJSONObjectWithKeyValue("op_time", easyjson.NewJSON(future))
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete",
		s.SetThisDomainPreffix("csk-1"), &payload, nil)
	s.NoError(err)

	// Honest recreate afterwards, through the normal client path. Whatever it
	// reports matters: silence here means the caller believes the object is
	// back while the graph holds a torn one.
	recreateErr := s.dbc.CMDB.ObjectUpdate("csk-1", easyjson.NewJSONObject(), false, objType)
	s.T().Logf("recreate after future-stamped delete returned: %v", recreateErr)

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	assertObjectIntegrity(s.T(), c, dm, "csk-1", objType,
		"recreate after a future-stamped delete must not leave a half-written object")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false), "objects vertex")
	assertMirrorSymmetry(s.T(), c, dm.CreateObjectIDWithHubDomain(objType, false), "type vertex")
}
