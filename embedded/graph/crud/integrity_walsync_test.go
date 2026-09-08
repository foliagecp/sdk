package crud_test

// Does a write the LWW guard REJECTED still travel to KV?
//
// This runs on a live runtime, because that is the only place WAL publishing
// is active: it needs a transaction generator (domain.go) and
// SetWALWriteEnabled (runtime.go). A store built by hand publishes nothing, so
// the question cannot be asked in the cache package at all.
//
// The question matters because publishDirtyOp is called regardless of what the
// guard decided (cache.go, SetValue/SetValueJSON), tieredSet reports "handled"
// regardless (tiering.go), and SetValue returns true either way. If the
// rejected value does reach KV, then KV and the operating representation
// disagree — and the disagreement survives until the next restart, when
// rehydrate pulls KV back in. That is exactly the production picture: KV whole,
// memory missing a half, the object unreadable as an object.
//
// Rehydrate is the probe: it reloads the cache from KV, so whatever KV holds
// becomes visible without reaching into the bucket by hand.

import (
	"context"
	"fmt"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
)

func (s *CMDBClientContractTestSuite) waitWALDrained() {
	s.T().Helper()
	c := s.Runtime().Domain.Cache()
	deadline := time.Now().Add(30 * time.Second)
	for c.HasPendingWrites() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	s.Require().False(c.HasPendingWrites(), "WAL did not drain in time")
}

// A stale write must be rejected everywhere, not only in memory: if it reaches
// KV, the next rehydrate installs the value the cache refused.
func (s *CMDBClientContractTestSuite) Test_RootCause_RejectedWriteMustNotReachKV() {
	s.bootstrap()
	c := s.Runtime().Domain.Cache()
	key := s.SetThisDomainPreffix("wal-obj") + ".in." + s.SetThisDomainPreffix("objects") + ".wal-obj"

	s.Require().True(c.SetValue(key, []byte("__object"), true, 2000))
	c.SetValue(key, []byte("STALE"), true, 1000) // older than the write above

	got, err := c.GetValue(key)
	s.Require().NoError(err)
	s.Require().Equal("__object", string(got), "precondition: the guard must reject the stale write in memory")

	s.waitWALDrained()
	s.Require().NoError(c.RehydrateFromKV(context.Background()))

	after, err := c.GetValue(key)
	s.Require().NoErrorf(err, "the key vanished after rehydrate: KV never received the accepted write")
	s.Equalf("__object", string(after),
		"the write the cache REJECTED still reached KV — cache and KV diverge, and rehydrate installs the rejected value")
}

// The mirror case: a stale DELETE the cache refused must not erase the key in
// KV either, or the next restart loses what the cache considered alive.
func (s *CMDBClientContractTestSuite) Test_RootCause_RejectedDeleteMustNotReachKV() {
	s.bootstrap()
	c := s.Runtime().Domain.Cache()
	key := s.SetThisDomainPreffix("wal-obj2") + ".in." + s.SetThisDomainPreffix("objects") + ".wal-obj2"

	s.Require().True(c.SetValue(key, []byte("__object"), true, 2000))
	c.DeleteValue(key, true, 1000) // older than the write: must be refused

	s.Require().Truef(c.Exists(key), "precondition: the guard must reject the stale delete in memory")

	s.waitWALDrained()
	s.Require().NoError(c.RehydrateFromKV(context.Background()))

	after, err := c.GetValue(key)
	s.Require().NoErrorf(err, "the delete the cache REJECTED still erased the key in KV; after rehydrate it is gone")
	s.Equal("__object", string(after))
}

// Same question at the CRUD level, on a real object: a body write stamped older
// than the object's own creation must not make KV and the cache disagree.
func (s *CMDBClientContractTestSuite) Test_RootCause_StaleObjectWriteKeepsCacheAndKVInSync() {
	s.bootstrap()
	const objType = "WalType"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))
	s.NoError(s.dbc.CMDB.ObjectUpdate("wal-1", easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1)), false, objType))

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	assertObjectIntegrity(s.T(), c, dm, "wal-1", objType, "precondition")

	s.waitWALDrained()
	s.Require().NoError(c.RehydrateFromKV(context.Background()))
	assertObjectIntegrity(s.T(), c, dm, "wal-1", objType,
		"after rehydrate the object must still be whole: KV must agree with the cache")
}

// The other direction — the one that actually matches production, where KV was
// WHOLE and memory was missing a half.
//
// WAL publishing is switched off while a runtime is passive
// (runtime.go: SetWALWriteEnabled(false) on becomePassive). publishDirtyOp
// returns immediately in that state (cache.go), so anything that still manages
// to change the cache while the flag is down changes it FOR THE CACHE ONLY:
// KV keeps the old, whole picture and the operating representation quietly
// loses a key. The divergence then lives until the next promotion, because
// only a promotion rehydrates from KV.
func (s *CMDBClientContractTestSuite) Test_RootCause_CacheChangeWhileWALDisabledDivergesFromKV() {
	s.bootstrap()
	const objType = "PasType"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))
	s.NoError(s.dbc.CMDB.ObjectUpdate("pas-1", easyjson.NewJSONObject(), false, objType))

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	assertObjectIntegrity(s.T(), c, dm, "pas-1", objType, "precondition")
	s.waitWALDrained()

	objID := dm.CreateObjectIDWithThisDomain("pas-1", false)
	objectsID := dm.CreateObjectIDWithHubDomain("objects", false)
	inKey := objID + ".in." + objectsID + ".pas-1"
	s.Require().True(c.Exists(inKey), "the mirror in-key must exist to begin with")

	// The runtime goes passive: WAL publishing stops.
	c.SetWALWriteEnabled(false)
	c.DeleteValue(inKey, true, time.Now().UnixNano()) // legitimate delete, honest time
	s.Require().False(c.Exists(inKey), "the key is gone from the cache")
	c.SetWALWriteEnabled(true)

	// KV never heard about it. A promotion rehydrates from KV and the key is
	// back — proving the cache had diverged from KV while the flag was down.
	s.Require().NoError(c.RehydrateFromKV(context.Background()))
	s.Truef(c.Exists(inKey),
		"KV still holds the key the cache dropped — this is the production divergence: KV whole, memory missing a half")

	assertObjectIntegrity(s.T(), c, dm, "pas-1", objType, "after rehydrate the object must be whole again")
}

// The window that produces the production shape.
//
// The passive guard sits at the START of handling a message
// (function_type.go: handleMsgForID). An operation that passed it and is still
// running when the runtime steps down keeps writing — and by then
// SetWALWriteEnabled(false) has already been applied, so those writes land in
// the cache ALONE. A CMDB write touches twelve keys one by one, so the ones
// written before the demotion reach KV and the ones after do not: the two
// representations end up holding different halves of the same object.
//
// Here the flag is lowered around a whole ObjectDelete, which is the same
// window widened to something a test can hit deterministically.
func (s *CMDBClientContractTestSuite) Test_RootCause_CrudDuringDemotionDivergesFromKV() {
	s.bootstrap()
	const objType = "DemType"
	s.NoError(s.dbc.CMDB.TypeCreate(objType))
	s.NoError(s.dbc.CMDB.ObjectUpdate("dem-1", easyjson.NewJSONObject(), false, objType))

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	assertObjectIntegrity(s.T(), c, dm, "dem-1", objType, "precondition")
	s.waitWALDrained()

	// The runtime loses the lock: publishing stops, in-flight work does not.
	c.SetWALWriteEnabled(false)
	delErr := s.dbc.CMDB.ObjectDelete("dem-1")
	s.T().Logf("ObjectDelete while WAL publishing is down returned: %v", delErr)

	objectsID := dm.CreateObjectIDWithHubDomain("objects", false)
	_, stillEnumerated := c.GetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, objectsID, "dem-1"))
	s.T().Logf("after the delete, cache still enumerates the object: %v", stillEnumerated == nil)

	c.SetWALWriteEnabled(true)
	s.Require().NoError(c.RehydrateFromKV(context.Background()))

	// KV never saw the delete, so rehydrate brings the object back whole. The
	// cache and KV had been describing different graphs.
	assertObjectIntegrity(s.T(), c, dm, "dem-1", objType,
		"KV never learned about the delete performed while publishing was down")
}
