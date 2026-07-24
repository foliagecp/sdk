package crud_test

import (
	"strings"
	"testing"
	"time"

	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

// InheritanceCacheTestSuite guards RecalculateInheritanceCacheForTypeAtSelfIDIfNeeded
// after its fast path (direct in-memory version reads instead of mediator
// vertex.reads): a subtype link must still be reflected in the child's
// cache.parent_types on the next ReadType, and stay stable on subsequent reads
// (when the fast path short-circuits because versions match).
type InheritanceCacheTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestInheritanceCacheTestSuite(t *testing.T) {
	suite.Run(t, new(InheritanceCacheTestSuite))
}

func (s *InheritanceCacheTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_TYPES, 15*time.Second)
	s.waitForVertex(crud.BUILT_IN_OBJECTS, 15*time.Second)

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

func (s *InheritanceCacheTestSuite) waitForVertex(id string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear within %s", id, timeout)
}

func (s *InheritanceCacheTestSuite) parentTypes(typeName string) []string {
	data, err := s.dbc.CMDB.TypeRead(typeName)
	s.NoError(err)
	arr, _ := data.GetByPath("body.cache.parent_types").AsArrayString()
	return arr
}

func containsToken(arr []string, token string) bool {
	for _, e := range arr {
		if strings.Contains(e, token) {
			return true
		}
	}
	return false
}

func (s *InheritanceCacheTestSuite) Test_InheritanceRecomputedAfterSubtypeLink() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("itbase"))
	s.NoError(s.dbc.CMDB.TypeCreate("itchild"))

	// Before any subtype link the child has no parent in its inheritance cache.
	s.False(containsToken(s.parentTypes("itchild"), "itbase"),
		"child must have no parent before the subtype link")

	// Linking as a subtype bumps the global type-model version, so the child's
	// cached version no longer matches: the next ReadType must recompute.
	s.NoError(s.dbc.CMDB.TypeSetSubType("itbase", "itchild"))

	s.True(containsToken(s.parentTypes("itchild"), "itbase"),
		"child must list itbase as a parent after the subtype link (cache recomputed)")

	// Second read: versions now match, the fast path short-circuits the
	// recompute — the cache must remain correct, not be lost.
	s.True(containsToken(s.parentTypes("itchild"), "itbase"),
		"parent must remain on a second read (fast-path skip must not drop it)")
}

// Test_SetSubTypeIdempotentAcrossBootstrapRestarts pins the schema-bootstrap
// contract: applications re-run their full type-schema setup on every restart,
// so a repeated TypeSetSubType for an already-linked pair must be a clean
// no-op — nil error, inheritance intact — NOT a "link already exists" failure.
// (TypeSetSubType issues link.update with upsert=true: absent edge delegates
// to link.create with full invariant checks; present edge is an IDLE no-op.)
func (s *InheritanceCacheTestSuite) Test_SetSubTypeIdempotentAcrossBootstrapRestarts() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("itbase3"))
	s.NoError(s.dbc.CMDB.TypeCreate("itchild3"))

	// First bootstrap: fresh link, must succeed exactly as before.
	s.NoError(s.dbc.CMDB.TypeSetSubType("itbase3", "itchild3"))
	s.True(containsToken(s.parentTypes("itchild3"), "itbase3"),
		"child must list itbase3 as a parent after the first subtype link")

	// Simulated restarts: same call repeated must stay error-free and must
	// not disturb the existing inheritance state.
	for i := 0; i < 3; i++ {
		s.NoErrorf(s.dbc.CMDB.TypeSetSubType("itbase3", "itchild3"),
			"repeat #%d of TypeSetSubType must be an idempotent no-op", i+1)
	}
	s.True(containsToken(s.parentTypes("itchild3"), "itbase3"),
		"parent must survive repeated subtype-set calls")

	// The no-op repeats must not have broken removal either.
	s.NoError(s.dbc.CMDB.TypeRemoveSubType("itbase3", "itchild3"))
	s.False(containsToken(s.parentTypes("itchild3"), "itbase3"),
		"parent must be gone after removal following repeated sets")
}

func (s *InheritanceCacheTestSuite) Test_InheritanceRecomputedAfterSubtypeRemoval() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("itbase2"))
	s.NoError(s.dbc.CMDB.TypeCreate("itchild2"))
	s.NoError(s.dbc.CMDB.TypeSetSubType("itbase2", "itchild2"))
	s.True(containsToken(s.parentTypes("itchild2"), "itbase2"))

	// Removing the subtype link bumps the version again; the parent must drop.
	s.NoError(s.dbc.CMDB.TypeRemoveSubType("itbase2", "itchild2"))
	s.False(containsToken(s.parentTypes("itchild2"), "itbase2"),
		"parent must be gone after the subtype removal (cache recomputed)")
}
