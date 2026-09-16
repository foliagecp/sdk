package crud_test

// Deleting a types-link after the type model has moved.
//
// Every schema change that runs the polytype cascade — a type delete, a
// types-link delete, a subtype set or delete — stamps the types root with a new
// version, and every type's inheritance cache is stale from then until the
// type is next read, when it is recomputed under a write lock on the type.
//
// DeleteTypesLink read-guarded the from-type and, holding that guard, asked
// type.read for the type's objects. The read found the cache stale, went to
// recompute it, and waited for a write lock on the very type its caller was
// holding — for the whole key-lock timeout, five minutes by default, after
// which it proceeded without the lock and logged a warning. So the second
// types-link delete after any schema change stalled the schema operation for
// five minutes, and the client that asked for it timed out long before.
//
// The recompute now happens before the guard is taken, the way ReadType itself
// orders it. Pinned with a short lock timeout: with the stall the delete takes
// the whole timeout, without it a few milliseconds.
//
// The timeout is process-wide and every worker reads it, so it is put back in
// T().Cleanup — after TearDownTest has shut this runtime down — not in a defer,
// which would run while the runtime's goroutines are still working.

import (
	"testing"
	"time"

	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type TypesLinkDeleteStallTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestTypesLinkDeleteStallTestSuite(t *testing.T) {
	suite.Run(t, new(TypesLinkDeleteStallTestSuite))
}

func (s *TypesLinkDeleteStallTestSuite) Test_SecondTypesLinkDeleteDoesNotStall() {
	const lockTimeout = 2 * time.Second
	previous := crud.SetGraphKeyLockTimeoutForTest(lockTimeout)
	s.T().Cleanup(func() { crud.SetGraphKeyLockTimeoutForTest(previous) })

	crud.RegisterAllFunctionTypes(s.Runtime())
	s.Require().NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_TYPES); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.Require().NoError(err)
	s.cmdb = dbc.CMDB

	for _, t := range []string{"tls_a", "tls_b", "tls_c"} {
		s.Require().NoError(s.cmdb.TypeCreate(t))
	}
	s.Require().NoError(s.cmdb.TypesLinkCreate("tls_a", "tls_b", "tls_r1", nil))
	s.Require().NoError(s.cmdb.TypesLinkCreate("tls_a", "tls_c", "tls_r2", nil))

	// The first delete moves the type model version; tls_a is stale from here.
	s.Require().NoError(s.cmdb.TypesLinkDelete("tls_a", "tls_b"))

	start := time.Now()
	s.Require().NoError(s.cmdb.TypesLinkDelete("tls_a", "tls_c"))
	took := time.Since(start)
	s.Lessf(took, lockTimeout/2, "the second types-link delete took %s: it waited out the key-lock timeout (%s) on its own type", took, lockTimeout)

	_, err = s.cmdb.TypesLinkRead("tls_a", "tls_c")
	s.Error(err, "the link must be gone")
	_, err = s.cmdb.TypeRead("tls_a")
	s.NoError(err, "and the type must still read")
}
