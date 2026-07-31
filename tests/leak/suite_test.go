//go:build leak

package leak

import (
	"time"

	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/test"
)

// leakSuite is the shared base for runtime-backed leak scenarios: embedded
// NATS + fresh runtime per test method (test.StatefunTestSuite), the CMDB
// boot sequence, and the quiesce protocol every sample depends on.
//
// A scenario's whole warmup+measure loop MUST live inside one Test method —
// SetupTest rebuilds the world (server, runtime, cache) for each method.
type leakSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

// bootCRUD registers graph CRUD (plus any extra registrars), starts the
// runtime, waits for the built-in vertices and wires the DB client. Copied
// from the established pattern in embedded/graph/debug and embedded/graph/
// batch suites.
func (s *leakSuite) bootCRUD(register ...func(*statefun.Runtime)) {
	crud.RegisterAllFunctionTypes(s.Runtime())
	for _, reg := range register {
		reg(s.Runtime())
	}
	s.Require().NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_TYPES)
	s.waitForVertex(crud.BUILT_IN_OBJECTS)
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.Require().NoError(err)
	s.dbc = dbc
}

func (s *leakSuite) waitForVertex(id string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear in time", id)
}

func (s *leakSuite) cacheStore() *cache.Store {
	return s.Runtime().Domain.Cache()
}

// domainID prefixes an id with this runtime's domain, the form under which
// graph keys actually live in the cache.
func (s *leakSuite) domainID(id string) string {
	return s.SetThisDomainPreffix(id)
}

// kvCount returns the number of live cache keys matching the pattern. The
// cache supports a trailing `.*` (one level) or `.>` (recursive) wildcard.
func (s *leakSuite) kvCount(pattern string) int {
	return len(s.cacheStore().GetKeysByPattern(pattern))
}

// quiesce drives the runtime to a measurable steady state: WAL fully drained,
// then at least two maintenance sweeps completed strictly after the drain so
// every tombstone cascade from the cycle's deletes has been collapsed.
func (s *leakSuite) quiesce() {
	cs := s.cacheStore()

	drainDeadline := time.Now().Add(30 * time.Second)
	for cs.HasPendingWrites() && time.Now().Before(drainDeadline) {
		time.Sleep(20 * time.Millisecond)
	}
	s.Require().False(cs.HasPendingWrites(), "WAL did not drain within 30s")

	sweepStart := cs.StatsForTest().SweepRuns
	sweepDeadline := time.Now().Add(30 * time.Second)
	for cs.StatsForTest().SweepRuns < sweepStart+2 && time.Now().Before(sweepDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	s.Require().GreaterOrEqual(cs.StatsForTest().SweepRuns, sweepStart+2,
		"cache maintenance sweep did not advance within 30s (lazy writer stalled?)")
}

// newRunner builds a CycleRunner wired to this suite's quiesce protocol with
// the standard env-driven sizing.
func (s *leakSuite) newRunner(scenario string, cycle func(i int) error, collect func(*Sample)) *CycleRunner {
	return &CycleRunner{
		Scenario: scenario,
		Warmup:   warmupCycles(),
		Measure:  measureCycles(),
		Cycle:    cycle,
		Collect:  collect,
		Quiesce:  s.quiesce,
	}
}
