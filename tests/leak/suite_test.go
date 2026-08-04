//go:build leak

package leak

import (
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
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
		// The embedded NATS server shares this process: JetStream/KV churn
		// grows ITS heap by design (per-subject state, retained tombstones).
		// Assert the SDK's own share; report the server's.
		SplitNatsHeap: true,
	}
}

// coreMetrics are the deterministic invariants every graph-churn scenario
// asserts: cache tree population, WAL backlog, the graph key mutex and the
// process-global object-type cache must all return to their post-warmup
// baseline once a cycle's state has been fully deleted.
var coreMetrics = []string{
	"cache_live_values",
	"cache_total_nodes",
	"cache_tombstones",
	"cache_pending_txs",
	"cache_active_ops",
	"graph_keymutex_entries",
	"object_type_cache",
	"type_edge_cache",
	"type_object_triggers_cache",
	"types_link_triggers_cache",
	"type_hrn_field_cache",
}

func (s *leakSuite) collectCore(smp *Sample) {
	st := s.cacheStore().StatsForTest()
	smp.Custom["cache_live_values"] = float64(st.LiveValues)
	smp.Custom["cache_total_nodes"] = float64(st.TotalNodes)
	smp.Custom["cache_tombstones"] = float64(st.Tombstones)
	smp.Custom["cache_pending_txs"] = float64(st.PendingTxs)
	smp.Custom["cache_active_ops"] = float64(st.ActiveOps)
	smp.Custom["graph_keymutex_entries"] = float64(crud.GraphKeyMutexEntriesForTest())
	// EVERY process-global crud cache is under an invariant. Lesson learned
	// the hard way: a per-entry structural leak (one sync.Map record per
	// call) is far below any statistically honest heap floor — deterministic
	// counters are the only instrument that catches it, so their coverage
	// must be exhaustive, not representative.
	smp.Custom["object_type_cache"] = float64(crud.ObjectTypeCacheSizeForTest())
	smp.Custom["type_edge_cache"] = float64(crud.TypeEdgeCacheSizeForTest())
	smp.Custom["type_object_triggers_cache"] = float64(crud.TypeObjectTriggersCacheSizeForTest())
	smp.Custom["types_link_triggers_cache"] = float64(crud.TypesLinkTriggersCacheSizeForTest())
	smp.Custom["type_hrn_field_cache"] = float64(crud.TypeHRNFieldCacheSizeForTest())
}

func (s *leakSuite) assertCoreStable(rep *Report) {
	for _, m := range coreMetrics {
		rep.AssertStable(s.T(), m)
	}
}

// leakBody builds a vertex/object body with a blob of roughly `size` bytes so
// churned state has realistic weight.
func leakBody(size int) easyjson.JSON {
	b := easyjson.NewJSONObject()
	b.SetByPath("payload", easyjson.NewJSON(strings.Repeat("x", size)))
	return b
}
