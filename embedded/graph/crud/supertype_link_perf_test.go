package crud_test

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

// SuperTypeLinkPerfTestSuite guards against the cross-pack (supertype) link
// regression reproduced by the standalone `linkbench`: creating a link through
// the polytype supertype API re-resolved each endpoint's inheritance via a
// nested type.read, and because statefun processes one message per id at a
// time, every link re-reading the SAME few endpoint types queued on those
// types' single-threaded per-id workers. The per-link cost then grew with
// fan-out instead of parallelising — a hub object emitting thousands of
// cross-pack links collapsed to ~150 links/s, ~20x slower than a plain link,
// regardless of concurrency.
//
// The fix (getObjectAllTypesBaseAndParents -> getTypeResolvedParentTypes reads
// body.cache.parent_types straight from the in-memory cache once warm, with no
// mediator round-trip) removes that per-link serialization. This test asserts
// that, once the type model is warm, a supertype link costs about the same as a
// plain link under concurrency. It FAILS on the unfixed code (ratio ~20x) and
// passes on the fixed code.
type SuperTypeLinkPerfTestSuite struct {
	test.StatefunTestSuite
}

func TestSuperTypeLinkPerfTestSuite(t *testing.T) {
	suite.Run(t, new(SuperTypeLinkPerfTestSuite))
}

// measureP50 runs op(0..n-1) at the given concurrency and returns the median
// per-op latency. It fails the test if any op errors.
func (s *SuperTypeLinkPerfTestSuite) measureP50(conc, n int, op func(i int) error) time.Duration {
	durs := make([]time.Duration, n)
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var failed int64
	var firstErr atomic.Value
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			if err := op(i); err != nil {
				atomic.AddInt64(&failed, 1)
				firstErr.CompareAndSwap(nil, err.Error())
			}
			durs[i] = time.Since(t0)
		}(i)
	}
	wg.Wait()
	s.Require().Zero(failed, "all link ops must succeed; first error: %v", firstErr.Load())
	sort.Slice(durs, func(a, b int) bool { return durs[a] < durs[b] })
	return durs[n/2]
}

func (s *SuperTypeLinkPerfTestSuite) swallowExists(err error) {
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		s.NoError(err)
	}
}

func (s *SuperTypeLinkPerfTestSuite) Test_SuperTypeLinkDoesNotSerializePerLink() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_TYPES); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	cmdb := dbc.CMDB

	// Minimal polytype schema, mirroring linkbench / osm-app ApplySchema:
	// concrete types c_src/c_dst, claim (interface) types i_src/i_dst, concrete
	// IS-A interface subtype edges, and a type-level link for each pair.
	for _, tp := range []string{"c_src", "c_dst", "i_src", "i_dst"} {
		s.NoError(cmdb.TypeUpdate(tp, easyjson.NewJSONObject(), false, true))
	}
	s.swallowExists(cmdb.TypeSetSubType("i_src", "c_src")) // c_src IS-A i_src
	s.swallowExists(cmdb.TypeSetSubType("i_dst", "c_dst")) // c_dst IS-A i_dst
	s.swallowExists(cmdb.TypeSetSubType("i_src", "i_src")) // self-subtype (osm pattern)
	s.swallowExists(cmdb.TypeSetSubType("i_dst", "i_dst"))
	s.NoError(cmdb.TypesLinkUpdate("c_src", "c_dst", nil, easyjson.NewJSONObject(), false, "owns"))
	s.NoError(cmdb.TypesLinkUpdate("i_src", "i_dst", nil, easyjson.NewJSONObject(), false, "claims"))

	const n = 300
	const conc = 32
	// Separate target sets so a plain link and a supertype link never land on
	// the same root->target pair (they resolve to the same link type, which the
	// one-link-per-type-and-direction constraint would reject).
	pID := func(i int) string { return fmt.Sprintf("p%d", i) } // plain-link targets
	sID := func(i int) string { return fmt.Sprintf("s%d", i) } // supertype-link targets
	body := easyjson.NewJSONObject()

	// One shared root (c_src) — the hub — fanning out to N+N targets (c_dst).
	s.NoError(cmdb.ObjectUpdate("root", easyjson.NewJSONObject(), true, "c_src"))
	for i := 0; i < n; i++ {
		s.NoError(cmdb.ObjectUpdate(pID(i), easyjson.NewJSONObject(), true, "c_dst"))
		s.NoError(cmdb.ObjectUpdate(sID(i), easyjson.NewJSONObject(), true, "c_dst"))
	}

	// Warm the type model: a short serial pass of supertype links so the
	// endpoint types' inheritance caches (cache.parent_types + version) and the
	// type-edge cache are populated. This is the steady state the fix targets;
	// the unfixed code stays slow even warm because it still issues a mediator
	// type.read per link.
	for i := 0; i < 25; i++ {
		s.NoError(cmdb.ObjectsLinkSuperTypeUpdate("root", sID(i), "i_src", "i_dst", sID(i), nil, body, true))
	}

	plainP50 := s.measureP50(conc, n, func(i int) error {
		return cmdb.ObjectsLinkUpdate("root", pID(i), nil, body, true, "plain"+pID(i))
	})
	superP50 := s.measureP50(conc, n, func(i int) error {
		return cmdb.ObjectsLinkSuperTypeUpdate("root", sID(i), "i_src", "i_dst", sID(i), nil, body, true)
	})

	ratio := float64(superP50) / float64(plainP50)
	s.T().Logf("warm cross-pack link cost @ concurrency=%d, n=%d: plain p50=%v, supertype p50=%v, ratio=%.1fx",
		conc, n, plainP50.Round(time.Microsecond), superP50.Round(time.Microsecond), ratio)

	// Unfixed code serialises every supertype link on the shared endpoint
	// types' per-id workers => ~20x a plain link and growing with concurrency.
	// The cache fix brings it next to a plain link (~1-3x). The 6x bound fails
	// loudly on the regression while tolerating measurement noise.
	s.Less(ratio, 6.0,
		fmt.Sprintf("supertype link is %.1fx a plain link once warm — the per-link inheritance "+
			"resolution is serialising on the shared types (getObjectAllTypesBaseAndParents); "+
			"expected it to read parent_types from the in-memory cache", ratio))
}
