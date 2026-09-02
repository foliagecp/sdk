//go:build perf

package crud_test

// Does the cost of updating one link depend on how many links its FROM-vertex
// has?
//
// It did: both the update and the delete path resolved the edge by searching
// (from → to) over every out-link of the from-vertex. On a hub vertex — a rack
// with a thousand hosts, a type with its objects — that is the largest single
// share of the runtime's CPU under an inventory rebuild. Resolution by name
// makes it one key read; this bench watches the shape of the curve.
//
// Run: scripts/run-perf-tests.sh --embedded --scenario link-resolve
// Tunables: PERF_LR_BATCH (default 200), PERF_LR_FANOUTS (default "100 800 1600").

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type LinkResolveScalePerfTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestLinkResolveScalePerfTestSuite(t *testing.T) {
	suite.Run(t, new(LinkResolveScalePerfTestSuite))
}

func (s *LinkResolveScalePerfTestSuite) boot() {
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
	s.cmdb = dbc.CMDB
}

func perfLRInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func perfLRFanouts() []int {
	raw := os.Getenv("PERF_LR_FANOUTS")
	if raw == "" {
		raw = "100 800 1600"
	}
	out := []int{}
	for _, f := range strings.Fields(raw) {
		if n, err := strconv.Atoi(f); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

// perOp returns the MEDIAN per-operation cost. A mean over a short batch is at
// the mercy of a single GC pause or a WAL publish landing mid-run — which is
// how a flat curve can look like a rising one; the median answers the question
// actually asked, "what does one operation cost here".
func (s *LinkResolveScalePerfTestSuite) perOp(n int, op func(i int)) time.Duration {
	durs := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		op(i)
		durs = append(durs, time.Since(t0))
	}
	sort.Slice(durs, func(a, b int) bool { return durs[a] < durs[b] })
	return durs[len(durs)/2]
}

func (s *LinkResolveScalePerfTestSuite) Test_LinkOpCostDoesNotTrackFanout() {
	s.boot()
	batch := perfLRInt("PERF_LR_BATCH", 200)
	fanouts := perfLRFanouts()

	type row struct {
		fanout      int
		update, del time.Duration
	}
	rows := []row{}

	for _, fanout := range fanouts {
		tag := fmt.Sprintf("lrp%d", fanout)
		s.Require().NoError(s.cmdb.TypeCreate(tag + "_t"))
		s.Require().NoError(s.cmdb.TypesLinkCreate(tag+"_t", tag+"_t", tag+"_rel", nil))
		from := tag + "_from"
		s.Require().NoError(s.cmdb.ObjectCreate(from, tag+"_t", easyjson.NewJSONObject()))

		// One hub vertex with `fanout` out-links; the measured batch is a subset.
		for i := 0; i < fanout; i++ {
			to := fmt.Sprintf("%s_to%d", tag, i)
			s.Require().NoError(s.cmdb.ObjectCreate(to, tag+"_t", easyjson.NewJSONObject()))
			s.Require().NoError(s.cmdb.ObjectsLinkCreate(from, to, to, nil, easyjson.NewJSONObject()))
		}

		// The measured batch cannot exceed what the hub actually has.
		n := batch
		if n > fanout {
			n = fanout
		}

		body := easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))
		// Warm-up: the first calls on a fresh hub pay for lazily built per-id
		// machinery, which has nothing to do with fanout.
		for i := 0; i < 20 && i < n; i++ {
			s.Require().NoError(s.cmdb.ObjectsLinkUpdate(from, fmt.Sprintf("%s_to%d", tag, i), nil, body, false))
		}
		update := s.perOp(n, func(i int) {
			s.Require().NoError(s.cmdb.ObjectsLinkUpdate(from, fmt.Sprintf("%s_to%d", tag, i), nil, body, false))
		})
		del := s.perOp(n, func(i int) {
			s.Require().NoError(s.cmdb.ObjectsLinkDelete(from, fmt.Sprintf("%s_to%d", tag, i)))
		})

		rows = append(rows, row{fanout, update, del})
		s.T().Logf("fanout=%-6d link.update=%-10v link.delete=%v",
			fanout, update.Round(time.Microsecond), del.Round(time.Microsecond))
	}

	const maxGrowth = 2.0
	base := rows[0]
	for _, r := range rows[1:] {
		s.LessOrEqualf(float64(r.update)/float64(base.update), maxGrowth,
			"link.update got %.1fx slower at fanout %d (%v vs %v) — the edge is being resolved by a search",
			float64(r.update)/float64(base.update), r.fanout, r.update, base.update)
		s.LessOrEqualf(float64(r.del)/float64(base.del), maxGrowth,
			"link.delete got %.1fx slower at fanout %d (%v vs %v)",
			float64(r.del)/float64(base.del), r.fanout, r.del, base.del)
	}
}
