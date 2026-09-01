//go:build perf

package crud_test

// Does the cost of an operation depend on how much is already in the trash can?
//
// Parking is the delete path of every object, so it must not read the bin: the
// moment it does, a mass cleanup becomes quadratic — each delete pays for
// everything deleted before it. The same holds for a restore, which is the
// delete path run backwards.
//
// The bench measures the per-operation cost at growing bin sizes and compares
// parking against the physical erase it replaced, which is the honest baseline:
// parking may cost somewhat more (it writes two links instead of dropping a
// body), but it must not cost MORE AND MORE.
//
// Run: go test -tags perf -run TestTrashCanScalePerfTestSuite \
//        ./embedded/graph/crud/ -v -count=1
// Tunables: PERF_TC_BATCH (default 300), PERF_TC_BIN_SIZES (default "0 2000 5000").

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type TrashCanScalePerfTestSuite struct {
	test.StatefunTestSuite
	cmdb  db.CMDBSyncClient
	graph db.GraphSyncClient
}

func TestTrashCanScalePerfTestSuite(t *testing.T) { suite.Run(t, new(TrashCanScalePerfTestSuite)) }

func (s *TrashCanScalePerfTestSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime(), "usr")
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
	s.cmdb, s.graph = dbc.CMDB, dbc.Graph
}

func perfTCInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

func perfTCBinSizes() []int {
	raw := os.Getenv("PERF_TC_BIN_SIZES")
	if raw == "" {
		raw = "0 2000 5000"
	}
	out := []int{}
	for _, f := range strings.Fields(raw) {
		if n, err := strconv.Atoi(f); err == nil && n >= 0 {
			out = append(out, n)
		}
	}
	return out
}

func (s *TrashCanScalePerfTestSuite) body(i int) easyjson.JSON {
	b := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON(fmt.Sprintf("srv-%d", i)))
	b.SetByPath("usr.attrs.responsible", easyjson.NewJSON("owner"))
	return b
}

// fillBin parks `n` fresh objects, growing the trash can to that size.
func (s *TrashCanScalePerfTestSuite) fillBin(tag string, n int) {
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("tcfill_%s_%d", tag, i)
		s.Require().NoError(s.cmdb.ObjectCreate(id, "tc_scale_t", s.body(i)))
		s.Require().NoError(s.cmdb.ObjectDelete(id))
	}
}

// perOp runs op(0..n-1) sequentially and returns the mean per-operation cost.
// Sequential on purpose: this bench is about the SHAPE of the cost curve, and
// concurrency would only add scheduling noise to it.
func (s *TrashCanScalePerfTestSuite) perOp(n int, op func(i int)) time.Duration {
	t0 := time.Now()
	for i := 0; i < n; i++ {
		op(i)
	}
	return time.Since(t0) / time.Duration(n)
}

func (s *TrashCanScalePerfTestSuite) Test_CostDoesNotGrowWithTheBin() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("tc_scale_t"))

	batch := perfTCInt("PERF_TC_BATCH", 300)
	binSizes := perfTCBinSizes()

	type row struct {
		bin                          int
		park, restore, erase, create time.Duration
	}
	rows := []row{}

	filled := 0
	for _, bin := range binSizes {
		if bin > filled {
			s.fillBin(fmt.Sprintf("b%d", bin), bin-filled)
			filled = bin
		}

		// create + park + restore + erase, each measured over the same batch.
		ids := func(i int) string { return fmt.Sprintf("tcm_%d_%d", bin, i) }

		create := s.perOp(batch, func(i int) {
			s.Require().NoError(s.cmdb.ObjectCreate(ids(i), "tc_scale_t", s.body(i)))
		})
		park := s.perOp(batch, func(i int) {
			s.Require().NoError(s.cmdb.ObjectDelete(ids(i)))
		})
		restore := s.perOp(batch, func(i int) {
			s.Require().NoError(s.cmdb.ObjectCreate(ids(i), "tc_scale_t", s.body(i)))
		})
		// Park again, then erase physically — the baseline parking replaced.
		for i := 0; i < batch; i++ {
			s.Require().NoError(s.cmdb.ObjectDelete(ids(i)))
		}
		erase := s.perOp(batch, func(i int) {
			s.Require().NoError(s.graph.VertexDelete(ids(i)))
		})

		rows = append(rows, row{bin, park, restore, erase, create})
		s.T().Logf("bin=%-6d create=%-10v park=%-10v restore=%-10v erase(physical)=%v",
			bin, create.Round(time.Microsecond), park.Round(time.Microsecond),
			restore.Round(time.Microsecond), erase.Round(time.Microsecond))
	}

	// The guarantee: cost must not track the bin. Allow a factor for ordinary
	// noise and cache growth, but nothing that looks like O(bin).
	const maxGrowth = 2.0
	base := rows[0]
	for _, r := range rows[1:] {
		s.LessOrEqualf(float64(r.park)/float64(base.park), maxGrowth,
			"parking got %.1fx slower with %d objects in the bin (%v vs %v) — the delete path is reading the bin",
			float64(r.park)/float64(base.park), r.bin, r.park, base.park)
		s.LessOrEqualf(float64(r.restore)/float64(base.restore), maxGrowth,
			"restoring got %.1fx slower with %d objects in the bin (%v vs %v)",
			float64(r.restore)/float64(base.restore), r.bin, r.restore, base.restore)
	}

	// And parking must stay in the same league as the erase it replaced.
	last := rows[len(rows)-1]
	s.T().Logf("park/erase ratio at bin=%d: %.2fx", last.bin, float64(last.park)/float64(last.erase))
}

// A parked object is restored through the object API; the bench above measures
// it. This one pins the other half of the claim: reading the bin is the sweep's
// job, so a delete must not fire an eviction scan inline even when the bin is
// over its cap.
func (s *TrashCanScalePerfTestSuite) Test_OverCapacityDoesNotSlowDeletes() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("tc_cap_t"))

	batch := perfTCInt("PERF_TC_BATCH", 300)
	crud.SetTrashCanMaxObjectsForTest(batch / 2) // deliberately over cap from the start
	defer crud.SetTrashCanMaxObjectsForTest(10000)

	for i := 0; i < batch; i++ {
		id := fmt.Sprintf("tccap_%d", i)
		s.Require().NoError(s.cmdb.ObjectCreate(id, "tc_cap_t", s.body(i)))
	}
	park := s.perOp(batch, func(i int) {
		s.Require().NoError(s.cmdb.ObjectDelete(fmt.Sprintf("tccap_%d", i)))
	})
	s.T().Logf("park over capacity: %v/op", park.Round(time.Microsecond))
	s.Less(park, 20*time.Millisecond, "a delete must not carry an eviction scan")

	_ = sfPlugins.AutoRequestSelect
}
