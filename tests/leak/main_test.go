//go:build leak

package leak

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/statefun/test"
)

// init sets the heap-profile resolution before anything else in this package
// allocates.
//
// The SDK/NATS heap split is derived from the heap profile, whose values are
// ESTIMATES: the runtime samples one allocation per MemProfileRate bytes and
// scales the result back up. At Go's 512KiB default the split can therefore
// only move in half-megabyte steps — EIGHT TIMES coarser than the 64KiB floor
// these checks assert against — so a couple of sampled allocations landing
// inside one measurement window draw a straight, "3-sigma significant" line
// through a scenario that leaks nothing. That is exactly how clean runs of
// s9/s10 were flagged: their sdk_inuse_bytes series is a staircase of 524432-
// byte steps, and in isolation the very same scenario reports slope=0 because
// no sampled allocation happened to land in the window at all.
//
// A fine rate makes the metric near-exact, so the floors mean what they say.
// It must be set ONCE and as early as possible (the profile writer scales every
// record by the CURRENT rate), which is why it lives here and not in TestMain.
func init() {
	runtime.MemProfileRate = system.GetEnvMustProceed("LEAK_MEMPROFILE_RATE", 4096)
}

// resultsDir is where every scenario writes its artifacts (CSV samples, heap
// profiles, diffs). Relative paths resolve against this package's directory,
// so the default lands in tests/leak/_results/ (gitignored, invisible to the
// go tool).
var resultsDir string

func TestMain(m *testing.M) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "127.0.0.1:0")
	}

	// Short GC cadence: idle per-id machinery and expired function contexts
	// must be reclaimed within seconds, not the 5s defaults, or every decay
	// assertion would stretch the run.
	test.RuntimeConfigMutatorForTest = func(cfg *statefun.RuntimeConfig) {
		cfg.SetGCIntervalSec(1)
		cfg.SetFunctionTypeIDLifetimeMs(500)
	}

	// A stuck graph key lock self-resolves in seconds instead of the 300s
	// production default, so an accidental lock leak cannot wedge the run.
	crud.SetGraphKeyLockTimeoutForTest(3 * time.Second)

	resultsDir = os.Getenv("LEAK_RESULTS_DIR")
	if resultsDir == "" {
		resultsDir = fmt.Sprintf("_results/leak-%s", time.Now().UTC().Format("20060102-150405"))
	}

	os.Exit(m.Run())
}
