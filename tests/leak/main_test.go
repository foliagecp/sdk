//go:build leak

package leak

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/statefun/test"
)

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
