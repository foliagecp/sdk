package statefun

// Package-wide test setup.
//
// system.GlobalPrometrics is a process-global that the worker-pool manager
// goroutine reads on every tick (SFWorkerPool.prometricsMeasures). Several test
// files used to lazily install it from within a test body (a sync.Once that
// writes unconditionally in export_integration_test.go, `if == nil` guards in
// worker_pool_test.go / ha_test.go). Those writes happened WHILE a worker-pool
// goroutine leaked from an earlier test was still reading the global — a data
// race surfacing under the full suite with -race ("Write ... ensurePrometrics
// vs Read ... prometricsMeasures").
//
// Installing it ONCE here, before any test (and therefore before any worker
// pool) runs, removes the concurrent write entirely: the global is set while
// the program is still single-threaded, and no test mutates it afterwards (the
// remaining install sites are now no-ops because it is already non-nil).

import (
	"os"
	"testing"

	"github.com/foliagecp/sdk/statefun/system"
)

func TestMain(m *testing.M) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	os.Exit(m.Run())
}
