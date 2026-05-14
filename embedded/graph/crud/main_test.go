package crud

import (
	"os"
	"testing"

	"github.com/foliagecp/sdk/statefun/system"
)

// TestMain initializes process-global state required by the SDK runtime
// before any test in this package runs. In particular, system.GlobalPrometrics
// must be non-nil; otherwise FunctionType message-delivery metric updates
// nil-deref on the very first request.
//
// The test prometrics server binds to a random local port (":0") and is
// effectively invisible — the goal is only to satisfy the SDK's invariant.
func TestMain(m *testing.M) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "127.0.0.1:0")
	}
	os.Exit(m.Run())
}
