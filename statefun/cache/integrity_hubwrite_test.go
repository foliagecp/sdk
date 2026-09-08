package cache

// Writing to a hub while the maintenance pass is rebuilding it.
//
// `objects` is the vertex every object in the graph hangs from, so it is both
// the busiest thing in the cache and the one whose links are constantly
// rebuilt underneath. A write must survive that: the two halves of an edge are
// written as separate keys, and a hub that swallows either one leaves a vertex
// that can no longer be read as an object.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The sweep runs alongside writes — that is its normal mode. An edge written
// during a pass must stay whole: losing one half here IS the production defect.
func Test_RootCause_MaintenanceConcurrentWithWritesKeepsEdgesWhole(t *testing.T) {
	for _, mode := range []string{"tree", "records"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()
			cs := NewStoreForTest("sweep_race_" + mode)

			const n = 300
			stop := make(chan struct{})
			sweeper := make(chan struct{})

			go func() {
				defer close(sweeper)
				for {
					select {
					case <-stop:
						return
					default:
						cs.RunMaintenanceForTest()
					}
				}
			}()

			for i := 0; i < n; i++ {
				to := fmt.Sprintf("dom/r-%04d", i)
				cs.SetValue(to, []byte(`{}`), false, int64(100+i))
				writeEdge(cs, "dom/objects", to, fmt.Sprintf("e%04d", i), "__object", int64(100+i), false)
			}
			close(stop)
			<-sweeper

			for i := 0; i < n; i++ {
				requireEdgeWhole(t, cs, "dom/objects", fmt.Sprintf("dom/r-%04d", i), fmt.Sprintf("e%04d", i), "__object",
					fmt.Sprintf("edge %d lost a half under a concurrent sweep", i))
			}
		})
	}
}

// The records half. A hub that lost most of its links has a directory bigger
// than it needs, and the maintenance pass rebuilds it at the smaller depth. The
// rebuild must not take a write that is in flight with it: the writer publishes
// its block into a slot the new directory does not point at, and the key is
// gone with nothing to report it.
func Test_RootCause_ShrinkingADirectoryUnderAWriteLosesNothing(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()
	cs := NewStoreForTest("shrink_records")

	// A hub with a directory, then emptied enough that a rebuild is due.
	for i := 0; i < 200; i++ {
		require.True(t, cs.SetValue(fmt.Sprintf("dom/hub.out.to.l%03d", i),
			[]byte("__object.dom/x"), false, int64(1000+i)))
	}
	for i := 0; i < 190; i++ {
		cs.DeleteValue(fmt.Sprintf("dom/hub.out.to.l%03d", i), false, int64(5000+i))
	}

	const key = "dom/hub.out.to.fresh"
	done := make(chan struct{})
	var once sync.Once
	afterSlotLockedForTest = func() {
		once.Do(func() {
			// The rebuild starts while this write holds its slot and has not
			// published. It must wait for it — before the fix it walked past,
			// and the key below was never in the graph.
			go func() {
				// shrinkDirs directly, not the whole maintenance pass: the
				// pass compacts every bucket first and would block on this
				// very slot before ever reaching the rebuild.
				r, ok := cs.records.get("dom/hub")
				require.True(t, ok)
				r.shrinkDirs()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(300 * time.Millisecond):
			}
		})
	}
	defer func() { afterSlotLockedForTest = nil }()

	require.True(t, cs.SetValue(key, []byte("__object.dom/y"), false, 9000))
	<-done
	require.Truef(t, cs.Exists(key), "the rebuilt directory does not hold the key written into it")
	require.Containsf(t, cs.GetKeysByPattern("dom/hub.out.to.>"), key,
		"the key is not in the enumeration the graph reads out-links through")
}
