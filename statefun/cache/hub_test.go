package cache

// Hub tests.
//
// A real graph has a vertex `objects` that everything converges on: 10 692
// links on the reference dump. The record format keeps links in buckets
// precisely because of it: without buckets, writing one link into such a
// vertex would rewrite the whole vertex, and the cost of a write would grow
// with the graph.
//
// These tests check that this does not happen: a write into a hub costs about
// as much as a write into a vertex of one bucket, and no link is lost under
// concurrent writes. That write throughput into a hub under records keeps up
// with the tree is checked by the gate Test_Hub_ConcurrentThroughputGate in
// hub_perf_test.go (tag perf): that is a measurement, and a measurement needs
// conditions that a correctness run under -race does not have.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// hubSize is the size of the `objects` hub on the reference dump.
const hubSize = 10692

func fillHub(cs *Store, id string, links int) {
	for i := 0; i < links; i++ {
		cs.SetValue(fmt.Sprintf("%s.out.to.l%05d", id, i), []byte("t.dom/tgt"), false, int64(i+1))
	}
}

// perWrite is the cost of writing one link, the best of three attempts.
func perWrite(cs *Store, id string, from, count int) time.Duration {
	best := time.Duration(1<<62 - 1)
	for attempt := 0; attempt < 3; attempt++ {
		base := from + attempt*count
		started := time.Now()
		for i := 0; i < count; i++ {
			cs.SetValue(fmt.Sprintf("%s.out.to.n%07d", id, base+i),
				[]byte("t.dom/tgt"), false, int64(1_000_000+base+i))
		}
		if d := time.Since(started) / time.Duration(count); d < best {
			best = d
		}
	}
	return best
}

// Test_Hub_WriteCostDoesNotGrowWithTheHub: writing a link into a hub of 10 692
// links must not cost noticeably more than writing into a vertex of one
// bucket.
func Test_Hub_WriteCostDoesNotGrowWithTheHub(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	small := NewStoreForTest("hub_small")
	fillHub(small, "dom/small", defaultBucketLinks)
	big := NewStoreForTest("hub_big")
	fillHub(big, "dom/big", hubSize)

	perSmall := perWrite(small, "dom/small", 0, 2000)
	perBig := perWrite(big, "dom/big", 0, 2000)
	t.Logf("вершина из %d связей: %s на запись", defaultBucketLinks, perSmall)
	t.Logf("хаб из %d связей:  %s на запись", hubSize, perBig)

	// The hub is 334 times larger. The spec requires "no more"; with a margin
	// for machine noise and for the hub's deeper directory, the threshold is
	// three times: rewriting the whole vertex would give three hundred.
	require.Lessf(t, perBig, perSmall*3,
		"запись в хаб (%s) дороже записи в маленькую вершину (%s) — цена растёт с размером вершины",
		perBig, perSmall)

	// A hub write costs no more than 100 µs on top of a write today.
	require.Lessf(t, perBig-perSmall, 100*time.Microsecond,
		"Н-4: запись в хаб дороже на %s", perBig-perSmall)
}

// Test_Hub_SequentialWritesKeepEverything: ten thousand sequential writes into
// a hub are all in place, and none is lost in splits.
func Test_Hub_SequentialWritesKeepEverything(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("hub_seq")
	const writes = 10000
	started := time.Now()
	for i := 0; i < writes; i++ {
		cs.SetValue(fmt.Sprintf("dom/hub.out.to.l%05d", i),
			[]byte(fmt.Sprintf("t.dom/tgt-%05d", i)), false, int64(i+1))
	}
	took := time.Since(started)

	for i := 0; i < writes; i++ {
		key := fmt.Sprintf("dom/hub.out.to.l%05d", i)
		v, err := cs.GetValue(key)
		require.NoErrorf(t, err, "связь %d потеряна", i)
		require.Equalf(t, fmt.Sprintf("t.dom/tgt-%05d", i), string(v), "связь %d испорчена", i)
	}
	require.Len(t, cs.GetKeysByPattern("dom/hub.out.to.>"), writes,
		"перечисление обязано вернуть все связи хаба")
	t.Logf("%d последовательных записей в хаб за %s (%s на запись)",
		writes, took.Round(time.Millisecond), (took / writes).Round(time.Nanosecond))
}

// hubBurst has goroutines goroutines each write perGoroutine distinct links
// into the hub dom/hub. Names alternate so that neighbours by number land in
// different buckets and those with the same remainder land in one: both
// situations are needed at once.
func hubBurst(cs *Store, goroutines, perGoroutine int) {
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				name := fmt.Sprintf("c%03d_%04d", g, i)
				cs.SetValue("dom/hub.out.to."+name,
					[]byte(fmt.Sprintf("t.dom/%s", name)), false, int64(2_000_000+g*perGoroutine+i))
			}
		}(g)
	}
	wg.Wait()
}

// hubBurstMissing counts the links of a burst that are lost or corrupted.
func hubBurstMissing(cs *Store, goroutines, perGoroutine int) int {
	missing := 0
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perGoroutine; i++ {
			name := fmt.Sprintf("c%03d_%04d", g, i)
			v, err := cs.GetValue("dom/hub.out.to." + name)
			if err != nil || string(v) != fmt.Sprintf("t.dom/%s", name) {
				missing++
			}
		}
	}
	return missing
}

// Test_Hub_ConcurrentWritesKeepEverything: 64 goroutines write distinct links
// into one hub, including into one bucket, in both modes, and nothing is lost.
//
// Time is not measured here. The old Test_Hub_ConcurrentThroughput compared the
// modes by one sample of ~150 ms under the race detector and failed because of
// machine load, not because of the code; Test_Hub_ConcurrentThroughputGate
// (hub_perf_test.go, tag perf) now compares speed, and this test keeps
// correctness where it is needed most: under -race.
func Test_Hub_ConcurrentWritesKeepEverything(t *testing.T) {
	const goroutines, perGoroutine = 64, 200
	for _, mode := range []string{"tree", "records"} {
		func() {
			restore := SetCacheModeForTest(mode)
			defer restore()
			for attempt := 0; attempt < 3; attempt++ {
				cs := NewStoreForTest("hub_conc")
				fillHub(cs, "dom/hub", defaultBucketLinks*4)
				hubBurst(cs, goroutines, perGoroutine)
				missing := hubBurstMissing(cs, goroutines, perGoroutine)
				require.Zerof(t, missing, "режим %s: потеряно или испорчено %d связей из %d",
					mode, missing, goroutines*perGoroutine)
			}
		}()
	}
}
