//go:build perf

package cache

// Hub tests, second half: write throughput into a hub under records keeps up
// with the tree.
//
// The threshold is unchanged: records are no more than a quarter slower than
// the tree, that is, they keep at least 80 % of its throughput. What changes is
// how it is measured, because the old check (Test_Hub_ConcurrentThroughput
// before 0.5.9) could not tell the code from the machine:
//
//   - One sample per mode, the best of three, tree first, then records. The
//     window is about 150 ms under the race detector and about 10 ms without
//     it: no longer than a scheduler period, so a slowdown of the machine
//     during one sample fell entirely on one mode.
//   - Under sustained CPU contention records really do lose more than the
//     tree: a bucket write holds its slot lock through the whole decode and
//     encode, a bucket split holds the directory lock, and a preempted lock
//     holder delays the other 63 goroutines; a write takes twice the memory
//     (680 B against 360). The old check turned both the noise and this
//     property into a random FAIL, alike on 0.5.6 and 0.5.8.
//
// Measured with scripts/hub-gate-experiment.sh (a container with 4 CPUs, a
// sibling on 2 cores): the old check under -race gives 0 FAIL out of 20 idle,
// 2 under steady load, 6 under flap; this gate gives 10 PASS out of 10 idle
// (median 1.09), 10 out of 10 under flap (1.06), and a median of 1.27 under
// steady load: a property of the code under load, not noise. With pairs
// dropped by their spread, 5 runs each: idle 5 PASS (1.10), flap 5 PASS
// (1.13), steady 5 FAIL (1.29) at 42–51 % of the nominal parallelism; 15 clean
// pairs in every run.
//
// Now:
//
//   - A measurement is a pair of tree and records samples back to back; the
//     order within the pair alternates. Each sample is a series of bursts on
//     fresh stores lasting at least gateSampleFloor in total; filling the hub
//     and garbage collection stay outside the timing.
//   - Before the pair, between its two samples and after it, the gate measures
//     the machine's throughput: the same work on every P. If the machine at
//     the slowest of these three moments is slower than at the fastest by
//     more than gateSpreadLimit, the pair is dropped: its samples ran on
//     different machines, and the ratio speaks of the machine, not of the
//     code. A pair on a uniformly slow machine is comparable, since the ratio
//     cancels absolute speed, so sustained load is not a reason to drop a
//     pair. Comparing against the fastest moment of the whole run, which is
//     how the gate started, dropped such pairs too, and on a virtual machine
//     sharing its hypervisor with neighbours it dropped almost all of them.
//   - The verdict is the median ratio of the clean pairs. With fewer than
//     gateMinClean clean pairs the run fails as INCONCLUSIVE: it proves
//     neither a regression nor its absence.
//   - The gate does not run under the race detector: instrumented code
//     measures something other than what runs in production. The correctness
//     of the same burst under -race is checked by
//     Test_Hub_ConcurrentWritesKeepEverything.
//
// To run it:
//
//	go test -tags perf -count=1 -v -run '^Test_Hub_ConcurrentThroughputGate$' ./statefun/cache
//
// It is also part of scripts/run-perf-tests.sh --gate and of phase 1b of
// scripts/run-all-tests.sh, where it only reports: the verdict is printed, but
// the whole run does not fail on it. scripts/hub-gate-experiment.sh describes
// how to reproduce the old FAIL and check the gate under load.

import (
	"crypto/sha256"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	gateGoroutines   = 64
	gatePerGoroutine = 200
	gateRatioLimit   = 1.25
	gateCleanPairs   = 15
	gateMinClean     = 8
	gateMaxPairs     = 30
	gateSpreadLimit  = 1.15
	gateSampleFloor  = 200 * time.Millisecond
	gateMinBursts    = 3
)

type hubGateSample struct {
	took    time.Duration
	writes  uint64
	mallocs uint64
	bytes   uint64
}

func (s hubGateSample) add(o hubGateSample) hubGateSample {
	return hubGateSample{s.took + o.took, s.writes + o.writes, s.mallocs + o.mallocs, s.bytes + o.bytes}
}

func (s hubGateSample) String() string {
	w := float64(max(s.writes, 1))
	return fmt.Sprintf("%s, %.1f allocations, %.0f B", (s.took / time.Duration(max(s.writes, 1))).Round(time.Nanosecond),
		float64(s.mallocs)/w, float64(s.bytes)/w)
}

type hubGatePair struct {
	first          string
	pre, mid, post time.Duration
	tree, records  hubGateSample
}

func (s hubGateSample) perWrite() float64 { return float64(s.took) / float64(max(s.writes, 1)) }

// ratio is how many times longer a write takes under records than in the
// tree. The two samples of a pair may consist of different numbers of bursts,
// so the time of one write is compared.
func (p hubGatePair) ratio() float64 { return p.records.perWrite() / p.tree.perWrite() }

// spread is how many times slower the machine is at the slowest moment of the
// pair (before it, between its samples, after it) than at the fastest.
func (p hubGatePair) spread() float64 {
	return float64(max(p.pre, p.mid, p.post)) / float64(min(p.pre, p.mid, p.post))
}

// hubGateMeasure runs bursts in mode, each on a fresh store, until their total
// time reaches floor, and no fewer than gateMinBursts. The number of bursts is
// not fixed in advance: a measured burst can be faster than a warm-up one, and
// a count calibrated on the warm-up gave samples shorter than floor.
func hubGateMeasure(mode string, floor time.Duration, prefill int) hubGateSample {
	restore := SetCacheModeForTest(mode)
	defer restore()
	var s hubGateSample
	var before, after runtime.MemStats
	for b := 0; b < gateMinBursts || s.took < floor; b++ {
		cs := NewStoreForTest("hub_gate")
		fillHub(cs, "dom/hub", prefill)
		runtime.GC()
		runtime.ReadMemStats(&before)
		started := time.Now()
		hubBurst(cs, gateGoroutines, gatePerGoroutine)
		s.took += time.Since(started)
		runtime.ReadMemStats(&after)
		s.mallocs += after.Mallocs - before.Mallocs
		s.bytes += after.TotalAlloc - before.TotalAlloc
		s.writes += gateGoroutines * gatePerGoroutine
	}
	return s
}

// cpuCapacity is how long the machine currently takes for the same work on
// every P: hashing with no shared data and no locks. Its spread within a pair
// is exactly "the machine changed": a neighbouring process, quota throttling,
// another container. It is the best of three short samples: a single scheduler
// hiccup during the calibration itself is not a shift, while a machine taken
// by a neighbour stays that way longer than three samples.
func cpuCapacity() time.Duration { return cpuWork(runtime.GOMAXPROCS(0)) }

// cpuParallelism is the share of the nominal parallelism the machine gives
// right now: the same work on one goroutine and on each of GOMAXPROCS. On an
// idle machine it is about 1 (less on SMT); with a neighbour holding some of
// the cores it is noticeably less. The gate sees changes within a pair by
// itself; it sees sustained load only this way, so the number is printed next
// to the verdict but does not change it.
func cpuParallelism() float64 {
	return float64(cpuWork(1)) / float64(cpuWork(runtime.GOMAXPROCS(0)))
}

func cpuWork(goroutines int) time.Duration {
	buf := make([]byte, 4096)
	best := time.Duration(1<<62 - 1)
	for attempt := 0; attempt < 3; attempt++ {
		var wg sync.WaitGroup
		started := time.Now()
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				h := sha256.New()
				for j := 0; j < 2048; j++ {
					h.Write(buf)
				}
				_ = h.Sum(nil)
			}()
		}
		wg.Wait()
		best = min(best, time.Since(started))
	}
	return best
}

func hubGatePassport() string {
	quota := "n/a"
	if b, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		quota = strings.TrimSpace(string(b))
	}
	return fmt.Sprintf("%s %s/%s NumCPU=%d GOMAXPROCS=%d cgroup cpu.max=%q race=%v",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0), quota, raceEnabled)
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func Test_Hub_ConcurrentThroughputGate(t *testing.T) {
	if raceEnabled {
		t.Skip("the throughput gate does not run under the race detector: instrumented code measures something " +
			"other than what runs in production; Test_Hub_ConcurrentWritesKeepEverything checks the burst's correctness under -race")
	}
	prefill := defaultBucketLinks * 4
	if v := os.Getenv("HUB_PREFILL"); v != "" {
		fmt.Sscanf(v, "%d", &prefill)
	}

	// Warm-up: the first bursts are slower than the measured ones and do not
	// go into the samples.
	hubGateMeasure("tree", 0, prefill)
	hubGateMeasure("records", 0, prefill)

	t.Logf("environment: %s", hubGatePassport())
	t.Logf("sample: %d goroutines × %d links into a hub of %d links; at least %d bursts per sample, "+
		"until their total time reaches %s — a sample covers many scheduler periods, not a fraction of one",
		gateGoroutines, gatePerGoroutine, prefill, gateMinBursts, gateSampleFloor)
	parallelStart := cpuParallelism()

	var pairs, ok []hubGatePair
	for len(pairs) < gateMaxPairs && len(ok) < gateCleanPairs {
		p := hubGatePair{first: "tree", pre: cpuCapacity()}
		second := "records"
		if len(pairs)%2 == 1 {
			p.first, second = "records", "tree"
		}
		a := hubGateMeasure(p.first, gateSampleFloor, prefill)
		p.mid = cpuCapacity()
		b := hubGateMeasure(second, gateSampleFloor, prefill)
		p.post = cpuCapacity()
		p.tree, p.records = a, b
		if p.first == "records" {
			p.tree, p.records = b, a
		}
		pairs = append(pairs, p)
		if p.spread() <= gateSpreadLimit {
			ok = append(ok, p)
		}
	}

	t.Logf("%4s  %-7s  %9s  %9s  %13s  %7s", "pair", "first", "tree", "records", "records/tree", "spread")
	for i, p := range pairs {
		mark := ""
		if p.spread() > gateSpreadLimit {
			mark = "  dropped: the machine changed within the pair"
		}
		t.Logf("%4d  %-7s  %9s  %9s  %13.3f  %7.2f%s", i+1, p.first,
			p.tree.took.Round(time.Millisecond), p.records.took.Round(time.Millisecond), p.ratio(), p.spread(), mark)
	}

	if len(ok) < gateMinClean {
		t.Fatalf("INCONCLUSIVE: within %d of %d pairs the machine changed by more than %.0f%%; "+
			"%d clean pairs, %d needed. Such a run says nothing about the code either way — "+
			"repeat it on a machine without outside load.",
			len(pairs)-len(ok), len(pairs), (gateSpreadLimit-1)*100, len(ok), gateMinClean)
	}
	ratios := make([]float64, 0, len(ok))
	var tree, records hubGateSample
	for _, p := range ok {
		ratios = append(ratios, p.ratio())
		tree, records = tree.add(p.tree), records.add(p.records)
	}
	sort.Float64s(ratios)
	med := median(ratios)
	parallel := fmt.Sprintf("the machine gave %.0f%% / %.0f%% of its nominal parallelism at the start / at the end",
		parallelStart*100, cpuParallelism()*100)
	t.Logf("clean pairs %d of %d; records/tree: min %.3f, median %.3f, max %.3f; median threshold %.2f",
		len(ok), len(pairs), ratios[0], med, ratios[len(ratios)-1], gateRatioLimit)
	t.Logf("per write: tree %s; records %s", tree, records)
	t.Logf("%s", parallel)
	if med > gateRatioLimit {
		t.Fatalf("records throughput is below 80%% of the tree's: median records/tree ratio %.3f over %d clean pairs (threshold %.2f); %s. "+
			"Under sustained CPU load records lose more than the tree — a property of the code, not noise; "+
			"compare against the threshold on a machine without outside load.",
			med, len(ok), gateRatioLimit, parallel)
	}
}
