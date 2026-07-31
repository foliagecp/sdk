//go:build leak

package leak

import (
	"runtime"
	"testing"
	"time"
)

// S0 proves the framework's detection power in BOTH directions before any
// real scenario runs: a planted leak MUST be flagged and a clean workload
// MUST pass. If either fails, no other scenario's verdict can be trusted.
// These tests run without a statefun runtime on purpose — they validate the
// harness math and plumbing in isolation.

// TestS0PlantedLeakIsFlagged plants an unmistakable leak — 1 MiB retained per
// cycle plus one parked goroutine per cycle — and requires the harness to
// flag both. Verdicts are inspected directly instead of AssertClean, since
// here a "leak" is the expected outcome.
func TestS0PlantedLeakIsFlagged(t *testing.T) {
	var sink [][]byte
	stop := make(chan struct{})
	defer close(stop) // release the parked goroutines at test end

	r := &CycleRunner{
		Scenario: "s0_planted",
		Warmup:   warmupCycles(),
		Measure:  measureCycles(),
		Cycle: func(i int) error {
			sink = append(sink, make([]byte, 1<<20))
			go func() { <-stop }()
			return nil
		},
		// The parked goroutines never exit — no point waiting the full 15s.
		SettleTimeout: 2 * time.Second,
	}
	rep := r.Run(t)

	heap := rep.VerdictFor("heap_alloc")
	if !heap.Leaking {
		emitCheck("s0_planted", "planted_leak_detected", "FAIL",
			"slope="+f1(heap.Slope), "se="+f1(heap.StdErr))
		t.Fatalf("framework lost detection power: 1MiB/cycle planted leak not flagged (slope=%.1f, se=%.1f, floor=%.1f)",
			heap.Slope, heap.StdErr, heap.Floor)
	}
	if rep.GoroutinesSettled {
		emitCheck("s0_planted", "planted_goroutines_detected", "FAIL")
		t.Fatalf("framework lost detection power: %d parked goroutines went unnoticed (baseline %d, final %d)",
			measureCycles(), rep.GoroutineBaseline, rep.GoroutineFinal)
	}
	emitCheck("s0_planted", "planted_leak_detected", "PASS",
		"slope="+f1(heap.Slope), "se="+f1(heap.StdErr))
	emitCheck("s0_planted", "planted_goroutines_detected", "PASS")

	runtime.KeepAlive(sink)
}

// TestS0ControlIsClean runs a workload that allocates transiently but retains
// nothing — the harness must report it clean (false-positive guard).
func TestS0ControlIsClean(t *testing.T) {
	r := &CycleRunner{
		Scenario: "s0_control",
		Warmup:   warmupCycles(),
		Measure:  measureCycles(),
		Cycle: func(i int) error {
			buf := make([]byte, 1<<20)
			for j := 0; j < len(buf); j += 4096 {
				buf[j] = byte(i)
			}
			return nil
		},
	}
	rep := r.Run(t)
	rep.AssertClean(t)
}
