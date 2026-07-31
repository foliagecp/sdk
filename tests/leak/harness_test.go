//go:build leak

package leak

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
)

// ---------------------------------------------------------------------------
// Sizing knobs. Code defaults are the "quick" mode; scripts/run-leak-tests.sh
// overrides them via env for the full (3-sigma) run.
// ---------------------------------------------------------------------------

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func warmupCycles() int  { return envInt("LEAK_WARMUP", 2) }
func measureCycles() int { return envInt("LEAK_CYCLES", 8) }

// scaled applies the LEAK_SCALE multiplier to a per-cycle workload size.
func scaled(n int) int {
	s := int(float64(n)*envFloat("LEAK_SCALE", 1) + 0.5)
	if s < 1 {
		return 1
	}
	return s
}

// ---------------------------------------------------------------------------
// Samples and verdicts
// ---------------------------------------------------------------------------

// Sample is one post-quiesce measurement at the end of a churn cycle.
type Sample struct {
	Cycle       int // -1 for the post-warmup baseline
	HeapAlloc   uint64
	HeapObjects uint64
	Goroutines  int
	Custom      map[string]float64
}

// Verdict is the statistical decision for one drift metric over the measured
// cycles: LEAK iff the OLS slope is 3-sigma significant, above the absolute
// floor AND the growth persists in the tail of the window (TailSlope). The
// tail condition separates a real leak — a trend that keeps going — from a
// one-time STEP to a high-water mark (pools, request-path timers, map
// capacities crossing a growth threshold mid-window), which a small-M OLS
// fit would otherwise flag as a significant slope. Bound3Sigma and
// DetectionFloor make the claim honest — they state what residual drift is
// still compatible with the data and what leak rate this run could not have
// seen at all.
type Verdict struct {
	Metric         string
	Slope          float64 // units per cycle, full window
	StdErr         float64 // standard error of the slope
	TailSlope      float64 // slope over the second half of the window
	Floor          float64 // absolute significance floor
	Bound3Sigma    float64 // slope + 3*SE: residual-drift upper bound
	DetectionFloor float64 // max(3*SE, floor): smallest leak this run could flag
	Leaking        bool
	// ReportOnly verdicts are informational: emitted as REPORT lines and
	// never fail the test. Used for growth that is real but lives outside
	// the SDK's own heap (the embedded NATS server shares the test process;
	// in production it is a separate process).
	ReportOnly bool
}

// olsSlope fits y = a + b*x over x = 0..n-1 and returns the slope with its
// standard error. Fewer than 3 points cannot support an error estimate.
func olsSlope(y []float64) (b, se float64) {
	n := float64(len(y))
	if len(y) < 3 {
		return 0, math.Inf(1)
	}
	var sx, sy float64
	for i, v := range y {
		sx += float64(i)
		sy += v
	}
	mx, my := sx/n, sy/n
	var sxx, sxy float64
	for i, v := range y {
		dx := float64(i) - mx
		sxx += dx * dx
		sxy += dx * (v - my)
	}
	if sxx == 0 {
		return 0, math.Inf(1)
	}
	b = sxy / sxx
	a := my - b*mx
	var sse float64
	for i, v := range y {
		e := v - (a + b*float64(i))
		sse += e * e
	}
	se = math.Sqrt(sse / (n - 2) / sxx)
	return b, se
}

func verdictFor(metric string, y []float64, floor float64) Verdict {
	b, se := olsSlope(y)
	// Tail slope: the second half of the window (needs >= 3 points to fit).
	// With too few samples the discriminator degrades to the full slope, so
	// short runs keep the plain criterion.
	tail := b
	if half := y[len(y)/2:]; len(half) >= 3 {
		tail, _ = olsSlope(half)
	}
	return Verdict{
		Metric:         metric,
		Slope:          b,
		StdErr:         se,
		TailSlope:      tail,
		Floor:          floor,
		Bound3Sigma:    b + 3*se,
		DetectionFloor: math.Max(3*se, floor),
		// A leak is a PERSISTENT trend: significant over the full window AND
		// still above the floor in the tail. A mid-window step (one-time
		// high-water-mark growth) fails the tail condition and passes.
		Leaking: b > 3*se && b > floor && tail > floor,
	}
}

// ---------------------------------------------------------------------------
// LEAKCHECK reporting: one machine-readable line per check, parsed by
// scripts/run-leak-tests.sh into the summary table.
// ---------------------------------------------------------------------------

func emitCheck(scenario, check, status string, kv ...string) {
	line := fmt.Sprintf("LEAKCHECK|scenario=%s|check=%s|status=%s", scenario, check, status)
	for _, p := range kv {
		line += "|" + p
	}
	fmt.Println(line)
}

func f1(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }

// ---------------------------------------------------------------------------
// CycleRunner: drives warmup + measured churn cycles with a full quiesce and
// sample after each, then fits the drift verdicts.
// ---------------------------------------------------------------------------

type CycleRunner struct {
	Scenario string
	Warmup   int
	Measure  int
	Cycle    func(i int) error // one full churn cycle; i counts warmup+measure
	Collect  func(s *Sample)   // optional custom counters, called post-quiesce
	Quiesce  func()            // optional runtime quiesce (leakSuite.quiesce)
	Floors   map[string]float64
	// SettleTimeout bounds the final wait for goroutines to return to the
	// baseline count. 0 means 15s.
	SettleTimeout time.Duration
	// SplitNatsHeap separates the process heap by allocation stack into the
	// SDK's own share and the embedded NATS server's share (per-sample heap
	// profile). The SDK share is ASSERTED; the server share and the raw
	// process totals become REPORT-only. Runtime-backed scenarios need this:
	// JetStream/KV churn grows server-side state (per-subject tree nodes,
	// file-store buffers, retained KV tombstones) inside the same process,
	// which in production belongs to a separate NATS process.
	SplitNatsHeap bool
}

type Report struct {
	Scenario          string
	Dir               string
	Baseline          Sample
	Samples           []Sample
	Verdicts          []Verdict
	GoroutineBaseline int
	GoroutineFinal    int
	GoroutinesSettled bool
}

func (rep *Report) VerdictFor(metric string) Verdict {
	for _, v := range rep.Verdicts {
		if v.Metric == metric {
			return v
		}
	}
	return Verdict{Metric: metric, StdErr: math.Inf(1)}
}

// Run executes the cycles and produces the report with its artifacts (CSV,
// heap profiles). It fails the test only on workload/plumbing errors; leak
// verdicts are asserted separately (AssertClean) so the S0 self-test can
// verify that a planted leak IS flagged.
func (r *CycleRunner) Run(t *testing.T) *Report {
	t.Helper()

	dir := filepath.Join(resultsDir, r.Scenario)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("[%s] cannot create artifacts dir %s: %v", r.Scenario, dir, err)
	}
	rep := &Report{Scenario: r.Scenario, Dir: dir}

	for i := 0; i < r.Warmup; i++ {
		if err := r.Cycle(i); err != nil {
			t.Fatalf("[%s] warmup cycle %d failed: %v", r.Scenario, i, err)
		}
	}
	rep.Baseline = r.sample(-1)
	rep.GoroutineBaseline = rep.Baseline.Goroutines
	writeHeapProfile(t, filepath.Join(dir, "heap-baseline.pb.gz"))

	for i := 0; i < r.Measure; i++ {
		if err := r.Cycle(r.Warmup + i); err != nil {
			t.Fatalf("[%s] measure cycle %d failed: %v", r.Scenario, i, err)
		}
		rep.Samples = append(rep.Samples, r.sample(i))
	}
	writeHeapProfile(t, filepath.Join(dir, "heap-final.pb.gz"))

	// Wait for goroutines to settle back to (or below) the baseline; the
	// count fluctuates with NATS internals, so this is a bounded retry, not
	// a single read.
	settleDeadline := time.Now().Add(r.settleTimeout())
	for {
		rep.GoroutineFinal = runtime.NumGoroutine()
		if rep.GoroutineFinal <= rep.GoroutineBaseline {
			rep.GoroutinesSettled = true
			break
		}
		if time.Now().After(settleDeadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !rep.GoroutinesSettled {
		dumpGoroutines(filepath.Join(dir, "goroutines-final.txt"))
	}

	bytesFloor := envFloat("LEAK_FLOOR_HEAP_BYTES", 64*1024)
	objectsFloor := envFloat("LEAK_FLOOR_HEAP_OBJECTS", 500)
	addVerdict := func(metric string, floorDef float64, reportOnly bool) {
		v := verdictFor(metric, rep.metricSeries(metric), r.floor(metric, floorDef))
		v.ReportOnly = reportOnly
		rep.Verdicts = append(rep.Verdicts, v)
	}
	if r.SplitNatsHeap {
		addVerdict("sdk_inuse_bytes", bytesFloor, false)
		addVerdict("sdk_inuse_objects", objectsFloor, false)
		addVerdict("nats_inuse_bytes", 0, true)
		addVerdict("nats_inuse_objects", 0, true)
		addVerdict("heap_alloc", bytesFloor, true)
		addVerdict("heap_objects", objectsFloor, true)
	} else {
		addVerdict("heap_alloc", bytesFloor, false)
		addVerdict("heap_objects", objectsFloor, false)
	}

	if err := rep.writeCSV(); err != nil {
		t.Fatalf("[%s] cannot write samples CSV: %v", r.Scenario, err)
	}
	return rep
}

// metricSeries extracts a metric's per-cycle series: built-in fields by name,
// everything else from Custom.
func (rep *Report) metricSeries(metric string) []float64 {
	y := make([]float64, len(rep.Samples))
	for i, s := range rep.Samples {
		switch metric {
		case "heap_alloc":
			y[i] = float64(s.HeapAlloc)
		case "heap_objects":
			y[i] = float64(s.HeapObjects)
		default:
			y[i] = s.Custom[metric]
		}
	}
	return y
}

func (r *CycleRunner) settleTimeout() time.Duration {
	if r.SettleTimeout > 0 {
		return r.SettleTimeout
	}
	return 15 * time.Second
}

func (r *CycleRunner) floor(metric string, def float64) float64 {
	if v, ok := r.Floors[metric]; ok {
		return v
	}
	return def
}

// sample quiesces the runtime (if configured), forces GC and takes one
// measurement. The goroutine count is the minimum of three spaced reads to
// skate over transient NATS timer churn.
func (r *CycleRunner) sample(cycle int) Sample {
	if r.Quiesce != nil {
		r.Quiesce()
	}
	runtime.GC()
	runtime.GC()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	g := runtime.NumGoroutine()
	for i := 0; i < 2; i++ {
		time.Sleep(75 * time.Millisecond)
		if n := runtime.NumGoroutine(); n < g {
			g = n
		}
	}
	s := Sample{
		Cycle:       cycle,
		HeapAlloc:   ms.HeapAlloc,
		HeapObjects: ms.HeapObjects,
		Goroutines:  g,
		Custom:      map[string]float64{},
	}
	if r.SplitNatsHeap {
		sdkB, sdkO, natsB, natsO := heapSplitByOwner()
		s.Custom["sdk_inuse_bytes"] = float64(sdkB)
		s.Custom["sdk_inuse_objects"] = float64(sdkO)
		s.Custom["nats_inuse_bytes"] = float64(natsB)
		s.Custom["nats_inuse_objects"] = float64(natsO)
	}
	if r.Collect != nil {
		r.Collect(&s)
	}
	return s
}

// heapSplitByOwner snapshots the in-use heap (as of the GC that just ran) and
// splits it by allocation stack: a sample whose stack contains any
// nats-io/nats-server frame is the embedded server's, everything else is the
// SDK's (tests, sdk packages, stdlib on their behalf).
func heapSplitByOwner() (sdkBytes, sdkObjects, natsBytes, natsObjects int64) {
	var buf bytes.Buffer
	if err := pprof.WriteHeapProfile(&buf); err != nil {
		return 0, 0, 0, 0
	}
	prof, err := profile.Parse(&buf)
	if err != nil {
		return 0, 0, 0, 0
	}
	bytesIdx, objectsIdx := -1, -1
	for i, st := range prof.SampleType {
		switch st.Type {
		case "inuse_space":
			bytesIdx = i
		case "inuse_objects":
			objectsIdx = i
		}
	}
	if bytesIdx == -1 || objectsIdx == -1 {
		return 0, 0, 0, 0
	}
	for _, s := range prof.Sample {
		nats := false
	frames:
		for _, loc := range s.Location {
			for _, line := range loc.Line {
				if line.Function != nil && strings.HasPrefix(line.Function.Name, "github.com/nats-io/nats-server") {
					nats = true
					break frames
				}
			}
		}
		if nats {
			natsBytes += s.Value[bytesIdx]
			natsObjects += s.Value[objectsIdx]
		} else {
			sdkBytes += s.Value[bytesIdx]
			sdkObjects += s.Value[objectsIdx]
		}
	}
	return sdkBytes, sdkObjects, natsBytes, natsObjects
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

// AssertClean asserts the statistical heap verdicts and the goroutine settle.
// On a heap FAIL it also writes the per-function heap diff artifact.
func (rep *Report) AssertClean(t *testing.T) {
	t.Helper()
	diffWritten := false
	for _, v := range rep.Verdicts {
		kv := []string{
			"slope=" + f1(v.Slope), "se=" + f1(v.StdErr), "tail=" + f1(v.TailSlope),
			"floor=" + f1(v.Floor), "bound3s=" + f1(v.Bound3Sigma), "dfloor=" + f1(v.DetectionFloor),
		}
		if v.ReportOnly {
			emitCheck(rep.Scenario, v.Metric, "REPORT", kv...)
			continue
		}
		if v.Leaking {
			emitCheck(rep.Scenario, v.Metric, "FAIL", kv...)
			t.Errorf("[%s] %s leaking: slope %.1f/cycle (SE %.1f, floor %.1f)",
				rep.Scenario, v.Metric, v.Slope, v.StdErr, v.Floor)
			if !diffWritten {
				if path, err := rep.writeHeapDiff(); err == nil {
					t.Logf("[%s] per-function heap diff: %s", rep.Scenario, path)
				} else {
					t.Logf("[%s] heap diff unavailable: %v", rep.Scenario, err)
				}
				diffWritten = true
			}
		} else {
			emitCheck(rep.Scenario, v.Metric, "PASS", kv...)
		}
	}

	gkv := []string{
		"baseline=" + strconv.Itoa(rep.GoroutineBaseline),
		"final=" + strconv.Itoa(rep.GoroutineFinal),
	}
	if rep.GoroutinesSettled {
		emitCheck(rep.Scenario, "goroutines", "PASS", gkv...)
	} else {
		emitCheck(rep.Scenario, "goroutines", "FAIL", gkv...)
		t.Errorf("[%s] goroutines did not settle: baseline %d, final %d (stacks: %s)",
			rep.Scenario, rep.GoroutineBaseline, rep.GoroutineFinal,
			filepath.Join(rep.Dir, "goroutines-final.txt"))
	}
}

// AssertStable requires a custom counter to return exactly to its post-warmup
// baseline in the last sample — the deterministic (no-sigma) invariant.
func (rep *Report) AssertStable(t *testing.T, metric string) {
	t.Helper()
	if len(rep.Samples) == 0 {
		t.Fatalf("[%s] no samples for metric %q", rep.Scenario, metric)
	}
	base := rep.Baseline.Custom[metric]
	last := rep.Samples[len(rep.Samples)-1].Custom[metric]
	delta := last - base
	kv := []string{"baseline=" + f1(base), "last=" + f1(last), "delta=" + f1(delta)}
	if delta != 0 {
		emitCheck(rep.Scenario, metric, "FAIL", kv...)
		t.Errorf("[%s] counter %q leaked: baseline %g -> %g (delta %+g)",
			rep.Scenario, metric, base, last, delta)
	} else {
		emitCheck(rep.Scenario, metric, "PASS", kv...)
	}
}

// ReportMetric emits an informational (non-asserting) LEAKCHECK line with the
// counter's per-cycle slope — used for by-design growth we track but do not
// fail on (e.g. NATS-side KV tombstones).
func (rep *Report) ReportMetric(t *testing.T, metric string) {
	t.Helper()
	y := make([]float64, len(rep.Samples))
	for i, s := range rep.Samples {
		y[i] = s.Custom[metric]
	}
	b, se := olsSlope(y)
	base := rep.Baseline.Custom[metric]
	last := rep.Samples[len(rep.Samples)-1].Custom[metric]
	emitCheck(rep.Scenario, metric, "REPORT",
		"slope="+f1(b), "se="+f1(se), "baseline="+f1(base), "last="+f1(last))
	t.Logf("[%s] %s: %.1f -> %.1f, slope %.1f/cycle (informational)",
		rep.Scenario, metric, base, last, b)
}

// ---------------------------------------------------------------------------
// Artifacts
// ---------------------------------------------------------------------------

func (rep *Report) writeCSV() error {
	keys := map[string]struct{}{}
	for k := range rep.Baseline.Custom {
		keys[k] = struct{}{}
	}
	for _, s := range rep.Samples {
		for k := range s.Custom {
			keys[k] = struct{}{}
		}
	}
	custom := make([]string, 0, len(keys))
	for k := range keys {
		custom = append(custom, k)
	}
	sort.Strings(custom)

	f, err := os.Create(filepath.Join(rep.Dir, "samples.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	header := append([]string{"cycle", "heap_alloc", "heap_objects", "goroutines"}, custom...)
	if err := w.Write(header); err != nil {
		return err
	}
	row := func(s Sample) []string {
		r := []string{
			strconv.Itoa(s.Cycle),
			strconv.FormatUint(s.HeapAlloc, 10),
			strconv.FormatUint(s.HeapObjects, 10),
			strconv.Itoa(s.Goroutines),
		}
		for _, k := range custom {
			r = append(r, f1(s.Custom[k]))
		}
		return r
	}
	if err := w.Write(row(rep.Baseline)); err != nil {
		return err
	}
	for _, s := range rep.Samples {
		if err := w.Write(row(s)); err != nil {
			return err
		}
	}
	return nil
}

func writeHeapProfile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("cannot create heap profile %s: %v", path, err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatalf("cannot write heap profile %s: %v", path, err)
	}
}

func dumpGoroutines(path string) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	_ = pprof.Lookup("goroutine").WriteTo(f, 2)
}

// writeHeapDiff renders a per-function inuse_space delta table between the
// baseline and final heap profiles (same aggregation as the heap watcher: the
// innermost named function of each sample's stack).
func (rep *Report) writeHeapDiff() (string, error) {
	baseM, err := summarizeInuse(filepath.Join(rep.Dir, "heap-baseline.pb.gz"))
	if err != nil {
		return "", err
	}
	currM, err := summarizeInuse(filepath.Join(rep.Dir, "heap-final.pb.gz"))
	if err != nil {
		return "", err
	}

	type change struct {
		name  string
		delta int64
		curr  int64
	}
	seen := map[string]struct{}{}
	for k := range baseM {
		seen[k] = struct{}{}
	}
	for k := range currM {
		seen[k] = struct{}{}
	}
	var changes []change
	for name := range seen {
		d := currM[name] - baseM[name]
		if d != 0 {
			changes = append(changes, change{name, d, currM[name]})
		}
	}
	sort.Slice(changes, func(i, j int) bool {
		ai, aj := changes[i].delta, changes[j].delta
		if ai < 0 {
			ai = -ai
		}
		if aj < 0 {
			aj = -aj
		}
		return ai > aj
	})
	if len(changes) > 40 {
		changes = changes[:40]
	}

	path := filepath.Join(rep.Dir, "heap-diff.txt")
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fmt.Fprintf(f, "inuse_space delta, baseline -> final (%s), top %d by |delta|:\n",
		rep.Scenario, len(changes))
	for _, c := range changes {
		fmt.Fprintf(f, "  %-70s %+10.1f KB (now %10.1f KB)\n",
			c.name, float64(c.delta)/1024, float64(c.curr)/1024)
	}
	return path, nil
}

func summarizeInuse(path string) (map[string]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	prof, err := profile.Parse(f)
	if err != nil {
		return nil, err
	}
	inuseIdx := -1
	for i, st := range prof.SampleType {
		if st.Type == "inuse_space" {
			inuseIdx = i
			break
		}
	}
	if inuseIdx == -1 {
		return nil, fmt.Errorf("inuse_space not found in %s", path)
	}
	m := map[string]int64{}
	for _, s := range prof.Sample {
		name := "unknown"
		for _, loc := range s.Location {
			for _, line := range loc.Line {
				if line.Function != nil && line.Function.Name != "" {
					name = line.Function.Name
					break
				}
			}
			if name != "unknown" {
				break
			}
		}
		m[name] += s.Value[inuseIdx]
	}
	return m, nil
}
