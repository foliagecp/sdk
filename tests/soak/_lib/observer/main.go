// Command observer is the host-side probe for the docker-compose soak tests
// under tests/soak/. It runs for the duration of a scenario, sampling the
// runtime's liveness and Prometheus metrics on a fixed interval, and writing
// every sample to CSV. At end-of-run it asserts a small set of SLOs and exits
// non-zero on violation.
//
// The observer is intentionally a separate binary from the system-tests
// assert client: assert exists to drive workloads (seed, soak, verify), while
// observer exists to *watch*. The scenarios run both concurrently.
//
// What it checks:
//
//   * Liveness: every -interval seconds, try a unique CMDB TypeCreate. A
//     successful round-trip means an active runtime is up and serving. A
//     stretch of consecutive failures longer than -max-stall-sec is flagged
//     as an unrecovered stall, which is the primary FAILURE signal (the
//     116-class regression).
//   * Memory drift: scrape fg_runtime_mem_alloc_bytes. Fits a least-squares
//     slope over the WHOLE observation window (minus an initial warmup),
//     not a trailing slice — that way a GC saw-tooth landing inside an
//     arbitrary trailing window cannot raise a false-positive on a process
//     whose long-term memory is flat. If the slope is above
//     -max-mem-drift-bytes-per-hour, FAIL.
//   * Goroutine drift: scrape fg_runtime_routines_counter. Same shape; bounds
//     a leak in spawned goroutines (becomePassive that never finishes, etc).
//   * Exclusivity (HA mode): if -prom-urls has multiple endpoints, at most
//     one runtime should accept liveness writes at a time (the active). The
//     observer pings all of them; more than one OK reply in the same tick
//     means split-brain.
//
// Liveness is the gate: a 30s stall in nats-stall-recovery, or a 30s gap
// after killing the active in ha-promotion-flap, is the failure mode we are
// chasing. Memory/goroutine drift are secondary "is the box healthy" checks
// for the long-running scenarios where wedges are slow.
//
// Output: rows in CSV with one row per sample. Header:
//
//	ts,liveness_ok,liveness_latency_ms,active_runtime_url,mem_alloc_bytes,
//	goroutines,observed_endpoints,active_count,stall_consec_sec
//
// The first column is unix seconds. active_count is the number of endpoints
// that answered OK on this tick (HA exclusivity check).
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/foliagecp/sdk/clients/go/db"
)

// flags --------------------------------------------------------------------

type flags struct {
	natsURLs    string // comma-separated NATS URLs; one per runtime endpoint
	promURLs    string // comma-separated http://host:port/metrics endpoints
	csvPath     string
	domain      string
	duration    time.Duration
	interval    time.Duration
	maxStall    time.Duration
	memDriftBph int64 // bytes/hour
	grDriftPh   int64 // goroutines/hour
	warmup      time.Duration // skip this much at the start of the run before drift fits begin
	timeout     time.Duration
	preflight   time.Duration // wait this long for the first OK before starting the clock
}

func parseFlags() *flags {
	f := &flags{}
	flag.StringVar(&f.natsURLs, "nats", "nats://nats:foliage@localhost:4222", "comma-separated NATS URLs (one per runtime endpoint)")
	flag.StringVar(&f.promURLs, "prom", "http://localhost:9901/metrics", "comma-separated Prometheus /metrics endpoints (one per runtime)")
	flag.StringVar(&f.csvPath, "csv", "", "output CSV path (required)")
	flag.StringVar(&f.domain, "domain", "hub", "hub domain name")
	flag.DurationVar(&f.duration, "duration", time.Hour, "total observation window")
	flag.DurationVar(&f.interval, "interval", 10*time.Second, "sample interval")
	flag.DurationVar(&f.maxStall, "max-stall", 30*time.Second, "max consecutive liveness-fail window before FAIL")
	flag.Int64Var(&f.memDriftBph, "max-mem-drift-bph", 200*1024*1024, "max allowed memory drift (bytes/hour) over the trailing window; <=0 disables")
	flag.Int64Var(&f.grDriftPh, "max-goroutine-drift-ph", 200, "max allowed goroutine drift (goroutines/hour) over the trailing window; <=0 disables")
	flag.DurationVar(&f.warmup, "drift-warmup", 60*time.Second, "ignore this much of the run at the start before fitting drift (lets caches settle)")
	flag.DurationVar(&f.timeout, "timeout", 5*time.Second, "per-probe timeout for both NATS and Prometheus")
	flag.DurationVar(&f.preflight, "preflight", 2*time.Minute, "wait up to this long for the first OK before starting the duration clock")
	flag.Parse()
	return f
}

// sample is one row of the CSV.
type sample struct {
	ts                time.Time
	livenessOK        bool
	livenessLatency   time.Duration
	activeURL         string
	memAllocBytes     int64
	heapObjects       int64
	goroutines        int64
	observedEndpoints int
	activeCount       int
	stallConsec       time.Duration
}

func main() {
	f := parseFlags()
	if f.csvPath == "" {
		fmt.Fprintln(os.Stderr, "-csv is required")
		os.Exit(2)
	}
	natsList := splitCSV(f.natsURLs)
	promList := splitCSV(f.promURLs)
	if len(natsList) == 0 {
		fmt.Fprintln(os.Stderr, "-nats: at least one URL required")
		os.Exit(2)
	}
	if len(promList) != 0 && len(promList) != len(natsList) {
		// promList is allowed to be empty (no metrics scraping); otherwise it
		// must match natsList one-for-one so memory/goroutine readings can be
		// attributed to the correct endpoint.
		fmt.Fprintln(os.Stderr, "-prom must have same number of URLs as -nats, or be empty")
		os.Exit(2)
	}

	csvFile, err := os.Create(f.csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open csv: %v\n", err)
		os.Exit(2)
	}
	defer csvFile.Close()
	csv := bufio.NewWriter(csvFile)
	defer csv.Flush()
	fmt.Fprintln(csv, "ts,liveness_ok,liveness_latency_ms,active_url,mem_alloc_bytes,heap_objects,goroutines,observed_endpoints,active_count,stall_consec_sec")

	// One persistent DB client per NATS URL. db.NewDBSyncClient opens a new
	// NATS connection every call and never closes it; at 360 probes/hour we
	// would leak ~360 connections per endpoint per run. The clients are safe
	// to reuse — the underlying *nats.Conn is goroutine-safe — and the
	// request function inside DBSyncClient publishes a fresh request on each
	// call, so reuse is correct for liveness too.
	clients := make([]db.DBSyncClient, len(natsList))
	for i, u := range natsList {
		c, err := db.NewDBSyncClient(u, int(f.timeout.Seconds()), f.domain)
		if err != nil {
			fmt.Fprintf(os.Stderr, "observer: cannot connect to %s: %v\n", u, err)
			os.Exit(1)
		}
		clients[i] = c
	}

	// Pre-flight: block until at least one endpoint answers OK or the
	// preflight budget elapses. This keeps the duration clock honest — soak
	// runs are 1h of *running* time, not 1h of "wait for the container to
	// build then time-out".
	fmt.Printf("observer: preflight wait up to %s for first OK across %d endpoint(s)\n", f.preflight, len(natsList))
	preCtx, preCancel := context.WithTimeout(context.Background(), f.preflight)
	if !waitFirstOK(preCtx, clients) {
		preCancel()
		fmt.Fprintln(os.Stderr, "observer: no endpoint answered within preflight window")
		os.Exit(1)
	}
	preCancel()

	fmt.Printf("observer: starting %s duration, %s interval, max-stall %s\n", f.duration, f.interval, f.maxStall)

	ctx, cancel := context.WithTimeout(context.Background(), f.duration)
	defer cancel()

	var (
		samples       []sample
		stallStart    time.Time
		maxStallSeen  time.Duration
		stallViolated bool
		ticks         int
	)

	tick := time.NewTicker(f.interval)
	defer tick.Stop()

	// first tick immediately, then on the interval
	for {
		ticks++
		s := probeOnce(clients, natsList, promList, f.timeout)
		samples = append(samples, s)
		// stall tracking — keyed on "at least one endpoint OK on this tick".
		now := s.ts
		if s.activeCount == 0 {
			if stallStart.IsZero() {
				stallStart = now
			}
			s.stallConsec = now.Sub(stallStart)
			if s.stallConsec > maxStallSeen {
				maxStallSeen = s.stallConsec
			}
			if s.stallConsec > f.maxStall && !stallViolated {
				stallViolated = true
				fmt.Fprintf(os.Stderr, "observer: STALL exceeded max (%s > %s) at %s\n",
					s.stallConsec, f.maxStall, now.Format(time.RFC3339))
			}
		} else {
			stallStart = time.Time{}
			s.stallConsec = 0
		}
		writeRow(csv, s)
		csv.Flush()

		select {
		case <-ctx.Done():
			goto done
		case <-tick.C:
		}
	}
done:

	// SLO judgement
	fail := stallViolated
	fmt.Printf("observer: collected %d samples; max stall observed %s\n", len(samples), maxStallSeen)

	// HA exclusivity: any tick where more than one endpoint answered OK is a
	// split-brain. Some interleavings during failover can briefly show two
	// candidates, but only if more than one endpoint *successfully accepted a
	// write* — both probes were active leaders, which is the violation.
	splitBrainTicks := 0
	for _, s := range samples {
		if s.activeCount > 1 {
			splitBrainTicks++
		}
	}
	if splitBrainTicks > 0 && len(natsList) > 1 {
		fmt.Fprintf(os.Stderr, "observer: SPLIT-BRAIN on %d/%d ticks (>1 endpoint accepted writes)\n",
			splitBrainTicks, len(samples))
		fail = true
	}

	// Memory / goroutine drift over the full observation window (after the
	// warmup window so cache initial-load + worker-pool spin-up don't bias the
	// slope upward). A least-squares fit over O(360+) samples averages out
	// the GC saw-tooth; a stable process whose Alloc oscillates 555→594 MB
	// settles to slope ≈ 0, instead of catching the last 60 samples' worth
	// of saw-tooth and reporting a false leak. Need at least 2 points after
	// warmup; otherwise we skip the check entirely.
	if len(promList) > 0 {
		fitSamples := samplesAfterWarmup(samples, f.warmup)
		if len(fitSamples) >= 2 {
			memBph, grPh := computeDrifts(fitSamples, f.interval)
			windowSec := fitSamples[len(fitSamples)-1].ts.Sub(fitSamples[0].ts).Seconds()
			fmt.Printf("observer: full-window memory drift %d B/hr, goroutine drift %.1f /hr (fit over %.0fs, %d samples)\n",
				memBph, grPh, windowSec, len(fitSamples))
			if f.memDriftBph > 0 && memBph > f.memDriftBph {
				fmt.Fprintf(os.Stderr, "observer: MEM-DRIFT %d B/hr > limit %d B/hr\n", memBph, f.memDriftBph)
				fail = true
			}
			if f.grDriftPh > 0 && int64(grPh) > f.grDriftPh {
				fmt.Fprintf(os.Stderr, "observer: GOROUTINE-DRIFT %.1f /hr > limit %d /hr\n", grPh, f.grDriftPh)
				fail = true
			}
		} else {
			fmt.Printf("observer: drift check skipped (need >=2 samples after %s warmup; have %d)\n",
				f.warmup, len(fitSamples))
		}
	}

	if fail {
		fmt.Fprintln(os.Stderr, "observer: SLO violations — FAIL")
		os.Exit(1)
	}
	fmt.Println("observer: OK")
}

// helpers ------------------------------------------------------------------

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// probeOnce hits every endpoint in parallel, one liveness write per endpoint
// plus its Prometheus scrape. Returns an aggregated sample.
func probeOnce(clients []db.DBSyncClient, natsURLs, promURLs []string, timeout time.Duration) sample {
	now := time.Now()
	s := sample{ts: now, observedEndpoints: len(clients)}

	type endpointResult struct {
		idx     int
		ok      bool
		latency time.Duration
		mem     int64
		objs    int64
		gor     int64
		url     string
	}
	results := make([]endpointResult, len(clients))
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := endpointResult{idx: i, url: natsURLs[i]}
			r.ok, r.latency = liveness(clients[i])
			if i < len(promURLs) {
				r.mem, r.objs, r.gor = scrapeProm(promURLs[i], timeout)
			}
			results[i] = r
		}(i)
	}
	wg.Wait()

	// Aggregate. The "active_url" reported is the first endpoint that
	// answered OK this tick (HA: that's the active leader; single-node: the
	// only endpoint). active_count is how many OK'd at all.
	// Memory / goroutines reported are the max across endpoints — useful for
	// catching "passive is leaking" too, not just the leader.
	for _, r := range results {
		if r.ok {
			s.activeCount++
			if s.activeURL == "" {
				s.activeURL = r.url
				s.livenessOK = true
				s.livenessLatency = r.latency
			}
		}
		if r.mem > s.memAllocBytes {
			s.memAllocBytes = r.mem
		}
		if r.objs > s.heapObjects {
			s.heapObjects = r.objs
		}
		if r.gor > s.goroutines {
			s.goroutines = r.gor
		}
	}
	return s
}

// liveness sends a unique CMDB TypeCreate via the given client and returns
// (ok, latency). The probe name embeds the current nanosecond timestamp so a
// late reply to a previous probe can never be mistaken for this one's OK.
//
// Reuses the caller-provided client (one persistent NATS connection per URL
// for the whole run) — see the comment on the clients slice in main.
func liveness(c db.DBSyncClient) (bool, time.Duration) {
	start := time.Now()
	probe := fmt.Sprintf("__soak_probe_%d", time.Now().UnixNano())
	if err := c.CMDB.TypeCreate(probe); err != nil {
		return false, time.Since(start)
	}
	return true, time.Since(start)
}

// waitFirstOK polls every endpoint in turn until one answers OK, or ctx
// expires.
func waitFirstOK(ctx context.Context, clients []db.DBSyncClient) bool {
	for {
		for _, c := range clients {
			ok, _ := liveness(c)
			if ok {
				return true
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
}

// scrapeProm pulls fg_runtime_mem_alloc_bytes, go_memstats_heap_objects, and
// fg_runtime_routines_counter from a Prometheus /metrics endpoint. Returns
// (mem, heap_objects, goroutines); zeros on any error (caller can tell from
// the CSV that this tick had no metric).
//
// heap_objects is the key signal for cache-tombstone leaks: on a stable
// process where the in-KV graph isn't growing, this should plateau. A
// linear ramp here = orphaned StoreValue nodes (or other Go objects) the
// runtime never reclaims — see the discussion in tests/soak/leak-hunt.
func scrapeProm(url string, timeout time.Duration) (int64, int64, int64) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, 0, 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		// drain so the connection can be reused
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0, 0, 0
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var mem, objs, gor int64
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		if v, ok := parseSimpleMetric(line, "fg_runtime_mem_alloc_bytes"); ok {
			mem = v
			continue
		}
		if v, ok := parseSimpleMetric(line, "go_memstats_heap_objects"); ok {
			objs = v
			continue
		}
		if v, ok := parseSimpleMetric(line, "fg_runtime_routines_counter"); ok {
			gor = v
			continue
		}
	}
	return mem, objs, gor
}

// parseSimpleMetric returns the integer value of a no-label Prometheus
// metric line like "name 12345" or "name{} 12345.0".
func parseSimpleMetric(line, name string) (int64, bool) {
	if !strings.HasPrefix(line, name) {
		return 0, false
	}
	rest := strings.TrimPrefix(line, name)
	// Tolerate the empty-label "{}" form.
	rest = strings.TrimPrefix(rest, "{}")
	rest = strings.TrimSpace(rest)
	// Some emitters append a timestamp; the value is the first whitespace
	// token.
	if i := strings.IndexAny(rest, " \t"); i >= 0 {
		rest = rest[:i]
	}
	if rest == "" {
		return 0, false
	}
	// Accept floats — Prometheus may write "1.2345e+08".
	v, err := strconv.ParseFloat(rest, 64)
	if err != nil {
		return 0, false
	}
	return int64(v), true
}

// samplesAfterWarmup returns the suffix of s where ts >= s[0].ts + warmup.
// Used to drop the initial cache-load + worker-pool spin-up samples before
// fitting drift — those don't reflect steady-state.
func samplesAfterWarmup(s []sample, warmup time.Duration) []sample {
	if len(s) == 0 || warmup <= 0 {
		return s
	}
	cutoff := s[0].ts.Add(warmup)
	for i := range s {
		if !s[i].ts.Before(cutoff) {
			return s[i:]
		}
	}
	return nil
}

// computeDrifts fits a least-squares slope over the supplied samples and
// returns (mem bytes/hour, goroutines/hour). Caller passes the post-warmup
// slice; we DO NOT pick a trailing window here, because a trailing window
// of a stable-but-saw-toothing process gives spurious slopes — the longer
// the fit, the better the saw-tooth averages out.
func computeDrifts(s []sample, interval time.Duration) (int64, float64) {
	if len(s) < 2 {
		return 0, 0
	}
	// x is the index in interval units; y is the metric.
	var sx, sxx float64
	var smem, sxmem float64
	var sgor, sxgor float64
	for i, smp := range s {
		x := float64(i)
		sx += x
		sxx += x * x
		smem += float64(smp.memAllocBytes)
		sxmem += x * float64(smp.memAllocBytes)
		sgor += float64(smp.goroutines)
		sxgor += x * float64(smp.goroutines)
	}
	n := float64(len(s))
	denom := n*sxx - sx*sx
	if denom == 0 {
		return 0, 0
	}
	memSlope := (n*sxmem - sx*smem) / denom // bytes per sample
	grSlope := (n*sxgor - sx*sgor) / denom  // goroutines per sample
	samplesPerHour := float64(time.Hour) / float64(interval)
	return int64(memSlope * samplesPerHour), grSlope * samplesPerHour
}

func writeRow(w io.Writer, s sample) {
	fmt.Fprintf(w, "%d,%t,%d,%s,%d,%d,%d,%d,%d,%d\n",
		s.ts.Unix(),
		s.livenessOK,
		s.livenessLatency.Milliseconds(),
		s.activeURL,
		s.memAllocBytes,
		s.heapObjects,
		s.goroutines,
		s.observedEndpoints,
		s.activeCount,
		int64(s.stallConsec.Seconds()),
	)
}
