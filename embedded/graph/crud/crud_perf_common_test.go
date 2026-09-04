//go:build perf

package crud_test

// Shared helpers for the EMBEDDED (in-process) perf benches — crud-read /
// crud-update / crud-delete. These run on an embedded NATS test runtime, so
// they measure the SERVER-SIDE cost of each operation with NO NATS round-trips
// (unlike the docker scenarios under tests/perf/<name>/, which measure
// end-to-end latency over a real NATS transport). Run them via
//   scripts/run-perf-tests.sh --embedded [--scenario crud-read|crud-update|crud-delete]
//
// Build-tagged `perf` so a normal `go test ./...` never compiles them.

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
)

// perfResult is one measured latency distribution + throughput.
type perfResult struct {
	conc, n, degree     int
	p50, p95, p99       time.Duration
	throughputOpsPerSec float64
	benign              int64 // idempotent "does not exist" outcomes
}

// measurePerf runs op(0..n-1) at the given concurrency and returns latency
// percentiles + wall-clock throughput. A "does not exist" error is a benign
// idempotent outcome (counted, not failed); any other error fails the test.
func measurePerf(t *testing.T, conc, n int, op func(i int) error) perfResult {
	durs := make([]time.Duration, n)
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var failed, benign int64
	var firstErr atomic.Value
	wall0 := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			err := op(i)
			durs[i] = time.Since(t0)
			if err != nil {
				if strings.Contains(err.Error(), "does not exist") {
					atomic.AddInt64(&benign, 1)
				} else {
					atomic.AddInt64(&failed, 1)
					firstErr.CompareAndSwap(nil, err.Error())
				}
			}
		}(i)
	}
	wg.Wait()
	wall := time.Since(wall0)
	if failed > 0 {
		t.Fatalf("perf op failures=%d (benign idempotent excepted); first hard error: %v", failed, firstErr.Load())
	}
	sort.Slice(durs, func(a, b int) bool { return durs[a] < durs[b] })
	pct := func(p float64) time.Duration {
		idx := int(p * float64(n))
		if idx >= n {
			idx = n - 1
		}
		return durs[idx]
	}
	return perfResult{
		conc: conc, n: n,
		p50: pct(0.50), p95: pct(0.95), p99: pct(0.99),
		throughputOpsPerSec: float64(n) / wall.Seconds(),
		benign:              benign,
	}
}

// recordPerf logs the result and, if PERF_EMBEDDED_CSV is set, appends a row.
func recordPerf(t *testing.T, scenario, op string, r perfResult) {
	t.Logf("[%s] mode=%s op=%s conc=%-3d n=%-6d degree=%-4d  p50=%-10v p95=%-10v p99=%-10v  throughput=%.0f ops/s  benign=%d",
		scenario, cache.CacheMode(), op, r.conc, r.n, r.degree,
		r.p50.Round(time.Microsecond), r.p95.Round(time.Microsecond), r.p99.Round(time.Microsecond),
		r.throughputOpsPerSec, r.benign)

	csvPath := os.Getenv("PERF_EMBEDDED_CSV")
	if csvPath == "" {
		return
	}
	f, err := os.OpenFile(csvPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Logf("warn: cannot open PERF_EMBEDDED_CSV %s: %v", csvPath, err)
		return
	}
	defer f.Close()
	if st, e := f.Stat(); e == nil && st.Size() == 0 {
		fmt.Fprintln(f, "git_sha,host,cache_mode,scenario,op,concurrency,n,degree,p50_us,p95_us,p99_us,throughput_ops_s,benign")
	}
	fmt.Fprintf(f, "%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%.1f,%d\n",
		os.Getenv("PERF_GIT_SHA"), os.Getenv("PERF_HOST"), cache.CacheMode(),
		scenario, op, r.conc, r.n, r.degree,
		r.p50.Microseconds(), r.p95.Microseconds(), r.p99.Microseconds(),
		r.throughputOpsPerSec, r.benign)
}

func perfConcurrencies() []int {
	raw := os.Getenv("PERF_CONCURRENCIES")
	if raw == "" {
		raw = "1 4 16"
	}
	var out []int
	for _, tok := range strings.Fields(raw) {
		if v, err := strconv.Atoi(tok); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		out = []int{1, 4, 16}
	}
	return out
}

func perfEnvInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

// drainPendingWAL waits (bounded) until the async WAL writer has published the
// cache's buffered transactions, so the shared harness's emergency teardown has
// nothing left to fail-publish against the closed NATS conn. (The runner also
// filters the residual teardown lines, but draining keeps the backlog small.)
func drainPendingWAL(rt *statefun.Runtime) {
	deadline := time.Now().Add(15 * time.Second)
	for rt.Domain.Cache().HasPendingWrites() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}
