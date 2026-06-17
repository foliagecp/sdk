// Command perfclient runs timed performance workloads against a containerised
// foliage runtime and emits one CSV row per workload — latency percentiles
// (p50/p95/p99), throughput (ops/sec), error count.
//
// It is a sibling of tests/system/_lib/assert: same db client surface,
// different goal — numbers, not pass/fail. Orchestration (up / seed /
// down) lives in tests/perf/<scenario>/run.sh; this binary only drives a
// single timed workload and records the result row.
//
// Subcommands:
//
//	seed         create N objects in parallel, with progress (no chain links)
//	crud-write   sustained ObjectUpdate (upsert) on unique ids
//	crud-read    sustained ObjectRead on a pre-seeded id range [0..n)
//	crud-mixed   read/write mix (default 80/20)
//	crud-delete  sustained create+delete churn on unique ids (delete-path)
//
// Shared flags:
//
//	-nats         NATS URL                       (default nats://nats:foliage@localhost:4222)
//	-domain       hub domain name                (default hub)
//	-timeout      per-request timeout sec        (default 10)
//	-concurrency  number of concurrent workers   (default 4)
//	-warmup       warm-up seconds (discarded)    (default 15)
//	-duration     measurement window seconds     (default 30)
//	-csv          CSV file to append the row to  (empty = stdout only)
//	-scenario     CSV label, e.g. "crud-write"
//	-scale        CSV label, e.g. "10k"
//	-label        free-form label (query type, etc.)
//
// Host metadata for the CSV row is read from env (set by tests/perf/_lib/compose.sh):
//
//	PERF_GIT_SHA  PERF_HOST  PERF_CPU  PERF_MEM_GB  PERF_OS
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
)

// -----------------------------------------------------------------------------
// Shared workload runner
// -----------------------------------------------------------------------------

type sharedFlags struct {
	nats        string
	domain      string
	timeoutSec  int
	concurrency int
	warmupSec   int
	durationSec int
	csvPath     string
	scenario    string
	scale       string
	label       string
}

func registerSharedFlags(fs *flag.FlagSet, s *sharedFlags) {
	fs.StringVar(&s.nats, "nats", "nats://nats:foliage@localhost:4222", "NATS URL")
	fs.StringVar(&s.domain, "domain", "hub", "hub domain name")
	fs.IntVar(&s.timeoutSec, "timeout", 10, "per-request timeout seconds")
	fs.IntVar(&s.concurrency, "concurrency", 4, "number of concurrent workers")
	fs.IntVar(&s.warmupSec, "warmup", 15, "warm-up seconds (discarded from results)")
	fs.IntVar(&s.durationSec, "duration", 30, "measurement window seconds")
	fs.StringVar(&s.csvPath, "csv", "", "CSV file to append the result row to (empty = stdout only)")
	fs.StringVar(&s.scenario, "scenario", "", "scenario label (e.g. crud-write)")
	fs.StringVar(&s.scale, "scale", "", "scale label (e.g. 10k)")
	fs.StringVar(&s.label, "label", "", "free-form label")
}

type workFn func(ctx context.Context, workerID int, opIdx uint64) error

// runWorkload spawns sharedFlags.concurrency workers, each calling fn in a tight
// loop. The first sharedFlags.warmupSec are discarded; the next
// sharedFlags.durationSec form the measurement window. At the end p50/p95/p99
// latencies and throughput are printed and (optionally) appended to a CSV.
func runWorkload(s *sharedFlags, fn workFn) {
	if s.concurrency <= 0 {
		s.concurrency = 1
	}

	perWorker := make([][]time.Duration, s.concurrency)
	for w := range perWorker {
		perWorker[w] = make([]time.Duration, 0, 1<<14)
	}
	var errs atomic.Uint64
	var measuring atomic.Bool

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	var wg sync.WaitGroup
	var opCounter atomic.Uint64

	for w := 0; w < s.concurrency; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				idx := opCounter.Add(1)
				t0 := time.Now()
				err := fn(ctx, w, idx)
				dt := time.Since(t0)
				if measuring.Load() {
					if err != nil {
						errs.Add(1)
					} else {
						perWorker[w] = append(perWorker[w], dt)
					}
				}
			}
		}()
	}

	fmt.Fprintf(os.Stderr, "  warmup %ds (concurrency=%d)\n", s.warmupSec, s.concurrency)
	if !sleepCtx(ctx, time.Duration(s.warmupSec)*time.Second) {
		wg.Wait()
		return
	}

	measuring.Store(true)
	measureStart := time.Now()
	fmt.Fprintf(os.Stderr, "  measure %ds\n", s.durationSec)
	_ = sleepCtx(ctx, time.Duration(s.durationSec)*time.Second)
	measuring.Store(false)
	measureElapsed := time.Since(measureStart)

	cancel()
	wg.Wait()

	var all []time.Duration
	for _, lat := range perWorker {
		all = append(all, lat...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	var p50, p95, p99 time.Duration
	if n := len(all); n > 0 {
		p50 = all[idxAt(n, 50)]
		p95 = all[idxAt(n, 95)]
		p99 = all[idxAt(n, 99)]
	}
	ops := uint64(len(all))
	opsPerSec := 0.0
	if measureElapsed.Seconds() > 0 {
		opsPerSec = float64(ops) / measureElapsed.Seconds()
	}

	fmt.Printf("  -> ops=%d errors=%d ops/sec=%.1f p50=%.2fms p95=%.2fms p99=%.2fms\n",
		ops, errs.Load(), opsPerSec, ms(p50), ms(p95), ms(p99))

	if s.csvPath != "" {
		if err := appendCSVRow(s.csvPath, csvRow{
			ts:          time.Now().UTC().Format(time.RFC3339),
			gitSHA:      os.Getenv("PERF_GIT_SHA"),
			host:        os.Getenv("PERF_HOST"),
			cpu:         os.Getenv("PERF_CPU"),
			memGB:       os.Getenv("PERF_MEM_GB"),
			osName:      os.Getenv("PERF_OS"),
			scenario:    s.scenario,
			scale:       s.scale,
			label:       s.label,
			concurrency: s.concurrency,
			durationSec: int(measureElapsed.Seconds() + 0.5),
			opsTotal:    ops,
			opsPerSec:   opsPerSec,
			p50ms:       ms(p50), p95ms: ms(p95), p99ms: ms(p99),
			errors: errs.Load(),
		}); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: failed to append CSV: %v\n", err)
		}
	}
}

func idxAt(n, percentile int) int {
	i := n * percentile / 100
	if i >= n {
		i = n - 1
	}
	return i
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// -----------------------------------------------------------------------------
// CSV
// -----------------------------------------------------------------------------

type csvRow struct {
	ts, gitSHA, host, cpu, memGB, osName  string
	scenario, scale, label                string
	concurrency, durationSec              int
	opsTotal, errors                      uint64
	opsPerSec, p50ms, p95ms, p99ms        float64
}

func appendCSVRow(path string, r csvRow) error {
	needHeader := false
	if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
		needHeader = true
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if needHeader {
		if err := w.Write([]string{
			"timestamp", "git_sha", "host", "cpu", "mem_gb", "os",
			"scenario", "scale", "concurrency", "label",
			"duration_sec", "ops_total", "ops_per_sec",
			"p50_ms", "p95_ms", "p99_ms", "errors",
		}); err != nil {
			return err
		}
	}
	return w.Write([]string{
		r.ts, r.gitSHA, r.host, r.cpu, r.memGB, r.osName,
		r.scenario, r.scale, strconv.Itoa(r.concurrency), r.label,
		strconv.Itoa(r.durationSec),
		strconv.FormatUint(r.opsTotal, 10),
		fmt.Sprintf("%.2f", r.opsPerSec),
		fmt.Sprintf("%.3f", r.p50ms),
		fmt.Sprintf("%.3f", r.p95ms),
		fmt.Sprintf("%.3f", r.p99ms),
		strconv.FormatUint(r.errors, 10),
	})
}

// -----------------------------------------------------------------------------
// Subcommands
// -----------------------------------------------------------------------------

// cmdSeed creates N objects of a given type in parallel with progress output.
// Unlike `assert seed`, this skips the chain-of-links step — perf CRUD reads
// only need a pool of N objects to randomly hit, not a traversable chain.
// Without parallelism + no-links, seeding 100k objects took >20 min and
// stressed the runtime more than the actual measurement window did.
func cmdSeed(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	nats := fs.String("nats", "nats://nats:foliage@localhost:4222", "NATS URL")
	domain := fs.String("domain", "hub", "hub domain name")
	timeoutSec := fs.Int("timeout", 10, "per-request timeout seconds")
	concurrency := fs.Int("concurrency", 16, "concurrent seed workers")
	n := fs.Int("n", 1000, "number of objects to create")
	typ := fs.String("type", "perf_node", "object type")
	prefix := fs.String("prefix", "node", "id prefix (ids = <prefix>-0 .. <prefix>-(n-1))")
	progressEvery := fs.Int("progress-every", 1000, "print progress every N seeded objects")
	_ = fs.Parse(args)

	dbc, err := db.NewDBSyncClient(*nats, *timeoutSec, *domain)
	if err != nil {
		fatalf("db client: %v", err)
	}
	if err := dbc.CMDB.TypeCreate(*typ); err != nil && !isAlreadyExists(err) {
		fatalf("type create %q: %v", *typ, err)
	}

	fmt.Fprintf(os.Stderr, ">> seeding %d objects of type %q  (concurrency=%d)\n", *n, *typ, *concurrency)
	t0 := time.Now()
	var done, errs atomic.Int64

	work := make(chan int, *concurrency*4)
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				body := easyjson.NewJSONObjectWithKeyValue("idx", easyjson.NewJSON(int64(i)))
				if err := dbc.CMDB.ObjectUpdate(fmt.Sprintf("%s-%d", *prefix, i), body, false, *typ); err != nil {
					errs.Add(1)
				}
				d := done.Add(1)
				if int(d)%*progressEvery == 0 || int(d) == *n {
					el := time.Since(t0).Seconds()
					rate := float64(d) / el
					eta := float64(*n-int(d)) / rate
					fmt.Fprintf(os.Stderr, "   %d/%d (%.0f%%) • %.0f obj/s • elapsed %.1fs • ETA %.0fs\n",
						d, *n, float64(d)*100/float64(*n), rate, el, eta)
				}
			}
		}()
	}
	for i := 0; i < *n; i++ {
		work <- i
	}
	close(work)
	wg.Wait()

	fmt.Fprintf(os.Stderr, ">> seeded %d (errors=%d) in %.1fs (%.0f obj/s)\n",
		done.Load(), errs.Load(), time.Since(t0).Seconds(),
		float64(done.Load())/time.Since(t0).Seconds())
	if errs.Load() > 0 {
		os.Exit(1)
	}
}

func cmdCrudWrite(args []string) {
	fs := flag.NewFlagSet("crud-write", flag.ExitOnError)
	var s sharedFlags
	registerSharedFlags(fs, &s)
	typ := fs.String("type", "perf_node", "object type name")
	prefix := fs.String("prefix", "pw", "id prefix")
	_ = fs.Parse(args)
	if s.scenario == "" {
		s.scenario = "crud-write"
	}

	dbc, err := db.NewDBSyncClient(s.nats, s.timeoutSec, s.domain)
	if err != nil {
		fatalf("db client: %v", err)
	}
	// Type create is idempotent at the runtime level — ignore "already exists".
	if err := dbc.CMDB.TypeCreate(*typ); err != nil && !isAlreadyExists(err) {
		fatalf("type create %q: %v", *typ, err)
	}

	runWorkload(&s, func(ctx context.Context, w int, idx uint64) error {
		id := fmt.Sprintf("%s-w%d-i%d", *prefix, w, idx)
		body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(int64(idx)))
		return dbc.CMDB.ObjectUpdate(id, body, false, *typ)
	})
}

func cmdCrudRead(args []string) {
	fs := flag.NewFlagSet("crud-read", flag.ExitOnError)
	var s sharedFlags
	registerSharedFlags(fs, &s)
	n := fs.Int("n", 1000, "number of seeded ids to read from (range [0..n))")
	prefix := fs.String("prefix", "node", "id prefix used at seed time")
	_ = fs.Parse(args)
	if s.scenario == "" {
		s.scenario = "crud-read"
	}

	dbc, err := db.NewDBSyncClient(s.nats, s.timeoutSec, s.domain)
	if err != nil {
		fatalf("db client: %v", err)
	}

	// Per-worker RNG to avoid lock contention.
	rngs := make([]*rand.Rand, s.concurrency)
	for i := range rngs {
		rngs[i] = rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
	}

	runWorkload(&s, func(ctx context.Context, w int, idx uint64) error {
		id := fmt.Sprintf("%s-%d", *prefix, rngs[w].Intn(*n))
		_, err := dbc.CMDB.ObjectRead(id)
		return err
	})
}

func cmdCrudMixed(args []string) {
	fs := flag.NewFlagSet("crud-mixed", flag.ExitOnError)
	var s sharedFlags
	registerSharedFlags(fs, &s)
	n := fs.Int("n", 1000, "number of seeded ids in the read pool")
	prefix := fs.String("prefix", "node", "id prefix used at seed time")
	typ := fs.String("type", "perf_mix", "object type for writes")
	readPct := fs.Int("read-pct", 80, "percent of reads in the mix (0..100)")
	_ = fs.Parse(args)
	if s.scenario == "" {
		s.scenario = "crud-mixed"
	}
	if s.label == "" {
		s.label = fmt.Sprintf("r%d/w%d", *readPct, 100-*readPct)
	}

	dbc, err := db.NewDBSyncClient(s.nats, s.timeoutSec, s.domain)
	if err != nil {
		fatalf("db client: %v", err)
	}
	if err := dbc.CMDB.TypeCreate(*typ); err != nil && !isAlreadyExists(err) {
		fatalf("type create %q: %v", *typ, err)
	}

	rngs := make([]*rand.Rand, s.concurrency)
	for i := range rngs {
		rngs[i] = rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
	}

	runWorkload(&s, func(ctx context.Context, w int, idx uint64) error {
		if rngs[w].Intn(100) < *readPct {
			id := fmt.Sprintf("%s-%d", *prefix, rngs[w].Intn(*n))
			_, err := dbc.CMDB.ObjectRead(id)
			return err
		}
		id := fmt.Sprintf("mix-w%d-i%d", w, idx)
		body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(int64(idx)))
		return dbc.CMDB.ObjectUpdate(id, body, false, *typ)
	})
}

func cmdCrudDelete(args []string) {
	fs := flag.NewFlagSet("crud-delete", flag.ExitOnError)
	var s sharedFlags
	registerSharedFlags(fs, &s)
	typ := fs.String("type", "perf_del", "object type for the create+delete churn")
	_ = fs.Parse(args)
	if s.scenario == "" {
		s.scenario = "crud-delete"
	}

	dbc, err := db.NewDBSyncClient(s.nats, s.timeoutSec, s.domain)
	if err != nil {
		fatalf("db client: %v", err)
	}
	if err := dbc.CMDB.TypeCreate(*typ); err != nil && !isAlreadyExists(err) {
		fatalf("type create %q: %v", *typ, err)
	}

	// ObjectDelete is destructive, so a sustained duration workload cannot run on
	// a fixed id pool. Each op instead creates a fresh unique object and deletes
	// it — a create+delete churn that drives the ObjectDelete cascade at full
	// rate without depleting a pool, and mirrors the create/delete churn the HLM
	// aging path (managerControl GC) generates. The timed op is the create+delete
	// pair; the delete-path cost is what moves the number between runs.
	body := easyjson.NewJSONObject()
	runWorkload(&s, func(ctx context.Context, w int, idx uint64) error {
		id := fmt.Sprintf("del-w%d-i%d", w, idx)
		if err := dbc.CMDB.ObjectUpdate(id, body, false, *typ); err != nil {
			return err
		}
		return dbc.CMDB.ObjectDelete(id)
	})
}

// -----------------------------------------------------------------------------

func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: perfclient <seed|crud-write|crud-read|crud-mixed|crud-delete> [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	switch cmd {
	case "seed":
		cmdSeed(args)
	case "crud-write":
		cmdCrudWrite(args)
	case "crud-read":
		cmdCrudRead(args)
	case "crud-mixed":
		cmdCrudMixed(args)
	case "crud-delete":
		cmdCrudDelete(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", cmd)
		os.Exit(2)
	}
}
