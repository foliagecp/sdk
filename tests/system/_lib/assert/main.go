// Command assert is the shared assertion client for the docker-compose system
// tests under tests/system/. Each test's run.sh drives a real deployment
// (NATS + a containerised foliage runtime) and then calls this tool over NATS,
// reusing the public clients/go/db surface, to seed a known graph and verify
// invariants. Exit code 0 == pass, non-zero == fail, so it composes directly
// with shell `if`/`set -e`.
//
// It is intentionally a plain CLI (no test framework): the orchestration —
// up / wait / signal / restart / down — lives in run.sh, and the SDK-level
// assertions live here where the typed db client is available.
//
// Subcommands:
//
//	ping       wait until the runtime answers a CMDB request (readiness probe)
//	seed       create a type and a deterministic chain of N objects + links
//	verify     read every seeded object back and assert it is present
//	count      print how many objects of a type JPGQL enumeration returns
//	consistency  enumerate a type and assert every member is readable & typed
//
// Connection flags are shared by all subcommands:
//
//	-nats     NATS URL                (default nats://nats:foliage@localhost:4222)
//	-domain   hub domain name         (default hub — samples/simple effective domain)
//	-timeout  per-request timeout sec (default 10)
package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: assert <ping|seed|verify|count|consistency> [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "ping":
		cmdPing(args)
	case "seed":
		cmdSeed(args)
	case "verify":
		cmdVerify(args)
	case "count":
		cmdCount(args)
	case "consistency":
		cmdConsistency(args)
	case "soak":
		cmdSoak(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", cmd)
		os.Exit(2)
	}
}

// conn holds the shared connection flags and binds them onto a flag set.
type conn struct {
	nats    string
	domain  string
	timeout int
}

func bindConn(fs *flag.FlagSet) *conn {
	c := &conn{}
	fs.StringVar(&c.nats, "nats", "nats://nats:foliage@localhost:4222", "NATS URL")
	fs.StringVar(&c.domain, "domain", "hub", "hub domain name")
	fs.IntVar(&c.timeout, "timeout", 10, "per-request timeout (seconds)")
	return c
}

func (c *conn) client() (db.DBSyncClient, error) {
	return db.NewDBSyncClient(c.nats, c.timeout, c.domain)
}

func objID(prefix string, i int) string { return fmt.Sprintf("%s-%d", prefix, i) }

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

// ---- ping -------------------------------------------------------------------

// cmdPing blocks until the runtime answers a trivial CMDB request, i.e. an
// active instance is up and serving. Used as the health gate in run.sh before
// any scenario runs.
func cmdPing(args []string) {
	fs := flag.NewFlagSet("ping", flag.ExitOnError)
	c := bindConn(fs)
	wait := fs.Int("wait", 120, "max seconds to wait for readiness")
	_ = fs.Parse(args)

	deadline := time.Now().Add(time.Duration(*wait) * time.Second)
	attempt := 0
	for {
		attempt++
		dbc, err := c.client()
		if err == nil {
			// Only an active, serving instance answers a write with OK. A
			// timeout / no-responder (passive or still starting) is reported
			// by the client as a FAILED reply too, so we can't tell it apart
			// by error type — but an OK can only come from a serving runtime.
			// Use a UNIQUE type name per attempt so a prior probe's "already
			// exists", or a late-processed request, can never mask readiness.
			probe := fmt.Sprintf("__systest_probe_%d", time.Now().UnixNano())
			if err = dbc.CMDB.TypeCreate(probe); err == nil {
				fmt.Printf("ready after %d attempt(s)\n", attempt)
				return
			}
		}
		if time.Now().After(deadline) {
			die("not ready within %ds (last error: %v)", *wait, err)
		}
		time.Sleep(1 * time.Second)
	}
}

// ---- seed -------------------------------------------------------------------

// cmdSeed writes a deterministic graph: one type, a permissive self type-link,
// N objects each carrying {idx:i}, and a chain of object links 0->1->...->N-1.
// Every op is client-acknowledged before the next, so on success all N objects
// are committed (used by durability assertions after a restart/signal).
func cmdSeed(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	c := bindConn(fs)
	n := fs.Int("n", 100, "number of objects")
	typ := fs.String("type", "systest_node", "object type")
	prefix := fs.String("prefix", "node", "object id prefix")
	_ = fs.Parse(args)

	dbc, err := c.client()
	if err != nil {
		die("connect: %v", err)
	}

	if err := dbc.CMDB.TypeCreate(*typ); err != nil {
		die("type create %q: %v", *typ, err)
	}
	// Permit same-type object links so the chain below is schema-valid.
	if err := dbc.CMDB.TypesLinkCreate(*typ, *typ, "chain", nil); err != nil {
		die("types link create %s->%s: %v", *typ, *typ, err)
	}

	for i := 0; i < *n; i++ {
		body := easyjson.NewJSONObjectWithKeyValue("idx", easyjson.NewJSON(i))
		if err := dbc.CMDB.ObjectCreate(objID(*prefix, i), *typ, body); err != nil {
			die("object create %s: %v", objID(*prefix, i), err)
		}
	}
	for i := 1; i < *n; i++ {
		name := fmt.Sprintf("link-%d", i)
		if err := dbc.CMDB.ObjectsLinkCreate(objID(*prefix, i-1), objID(*prefix, i), name, nil); err != nil {
			die("object link %s->%s: %v", objID(*prefix, i-1), objID(*prefix, i), err)
		}
	}
	fmt.Printf("seeded %d objects of type %q (prefix %q)\n", *n, *typ, *prefix)
}

// ---- verify -----------------------------------------------------------------

// cmdVerify reads every seeded object back and fails if any is missing or its
// {idx} body does not round-trip. This is the "no committed op lost" check.
func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	c := bindConn(fs)
	n := fs.Int("n", 100, "number of objects expected")
	typ := fs.String("type", "systest_node", "object type")
	prefix := fs.String("prefix", "node", "object id prefix")
	_ = fs.Parse(args)
	_ = typ

	dbc, err := c.client()
	if err != nil {
		die("connect: %v", err)
	}

	missing, mismatched := 0, 0
	for i := 0; i < *n; i++ {
		id := objID(*prefix, i)
		data, err := dbc.CMDB.ObjectReadV2(id)
		if err != nil {
			missing++
			fmt.Fprintf(os.Stderr, "  MISSING %s: %v\n", id, err)
			continue
		}
		if !data.PathExists("body.idx") {
			// Body shape changed unexpectedly (idx did not round-trip).
			mismatched++
			fmt.Fprintf(os.Stderr, "  NO-BODY %s\n", id)
			continue
		}
		if got := int(data.GetByPath("body.idx").AsNumericDefault(-1)); got != i {
			mismatched++
			fmt.Fprintf(os.Stderr, "  MISMATCH %s: idx=%d want %d\n", id, got, i)
		}
	}
	if missing > 0 || mismatched > 0 {
		die("verify FAILED: %d missing, %d mismatched of %d", missing, mismatched, *n)
	}
	fmt.Printf("verified %d objects present and intact\n", *n)
}

// ---- count ------------------------------------------------------------------

// cmdCount prints how many objects a JPGQL type-enumeration returns. Handy for
// soak/HA assertions where the exact N may vary but a lower bound is expected.
func cmdCount(args []string) {
	fs := flag.NewFlagSet("count", flag.ExitOnError)
	c := bindConn(fs)
	typ := fs.String("type", "systest_node", "object type")
	min := fs.Int("min", -1, "if >=0, fail unless count >= min")
	_ = fs.Parse(args)

	dbc, err := c.client()
	if err != nil {
		die("connect: %v", err)
	}
	ids, err := dbc.Query.JPGQLCtraQuery(*typ, ".*[l:type('__object')]")
	if err != nil {
		die("jpgql enumerate %q: %v", *typ, err)
	}
	fmt.Printf("%d\n", len(ids))
	if *min >= 0 && len(ids) < *min {
		die("count %d < min %d", len(ids), *min)
	}
}

// ---- consistency ------------------------------------------------------------

// cmdConsistency enumerates a type and asserts each member is readable and
// carries a type association — a live-runtime analogue of the in-process
// graph-consistency invariant (no phantom/orphan objects survive).
func cmdConsistency(args []string) {
	fs := flag.NewFlagSet("consistency", flag.ExitOnError)
	c := bindConn(fs)
	typ := fs.String("type", "systest_node", "object type")
	expect := fs.Int("n", -1, "if >=0, also assert exactly N members")
	settle := fs.Int("settle", 0, "seconds to re-check before failing (tolerate index settle lag)")
	_ = fs.Parse(args)

	dbc, err := c.client()
	if err != nil {
		die("connect: %v", err)
	}

	// Enumerate the type's members and confirm each is readable. Under
	// concurrent delete load a JPGQL __object enumeration may briefly list an
	// id whose vertex is already gone (index settle lag); with -settle we
	// re-check those stragglers for a bounded window. A phantom that survives
	// the whole window is a real dangling index, and we fail.
	check := func() (total, bad int, badIDs []string) {
		ids, err := dbc.Query.JPGQLCtraQuery(*typ, ".*[l:type('__object')]")
		if err != nil {
			die("jpgql enumerate %q: %v", *typ, err)
		}
		total = len(ids)
		for _, id := range ids {
			data, rerr := dbc.CMDB.ObjectReadV2(id)
			if rerr != nil || !data.IsNonEmptyObject() {
				bad++
				badIDs = append(badIDs, id)
			}
		}
		return
	}

	total, bad, badIDs := check()
	deadline := time.Now().Add(time.Duration(*settle) * time.Second)
	for bad > 0 && time.Now().Before(deadline) {
		fmt.Printf("  %d/%d members not yet consistent (e.g. %v); re-checking...\n", bad, total, firstN(badIDs, 3))
		time.Sleep(2 * time.Second)
		total, bad, badIDs = check()
	}

	if bad > 0 {
		for _, id := range firstN(badIDs, 10) {
			fmt.Fprintf(os.Stderr, "  BAD %s (enumerated but unreadable — dangling __object index)\n", id)
		}
		die("consistency FAILED: %d/%d members bad after %ds settle", bad, total, *settle)
	}
	if *expect >= 0 && total != *expect {
		die("consistency FAILED: %d members, want %d", total, *expect)
	}
	fmt.Printf("consistency OK: %d members of %q all readable & typed\n", total, *typ)
}

func firstN(s []string, n int) []string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

// ---- soak -------------------------------------------------------------------

// cmdSoak drives sustained concurrent CRUD for a window using a **bounded**
// working set per worker:
//
//   1. Pre-seed: each worker creates -pool objects + chain links between
//      them. After this step the worker's id namespace is fixed.
//   2. Steady loop until deadline:
//      * every iteration: ObjectUpdate over a rolling slot (replace=true)
//      * every -refresh-every iterations: ObjectDelete + ObjectCreate for
//        the same slot, exercising the full CRUD path while keeping
//        cardinality exactly = -pool.
//
// Why bounded — earlier versions had a growth shape (one ObjectCreate per
// iteration with sparse deletes). It was fine for the 25-second
// tests/system/crud-soak burst, but at hour scales the memory drift signal
// of the soak observer (tests/soak/) became dominated by "the graph really
// did grow" and a real leak would hide in the noise. Cardinality = workers
// * pool from end-of-seed onward makes the memory drift signal honest:
// positive drift is a leak, not just more data.
//
// What it still tests (vs. the older growth shape): runtime responsiveness
// under sustained CRUD, worker-pool / WAL pressure, every CRUD op type on
// the hot path (Create, LinkCreate, Update, Delete) — just at a stable
// graph size instead of a monotonically growing one. The follow-up
// `consistency` call in run.sh asserts the same post-soak invariant.
func cmdSoak(args []string) {
	fs := flag.NewFlagSet("soak", flag.ExitOnError)
	c := bindConn(fs)
	workers := fs.Int("workers", 8, "concurrent workers")
	duration := fs.Int("duration", 20, "soak duration (seconds)")
	typ := fs.String("type", "systest_node", "object type")
	prefix := fs.String("prefix", "soak", "object id prefix")
	pool := fs.Int("pool", 200, "objects pre-seeded per worker (working set size)")
	refreshEvery := fs.Int("refresh-every", 50, "every Nth iteration, delete+recreate the current slot (exercises CRUD without growth)")
	_ = fs.Parse(args)

	if *pool < 2 {
		die("-pool must be >= 2 (got %d)", *pool)
	}
	if *refreshEvery < 1 {
		die("-refresh-every must be >= 1 (got %d)", *refreshEvery)
	}

	// One client sets up the schema up front so workers only do object ops.
	setup, err := c.client()
	if err != nil {
		die("connect: %v", err)
	}
	if err := setup.CMDB.TypeCreate(*typ); err != nil {
		die("type create %q: %v", *typ, err)
	}
	if err := setup.CMDB.TypesLinkCreate(*typ, *typ, "chain", nil); err != nil {
		die("types link create: %v", err)
	}

	var created, updated, deleted, errCount int64
	deadline := time.Now().Add(time.Duration(*duration) * time.Second)

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			dbc, err := c.client()
			if err != nil {
				atomic.AddInt64(&errCount, 1)
				fmt.Fprintf(os.Stderr, "  worker %d connect: %v\n", w, err)
				return
			}
			id := func(i int) string { return fmt.Sprintf("%s-w%d-%d", *prefix, w, i) }
			N := *pool

			// Step 1 — pre-seed: N objects + N-1 chain links.
			for j := 0; j < N && time.Now().Before(deadline); j++ {
				body := easyjson.NewJSONObjectWithKeyValue("idx", easyjson.NewJSON(j))
				if err := dbc.CMDB.ObjectCreate(id(j), *typ, body); err != nil {
					atomic.AddInt64(&errCount, 1)
					fmt.Fprintf(os.Stderr, "  worker %d seed-create %s: %v\n", w, id(j), err)
					return
				}
				atomic.AddInt64(&created, 1)
				if j > 0 {
					if err := dbc.CMDB.ObjectsLinkCreate(id(j-1), id(j), fmt.Sprintf("l%d", j), nil); err != nil {
						atomic.AddInt64(&errCount, 1)
						fmt.Fprintf(os.Stderr, "  worker %d seed-link: %v\n", w, err)
						return
					}
				}
			}

			// Step 2 — steady loop: mostly updates, periodic delete+recreate.
			counter := 0
			for time.Now().Before(deadline) {
				j := counter % N
				if counter > 0 && counter%*refreshEvery == 0 {
					// CRUD-full path: drop and recreate this slot. Links
					// involving it go with the delete; we do NOT restore
					// them (chain becomes progressively sparser as the run
					// continues, which itself is fine signal — what we
					// guard is cardinality stability, not topology).
					if err := dbc.CMDB.ObjectDelete(id(j)); err != nil {
						atomic.AddInt64(&errCount, 1)
						fmt.Fprintf(os.Stderr, "  worker %d delete %s: %v\n", w, id(j), err)
						return
					}
					atomic.AddInt64(&deleted, 1)
					body := easyjson.NewJSONObjectWithKeyValue("idx", easyjson.NewJSON(j))
					body.SetByPath("recreated_at", easyjson.NewJSON(counter))
					if err := dbc.CMDB.ObjectCreate(id(j), *typ, body); err != nil {
						atomic.AddInt64(&errCount, 1)
						fmt.Fprintf(os.Stderr, "  worker %d recreate %s: %v\n", w, id(j), err)
						return
					}
					atomic.AddInt64(&created, 1)
				} else {
					ub := easyjson.NewJSONObjectWithKeyValue("idx", easyjson.NewJSON(j))
					ub.SetByPath("counter", easyjson.NewJSON(counter))
					if err := dbc.CMDB.ObjectUpdate(id(j), ub, true); err != nil {
						atomic.AddInt64(&errCount, 1)
						fmt.Fprintf(os.Stderr, "  worker %d update %s: %v\n", w, id(j), err)
						return
					}
					atomic.AddInt64(&updated, 1)
				}
				counter++
			}
		}(w)
	}
	wg.Wait()

	fmt.Printf("soak done: workers=%d duration=%ds pool=%d created=%d updated=%d deleted=%d errors=%d\n",
		*workers, *duration, *pool, created, updated, deleted, errCount)
	if errCount > 0 {
		die("soak FAILED: %d operation error(s)", errCount)
	}
}
