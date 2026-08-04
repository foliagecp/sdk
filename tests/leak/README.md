# Memory-leak test suite

Hermetic leak hunting for the SDK: fixed churn cycles against an embedded
NATS + fresh runtime, with two classes of evidence per scenario:

1. **Deterministic invariants** — exact counters must return to their own
   post-warmup baseline (delta == 0): cache-tree population (live values /
   total nodes / tombstones), WAL backlog (`pendingTxs`, `activeOps`), the
   graph key mutex, ALL five process-global crud caches (object-type,
   type-edge, object-trigger, link-trigger, HRN), per-id statefun machinery,
   the mediator reply store, export sessions, goroutines. Counter coverage
   is deliberately exhaustive: a per-entry structural leak (one map record
   per call, tens of bytes) sits far below any statistically honest heap
   floor, so counters — not heap slopes — are the instrument that catches
   that class.
2. **Statistical drift bound** — over M measured cycles the OLS slope `b`
   of each heap metric is computed with its standard error. **LEAK ⇔
   `b > 3·SE(b)` AND `b > floor` AND the tail slope (second half of the
   window) `> floor`, and the series is NOT better explained as a one-time
   STEP.** These conditions separate a real leak — a trend that keeps going —
   from a one-time plateau shift to a high-water mark (pools, request-path
   timers, map capacities crossing a growth threshold mid-window), which a
   small-M OLS fit would otherwise flag: an early step flattens the tail, a
   late one is caught by explicitly fitting the two-plateau step model
   against the line (the step must fit clearly better AND neither plateau may
   grow internally). Every run
   also reports the residual bound `b + 3·SE` ("growth is ≤ this per cycle
   at 3σ") and the detection floor `max(3·SE, floor)` ("a leak slower than
   this was not detectable in this run") — the honest limits of any
   "no leaks" claim.

Every sample is taken after a full **quiesce**: WAL drained
(`HasPendingWrites`), ≥2 cache maintenance sweeps strictly after the drain
(tombstone cascades collapsed), double `runtime.GC()`.

**SDK vs embedded NATS heap.** The embedded `nats-server` shares the test
process, and JetStream/KV churn grows *its* state by design (per-subject
tree nodes, file-store buffers, retained KV DEL tombstones). The harness
splits the in-use heap by allocation stack: the SDK share is **asserted**,
the server share and raw process totals are **REPORT-only**. In production
the server is a separate process; its by-design growth is quantified by S12.

**Process isolation.** `run-leak-tests.sh` runs every scenario in its OWN
`go test` process. Scenarios sharing one process share heap, timers and
whatever each per-scenario emergency teardown leaves behind — process-wide
plateau shifts then land in whichever scenario happens to be measuring and
read as false leaks. A fresh process per scenario gives clean, deterministic
baselines and zero cross-contamination (the test binary is compiled once and
reused).

## Running

```sh
scripts/run-leak-tests.sh                          # quick smoke, all scenarios
scripts/run-leak-tests.sh --mode full              # the 3-sigma run
scripts/run-leak-tests.sh --scenario s4            # one scenario
scripts/run-leak-tests.sh --scenario core          # s0 s1 s2 s5 s9
scripts/run-leak-tests.sh --mode soak              # docker leak-hunt (tests/soak)
go test -tags leak -count=1 -v ./tests/leak/       # raw
```

Plain `go test ./...` never builds this package (build tag `leak`).
Artifacts land in `tests/leak/_results/<run>/<scenario>/`: `samples.csv`,
`heap-baseline.pb.gz` / `heap-final.pb.gz`, per-function `heap-diff.txt` on a
heap FAIL, goroutine stacks on a settle FAIL. Every check prints a
machine-readable `LEAKCHECK|...` line; the script aggregates them.

Env knobs: `LEAK_WARMUP`, `LEAK_CYCLES`, `LEAK_SCALE`,
`LEAK_FLOOR_HEAP_BYTES`, `LEAK_FLOOR_HEAP_OBJECTS`, `LEAK_RESULTS_DIR`.

## Scenarios

| # | Test | Pressure | Expected |
|---|---|---|---|
| S0 | `TestS0PlantedLeakIsFlagged` / `TestS0ControlIsClean` | framework self-test: a planted 1MiB/cycle + parked-goroutine leak MUST be flagged; a transient-alloc control MUST pass | PASS |
| S1 | `TestS1LLCrudChurn` | low-level vertex/link create-update-delete, fresh ids each cycle | PASS |
| S2 | `TestS2CMDBObjectChurn` (+probe) | CMDB objects/links churn; probe: object-type cache lifecycle on partial deletes (L3) | PASS |
| S3 | `TestS3TypeCascadeChurn` | fresh type + objects + `type.delete` cascade per cycle | PASS |
| S4 | `TestS4FunctionContexts` | (a) TTL context reclaimed; (b) namegen contexts expire even on executor error paths (L2); (c) contexts die with their object (L1); (d) salted-id contexts (`id===hash`) die with the vertex too (L1b); (e) batch create/delete with a real namegen trigger leaves no contexts | PASS |
| S5 | `TestS5JPGQL` | jpgql queries over static+churning graph; per-id machinery decay | PASS |
| S6 | `TestS6FPL` | FPL incl. vbody/obody with `links_in_body`/`links_out_body`; unique-id minting decay | PASS |
| S7 | `TestS7Batch` | sequential + parallel (sub-batch split) batches | PASS |
| S8 | `TestS8KeyMutex` (+probe) | key-mutex churn; probe: dotted ids rejected, legacy dotted data safe (L6) | PASS |
| S9 | `TestS9CacheStore` (+2 probes) | deep-key tombstone sweep; probes: `force=true` retarget is a clean replace (L4), orphaned `out.to` links delete via the ltype fallback (L5) | PASS |
| S10 | `TestS10GoroutineHygiene` | mixed churn+query+batch, exact goroutine settle | PASS |
| S11 | `TestS11ExportSessions` | chunked exports: completed + abandoned, TTL drain | PASS |
| S12 | `TestS12KVGrowthReport` | NATS-side KV/stream growth under fresh-id churn + delete-marker purge check | REPORT + PASS |
| S13 | `TestS13SaltedHLChurn` | HL CRUD (object create/read/delete, types.link upsert-update) driven entirely through salted ids with fresh salts per call; born from the salted type-edge cache leak | PASS |

## Fixed findings (probes are green regression guards)

Six leaks were found by this suite and fixed; each probe asserts the DESIRED
behavior, so a regression turns exactly one known check red again. **Any red
check is a leak** — either a regression of the findings below or a new one.

| Finding | Check | What used to leak / what is guarded now |
|---|---|---|
| **L1** | `s4c_ctx_orphans / orphan_ctx_keys` | contexts `<typename>.<id>` without a TTL outlived their object forever; `vertex.delete` now drops every registered function type's context for the deleted id |
| **L1b** | `s4d_salted_ctx / salted_ctx_keys` | contexts written by SALTED invocations (`<id>===<hash>`, the sequence-free parallelization suffix) live under sibling keys the exact delete missed; `vertex.delete` now also scans each typename's context level for `<id>===` prefixed keys (`s4e` guards the batch→trigger→context chain end-to-end) |
| **L2** | `s4b_namegen_builderr / namegen_ctx_no_ttl` | namegen stored object+type bodies as context and set the TTL only at the end; the mark is now set immediately after the store, so executor error paths cannot strand it |
| **L3** | `s2_otc_orphan / object_type_cache` | partial deletes (IDLE/abort paths) kept `objectTypeCache` entries forever; every delete path now purges unconditionally, and empty types are never cached |
| **L4** | `s9_force_retarget / cache tree` | `force=true` retarget stranded the old target's `ltype`/`in` keys; force is now an atomic replace (old link's keys dropped first) |
| **L5** | `s9_orphan_outto / cache tree` | a link missing `out.to` was permanently undeletable; delete paths now recover the target from the ltype family (key encodes type+target) |
| **L6** | `s8_dotted_id / keymutex, active_ops, pending_txs` | a dotted id leaked a permanently write-locked KeyMutex entry AND orphaned an activeOps entry, wedging the WAL publisher for good; ids are validated on create, lock records are hash-keyed (parse-proof), completion marking is decoupled from lock records |

NATS-side tombstone growth (S12) is also reclaimed: the active instance's
maintenance loop purges KV delete markers periodically
(`KV_PURGE_DELETES_INTERVAL_SEC`, `Store.PurgeKVDeleteMarkers`).

## Adding a scenario

One file `sNN_<name>_test.go` (`//go:build leak`), one suite type embedding
`leakSuite`, one `TestSNN...` entry function (the script's `-run` filter is
exact). Boot with `bootCRUD(...)`, express the workload as ONE cycle function
that returns the world to its logical baseline, and build the runner with
`s.newRunner(scenario, cycle, collect)`. Keep the whole warmup+measure loop
inside a single `Test` method — the harness rebuilds the runtime per method.
Assert with `rep.AssertClean` (heap+goroutines), `s.assertCoreStable(rep)`
and `rep.AssertStable(t, metric)` for scenario counters;
`rep.ReportMetric(t, metric)` emits an informational line without asserting.
