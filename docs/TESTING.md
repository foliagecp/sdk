# Testing

This document describes how the Foliage SDK is tested: what tests exist, what
they cover, how to run them (including a single full-run script), and the roadmap
for the docker-compose based system tests.

All tests are **English-only** and **pass/fail** (assertion-based). Benchmarks
are the only non-pass/fail items (they measure, they do not gate).

## 1. Test layers

| Layer | Where | Runner | Purpose |
|---|---|---|---|
| **L0 Unit** | pure functions (`*_test.go`) | `go test` | parsing, key codecs, `KeyMutex`, `TokenBucket`, filter parsing, op dedup |
| **L1 Component** | one module on embedded NATS | `go test` | cache, LL/HL CRUD, JPGQL, worker pool |
| **L2 Integration** | full runtime, embedded NATS | `go test` | CRUD end-to-end, signal/request, HA, WAL durability, triggers |
| **L3 Client contract** | `db.DBSyncClient` end-to-end | `go test` | the public client surface consumers depend on |
| **Adversarial hunts** | property / fault-injection / `-race` | `go test` | finds real bugs (state leaks, races, deadlocks) |
| **Integration (containers)** | `tests/integration/{e2e,export}` | `go test` (testcontainers) | NATS containers, Postgres/Neo4j export dumpers |
| **System (docker-compose)** | `tests/system/*` | `scripts/run-all-tests.sh` | graceful shutdown, crash/restart recovery, HA-3-node, CRUD soak |
| **Benchmarks** | `Benchmark*` | `go test -bench` | performance guards (not pass/fail) |

## 2. What exists today

### Go tests (run by `go test`)

- **`embedded/graph/crud`** — CMDB CRUD core:
  - `hl_crud_atomicity_test.go` — D1 rollback, D1B upsert repair, D2 idempotent
    delete (+ SuperType), D3 read amplification, D4 orphan recovery.
  - `ll_crud_test.go`, `ll_crud_noop_test.go` — LL vertex/link ops, no-op skip,
    tag matrix.
  - `cmdb_client_contract_test.go` — L3 client-API contract (schema + upsert,
    idempotency, `ObjectsLinkUpdateWithDetails` op_stack, `ObjectReadV2` shape,
    JPGQL enumeration).
  - `ll_index_invariants_test.go`, `hunt_link_lifecycle_test.go`,
    `hunt_object_delete_test.go`, `hunt_edge_payload_test.go` — storage/index
    invariants, delete completeness, edge payload round-trip.
  - `hunt_concurrent_test.go`, `hunt_jpgql_test.go` — concurrent CRUD and
    concurrent query+write (`-race`); JPGQL multi-hop.
  - `hunt_property_test.go` + `hunt_consistency_helper_test.go` — property-based
    random CRUD sequences with a graph-consistency invariant after every step.
  - `hunt_triggers_test.go` — CMDB trigger firing.
  - `restart_pending_wal_test.go`, `wal_apply_failure_test.go` and
    `wal_publish_gap_test.go` — cache/KV agreement after restart, retry after
    temporary broker refusal, and changes appended during a WAL publish.
  - `trash_can_bodyless_test.go`, `stale_delete_cascade_test.go`,
    `delete_recovery_controls_test.go`, `type_read_recovery_test.go` and
    `object_structure_recovery_test.go` — partial deletion cleanup, safe
    read repair around existing bodies, parked-object preservation and no
    recreation of missing bodies.
  - `stale_write_resurrect_test.go` — pre-recovery operations cannot recreate
    deleted vertices or links after a cache reload.
  - `hunt_wal_sync_test.go` — a second runtime syncs state from the shared KV/WAL.
- **`embedded/graph/jpgql`** — `body_filter_test.go`, `fanout_filter_test.go`
  (filters, parse-once, multi-hop).
- **`statefun`** — `worker_pool_test.go` (non-blocking `Stop` under a stuck
  handler — 116 regression), `worker_pool_tokens_test.go` (token no-leak),
  `ha_test.go` (two-runtime active/passive exclusivity + recovery),
  `function_type_msg_test.go` (nil-`Caller` guard), `semantic_translator_test.go`,
  `export_committer_test.go`, `export_consumer_test.go`, `export_integration_test.go`.
  `wal_recovery_test.go` covers applied receipts before ACK, safe redelivery,
  invalid evidence, storage failures, progress deadlines, receipt pruning,
  conditional delete reconciliation and cache reload after WAL replay.
- **`statefun/cache`** — `store_value_lazy_test.go`, `backup_barrier_test.go`.
- **`statefun/system`** — `keymutex_timeout_test.go`.
- **`clients/go/db`** — `common_test.go`.
- **`tests/integration/e2e/test/{nats,runtime}`**,
  **`tests/integration/export/pg_{high,low}_level/dumper`** — testcontainers
  E2E (real NATS / Postgres).

### Bugs found by these tests (with permanent regression coverage)

- **HA reentrant deadlock** in active/passive transitions (`runtime.go`): the
  recovery path self-deadlocked, so a passive instance could never become active
  — the 116 incident symptom. Fixed; covered by `ha_test.go`.
- **nil `Caller`** panicked the worker and silently dropped the task. Fixed;
  covered by `function_type_msg_test.go`.

## 3. Coverage snapshot

Measured with `go test -coverpkg=./...` (statement coverage of the whole SDK by
each test package; numbers are approximate and overlap, so they do not sum):

| Test package | SDK-wide coverage |
|---|---|
| `embedded/graph/crud` | ~53% |
| `embedded/graph/jpgql` | ~25% |
| `statefun` (self) | ~50% |
| `statefun/system` | ~21% |
| `clients/go/db` | ~6% |

**Combined: roughly ~60–65% of SDK statements.** Coverage is strong and
adversarial on the hot paths (CRUD integrity, concurrency, HA, WAL durability);
the remaining ~35–40% is concentrated in: cross-domain / shadow objects, FPL
queries, graceful-shutdown-via-signal, load/soak, and parts of export/semantic —
these are addressed by the planned system tests (Section 6).

Re-measure:

```sh
go test -coverpkg=./... -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | tail -1     # total
go tool cover -html=coverage.out               # per-line view
```

## 4. How to run

### Quick (developer loop)

```sh
go test ./...
```

Fast, but packages run in parallel and several suites spin up an **embedded NATS
server**; under contention they can produce a false failure (see Section 5).

### Full, reliable run (recommended)

```sh
scripts/run-all-tests.sh
```

Runs Go packages **serially** (`-p 1 -count=1`) for embedded-NATS reliability,
then the system tests (when present). Flags:

| Flag | Effect |
|---|---|
| `--race` | instrument Go tests and the containerized recovery regression fixture with the race detector |
| `--coverage` | write a merged `coverage.out` over `./...` |
| `--quick` | run Go packages in parallel (fast, may flake) |
| `--go-only` | skip the system-test phase |
| `--system-only` | skip the Go-test phase |

### Race detector / benchmarks

```sh
go test -race ./...                       # concurrency
go test -run '^$' -bench=. -benchmem ./...  # benchmarks (no pass/fail)
```

## 5. Determinism notes

- The shared test harness (`statefun/test`) runs the runtime **single-instance**
  (HA disabled) to avoid KV-lock contention; this removed the main flakiness
  source.
- A **parallel** `go test ./...` can still flake under heavy load: multiple
  embedded NATS servers (and any stale `*.test`/`nats-server` processes from an
  interrupted run) contend, which can fail a suite at startup (observed: a
  ~0.6 s "fail" of an otherwise-green package). This is **environmental, not a
  code defect** — the same package passes in isolation.
- For reliable results run serially (`-p 1`, what the script does), one package
  at a time, and ensure no stale test/NATS processes are left over.

## 6. Repository layout — where tests live

Tests are split by **how** they run; both kinds are executed by
`scripts/run-all-tests.sh`. See [`tests/README.md`](../tests/README.md) for the
full breakdown.

- **`samples/`** — demo docker-compose apps (`basic`, `simple`, `object`,
  `cluster`, `distributed`, `backup-barrier`). Examples, **not** assertions.
- **`tests/integration/`** — `go test` + testcontainers: real dependency
  containers (NATS, Postgres, Neo4j) with the runtime **in-process**. Picked up
  by `go test ./...` (Phase 1). Covers the export pipeline (`export/`) and
  runtime-on-real-NATS (`e2e/`).
- **`tests/system/<name>/`** — docker-compose projects, each with an executable
  `run.sh` (up → assert → down, exit 0 == pass), run **sequentially** in Phase 2.
  Here the **runtime itself runs in a container** (its own OS process), so these
  cover what an in-process `go test` cannot.

The per-test design notes (topology, steps, assertions) are in
[SYSTEM_TESTS_PLAN.md](./SYSTEM_TESTS_PLAN.md).

### System tests (implemented) and the zones they cover

| Zone | Go-test status | System test (`tests/system/`) |
|---|---|---|
| CRUD correctness / idempotency | covered | `crud-soak` (sustained concurrent CRUD) |
| JPGQL (filters, multi-hop) | covered | — |
| Concurrency (`-race`) | covered | exercised by `crud-soak` under load |
| HA active/passive + recovery | covered (2 in-proc) | **`ha-3-node`** failover/continuity |
| WAL durability across restart | covered (2 in-proc) | **`wal-restart-recovery`** (SIGKILL + restart) |
| Missing WAL record with a pending ACK | covered (observable-state fault injection) | **`wal-ack-recovery`** (real NATS 2.12.4 SIGKILL, archive/restore, proven repair and legacy refusal) |
| Graceful shutdown (drain) | not go-testable¹ | **`graceful-shutdown`** (real SIGTERM) |
| Load / soak (degradation) | — | `crud-soak` (responsive + consistent after load) |
| Coherent export (PG/Neo4j) | covered by `tests/integration/export` | — (already an integration test) |
| Backup write-barrier | covered (unit) | — (needs backup tooling first; out of scope) |
| Cross-domain / shadow objects | — | future (weak-cluster link + JPGQL) |

¹ The graceful-drain path is triggered only by a process-wide OS signal, which
also crashes the embedded NATS server in `go test`; it must be tested against a
real NATS instance with the runtime in its own container.

## 7. CI guidance

- Gate: `scripts/run-all-tests.sh` (serial Go tests) + `--race` on a nightly job.
- Keep a coverage gate on the P0/P1 packages once the system tests land.
- Do not run the embedded-NATS packages in parallel in CI; ensure a clean
  process state between runs.

## 8. Conventions

- English only; no consumer/customer names anywhere in the SDK.
- Every test is pass/fail (assertion-based); no log-only tests.
- Concurrency-sensitive tests run under `-race`.
- Mutating CRUD tests assert graph invariants (no orphans/phantoms/dangling
  indices; exactly one `__type` link) via the shared consistency helper.

The `wal-ack-recovery` fixture runs NATS 2.12.4 in Docker and the SDK in separate
host processes to control the broker crash point. It uses only generated data
and local temporary containers, without modifying a persisted state file.
See [WAL crash recovery](./domain_lifecycle.md#wal-crash-recovery) for guarantees,
retention limits, commands and prerequisites.

## Recovery regression gate

The tests imported from the WAL regression branch remain ordinary `*_test.go`
files in `embedded/graph/crud/`; no private snapshots are required. Their
assertions run in the normal Go phase and are also reused by the Linux
`cache-graph-recovery` system fixture. New cache boundary, conditional-write,
shutdown retry, structural repair and concurrent restore checks run alongside
them. Failing assertions fail the runner; there are no expected failures or
silent skips in this gate. The system fixture prints individual subtest
verdicts, including the synthetic snapshot replay which previously required
a private archive and was skipped when it was unavailable.

```sh
scripts/run-all-tests.sh                       # Go and all system fixtures
scripts/run-all-tests.sh --system-only         # includes recovery regressions
scripts/run-all-tests.sh --system-only --race  # race-checks recovery regressions
scripts/run-all-tests.sh --race                # full Go race phase + systems
```

The recovery fixture uses the Go major/minor version declared in `go.mod` and
passes through the selected cache mode. On macOS the Go race phase runs in
Linux Docker because the embedded NATS 2.10 sysmem code otherwise aborts in
checkptr before the SDK tests run. Pointer checks are not disabled. The full
Go phase mounts the local Docker socket for integration-test dependencies;
the focused recovery fixture does not need that access. Test containers are
removed on completion; dependency/build caches remain in named Docker volumes.

The HA component tests use a pinned NATS 2.12.4 container for concurrent runtime
startup. The old embedded NATS 2.10.12 broker has a reservation-accounting race
under concurrent stream creation; the SDK gate does not disable detection to
hide it. SDK code stays instrumented, and runtime client/module dependencies
are unchanged. Running the HA component tests therefore requires local Docker.


## HA startup regression gate

`tests/system/ha-startup-recovery/run.sh` reuses the Go domain and HA assertions
in both full and `--system-only` runs, with `--race` propagated to the Go tests.
It needs the local Docker socket to create pinned NATS test brokers. Runtimes
start concurrently; both must finish startup before active ownership is
measured. Startup errors are reported directly. Cleanup explicitly shuts down
and joins each runtime before removing its broker, instead of relying on
cancellation of a startup context.

The inventory tests create a real compatible stream or consumer during a
snapshot read, then assert that validation retries it. Controls cover a foreign
domain, persistent incomplete listings and non-inventory read failures. Domain
protection is kept fail-closed; no timeout or role-exclusivity assertion is
relaxed.

Shutdown-context controls also assert that renewal cancels the previous
application generation and cannot revive work after graceful or emergency
shutdown. Concurrent renewal/read/cancellation runs with the race detector;
these assertions remain part of the HA system fixture.


## WAL backpressure regression gate

`tests/system/wal-backpressure/run.sh` runs in full and `--system-only` modes,
including `--race`. It uses a real embedded JetStream server in Linux Docker
and intercepts KV PubAcks on the client connection before nats.go frees its
pending slots. No broker data or ACK payloads are fabricated. The publisher
uses the same 256-slot limit as the runtime. Holding ACKs longer than the
client's default 200 ms stall timeout must not fail a large WAL transaction.

The checks cover 2200 ordered PUT/DELETE operations, a full shared pending
pool, concurrent publisher saturation, a rejected KV write and successful retry,
cancellation/deadline expiry with a partially applied transaction, retry
while old ACKs are still outstanding, and a disconnected publisher. They
verify KV contents, retention of incomplete WAL records, absence of premature
completion receipts and removal of WAL records only after a successful apply.
The old 1024-operation burst fails the delayed-ACK regression at operation 256.
The SDK now publishes bounded windows below the configured pending limit and
waits for outstanding ACKs before retrying an incomplete attempt. Publication
backpressure and ACK collection share a 30-second deadline per window.


## Dynamic registration regression gate

`Test_HA_DynamicRegistration_*` extends the existing HA system fixture with
passive-first registration, expired/busy lease retry without promotion,
lease renewal past its original TTL, concurrent function registration,
registration rollback and deferred-subscription failure recovery. Controls
exercise failover, promotion/shutdown while stream creation is blocked,
existing subscription reuse and local calls during registry updates. A delayed
subscription must not postpone demotion or leave the old instance subscribed.
Promotion must retain subscription work deferred by concurrent registration,
even when there is no single-instance lease retry to trigger it again.
Tests use generated function names and pinned local NATS containers. They
run as ordinary Go tests and through `ha-startup-recovery` in full and
`--system-only` runs, with race instrumentation when requested.


## Cache hub throughput gate

`Test_Hub_ConcurrentThroughputGate` (`statefun/cache/hub_perf_test.go`, build
tag `perf`) requires concurrent writes into one hub to keep at least 80 % of
the tree's throughput under the records representation. It replaced the timing
half of `Test_Hub_ConcurrentThroughput`, which compared one best-of-three
sample per mode, tree first, inside the race-instrumented correctness run.
Those samples lasted about 150 ms under `-race` and 10 ms without it, so a
busy moment of the machine decided the verdict. The correctness
half stays in the ordinary suite as `Test_Hub_ConcurrentWritesKeepEverything`
and runs under `-race`.

The gate measures pairs of tree and records samples back to back, alternating
which goes first; each sample is a series of bursts on fresh stores lasting at
least 200 ms, with filling and garbage collection outside the timing. Before
and after each pair it times the same hashing work on every P. A pair taken
while the machine was more than 15 % slower than its best moment in the run is
dropped; the verdict is the median records/tree ratio of the remaining pairs.
Fewer than 8 clean pairs fail the run as `INCONCLUSIVE`, which says nothing
about the code either way. Sustained load is not a drift: under it the pairs
compare fairly, and records really lose more than the tree, because a bucket
write holds its slot lock through decode and encode and a split holds the
directory lock. The log prints how much of the nominal parallelism the machine
gave, so a failure on a loaded runner can be told from one on an idle runner.
The gate never runs under the race detector. It runs in phase 1b of
`scripts/run-all-tests.sh` and in `scripts/run-perf-tests.sh --gate`.

```sh
go test -tags perf -count=1 -v -run '^Test_Hub_ConcurrentThroughputGate$' ./statefun/cache
scripts/hub-gate-experiment.sh --ref v0.5.8-dev03112026 --runs 20   # the old check
scripts/hub-gate-experiment.sh --ref HEAD --runs 10                 # the gate
```

The experiment runs a ref's check in a container limited to 4 CPUs, idle,
with a sibling spinning on 2 cores (`steady`) and with that sibling spinning
3 s on and 3 s off (`flap`):

| check | idle | steady | flap |
|---|---|---|---|
| old, `-race`, 20 runs | 0 FAIL, median 1.01 | 2 FAIL, 1.12 | 6 FAIL, 0.91 |
| gate, 10 runs | 10 PASS, median 1.09 | median 1.27: 5 FAIL, 2 INCONCLUSIVE | 10 PASS, 1.06 |

Run alternately under one load, v0.5.6 and v0.5.8 failed the old check 3 and
2 times out of 20: the hub write path did not change between them. Reducing
what records lose under sustained CPU contention is separate work.


The long WAL replay regression uses real KV writes, applied receipts and
consumer ACKs with a controlled watchdog clock. It verifies that durable
progress resets the stall deadline even when total replay exceeds that
interval, and that delivery alone does not reset it. Broker scheduling is
not required to fit a 120 ms test budget. Production recovery continues to
use the real clock and poll ticker with its existing timeout.
