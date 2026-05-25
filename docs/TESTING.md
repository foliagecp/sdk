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
| **E2E (containers)** | `tests/e2e`, `tests/export` | `go test` (testcontainers) | NATS containers, Postgres/Neo4j export dumpers |
| **System (docker-compose)** | `tests/system/*` (**planned**) | `scripts/run-all-tests.sh` | graceful shutdown, load, HA-3-node, backup/restore, coherent export |
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
  - `hunt_wal_sync_test.go` — a second runtime syncs state from the shared KV/WAL.
- **`embedded/graph/jpgql`** — `body_filter_test.go`, `fanout_filter_test.go`
  (filters, parse-once, multi-hop).
- **`statefun`** — `worker_pool_test.go` (non-blocking `Stop` under a stuck
  handler — 116 regression), `worker_pool_tokens_test.go` (token no-leak),
  `ha_test.go` (two-runtime active/passive exclusivity + recovery),
  `function_type_msg_test.go` (nil-`Caller` guard), `semantic_translator_test.go`,
  `export_committer_test.go`, `export_consumer_test.go`, `export_integration_test.go`.
- **`statefun/cache`** — `store_value_lazy_test.go`, `backup_barrier_test.go`.
- **`statefun/system`** — `keymutex_timeout_test.go`.
- **`clients/go/db`** — `common_test.go`.
- **`tests/e2e/test/{nats,runtime}`**, **`tests/export/pg_{high,low}_level/dumper`**
  — testcontainers E2E (real NATS / Postgres).

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
| `--race` | run Go tests under the race detector (slower) |
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

## 6. Roadmap — docker-compose system tests

The areas that cannot be exercised by `go test` (OS-signal graceful shutdown,
multi-node HA, load/soak, full backup/restore, cross-domain) will be covered by
**docker-compose driven system tests** that run **after** the Go tests, one by
one (sequentially, so they do not contend), orchestrated by
`scripts/run-all-tests.sh`.

The detailed, per-test plan (topology, steps, assertions, pass criteria,
priorities) is in [SYSTEM_TESTS_PLAN.md](./SYSTEM_TESTS_PLAN.md).

### Target repository layout

- **`samples/`** — the demo docker-compose apps currently under `tests/`
  (`basic`, `simple`, `object`, `cluster`, `distributed`, `backup-barrier`).
  These are examples, not assertions, and will be moved out of `tests/`.
- **`tests/`** — actual automated tests only:
  - Go tests (as today),
  - `tests/system/<name>/` — each a docker-compose project plus an executable
    `run.sh` that brings it up, asserts, and tears it down (exit 0 == pass). The
    runner discovers and executes these.

### Planned system-test map (prod-critical zones)

| Zone | Go-test status | System test (planned) |
|---|---|---|
| CRUD correctness / idempotency | covered | full multi-service CRUD soak |
| JPGQL (filters, multi-hop) | covered | — |
| Concurrency (`-race`) | covered | — |
| HA active/passive + recovery | covered (2 in-proc) | **HA 3-node** failover/leadership |
| WAL durability / replication | covered (2 in-proc) | restart & replay across restart |
| Backup write-barrier | covered (unit) | **full backup + restore** round-trip |
| Graceful shutdown (drain) | not go-testable¹ | SIGTERM drain on real NATS |
| Load / soak (degradation) | — | throughput + no-leak under sustained load |
| Coherent export (PG/Neo4j) | covered (dumpers) | end-to-end export consistency |
| Cross-domain / shadow objects | — | weak-cluster cross-domain link + JPGQL |

¹ The graceful-drain path is triggered only by a process-wide OS signal, which
also crashes the embedded NATS server in `go test`; it must be tested against a
real NATS instance in a subprocess/container.

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
