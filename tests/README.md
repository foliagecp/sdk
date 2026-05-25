# `tests/` — automated tests

This directory holds the SDK's automated tests, split by **how** they run.
Both kinds are executed by the single entry point
[`scripts/run-all-tests.sh`](../scripts/run-all-tests.sh).

> Demo applications are **not** here — they live in [`samples/`](../samples).
> `tests/` contains assertions only.

## Two kinds of tests

| | `tests/integration/` | `tests/system/` |
|---|---|---|
| **Driver** | `go test` + [testcontainers-go] | `docker-compose` + `run.sh` |
| **What runs in containers** | dependencies only (NATS, Postgres, Neo4j) | the **whole deployment**, including the foliage runtime binary |
| **Where the runtime runs** | in-process (inside the `go test` binary) | in its own container (a real OS process) |
| **Orchestrated by** | Go test code | a shell script |
| **Run in the full suite by** | Phase 1 (`go test ./...`) | Phase 2 (each `run.sh`) |

### Why two kinds?

Most behaviour is covered by Go tests — fast, assertion-rich, self-cleaning,
and able to spin real dependency containers via testcontainers. But some
behaviour **cannot** be exercised with the runtime in-process:

- a process-wide `SIGTERM` (graceful drain) inside a `go test` binary also
  crashes the embedded NATS server;
- true multi-process failover and ungraceful crash/restart need the runtime
  as a separate OS process.

Those run as `tests/system/` projects, where the runtime itself is a
container and the scenario is driven by a shell script.

Together the two layers cover the SDK end to end. The unit tests living next
to the code (`*_test.go` throughout the repository) are the foundation; this
directory adds the real-container layers on top.

## `tests/integration/` (go test)

Real dependency containers + in-process runtime. Picked up automatically by
`go test ./...`, so the full suite already runs them.

- `e2e/` — runtime against a real NATS cluster (key-mutex correctness); the
  reusable NATS-cluster testcontainers helper lives in `e2e/test/nats`.
- `export/` — the coherent-export pipeline against real Postgres / Neo4j
  containers (`pg_high_level`, `pg_low_level`, `pg_neo4j`): drive CMDB CRUD,
  then assert the external store reflects the graph (counts, bodies,
  idempotency on redelivery).

Run just these:

```sh
go test ./tests/integration/...
```

(They need a running Docker daemon; without one they fail to start the
containers.)

## `tests/system/` (docker-compose)

Each subdirectory is a self-contained project: a `docker-compose.yaml`
topology plus an executable `run.sh` that brings it up, waits for health,
drives the scenario, asserts, and tears it down — **exit 0 == pass**. They
run **sequentially** (one at a time) so they do not contend for ports/CPU.

| Project | Verifies |
|---|---|
| `graceful-shutdown` | clean drain on real SIGTERM (exit 0, bounded, no committed op lost) |
| `wal-restart-recovery` | committed data survives an ungraceful SIGKILL + restart |
| `ha-3-node` | leadership/failover across 3 runtimes; service continuity + no data loss |
| `crud-soak` | sustained concurrent CRUD; runtime stays responsive; graph stays consistent |
| `_lib/` | shared harness (compose helpers + the `assert` client) — not a test |

Run one standalone:

```sh
tests/system/graceful-shutdown/run.sh
```

Run the whole repo (both phases):

```sh
scripts/run-all-tests.sh            # add --go-only or --system-only to scope
```

See [`docs/TESTING.md`](../docs/TESTING.md) for the overall strategy and
[`docs/SYSTEM_TESTS_PLAN.md`](../docs/SYSTEM_TESTS_PLAN.md) for the system-test
roadmap.

[testcontainers-go]: https://golang.testcontainers.org/
