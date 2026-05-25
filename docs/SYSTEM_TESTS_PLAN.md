# System Tests Plan (docker-compose)

This is the plan for the **system tests** that live under `tests/system/` and run
**after** the Go tests, sequentially, orchestrated by `scripts/run-all-tests.sh`.
They cover what `go test` cannot exercise in-process: real multi-node topologies,
OS-signal graceful shutdown, backup/restore, sustained load, and cross-domain
behavior — i.e. the production-critical zones.

See [TESTING.md](./TESTING.md) for the overall test strategy and the Go-test
inventory.

## 1. Why these are separate from `go test`

- They need **multiple real processes/containers** (HA nodes, separate NATS,
  Postgres/Neo4j) and **real OS signals** — a process-wide `SIGTERM` in a `go
  test` binary also crashes the embedded NATS server, so graceful shutdown can
  only be tested against a real NATS in its own container/process.
- They are **slow and stateful** (compose up/down, soak windows) and must run
  **one at a time** so they do not contend for ports/CPU.

## 2. Conventions

Layout — one directory per test:

```
tests/system/<name>/
  docker-compose.yaml      # the topology under test
  run.sh                   # executable: up -> wait healthy -> assert -> down; exit 0 == pass
  assert/                  # optional: a small Go program or scripts that make assertions
  README.md                # what it verifies, how to run standalone
```

`run.sh` contract:

1. `docker compose up -d --build` (project-scoped name to avoid clashes).
2. Wait for health (NATS ready, runtimes active) with a bounded timeout.
3. Drive the scenario and assert (via the `assert/` client or `nats`/HTTP calls).
4. On any failure: dump `docker compose logs` and exit non-zero.
5. Always `docker compose down -v` (trap EXIT) to clean volumes/containers.

Shared helpers to build once (under `tests/system/_lib/`):
- health-wait (poll a runtime's readiness / `IsActiveInstance` via an admin
  endpoint or a probe function),
- a thin assertion client (CMDB CRUD + JPGQL over NATS, reusing `clients/go/db`),
- log/metric collectors (Prometheus scrape snapshot for soak assertions).

CI: invoked by `scripts/run-all-tests.sh` (Phase 2). Heavy/soak tests gated to a
nightly job; smoke subset on PRs.

## 3. Test catalog

Priorities: **P0** = production-critical correctness; **P1** = important hardening;
**P2** = nice-to-have.

| Test | Zone | Prio |
|---|---|---|
| `graceful-shutdown` | clean drain on SIGTERM | **P0** |
| `ha-3-node` | leadership, failover, exclusivity | **P0** |
| `backup-restore` | data durability round-trip | **P0** |
| `coherent-export` | export consistency (PG/Neo4j) | **P0** |
| `crud-soak` | CRUD correctness under load | P1 |
| `load` | graceful degradation, no leaks | P1 |
| `wal-restart-recovery` | durability across restart | P1 |
| `cross-domain` | weak-cluster cross-domain link + JPGQL | P1 |
| `nats-outage` | resilience to broker blips | P2 |

### 3.1 `graceful-shutdown` (P0)
- **Topology:** 1 NATS + 1 runtime (with in-flight work generator).
- **Steps:** start; push a steady stream of CRUD ops; send `SIGTERM` to the
  runtime container; observe shutdown.
- **Assert:** the runtime drains in-flight handlers, flushes the WAL committer,
  releases the runtime lock, and exits 0 within the drain window; no committed
  op is lost (re-read after restart matches); process does not hang.
- **Pass:** clean exit code + zero lost committed ops + bounded drain time.

### 3.2 `ha-3-node` (P0)
- **Topology:** 1 NATS (or cluster) + 3 runtimes, same domain, `activePassiveMode=true`.
- **Steps:** wait until exactly one is active; write data via the active; kill the
  active; repeat (kill the new active); restore nodes.
- **Assert:** at all times **exactly one** active (lock exclusivity, no
  split-brain); after each kill a passive becomes active within the soft-TTL
  window; passives reject writes; no data loss across failovers; the recovered
  node rejoins as passive and its cache re-syncs.
- **Pass:** invariant "exactly one active" never violated; every failover
  recovers; data intact.

### 3.3 `backup-restore` (P0)
- **Topology:** 1 NATS + 1 runtime + backup tooling.
- **Steps:** populate a known graph; trigger a backup (engage the write barrier,
  snapshot KV); continue writes; wipe state; restore from the backup; restart.
- **Assert:** the restored graph equals the snapshot point (barrier consistency —
  writes newer than the barrier are not in the backup); post-restore the runtime
  serves reads/writes normally; no orphans/dangling indices (graph-consistency
  invariant).
- **Pass:** restored == snapshot; consistency invariant holds; runtime healthy.

### 3.4 `coherent-export` (P0)
- **Topology:** 1 NATS + 1 runtime + export consumer + Postgres (and/or Neo4j).
- **Steps:** apply a sequence of CRUD ops; let the export pipeline drain.
- **Assert:** the external store reflects exactly the committed graph (every
  vertex/link/tag present, no extras, correct bodies); idempotent under
  redelivery (replaying WAL does not duplicate rows); ordering preserved.
- **Pass:** external store == graph; no dupes on redelivery.
- *Note:* the Go-level dumper tests (`tests/export/...`) cover units; this is the
  full end-to-end pipeline.

### 3.5 `crud-soak` (P1)
- **Topology:** 1 NATS + 1 runtime + N concurrent CRUD clients.
- **Steps:** run mixed create/update/delete/link/unlink at high rate for a window.
- **Assert:** after quiesce, the graph-consistency invariant holds (no orphans/
  phantoms/dangling indices, exactly one `__type` per object); no lost updates;
  no goroutine/heap leak (Prometheus snapshot stable).
- **Pass:** invariants hold; resources return to baseline.

### 3.6 `load` (P1)
- **Topology:** 1 NATS + 1 runtime + load generator at increasing rates (1x→8x).
- **Steps:** ramp load; then push beyond capacity (overload).
- **Assert:** **graceful degradation** — latency rises but the system stays live
  (a liveness watchdog never stalls); success rate holds or back-pressures
  (bounded queue), never deadlocks; heap/goroutines bounded; on load drop it
  recovers to baseline. No "becomes passive and never returns" (116 class).
- **Pass:** no deadlock/leak; bounded degradation; recovery after overload.

### 3.7 `wal-restart-recovery` (P1)
- **Topology:** 1 NATS + 1 runtime.
- **Steps:** write data; hard-stop the runtime; restart it.
- **Assert:** on restart the runtime reconstructs the exact graph from the
  startup snapshot + WAL; no double-applied triggers on WAL redelivery; no data
  loss.
- **Pass:** post-restart graph == pre-stop graph; triggers fire once.

### 3.8 `cross-domain` (P1)
- **Topology:** 2 NATS-or-domains in a weak cluster (e.g. `d1`, `d2`) + a runtime
  per domain.
- **Steps:** create an object in each domain; create a cross-domain link
  (shadow object); run a JPGQL query that traverses the cross-domain hop.
- **Assert:** the shadow object is created correctly; the cross-domain JPGQL hop
  returns the far-domain target; cross-domain reads resolve.
- **Pass:** cross-domain traversal/read returns the expected results.

### 3.9 `nats-outage` (P2)
- **Topology:** 1 runtime + NATS that is briefly stopped/started.
- **Steps:** induce a short broker outage during traffic.
- **Assert:** the runtime survives, reconnects, resumes serving; no permanent
  wedge; no cascade into a stuck passive state.
- **Pass:** full recovery after the blip.

## 4. Shared infrastructure to build first

1. `tests/system/_lib/` — health-wait, assertion client (CMDB/JPGQL over NATS),
   log/metric collectors, a `compose.sh` helper (up/wait/down with trap).
2. A reusable **graph-consistency assertion** (mirror of the in-process
   `assertObjectConsistent`) callable against a live runtime, for soak/backup/HA.
3. A small **load/liveness generator** with a watchdog (reused by `load` and
   `crud-soak`).

## 5. Rollout order

1. `_lib` shared harness (health-wait, assert client, compose helper).
2. **P0:** `graceful-shutdown` → `ha-3-node` → `backup-restore` → `coherent-export`.
3. **P1:** `crud-soak` → `load` → `wal-restart-recovery` → `cross-domain`.
4. **P2:** `nats-outage`.

Each new test drops into `tests/system/<name>/run.sh` and is picked up
automatically by `scripts/run-all-tests.sh`.
