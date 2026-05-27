# `tests/soak/` — endurance / soak tests

Long-running scenarios that exercise the runtime for an hour by default and
look for failure modes too slow for `tests/system/` (which runs in tens of
seconds): memory or goroutine leaks, repeated HA promotion-flap wedges, and
the **116-class incident** — production saw NATS stall briefly, the runtime
ran `becomePassive`, then a reentrant lifecycle deadlock kept it passive
for 13 hours.

Soak is **not** wired into `scripts/run-all-tests.sh`. Run it explicitly:

```sh
scripts/run-soak-tests.sh                            # all three, 60 min each (~3h)
scripts/run-soak-tests.sh --scenario nats-stall-recovery
scripts/run-soak-tests.sh --duration-min 15          # smoke run
```

## Scenarios

| Scenario | What it stresses | Primary failure mode it guards |
|---|---|---|
| `nats-stall-recovery` | NATS pause/unpause at random intervals while load runs | 116-class: runtime never returns to active after a NATS hiccup |
| `steady-state-1h` | Continuous CRUD with no induced faults | Slow memory / goroutine drift; gradual throughput collapse |
| `ha-promotion-flap` | 3 runtimes, the active is SIGKILLed every ~10 min | Wedged promotion (no node serves); data loss across kills |

Each scenario lives in its own subdirectory with a `docker-compose.yaml`
topology + an executable `run.sh`. They run sequentially.

## How a scenario decides pass / fail

Each `run.sh` brings the stack up, kicks off a background CRUD workload via
the existing `tests/system/_lib/assert` client, then runs the **observer**
(a host-side Go binary at `tests/soak/_lib/observer/`) in the foreground for
the soak duration.

The observer samples every 10 seconds:

* **Liveness probe** — one unique CMDB `TypeCreate` per endpoint per tick.
  Any stretch of consecutive failures longer than `-max-stall` (default 30 s)
  is the primary FAILURE signal. This is what catches the 116-class wedge:
  the runtime can be "up" yet not serving, and the probe will see exactly
  that.
* **Prometheus scrape** — `fg_runtime_mem_alloc_bytes` and
  `fg_runtime_routines_counter`. A trailing-window least-squares slope fit
  flags growth above `-max-mem-drift-bph` / `-max-goroutine-drift-ph` as a
  leak.

Every sample is written to `tests/soak/_results/<run>/observer.csv` so the
data survives a fail-fast exit and is available for post-mortem.

`ha-promotion-flap` records every chaos action (`kill`, `restart`) into
`events.csv` so the operator can correlate stalls to the action that caused
them; recovery latency is then visible as the gap between a `chaos_…` event
and the next OK sample.

The scenario passes iff:

1. The observer exits 0 (no SLO violations).
2. `assert ping` succeeds at the end (the runtime is still responsive).
3. `assert consistency` succeeds (the post-soak graph has no orphaned
   index entries — no slow drift in graph integrity).

## What soak does NOT cover

* **HA active-flag exclusivity** (only one runtime holds
  `isActiveInstance==true` at a time) is an internal-state invariant and is
  not visible from outside. `statefun/ha_test.go` exercises it in-process
  with directly inspectable runtimes; soak intentionally does not duplicate
  that.
* **Per-operation throughput targets**. Those live in `tests/perf/` and run
  for tens of seconds; soak just makes sure throughput doesn't *collapse*
  over the hour.

## Tunables (env)

Global:

| Env | Default | Meaning |
|---|---|---|
| `SOAK_DURATION_MIN` | `60` | total run duration in minutes (every scenario) |

Per-scenario:

| Env | Scenario | Default | Meaning |
|---|---|---|---|
| `SOAK_CHAOS_PERIOD` | nats-stall-recovery | `600` s | seconds between NATS pauses |
| `SOAK_MAX_STALL_SEC` | nats-stall-recovery | `30` s | recovery SLO after unpause |
| `SOAK_WORKERS` | steady-state-1h | `8` | concurrent CRUD workers |
| `SOAK_STEADY_POOL` | steady-state-1h | `1000` | objects pre-seeded per worker (constant working set) |
| `SOAK_MEM_DRIFT_BPH` | steady-state-1h | `209715200` B/hr | leak threshold |
| `SOAK_GOROUTINE_PH` | steady-state-1h | `200` /hr | leak threshold |
| `SOAK_KILL_PERIOD` | ha-promotion-flap | `600` s | seconds between SIGKILLs |
| `SOAK_FAILOVER_SLO` | ha-promotion-flap | `30` s | max liveness gap after a kill |

### The workload always has a bounded working set

`assert soak` is the single workload driver used by both
`tests/system/crud-soak` (25 s burst) and `tests/soak/steady-state-1h`
(1 h endurance). It always uses a **bounded** working set: each worker
pre-seeds `-pool` objects + chain links, then loops ObjectUpdate over that
fixed pool, with a periodic ObjectDelete+ObjectCreate on the rolling slot
(`-refresh-every`) to keep the full CRUD path exercised.

Cardinality is exactly `workers * pool` from end of pre-seed onward.
That means the soak observer's memory-drift signal is **honest**: any
positive drift is a leak in the cache, the WAL queue, or somewhere
upstream of GC — not just "the graph grew". The 25-second `crud-soak`
doesn't care about drift but inherits the same shape because there is no
reason for it to grow the set either.

## Artefacts on failure

`run.sh` calls `fail` on any SLO violation. That triggers `dump_state`,
which writes to `tests/soak/_results/<run>/dump-failure/`:

* `compose.log` — full `docker compose logs` (every service)
* `ps.txt` — final container state
* `goroutine-<port>.txt` — `/debug/pprof/goroutine?debug=2` from each
  exposed pprof port (best-effort — needs `PPROF_ADDR=:6060` to be honoured
  by the runtime binary)
* `heap-<port>.pprof` — `/debug/pprof/heap` snapshot
* `metrics-<port>.txt` — final `/metrics` scrape

The container stack is then `docker compose down -v` to free ports, but the
artefact directory stays for inspection.
