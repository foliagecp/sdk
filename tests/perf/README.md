# Foliage SDK — performance tests

End-to-end performance measurements against a containerised foliage runtime.
Lives next to `tests/system/` but is **intentionally not part of
`scripts/run-all-tests.sh`**: perf runs are long, noisy, and produce numbers
to track over time — they are not pass/fail gates.

## Layout

```
tests/perf/
├── _lib/
│   ├── perfclient/      # Go CLI: timed workload + percentiles + CSV row
│   └── compose.sh       # extends tests/system/_lib/compose.sh with perf helpers
├── crud/                # CRUD scenarios (write / read / 80-20 mixed)
│   ├── docker-compose.yaml
│   ├── configs/nats/nats.conf
│   ├── .env
│   └── run.sh
└── _results/            # one CSV per run; created on first measurement
```

`jpgql/` and `fpl/` follow the same shape — added in a follow-up.

## Run

```sh
# default: scenario=all (currently just crud), scale=10k, warmup=15s, measure=30s
scripts/run-perf-tests.sh

# pick scale set
scripts/run-perf-tests.sh --scale 1k
scripts/run-perf-tests.sh --scale all      # 1k + 10k + 100k

# tune timing
scripts/run-perf-tests.sh --warmup 30 --duration 60

# tune concurrency sweep
scripts/run-perf-tests.sh --concurrencies "1 8 32"

# write to a specific CSV
scripts/run-perf-tests.sh --csv /tmp/perf-baseline.csv
```

## What gets measured

Per scale (graph size N) and per concurrency C, three workloads:

| Workload | What it does |
|---|---|
| `crud-write` | sustained `ObjectUpdate` (upsert) with unique ids |
| `crud-read` | sustained `ObjectRead` randomly drawn from the seeded pool of N ids |
| `crud-mixed` | 80/20 read/write mix on the seeded pool |

Each measurement: 15s warm-up (discarded) + 30s measurement window.
One CSV row per measurement:

```
timestamp,git_sha,host,cpu,mem_gb,os,scenario,scale,concurrency,label,
duration_sec,ops_total,ops_per_sec,p50_ms,p95_ms,p99_ms,errors
```

## Reading the results

```sh
# the runner prints the CSV path at the end; quick eyeballing:
column -t -s, < tests/perf/_results/perf-<ISO>.csv | less -S
```

For comparing two runs on the **same host**, diff the per-(scenario, scale,
concurrency) rows. Cross-host comparisons require equal `cpu` / `mem_gb` /
`os` columns to be meaningful — that's why each row carries them.

## Caveats

- Single-node runtime. HA/cluster perf is a separate concern.
- Numbers on a busy laptop are useful as a trend (same machine, before/after)
  but should not be quoted as absolutes. Use a stable host for absolutes.
- The runner deliberately does not include `jpgql` / `fpl` yet — those
  scenarios land in the next iteration.
