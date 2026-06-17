#!/usr/bin/env bash
#
# crud-delete — delete-path perf as a create+delete churn, on the SAME docker
# topology as the `crud` scenario (one NATS + one containerised foliage runtime),
# driven from the host by perfclient. Uniform with crud-write/read/mixed: same
# perfclient + runWorkload + compose. Reuses tests/perf/crud's compose, configs
# and runtime image (run from that dir) under a separate compose project.
#
# ObjectDelete is destructive, so there is no fixed read pool: each op creates a
# fresh unique object and deletes it (perfclient cmdCrudDelete) — the cascade is
# driven at full rate, and it mirrors the create/delete churn the HLM aging path
# (managerControl GC) generates. Latencies (p50/p95/p99) + throughput land in
# $PERF_CSV, one row per concurrency.
#
# Args:
#   $1 = scale label 1k | 10k | 100k   (CSV label only; the churn self-seeds)
set -uo pipefail
cd "$(dirname "$0")/../crud" || exit 1   # reuse crud's compose + configs + image
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

SCALE="${1:-10k}"
case "$SCALE" in
  1k|10k|100k) ;;
  *) echo "unknown scale: $SCALE (use 1k|10k|100k)" >&2; exit 2 ;;
esac

PROJECT="foliage-perf-crud-delete-$SCALE"
COMPOSE_FILE="docker-compose.yaml"
NATS_URL_HOST="nats://nats:foliage@localhost:4222"
WARMUP="${PERF_WARMUP_SEC:-15}"
DURATION="${PERF_DURATION_SEC:-30}"
CONCURRENCIES="${PERF_CONCURRENCIES:-1 4 16}"

install_trap
record_host_info
init_perf_csv
build_assert
build_perfclient

echo ">> [$SCALE] starting nats + runtime (crud topology)"
dc build runtime >/dev/null || fail "runtime build failed"
dc up -d nats runtime || fail "compose up failed"

wait_http "http://localhost:8222/healthz" 60 || fail "nats did not become healthy"
assert ping -nats "$NATS_URL_HOST" -wait 120 || fail "runtime never became ready"

# shellcheck disable=SC2086
for c in $CONCURRENCIES; do
  echo ">> [$SCALE] crud-delete  concurrency=$c"
  perfclient crud-delete \
    -nats "$NATS_URL_HOST" \
    -concurrency "$c" \
    -warmup "$WARMUP" \
    -duration "$DURATION" \
    -scenario crud-delete \
    -scale "$SCALE" \
    -csv "$PERF_CSV" \
    || fail "perfclient crud-delete @c=$c failed"
done

echo ">> [$SCALE] crud-delete perf: DONE"
