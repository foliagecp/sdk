#!/usr/bin/env bash
#
# crud — CRUD perf measurement against a single-node foliage runtime.
#
# Brings up nats + runtime, seeds a graph of $SCALE objects, then drives three
# sustained workloads under varying concurrency:
#
#   crud-write   ObjectUpdate (upsert) on unique ids
#   crud-read    ObjectRead on the seeded id pool
#   crud-mixed   80%/20% read/write mix on the seeded id pool
#
# Each measurement is 15s warm-up + 30s window. Latencies (p50/p95/p99) and
# throughput land in $PERF_CSV (one row per measurement).
#
# Args:
#   $1 = scale label: 1k | 10k | 100k   (default 10k)

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

SCALE="${1:-10k}"
case "$SCALE" in
  1k)    N=1000   ;;
  10k)   N=10000  ;;
  100k)  N=100000 ;;
  *)     echo "unknown scale: $SCALE (use 1k|10k|100k)" >&2; exit 2 ;;
esac

PROJECT="foliage-perf-crud-$SCALE"
COMPOSE_FILE="docker-compose.yaml"
NATS_URL_HOST="nats://nats:foliage@localhost:4222"
WARMUP="${PERF_WARMUP_SEC:-15}"
DURATION="${PERF_DURATION_SEC:-30}"
CONCURRENCIES="${PERF_CONCURRENCIES:-1 4 16}"
SEED_TYPE="${PERF_SEED_TYPE:-perf_node}"
SEED_PREFIX="${PERF_SEED_PREFIX:-node}"
SEED_CONCURRENCY="${PERF_SEED_CONCURRENCY:-16}"

install_trap
record_host_info
init_perf_csv
build_assert
build_perfclient

echo ">> [$SCALE] starting nats + runtime"
dc build runtime >/dev/null || fail "runtime build failed"
dc up -d nats runtime || fail "compose up failed"

wait_http "http://localhost:8222/healthz" 60 || fail "nats did not become healthy"
assert ping -nats "$NATS_URL_HOST" -wait 120 || fail "runtime never became ready"

echo ">> [$SCALE] seeding $N objects (type=$SEED_TYPE prefix=$SEED_PREFIX, concurrency=$SEED_CONCURRENCY)"
# We use perfclient seed, NOT assert seed: assert is sequential and also
# chain-links every consecutive pair (2N requests) which at 100k takes 20+ min
# and slams the runtime more than the actual measurement does. perf CRUD only
# needs a flat pool of N ids to read from — no chain.
perfclient seed \
  -nats "$NATS_URL_HOST" \
  -n "$N" -type "$SEED_TYPE" -prefix "$SEED_PREFIX" \
  -concurrency "$SEED_CONCURRENCY" \
  || fail "seed failed"

run_one() {
  local sub="$1" conc="$2"; shift 2
  echo ">> [$SCALE] $sub  concurrency=$conc"
  perfclient "$sub" \
    -nats "$NATS_URL_HOST" \
    -concurrency "$conc" \
    -warmup "$WARMUP" \
    -duration "$DURATION" \
    -scenario "$sub" \
    -scale "$SCALE" \
    -csv "$PERF_CSV" \
    "$@" \
    || fail "perfclient $sub @c=$conc failed"
}

# shellcheck disable=SC2086
for c in $CONCURRENCIES; do
  run_one crud-write $c -type "perf_w_${SCALE}" -prefix "w_${SCALE}"
done

# shellcheck disable=SC2086
for c in $CONCURRENCIES; do
  run_one crud-read  $c -n "$N" -prefix "$SEED_PREFIX"
done

# shellcheck disable=SC2086
for c in $CONCURRENCIES; do
  run_one crud-mixed $c -n "$N" -prefix "$SEED_PREFIX" -type "perf_mix_${SCALE}"
done

echo ">> [$SCALE] crud perf: DONE"
