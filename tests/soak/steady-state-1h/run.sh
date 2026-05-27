#!/usr/bin/env bash
#
# steady-state-1h — sustained CRUD with no induced faults.
#
# The fast tests/system/crud-soak runs for ~25 seconds — long enough to
# expose contention but far too short to expose a slow leak. This scenario
# runs the same kind of workload for an hour by default and the observer
# watches for:
#
#   * RSS / Go-heap drift (-max-mem-drift-bph)
#   * goroutine drift     (-max-goroutine-drift-ph)
#   * any liveness stall  (-max-stall)
#
# Pass iff the observer is happy at end-of-run AND a post-load consistency
# probe succeeds (no orphan/dangling-index aftertaste).
#
# Tunables:
#   SOAK_DURATION_MIN   total duration in minutes (default 60)
#   SOAK_WORKERS        background workers        (default 8)
#   SOAK_STEADY_POOL    objects per worker        (default 1000 — bounded set)
#   SOAK_MEM_DRIFT_BPH  allowed memory drift      (default 200 MB/hr)
#   SOAK_GOROUTINE_PH   allowed goroutine drift   (default 200 /hr)
#
# `assert soak` always uses a bounded working set: it pre-seeds
# workers*pool objects and then loops ObjectUpdate (with a periodic
# Delete+Create on the rolling slot to keep the full CRUD path covered)
# over that fixed set. Cardinality stays constant from end of seed to end
# of run, so any positive memory drift the observer reports IS a leak in
# the cache, WAL queue, op-stack pool, etc. — not just "more data exists".
# We pass a larger -pool here than the 25s crud-soak default because the
# hour gives us room to exercise more of the graph.

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-soak-steady"
COMPOSE_FILE="docker-compose.yaml"
WORKERS="${SOAK_WORKERS:-8}"
STEADY_POOL="${SOAK_STEADY_POOL:-1000}"
MEM_DRIFT_BPH="${SOAK_MEM_DRIFT_BPH:-209715200}"   # 200 MB/hr
GOROUTINE_PH="${SOAK_GOROUTINE_PH:-200}"

RUN_DIR="$(results_dir)"
install_trap
build_assert
build_observer

echo ">> run dir: $RUN_DIR"
echo ">> duration ${SOAK_DURATION_MIN}m, workers $WORKERS, steady-pool $STEADY_POOL per worker"

echo ">> bringing up topology (building runtime image if needed)"
dc up -d --build || fail "compose up failed"

echo ">> waiting for NATS monitoring (:8222)"
wait_http "http://localhost:8222/healthz" 90 || fail "NATS did not become healthy"
echo ">> waiting for runtime readiness"
assert ping -wait 120 || fail "runtime never became ready"
echo ">> waiting for Prometheus :9901"
wait_http "http://localhost:9901/metrics" 60 || fail "runtime Prometheus endpoint never came up"

WORKLOAD_LOG="$RUN_DIR/workload.log"
(
  assert soak -workers "$WORKERS" -duration "$SOAK_DURATION_SEC" \
              -prefix steady -pool "$STEADY_POOL" \
              >"$WORKLOAD_LOG" 2>&1 || true
) &
WORKLOAD_PID=$!
echo ">> workload PID=$WORKLOAD_PID -> $WORKLOAD_LOG"

OBSERVER_CSV="$RUN_DIR/observer.csv"
echo ">> running observer for ${SOAK_DURATION_SEC}s -> $OBSERVER_CSV"
if ! observe \
      -nats "nats://nats:foliage@localhost:4222" \
      -prom "http://localhost:9901/metrics" \
      -csv "$OBSERVER_CSV" \
      -duration "${SOAK_DURATION_SEC}s" \
      -interval 10s \
      -max-stall 30s \
      -max-mem-drift-bph "$MEM_DRIFT_BPH" \
      -max-goroutine-drift-ph "$GOROUTINE_PH"; then
  kill "$WORKLOAD_PID" 2>/dev/null || true
  fail "observer reported SLO violation (see $OBSERVER_CSV)"
fi

kill "$WORKLOAD_PID" 2>/dev/null || true
wait "$WORKLOAD_PID" 2>/dev/null || true

echo ">> post-soak readiness"
assert ping -wait 30 || fail "runtime unresponsive after soak ended"
echo ">> post-soak consistency"
assert consistency -type systest_node -settle 30 || fail "post-soak consistency failed"

echo ">> steady-state-1h: PASS"
echo ">> artefacts: $RUN_DIR"
