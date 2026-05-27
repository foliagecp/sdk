#!/usr/bin/env bash
#
# nats-stall-recovery — the primary 116-class regression test.
#
# Production incident 116: NATS had a transient stall, the runtime saw
# itself losing the runtime lock, ran `becomePassive`, and then a reentrant
# lifecycle deadlock prevented it from ever returning to active. The
# process stayed up but served nothing for 13 hours.
#
# What we do here:
#
#   1. Up a one-NATS / one-runtime topology and seed a known graph.
#   2. Drive a light continuous CRUD workload in the background so the
#      runtime is *processing* during the stalls (not idle).
#   3. Every ~10 min, pause the NATS container for 5 / 15 / 30s (picked at
#      random so the recovery path is exercised at multiple TTL boundaries
#      around the KV lock lifetime), then unpause it.
#   4. Run the observer in foreground for the full duration. It pings the
#      runtime every -interval and refuses to allow consecutive ping
#      failures longer than -max-stall.
#
# Pass iff the observer exits 0. Failure means the runtime did not come
# back within the SLO after at least one unpause — i.e. the 116 wedge
# (or an analogous lifecycle hang) is still reachable.
#
# Tunables:
#   SOAK_DURATION_MIN   total duration in minutes  (default 60)
#   SOAK_CHAOS_PERIOD   seconds between stalls    (default 600)
#   SOAK_MAX_STALL_SEC  recovery SLO              (default 30)

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh
# shellcheck source=../_lib/chaos.sh
source ../_lib/chaos.sh

PROJECT="foliage-soak-nats-stall"
COMPOSE_FILE="docker-compose.yaml"
CHAOS_PERIOD="${SOAK_CHAOS_PERIOD:-600}"
MAX_STALL_SEC="${SOAK_MAX_STALL_SEC:-30}"

RUN_DIR="$(results_dir)"
export CHAOS_EVENTS_FILE="$RUN_DIR/events.csv"

install_trap
build_assert
build_observer

echo ">> run dir: $RUN_DIR"
echo ">> duration ${SOAK_DURATION_MIN}m, chaos period ${CHAOS_PERIOD}s, max stall ${MAX_STALL_SEC}s"

echo ">> bringing up topology (building runtime image if needed)"
dc up -d --build || fail "compose up failed"

echo ">> waiting for NATS monitoring (:8222)"
wait_http "http://localhost:8222/healthz" 90 || fail "NATS did not become healthy"

echo ">> waiting for runtime readiness (CMDB ping)"
assert ping -wait 120 || fail "runtime never became ready"

echo ">> waiting for Prometheus :9901"
wait_http "http://localhost:9901/metrics" 60 || fail "runtime Prometheus endpoint never came up"

echo ">> seeding initial graph (200 objects)"
assert seed -n 200 || fail "seed failed"

# Background workload: keep a continuous trickle of CRUD ops running so the
# runtime is doing real work during every chaos event. `assert soak` runs
# until duration elapses; we don't fail the run on its exit (it may report
# an op-level error during a NATS stall, which is expected — observer
# decides pass/fail).
WORKLOAD_LOG="$RUN_DIR/workload.log"
(
  assert soak -workers 4 -duration "$SOAK_DURATION_SEC" -prefix soak-bg >"$WORKLOAD_LOG" 2>&1 || true
) &
WORKLOAD_PID=$!
echo ">> background workload PID=$WORKLOAD_PID -> $WORKLOAD_LOG"

# Chaos loop in the background: every CHAOS_PERIOD, pause NATS for a random
# duration from {5,15,30} seconds. Runs for the full SOAK_DURATION_SEC.
CHAOS_LOG="$RUN_DIR/chaos.log"
(
  end=$((SECONDS + SOAK_DURATION_SEC))
  # Stagger first stall so the workload has a few seconds to start.
  sleep 30
  while [ "$SECONDS" -lt "$end" ]; do
    dur="$(pick_random 5 15 30)"
    chaos_nats_stall "$dur" nats
    sleep "$CHAOS_PERIOD"
  done
) >"$CHAOS_LOG" 2>&1 &
CHAOS_PID=$!
echo ">> chaos loop PID=$CHAOS_PID -> $CHAOS_LOG"

# Observer in the foreground: this is the gate.
OBSERVER_CSV="$RUN_DIR/observer.csv"
echo ">> running observer for ${SOAK_DURATION_SEC}s -> $OBSERVER_CSV"
if ! observe \
      -nats "nats://nats:foliage@localhost:4222" \
      -prom "http://localhost:9901/metrics" \
      -csv "$OBSERVER_CSV" \
      -duration "${SOAK_DURATION_SEC}s" \
      -interval 10s \
      -max-stall "${MAX_STALL_SEC}s"; then
  # Best-effort: stop chaos + workload first so the dump captures a stable state.
  kill "$CHAOS_PID" "$WORKLOAD_PID" 2>/dev/null || true
  fail "observer reported SLO violation"
fi

# Tidy: stop chaos + workload.
kill "$CHAOS_PID" "$WORKLOAD_PID" 2>/dev/null || true
wait "$WORKLOAD_PID" 2>/dev/null || true

echo ">> post-soak readiness probe"
assert ping -wait 30 || fail "runtime unresponsive after soak ended"

echo ">> post-soak graph consistency (settle window tolerates index lag)"
assert consistency -type systest_node -settle 30 || fail "post-soak consistency failed"

echo ">> nats-stall-recovery: PASS"
echo ">> artefacts: $RUN_DIR"
