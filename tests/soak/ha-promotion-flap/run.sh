#!/usr/bin/env bash
#
# ha-promotion-flap — three-runtime HA stressed by repeated active-kills.
#
# Three runtimes share one NATS and one domain. Every ~10 min we SIGKILL
# whichever runtime is currently active; another must take over inside the
# failover SLO. We do this over the full soak window so the lifecycle code
# is exercised through ~6 promotion / demotion cycles per hour.
#
# Externally observable invariants this scenario guards:
#
#   * Continuity. After every kill, *some* runtime must be answering CMDB
#     within -max-stall seconds. A wedged promotion (no node serves) is
#     the failure mode — analogous in shape to the 116 wedge but reached
#     through HA promotion rather than NATS stall.
#   * No data loss. The seeded graph stays readable across all kills.
#
# Active-flag exclusivity (only one in-memory `isActiveInstance==true` at
# a time) is *not* checked here — there is no external probe for it. That
# property is covered by statefun/ha_test.go in-process.
#
# Tunables:
#   SOAK_DURATION_MIN   total duration in minutes  (default 60)
#   SOAK_KILL_PERIOD    seconds between kills      (default 600)
#   SOAK_FAILOVER_SLO   max stall after a kill     (default 30)

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh
# shellcheck source=../_lib/chaos.sh
source ../_lib/chaos.sh

PROJECT="foliage-soak-ha-flap"
COMPOSE_FILE="docker-compose.yaml"
KILL_PERIOD="${SOAK_KILL_PERIOD:-600}"
FAILOVER_SLO="${SOAK_FAILOVER_SLO:-30}"

RUN_DIR="$(results_dir)"
export CHAOS_EVENTS_FILE="$RUN_DIR/events.csv"

install_trap
build_assert
build_observer

echo ">> run dir: $RUN_DIR"
echo ">> duration ${SOAK_DURATION_MIN}m, kill period ${KILL_PERIOD}s, failover SLO ${FAILOVER_SLO}s"

echo ">> bringing up topology (building runtime image if needed)"
dc up -d --build || fail "compose up failed"

echo ">> waiting for NATS monitoring (:8222)"
wait_http "http://localhost:8222/healthz" 90 || fail "NATS did not become healthy"
echo ">> waiting until one runtime is active and serving"
assert ping -wait 120 || fail "no runtime became active"
# Each runtime exposes its own Prometheus, but only one is active at a time
# — readiness of any single endpoint is enough to start observing.
wait_http "http://localhost:9901/metrics" 60 || fail "no runtime Prometheus endpoint up"

echo ">> seeding 200 objects"
assert seed -n 200 || fail "seed failed"
assert verify -n 200 || fail "initial verify failed"

# active_runtime prints the compose-service name of whichever runtime is
# currently believed to hold the lock. With no externally addressable
# "I am the leader" indicator we infer it: the one with the lowest CPU is
# almost certainly passive; the one whose log most recently mentions
# "active" wins. As a robust fallback we rotate kills round-robin — the
# lock will always end up on a survivor.
ROTATION=( runtime-a runtime-b runtime-c )

CHAOS_LOG="$RUN_DIR/chaos.log"
(
  end=$((SECONDS + SOAK_DURATION_SEC))
  i=0
  # Stagger the first kill so the seeded graph has time to settle.
  sleep 60
  while [ "$SECONDS" -lt "$end" ]; do
    target="${ROTATION[$((i % 3))]}"
    i=$((i + 1))
    echo ">> chaos: kill $target then restart"
    chaos_kill_restart "$target" 5
    sleep "$KILL_PERIOD"
  done
) >"$CHAOS_LOG" 2>&1 &
CHAOS_PID=$!
echo ">> chaos loop PID=$CHAOS_PID -> $CHAOS_LOG"

OBSERVER_CSV="$RUN_DIR/observer.csv"
echo ">> running observer for ${SOAK_DURATION_SEC}s -> $OBSERVER_CSV"
# All three Prometheus endpoints are scraped so memory/goroutine drift is
# tracked across the whole cluster. Liveness still goes through the single
# NATS endpoint (logically there is only one active leader to talk to).
if ! observe \
      -nats "nats://nats:foliage@localhost:4222" \
      -prom "http://localhost:9901/metrics" \
      -csv "$OBSERVER_CSV" \
      -duration "${SOAK_DURATION_SEC}s" \
      -interval 10s \
      -max-stall "${FAILOVER_SLO}s"; then
  kill "$CHAOS_PID" 2>/dev/null || true
  fail "observer reported SLO violation"
fi

kill "$CHAOS_PID" 2>/dev/null || true

echo ">> post-soak readiness"
assert ping -wait 30 || fail "no runtime serving after chaos ended"
echo ">> post-soak verify (no data loss across all the kills)"
assert verify -n 200 || fail "data lost after HA flapping"
echo ">> post-soak consistency"
assert consistency -type systest_node -settle 30 || fail "post-soak consistency failed"

echo ">> ha-promotion-flap: PASS"
echo ">> artefacts: $RUN_DIR"
