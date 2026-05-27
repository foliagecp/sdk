#!/usr/bin/env bash
#
# run-soak-tests.sh — endurance/soak runner for Foliage SDK.
#
# Intentionally separate from run-all-tests.sh: soak tests run for an hour
# by default (configurable via SOAK_DURATION_MIN) and they exist to catch
# multi-hour failure modes that the short tests/system tests cannot — slow
# leaks, HA promotion-flap wedges, the 116-class "NATS stalled, runtime
# went passive and never came back" incident.
#
# These are NOT part of CI by default. Run them locally / overnight when
# you want a stability signal you can trust before tagging a release.
#
# Usage:
#   scripts/run-soak-tests.sh [--scenario nats-stall-recovery|steady-state-1h|ha-promotion-flap|leak-hunt|all]
#                             [--duration-min N]
#
# `all` runs the three production-grade scenarios (nats-stall-recovery,
# steady-state-1h, ha-promotion-flap) which gate pass/fail. `leak-hunt` is
# observation-only (unbounded unique-id workload to expose tombstone-cascade
# accumulation locally) and is NOT included in `all` — invoke it explicitly.
#
# Per-scenario tunables (env): see tests/soak/README.md.
#
set -uo pipefail
cd "$(dirname "$0")/.."

SCENARIO="all"
DURATION_MIN="${SOAK_DURATION_MIN:-60}"

while [ $# -gt 0 ]; do
  case "$1" in
    --scenario)     SCENARIO="$2"; shift 2 ;;
    --duration-min) DURATION_MIN="$2"; shift 2 ;;
    -h|--help)      sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1 (see --help)"; exit 2 ;;
  esac
done

export SOAK_DURATION_MIN="$DURATION_MIN"

SOAK_DIR="tests/soak"
case "$SCENARIO" in
  all)
    SCENARIOS=( nats-stall-recovery steady-state-1h ha-promotion-flap )
    ;;
  nats-stall-recovery|steady-state-1h|ha-promotion-flap|leak-hunt)
    SCENARIOS=( "$SCENARIO" )
    ;;
  *)
    echo "unknown scenario: $SCENARIO (use nats-stall-recovery|steady-state-1h|ha-promotion-flap|leak-hunt|all)"
    exit 2
    ;;
esac

# Soak scenarios bind the same host ports (NATS :4222/:8222, runtime :9901/:6060,
# extra HA ports :9902/:9903/:6061/:6062). Run them sequentially on a clean slate
# and pre-sweep leftovers, mirroring run-all-tests.sh Phase 2.
if command -v docker >/dev/null 2>&1; then
  echo ">> sweeping leftover foliage-soak containers/volumes"
  docker ps -aq --filter 'name=foliage-soak-' | xargs -r docker rm -f >/dev/null 2>&1 || true
  docker volume ls -q --filter 'name=foliage-soak-' | xargs -r docker volume rm -f >/dev/null 2>&1 || true
fi

echo "=================================================================="
echo "Foliage SDK soak run"
echo "  scenarios     : ${SCENARIOS[*]}"
echo "  duration      : ${DURATION_MIN} min each"
echo "=================================================================="

fail=0
for s in "${SCENARIOS[@]}"; do
  run="$SOAK_DIR/$s/run.sh"
  if [ ! -x "$run" ]; then
    echo ">> WARN: $run missing or not executable — skipping"
    continue
  fi
  echo "------------------------------------------------------------------"
  echo ">> soak: $s (duration ${DURATION_MIN} min)"
  start=$SECONDS
  if bash "$run"; then
    echo ">> $s: PASS (took $((SECONDS - start))s)"
  else
    echo ">> $s: FAIL (took $((SECONDS - start))s)"
    fail=1
  fi
done

echo "=================================================================="
if [ "$fail" -eq 0 ]; then
  echo "SOAK RUN: completed"
else
  echo "SOAK RUN: some scenarios failed"
fi
echo ">> artefacts under tests/soak/_results/ — observer CSVs and any failure dumps"
echo "=================================================================="
exit "$fail"
