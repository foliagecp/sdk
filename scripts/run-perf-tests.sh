#!/usr/bin/env bash
#
# run-perf-tests.sh — performance-measurement runner for Foliage SDK.
#
# TWO MODES — the banner at the top of every run says which one is active:
#
#   DOCKER (default)       end-to-end over a REAL NATS transport. The number is
#                          round-trip latency/throughput as a client sees it
#                          (transport-bound). Scenarios: crud, crud-delete,
#                          jpgql, fpl — docker-compose + perfclient under
#                          tests/perf/<name>/.
#
#   EMBEDDED (--embedded)  in-process Go suites on an embedded NATS test runtime
#                          — NO NATS round-trips. The number is the SERVER-SIDE
#                          cost of the operation. Scenarios: crud-read,
#                          crud-update, crud-delete — go test -tags perf under
#                          embedded/graph/crud/.
#
# Usage:
#   scripts/run-perf-tests.sh [--scenario ...] [--scale 1k|10k|100k|all]
#                             [--warmup SEC] [--duration SEC]
#                             [--concurrencies "1 4 16"] [--csv PATH] [--embedded]
#
#   docker (default): --scenario crud|crud-delete|jpgql|fpl|all   [--scale ...]
#   --embedded:       --scenario crud-read|crud-update|crud-delete|all
#                     (--scale / --warmup / --duration do not apply; sizing is
#                      via PERF_READ_N / PERF_UPDATE_N / PERF_DELETE_N env vars)
#
# Defaults: --scenario all  --scale 10k  --warmup 15  --duration 30
#
# All results land in one CSV under tests/perf/_results/ (override with --csv).
# Embedded results go to <csv>-embedded.csv.
#
set -uo pipefail
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"   # absolute self-path, captured BEFORE the cd below
cd "$(dirname "$0")/.."

SCENARIO="all"
SCALE="10k"
WARMUP=15
DURATION=30
CONCURRENCIES="1 4 16"
CSV=""
EMBEDDED=0

while [ $# -gt 0 ]; do
  case "$1" in
    --scenario)      SCENARIO="$2"; shift 2 ;;
    --scale)         SCALE="$2"; shift 2 ;;
    --warmup)        WARMUP="$2"; shift 2 ;;
    --duration)      DURATION="$2"; shift 2 ;;
    --concurrencies) CONCURRENCIES="$2"; shift 2 ;;
    --csv)           CSV="$2"; shift 2 ;;
    --embedded)      EMBEDDED=1; shift ;;
    -h|--help)       sed -n '2,35p' "$SELF"; exit 0 ;;
    *) echo "unknown flag: $1 (see --help)"; exit 2 ;;
  esac
done

# Resolve the per-run CSV up front so every scenario appends to the SAME file.
if [ -z "$CSV" ]; then
  mkdir -p tests/perf/_results
  CSV="$(pwd)/tests/perf/_results/perf-$(date -u +%Y%m%dT%H%M%SZ).csv"
fi
export PERF_CSV="$CSV"
export PERF_WARMUP_SEC="$WARMUP"
export PERF_DURATION_SEC="$DURATION"
export PERF_CONCURRENCIES="$CONCURRENCIES"
export PERF_GIT_SHA="${PERF_GIT_SHA:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
export PERF_HOST="${PERF_HOST:-$(hostname 2>/dev/null || echo unknown)}"

# Lines the embedded harness emits during its EMERGENCY teardown of a throwaway
# runtime — an un-drained WAL backlog failing to publish against the
# already-closed NATS conn. These are NOT test failures (see runtime.go
# Shutdown(emergency)); filter them so they don't masquerade as errors.
SHUTDOWN_NOISE='nats: connection closed|cannot publish tx|Failed to publish WAL|Failed to set KV as inconsistent|Failed to get consumer info|Shutting down\.\.\.'

if [ "$EMBEDDED" -eq 1 ]; then
  MODE="EMBEDDED — in-process Go suites, NO NATS round-trips (server-side cost)"
  case "$SCENARIO" in
    all) SCENARIOS="crud-read crud-update crud-delete" ;;
    crud-read|crud-update|crud-delete) SCENARIOS="$SCENARIO" ;;
    *) echo "unknown embedded scenario: $SCENARIO (use crud-read|crud-update|crud-delete|all)"; exit 2 ;;
  esac
  SCALES="(n/a — embedded sizes via PERF_*_N)"
else
  MODE="DOCKER — end-to-end over NATS, round-trip latency/throughput"
  case "$SCALE" in
    all) SCALES="1k 10k 100k" ;;
    1k|10k|100k) SCALES="$SCALE" ;;
    *) echo "unknown scale: $SCALE (use 1k|10k|100k|all)"; exit 2 ;;
  esac
  case "$SCENARIO" in
    all) SCENARIOS="crud crud-delete" ;;   # NOTE: jpgql/fpl land in a follow-up
    crud|crud-delete|jpgql|fpl) SCENARIOS="$SCENARIO" ;;
    *) echo "unknown scenario: $SCENARIO"; exit 2 ;;
  esac
fi

echo "=================================================================="
echo "Foliage SDK perf run"
echo "  MODE          : $MODE"
echo "  scenarios     : $SCENARIOS"
echo "  scales        : $SCALES"
echo "  concurrencies : $CONCURRENCIES"
echo "  csv           : $PERF_CSV"
echo "=================================================================="

fail=0

if [ "$EMBEDDED" -eq 1 ]; then
  export PERF_EMBEDDED_CSV="${PERF_CSV%.csv}-embedded.csv"
  echo ">> embedded csv: $PERF_EMBEDDED_CSV"
  embedded_suite_for() {
    case "$1" in
      crud-read)   echo "TestCRUDReadPerfTestSuite" ;;
      crud-update) echo "TestCRUDUpdatePerfTestSuite" ;;
      crud-delete) echo "TestCRUDDeletePerfTestSuite" ;;
      *) echo "" ;;
    esac
  }
  for s in $SCENARIOS; do
    suite="$(embedded_suite_for "$s")"
    echo "------------------------------------------------------------------"
    echo ">> perf[embedded]: $s  (in-process — NO NATS round-trips)"
    # Filter the emergency-teardown WAL noise; check go test's real exit via
    # PIPESTATUS so grep's status (1 if it filtered everything) can't mask it.
    go test -tags perf -count=1 -timeout 1800s -v -run "$suite" ./embedded/graph/crud/ 2>&1 \
      | grep -vE "$SHUTDOWN_NOISE"
    if [ "${PIPESTATUS[0]}" -eq 0 ]; then
      echo ">> $s [embedded]: OK"
    else
      echo ">> $s [embedded]: FAIL"; fail=1
    fi
  done
else
  for s in $SCENARIOS; do
    run="tests/perf/$s/run.sh"
    if [ ! -x "$run" ]; then
      echo ">> WARN: $run missing or not executable — skipping"
      continue
    fi
    for sc in $SCALES; do
      echo "------------------------------------------------------------------"
      echo ">> perf[docker]: scenario=$s scale=$sc"
      if bash "$run" "$sc"; then
        echo ">> $s [$sc]: OK"
      else
        echo ">> $s [$sc]: FAIL"; fail=1
      fi
    done
  done
fi

echo "=================================================================="
echo ">> results CSV: $PERF_CSV"
[ "$EMBEDDED" -eq 1 ] && echo ">> embedded CSV: $PERF_EMBEDDED_CSV"
if [ -f "$PERF_CSV" ]; then
  rows=$(($(wc -l < "$PERF_CSV") - 1))
  echo ">> rows recorded (docker CSV): $rows"
fi
if [ "$fail" -eq 0 ]; then
  echo "PERF RUN: completed"
else
  echo "PERF RUN: some scenarios failed"
fi
echo "=================================================================="
exit "$fail"
