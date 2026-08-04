#!/usr/bin/env bash
# run-leak-tests.sh — memory-leak hunting suite for the Foliage SDK.
#
# Usage:
#   scripts/run-leak-tests.sh [--mode quick|full|soak] [--scenario NAME] [--results DIR] [--race]
#
#   --mode quick     W=2 M=8 cycles, small workloads (default; smoke, ~15-20 min for 'all')
#   --mode full      W=5 M=20 cycles, 3x workloads, tighter floors — the "3-sigma claim" run
#   --mode soak      dispatch to scripts/run-soak-tests.sh --scenario leak-hunt (docker)
#   --scenario NAME  s0..s12, 'core' (s0 s1 s2 s5 s9), or 'all' (default)
#   --results DIR    artifacts root (default tests/leak/_results/leak-<UTC>/)
#   --race           run the Go suite under the race detector
#
# Env knobs (override mode presets): LEAK_WARMUP, LEAK_CYCLES, LEAK_SCALE,
# LEAK_FLOOR_HEAP_BYTES, LEAK_FLOOR_HEAP_OBJECTS.
#
# Every check prints one LEAKCHECK line; the summary table below aggregates
# them. All known findings are fixed and their probes are green regression
# guards (see tests/leak/README.md) — ANY red check is a leak. Exit code:
# 0 = suite passed, 1 = failures, 2 = usage.
set -uo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"   # absolute self-path, captured BEFORE the cd below
cd "$(dirname "$0")/.."

MODE="quick"
SCENARIO="all"
RESULTS=""
RACE=""
SOAK_ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --mode)     MODE="${2:-}"; shift 2 ;;
    --scenario) SCENARIO="${2:-}"; shift 2 ;;
    --results)  RESULTS="${2:-}"; shift 2 ;;
    --race)     RACE="-race"; shift ;;
    --duration-min) SOAK_ARGS+=("--duration-min" "${2:-}"); shift 2 ;;
    -h|--help)  sed -n '2,20p' "$SELF"; exit 0 ;;
    *) echo "unknown flag: $1 (see --help)"; exit 2 ;;
  esac
done

# ----------------------------------------------------------------------------
# soak mode: reuse the existing docker scenario, do not duplicate it
# ----------------------------------------------------------------------------
if [ "$MODE" = "soak" ]; then
  echo "Dispatching to the docker leak-hunt soak scenario..."
  echo "(container heap dumps need PPROF_ADDR support in the runtime binary)"
  exec scripts/run-soak-tests.sh --scenario leak-hunt ${SOAK_ARGS[@]+"${SOAK_ARGS[@]}"}
fi

# ----------------------------------------------------------------------------
# mode presets (explicit env always wins)
# ----------------------------------------------------------------------------
case "$MODE" in
  quick)
    : "${LEAK_WARMUP:=2}"; : "${LEAK_CYCLES:=8}"; : "${LEAK_SCALE:=1}"
    : "${LEAK_FLOOR_HEAP_BYTES:=65536}"; : "${LEAK_FLOOR_HEAP_OBJECTS:=500}"
    TIMEOUT="3600s"
    ;;
  full)
    : "${LEAK_WARMUP:=5}"; : "${LEAK_CYCLES:=20}"; : "${LEAK_SCALE:=3}"
    : "${LEAK_FLOOR_HEAP_BYTES:=16384}"; : "${LEAK_FLOOR_HEAP_OBJECTS:=200}"
    TIMEOUT="10800s"
    ;;
  *) echo "unknown mode: $MODE (quick|full|soak)"; exit 2 ;;
esac
export LEAK_WARMUP LEAK_CYCLES LEAK_SCALE LEAK_FLOOR_HEAP_BYTES LEAK_FLOOR_HEAP_OBJECTS

# ----------------------------------------------------------------------------
# scenario -> -run regex
# ----------------------------------------------------------------------------
rx_for() {
  case "$1" in
    s0)  echo '^TestS0' ;;
    s1)  echo '^TestS1LLCrudChurn$' ;;
    s2)  echo '^TestS2CMDBObjectChurn$' ;;
    s3)  echo '^TestS3TypeCascadeChurn$' ;;
    s4)  echo '^TestS4FunctionContexts$' ;;
    s5)  echo '^TestS5JPGQL$' ;;
    s6)  echo '^TestS6FPL$' ;;
    s7)  echo '^TestS7Batch$' ;;
    s8)  echo '^TestS8KeyMutex$' ;;
    s9)  echo '^TestS9CacheStore$' ;;
    s10) echo '^TestS10GoroutineHygiene$' ;;
    s11) echo '^TestS11ExportSessions$' ;;
    s12) echo '^TestS12KVGrowthReport$' ;;
    s13) echo '^TestS13SaltedHLChurn$' ;;
    core) echo '^(TestS0PlantedLeakIsFlagged|TestS0ControlIsClean|TestS1LLCrudChurn|TestS2CMDBObjectChurn|TestS5JPGQL|TestS9CacheStore)$' ;;
    all) echo '^TestS[0-9]' ;;
    *) return 1 ;;
  esac
}
# Scenario list: EACH scenario runs in its OWN go test process. All scenarios
# in one process would share heap, timers and whatever the per-scenario
# emergency teardown leaves behind — process-wide plateau shifts then land in
# whichever scenario happens to be measuring and read as false leaks. Fresh
# process per scenario = clean baselines, zero cross-contamination. The test
# binary is compiled once and reused by go test's build cache.
case "$SCENARIO" in
  all)  SCEN_LIST="s0 s1 s2 s3 s4 s5 s6 s7 s8 s9 s10 s11 s12 s13" ;;
  core) SCEN_LIST="s0 s1 s2 s5 s9" ;;
  *)    rx_for "$SCENARIO" >/dev/null || { echo "unknown scenario: $SCENARIO (s0..s12|core|all)"; exit 2; }
        SCEN_LIST="$SCENARIO" ;;
esac

# ----------------------------------------------------------------------------
# results dir
# ----------------------------------------------------------------------------
if [ -z "$RESULTS" ]; then
  RESULTS="$(pwd)/tests/leak/_results/leak-$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "$RESULTS"
export LEAK_RESULTS_DIR="$RESULTS"
LOG="$RESULTS/go-test.log"

# Emergency-teardown noise of the embedded harness (see run-perf-tests.sh) —
# not test failures, filtered from the live view only (the full log is kept).
SHUTDOWN_NOISE='nats: connection closed|cannot publish tx|Failed to publish WAL|Failed to set KV as inconsistent|Failed to get consumer info|Shutting down\.\.\.'

echo "=================================================================="
echo "Leak suite  mode=$MODE  scenario=$SCENARIO  (warmup=$LEAK_WARMUP cycles=$LEAK_CYCLES scale=$LEAK_SCALE)"
echo "artifacts:  $RESULTS"
echo "=================================================================="
: > "$LOG"
status=0
for sc in $SCEN_LIST; do
  RX="$(rx_for "$sc")"
  echo "---- $sc  (-run $RX) ----"
  # shellcheck disable=SC2086
  go test -tags leak $RACE -count=1 -timeout "$TIMEOUT" -v -run "$RX" ./tests/leak/ 2>&1 \
    | tee -a "$LOG" | grep -vE "$SHUTDOWN_NOISE"
  st="${PIPESTATUS[0]}"
  if [ "$st" -ne 0 ]; then
    status="$st"
  fi
done

# ----------------------------------------------------------------------------
# summary from LEAKCHECK lines
# ----------------------------------------------------------------------------
echo
echo "=================================================================="
echo "Leak-check summary"
echo "=================================================================="
awk -F'|' '
/^LEAKCHECK\|/ {
  scen=""; check=""; st=""; extra="";
  for (i = 2; i <= NF; i++) {
    n = index($i, "=");
    k = substr($i, 1, n - 1); v = substr($i, n + 1);
    if (k == "scenario") scen = v;
    else if (k == "check") check = v;
    else if (k == "status") st = v;
    else extra = extra " " $i;
  }
  printf "  %-22s %-26s %-7s%s\n", scen, check, st, extra;
  count[st]++;
}
END {
  printf "\n  PASS=%d  FAIL=%d  REPORT=%d\n", count["PASS"], count["FAIL"], count["REPORT"];
  if (count["FAIL"] == 0)
    print "  No leaks detected at the 3-sigma level within the reported detection floors.";
  else
    print "  FAIL rows above are leaks: either a regression of a fixed finding (tests/leak/README.md) or a new one.";
}' "$LOG"
echo
if [ "$status" -eq 0 ]; then
  echo ">> Leak suite: PASS"
else
  echo ">> Leak suite: FAIL (see $LOG and per-scenario artifacts in $RESULTS)"
fi
exit "$status"
