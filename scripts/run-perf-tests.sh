#!/usr/bin/env bash
#
# run-perf-tests.sh — performance-measurement runner for Foliage SDK.
#
# Intentionally separate from run-all-tests.sh: perf tests are long and noisy
# on shared hardware, and they produce numbers to track over time, not
# pass/fail signals to gate on. See tests/perf/README.md.
#
# Usage:
#   scripts/run-perf-tests.sh [--scenario crud|jpgql|fpl|all] [--scale 1k|10k|100k|all]
#                             [--warmup SEC] [--duration SEC] [--concurrencies "1 4 16"]
#                             [--csv PATH]
#
# Defaults: --scenario all  --scale 10k  --warmup 15  --duration 30
#
# All results land in a single CSV (one row per measurement) under
# tests/perf/_results/ with timestamp+host+CPU+git_sha metadata so runs can
# be compared. Override the file with --csv.
#
set -uo pipefail
cd "$(dirname "$0")/.."

SCENARIO="all"
SCALE="10k"
WARMUP=15
DURATION=30
CONCURRENCIES="1 4 16"
CSV=""

while [ $# -gt 0 ]; do
  case "$1" in
    --scenario)      SCENARIO="$2"; shift 2 ;;
    --scale)         SCALE="$2"; shift 2 ;;
    --warmup)        WARMUP="$2"; shift 2 ;;
    --duration)      DURATION="$2"; shift 2 ;;
    --concurrencies) CONCURRENCIES="$2"; shift 2 ;;
    --csv)           CSV="$2"; shift 2 ;;
    -h|--help)       sed -n '2,20p' "$0"; exit 0 ;;
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

# Expand scale + scenario sets.
case "$SCALE" in
  all)      SCALES="1k 10k 100k" ;;
  1k|10k|100k) SCALES="$SCALE" ;;
  *) echo "unknown scale: $SCALE (use 1k|10k|100k|all)"; exit 2 ;;
esac
case "$SCENARIO" in
  all)  SCENARIOS="crud" ;;     # NOTE: jpgql/fpl land in a follow-up
  crud|jpgql|fpl) SCENARIOS="$SCENARIO" ;;
  *) echo "unknown scenario: $SCENARIO"; exit 2 ;;
esac

echo "=================================================================="
echo "Foliage SDK perf run"
echo "  scenarios     : $SCENARIOS"
echo "  scales        : $SCALES"
echo "  warmup        : ${WARMUP}s"
echo "  duration      : ${DURATION}s"
echo "  concurrencies : $CONCURRENCIES"
echo "  csv           : $PERF_CSV"
echo "=================================================================="

fail=0
for s in $SCENARIOS; do
  run="tests/perf/$s/run.sh"
  if [ ! -x "$run" ]; then
    echo ">> WARN: $run missing or not executable — skipping"
    continue
  fi
  for sc in $SCALES; do
    echo "------------------------------------------------------------------"
    echo ">> perf: scenario=$s scale=$sc"
    if bash "$run" "$sc"; then
      echo ">> $s [$sc]: OK"
    else
      echo ">> $s [$sc]: FAIL"
      fail=1
    fi
  done
done

echo "=================================================================="
echo ">> results CSV: $PERF_CSV"
if [ -f "$PERF_CSV" ]; then
  rows=$(($(wc -l < "$PERF_CSV") - 1))
  echo ">> rows recorded: $rows"
fi
if [ "$fail" -eq 0 ]; then
  echo "PERF RUN: completed"
else
  echo "PERF RUN: some scenarios failed"
fi
echo "=================================================================="
exit "$fail"
