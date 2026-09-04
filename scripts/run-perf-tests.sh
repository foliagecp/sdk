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
#   --gate            run the embedded scenarios under BOTH cache modes and
#                     compare them: records must not fall below --floor of the
#                     tree's throughput on any scenario. Implies --embedded.
#                     Repeats each mode --repeats times and compares the BEST of
#                     them, because a single run is not a measurement: the same
#                     scenario varies by half between repeats on an idle
#                     machine, which is enough to invent a regression that is
#                     not there.
#
#   docker (default): --scenario crud|crud-delete|jpgql|fpl|all   [--scale ...]
#   --embedded:       --scenario crud-read|crud-update|crud-delete|trashcan-scale|link-resolve|all
#                     (--scale / --warmup / --duration do not apply; sizing is
#                      via PERF_READ_N / PERF_UPDATE_N / PERF_DELETE_N env vars,
#                      trashcan-scale via PERF_TC_BATCH / PERF_TC_BIN_SIZES,
#                      link-resolve via PERF_LR_BATCH / PERF_LR_FANOUTS)
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
GATE=0
REPEATS=5
FLOOR=0.8

while [ $# -gt 0 ]; do
  case "$1" in
    --scenario)      SCENARIO="$2"; shift 2 ;;
    --scale)         SCALE="$2"; shift 2 ;;
    --warmup)        WARMUP="$2"; shift 2 ;;
    --duration)      DURATION="$2"; shift 2 ;;
    --concurrencies) CONCURRENCIES="$2"; shift 2 ;;
    --csv)           CSV="$2"; shift 2 ;;
    --embedded)      EMBEDDED=1; shift ;;
    --gate)          GATE=1; EMBEDDED=1; shift ;;
    --repeats)       REPEATS="$2"; shift 2 ;;
    --floor)         FLOOR="$2"; shift 2 ;;
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
    all) SCENARIOS="crud-read crud-update crud-delete trashcan-scale link-resolve" ;;
    crud-read|crud-update|crud-delete|trashcan-scale|link-resolve) SCENARIOS="$SCENARIO" ;;
    *) echo "unknown embedded scenario: $SCENARIO (use crud-read|crud-update|crud-delete|trashcan-scale|link-resolve|all)"; exit 2 ;;
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
      # Cost curve against the size of the trash can: parking and restoring must
      # not track it (they used to — retention listed the bin on every delete).
      trashcan-scale) echo "TestTrashCanScalePerfTestSuite" ;;
      # Cost of a link update/delete against the out-degree of its from-vertex:
      # resolution by name must keep it flat where the search made it linear.
      link-resolve)   echo "TestLinkResolveScalePerfTestSuite" ;;
      *) echo "" ;;
    esac
  }
  if [ "$GATE" -eq 1 ]; then
    echo ">> gate: modes tree/records, $REPEATS repeats each, floor $FLOOR"
    # Modes are interleaved inside each repeat, not run one after the other:
    # a machine that drifts — thermal, a background build — would otherwise
    # charge the whole drift to whichever mode ran second.
    for rep in $(seq 1 "$REPEATS"); do
      for mode in tree records; do
        for s in $SCENARIOS; do
          suite="$(embedded_suite_for "$s")"
          echo ">> gate[$mode #$rep]: $s"
          CACHE_MODE="$mode" go test -tags perf -count=1 -timeout 1800s -v -run "$suite" ./embedded/graph/crud/ 2>&1 \
            | grep -vE "$SHUTDOWN_NOISE" | grep -E "^ +.*mode=" || true
          if [ "${PIPESTATUS[0]}" -ne 0 ]; then
            echo ">> gate[$mode #$rep]: $s FAILED TO RUN"; fail=1
          fi
        done
      done
    done

    echo "=================================================================="
    echo ">> gate comparison (best of $REPEATS per mode)"
    awk -F, -v floor="$FLOOR" '
      NR == 1 { next }
      {
        key = $4 "/" $5 " conc=" $6 " deg=" $8
        mode = $3
        thr = $12 + 0
        if (!((mode, key) in best) || thr > best[mode, key]) best[mode, key] = thr
        if (!((mode, key) in worst) || thr < worst[mode, key]) worst[mode, key] = thr
        seen[key] = 1
      }
      END {
        printf "%-52s %10s %10s %7s %8s\n", "scenario", "tree", "records", "ratio", "spread"
        bad = 0
        for (k in seen) {
          t = best["tree", k]; r = best["records", k]
          if (t <= 0 || r <= 0) continue
          ratio = r / t
          # How much the same mode varied between its own repeats. A ratio is
          # only worth reading when this is small: the embedded scenarios swing
          # by half on an idle laptop, which is enough to invent a regression.
          st = (worst["tree", k] > 0) ? best["tree", k] / worst["tree", k] : 0
          sr = (worst["records", k] > 0) ? best["records", k] / worst["records", k] : 0
          spread = (st > sr) ? st : sr
          mark = ""
          if (ratio < floor) {
            if (spread > 1 / floor) { mark = "  (noisy: spread exceeds the gap)" }
            else { mark = "  <-- BELOW FLOOR"; bad++ }
          }
          printf "%-52s %10.0f %10.0f %7.2f %8.2f%s\n", k, t, r, ratio, spread, mark
        }
        if (bad > 0) { printf "\ngate: %d scenario(s) below floor %.2f\n", bad, floor; exit 1 }
        printf "\ngate: nothing below floor %.2f that the noise cannot explain\n", floor
      }' "$PERF_EMBEDDED_CSV" || fail=1
    echo "=================================================================="
  else
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
  fi
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
