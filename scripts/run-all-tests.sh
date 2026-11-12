#!/usr/bin/env bash
#
# run-all-tests.sh — full Foliage SDK test run.
#
# Phase 1 (now):     Go tests (unit / component / integration) via `go test`.
# Phase 1b:          performance gates (-tags perf), never under the race
#                    detector: the cache hub throughput gate. Report only:
#                    its verdict (PASS, FAIL or INCONCLUSIVE) does not fail
#                    this run.
# Phase 2 (planned): docker-compose system tests, executed one by one.
#
# See docs/TESTING.md for the full picture, the test map and the roadmap.
#
# Usage:
#   scripts/run-all-tests.sh [--race] [--coverage] [--quick] [--go-only]
#                            [--system-only] [--cache-mode MODE]
#
#   --race        race-check Go tests and containerized recovery regressions
#   --coverage    write a merged coverage profile (coverage.out) over ./...
#   --quick       run Go packages in parallel (fast, but embedded-NATS suites
#                 may flake under contention; default is serial for reliability)
#   --go-only     skip the system-test phase
#   --system-only skip the Go-test phase
#   --cache-mode  which representation the cache holds the graph in for this
#                 run: tree, records, zstd or zstd-dict. Defaults to whatever
#                 the SDK ships as its default, and applies to everything the
#                 run starts, containers included.
#
set -uo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"   # absolute self-path, captured BEFORE the cd below
cd "$(dirname "$0")/.."

RACE=""
COVER=""
PARALLEL="-p 1"      # serial packages by default: embedded-NATS suites contend in parallel
GO_ONLY=0
SYSTEM_ONLY=0
CACHE_MODE_ARG=""

ARGS=("$@")
for i in "${!ARGS[@]}"; do
  arg="${ARGS[$i]}"
  case "$arg" in
    --race)        RACE="-race" ;;
    --coverage)    COVER="-coverprofile=coverage.out -coverpkg=./..." ;;
    --quick)       PARALLEL="" ;;
    --go-only)     GO_ONLY=1 ;;
    --system-only) SYSTEM_ONLY=1 ;;
    --cache-mode)  CACHE_MODE_ARG="${ARGS[$((i+1))]:-}" ;;
    --cache-mode=*) CACHE_MODE_ARG="${arg#*=}" ;;
    -h|--help)     sed -n '2,27p' "$SELF"; exit 0 ;;
    *)
      # the value of --cache-mode, already consumed above
      [ "$i" -gt 0 ] && [ "${ARGS[$((i-1))]}" = "--cache-mode" ] && continue
      echo "unknown flag: $arg (see --help)"; exit 2 ;;
  esac
done

# shellcheck source=_lib/cache-mode.sh
source "$(dirname "$SELF")/_lib/cache-mode.sh"
cache_mode_resolve "$CACHE_MODE_ARG" || exit 2
cache_mode_banner

# Propagate race mode to system fixtures which compile Go regression tests.
SDK_TEST_RACE=0
[ -n "$RACE" ] && SDK_TEST_RACE=1
export SDK_TEST_RACE

# NATS 2.10's macOS sysmem implementation fails checkptr under Go 1.27
# before SDK race tests start. Keep checks enabled and run that phase in Linux.
run_go_tests() {
  if [ -n "$RACE" ] && [ "$(uname -s)" = Darwin ]; then
    source scripts/_lib/linux-go-test.sh
    SDK_TEST_DOCKER_ACCESS=1 SDK_TEST_SOURCE_MODE=rw sdk_go_test_in_linux "$@"
  else
    go test "$@"
  fi
}

fail=0

# sweep_systest_leftovers removes containers/volumes surviving an interrupted
# prior run (or a manual run.sh). System tests need it for the host ports they
# bind; the GO phase needs it too — a leftover NATS+JetStream container is
# steady background load that skews timing-sensitive integration tests
# (observed: a stray backup-restore nats container flaking the statefun
# export-committer test). Covers ALL our compose project families — systest,
# soak (tests/soak/*) and perf (tests/perf/*) — they bind the same host ports
# (observed: a leftover foliage-soak nats holding :4222 failed every system
# test with "port is already allocated").
sweep_systest_leftovers() {
  if command -v docker >/dev/null 2>&1; then
    echo ">> sweeping leftover foliage test containers/volumes (systest/soak/perf)"
    for prefix in foliage-systest- foliage-soak- foliage-perf-; do
      docker ps -aq --filter "name=${prefix}" | xargs -r docker rm -f >/dev/null 2>&1 || true
      docker volume ls -q --filter "name=${prefix}" | xargs -r docker volume rm -f >/dev/null 2>&1 || true
    done
  fi
}

# -----------------------------------------------------------------------------
# Phase 1 — Go tests
# -----------------------------------------------------------------------------
if [ "$SYSTEM_ONLY" -eq 0 ]; then
  echo "=================================================================="
  echo "Phase 1: Go tests  (CACHE_MODE=${CACHE_MODE} go test ${PARALLEL} -count=1 ${RACE} ${COVER} ./...)"
  echo "=================================================================="
  sweep_systest_leftovers
  # shellcheck disable=SC2086
  if run_go_tests ${PARALLEL} -count=1 ${RACE} ${COVER} ./...; then
    echo ">> Go tests: PASS"
  else
    echo ">> Go tests: FAIL"
    fail=1
  fi
  if [ -n "$COVER" ] && [ -f coverage.out ]; then
    echo ">> Coverage summary:"
    go tool cover -func=coverage.out | tail -n 1
  fi

  # A wall-clock comparison is a measurement, not an invariant: it runs on its
  # own, without -race (the detector's overhead says nothing about the shipped
  # code) and without the other packages competing for the CPU. The gate drops
  # pairs measured while the machine changed and says INCONCLUSIVE rather than
  # blaming the code when too few pairs are left.
  #
  # Here its verdict is reported and does not fail the run: on a shared or
  # loaded runner it describes the machine as much as the code, and a
  # functional run must not turn red over it. scripts/run-perf-tests.sh --gate
  # still fails on it.
  #
  # go test reports INCONCLUSIVE as a failing test, so that the strict run does
  # not pass on a result it could not measure. Here it is named for what it is:
  # the machine changed speed within too many pairs, which is no verdict on the
  # code either way.
  echo "=================================================================="
  echo "Phase 1b: performance gates  (go test -tags perf -run '^Test_Hub_ConcurrentThroughputGate\$' ./statefun/cache — no race detector, report only)"
  echo "=================================================================="
  gate_log="$(mktemp)"
  go test -tags perf -count=1 -v -run '^Test_Hub_ConcurrentThroughputGate$' ./statefun/cache 2>&1 | tee "$gate_log"
  gate_rc=${PIPESTATUS[0]}
  if [ "$gate_rc" -eq 0 ]; then
    echo ">> performance gates: PASS"
  elif grep -q 'INCONCLUSIVE:' "$gate_log"; then
    echo ">> performance gates: INCONCLUSIVE (the machine changed speed within too many pairs to judge the code; report only, does not fail this run; see docs/TESTING.md \"Cache hub throughput gate\")"
  else
    echo ">> performance gates: FAIL (report only, does not fail this run; see docs/TESTING.md \"Cache hub throughput gate\")"
  fi
  rm -f "$gate_log"
fi

# -----------------------------------------------------------------------------
# Phase 2 — docker-compose system tests (planned)
#
# Convention (target): each system test lives in tests/system/<name>/ and exposes
# an executable run.sh that brings its docker-compose project up, asserts, and
# tears it down (exit 0 == pass). They run sequentially so they do not contend.
# -----------------------------------------------------------------------------
if [ "$GO_ONLY" -eq 0 ]; then
  echo "=================================================================="
  echo "Phase 2: system tests (docker-compose, CACHE_MODE=${CACHE_MODE})"
  echo "=================================================================="
  # Every system test binds the same host ports (e.g. NATS monitoring :8222), so
  # they must run one at a time on a clean slate: a leftover container keeps
  # holding those ports and makes the next `docker compose up` fail with "port
  # is already allocated" (and can be silently reused with stale data).
  sweep_systest_leftovers

  SYS_DIR="tests/system"
  if [ -d "$SYS_DIR" ] && compgen -G "$SYS_DIR/*/run.sh" > /dev/null; then
    for run in "$SYS_DIR"/*/run.sh; do
      name="$(basename "$(dirname "$run")")"
      echo "------------------------------------------------------------------"
      echo ">> system test: ${name}"
      if bash "$run"; then
        echo ">> ${name}: PASS"
      else
        echo ">> ${name}: FAIL"
        fail=1
      fi
    done
  else
    echo ">> No system tests present yet (${SYS_DIR}/*/run.sh)."
    echo ">> See docs/TESTING.md \"Roadmap\" for the planned system-test map."
  fi
fi

echo "=================================================================="
if [ "$fail" -eq 0 ]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
echo "=================================================================="
exit "$fail"
