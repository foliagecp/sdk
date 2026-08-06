#!/usr/bin/env bash
#
# run-all-tests.sh — full Foliage SDK test run.
#
# Phase 1 (now):     Go tests (unit / component / integration) via `go test`.
# Phase 2 (planned): docker-compose system tests, executed one by one.
#
# See docs/TESTING.md for the full picture, the test map and the roadmap.
#
# Usage:
#   scripts/run-all-tests.sh [--race] [--coverage] [--quick] [--go-only] [--system-only]
#
#   --race        run Go tests under the race detector (slower)
#   --coverage    write a merged coverage profile (coverage.out) over ./...
#   --quick       run Go packages in parallel (fast, but embedded-NATS suites
#                 may flake under contention; default is serial for reliability)
#   --go-only     skip the system-test phase
#   --system-only skip the Go-test phase
#
set -uo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"   # absolute self-path, captured BEFORE the cd below
cd "$(dirname "$0")/.."

RACE=""
COVER=""
PARALLEL="-p 1"      # serial packages by default: embedded-NATS suites contend in parallel
GO_ONLY=0
SYSTEM_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --race)        RACE="-race" ;;
    --coverage)    COVER="-coverprofile=coverage.out -coverpkg=./..." ;;
    --quick)       PARALLEL="" ;;
    --go-only)     GO_ONLY=1 ;;
    --system-only) SYSTEM_ONLY=1 ;;
    -h|--help)     sed -n '2,20p' "$SELF"; exit 0 ;;
    *) echo "unknown flag: $arg (see --help)"; exit 2 ;;
  esac
done

fail=0

# sweep_systest_leftovers removes containers/volumes surviving an interrupted
# prior run (or a manual run.sh). System tests need it for the host ports they
# bind; the GO phase needs it too — a leftover NATS+JetStream container is
# steady background load that skews timing-sensitive integration tests
# (observed: a stray backup-restore nats container flaking the statefun
# export-committer test).
sweep_systest_leftovers() {
  if command -v docker >/dev/null 2>&1; then
    echo ">> sweeping leftover foliage-systest containers/volumes"
    docker ps -aq --filter 'name=foliage-systest-' | xargs -r docker rm -f >/dev/null 2>&1 || true
    docker volume ls -q --filter 'name=foliage-systest-' | xargs -r docker volume rm -f >/dev/null 2>&1 || true
  fi
}

# -----------------------------------------------------------------------------
# Phase 1 — Go tests
# -----------------------------------------------------------------------------
if [ "$SYSTEM_ONLY" -eq 0 ]; then
  echo "=================================================================="
  echo "Phase 1: Go tests  (go test ${PARALLEL} -count=1 ${RACE} ${COVER} ./...)"
  echo "=================================================================="
  sweep_systest_leftovers
  # shellcheck disable=SC2086
  if go test ${PARALLEL} -count=1 ${RACE} ${COVER} ./...; then
    echo ">> Go tests: PASS"
  else
    echo ">> Go tests: FAIL"
    fail=1
  fi
  if [ -n "$COVER" ] && [ -f coverage.out ]; then
    echo ">> Coverage summary:"
    go tool cover -func=coverage.out | tail -n 1
  fi
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
  echo "Phase 2: system tests (docker-compose)"
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
