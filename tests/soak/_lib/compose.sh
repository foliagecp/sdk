#!/usr/bin/env bash
#
# compose.sh — shared helpers for the long-running endurance/soak tests under
# tests/soak/<name>/run.sh. Mirrors tests/system/_lib/compose.sh but with a few
# soak-specific differences:
#
#   * the observer binary is built once per run (Go program under
#     tests/soak/_lib/observer/) and proxied via `observe`,
#   * `dump_state` snapshots logs + goroutine dump + heap pprof on failure
#     instead of just `docker compose logs`,
#   * `wait_metrics` waits for the runtime's Prometheus endpoint instead of
#     NATS monitoring (the runtime exposes :9901 in soak compose files).
#
# Soak tests intentionally do NOT inherit run-all-tests.sh — they run for an
# hour by default and stress overnight failure modes (memory drift, goroutine
# leaks, post-NATS-stall recovery, HA flap). They are driven by the separate
# scripts/run-soak-tests.sh.
#
# Source it from a run.sh that has already set:
#
#   PROJECT       docker compose project name (keeps resources isolated)
#   COMPOSE_FILE  path to that test's docker-compose.yaml

set -uo pipefail

SOAK_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SOAK_LIB_DIR/../../.." && pwd)"
ASSERT_BIN="${ASSERT_BIN:-/tmp/foliage-soak-assert}"
OBSERVER_BIN="${OBSERVER_BIN:-/tmp/foliage-soak-observer}"

# Default duration in minutes. Individual scenarios may scale this further
# (e.g. ha-promotion-flap divides it into 10-minute kill windows).
SOAK_DURATION_MIN="${SOAK_DURATION_MIN:-60}"
SOAK_DURATION_SEC="$((SOAK_DURATION_MIN * 60))"

# Results directory: one subdirectory per scenario run, holding the observer
# CSV plus any dumped artefacts on failure. Survives the EXIT trap so the user
# can inspect what happened — soak failures are interesting and rare.
SOAK_RESULTS_ROOT="${SOAK_RESULTS_ROOT:-$REPO_ROOT/tests/soak/_results}"

# dc runs docker compose scoped to this test's project + file.
dc() { docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }

# build_assert compiles the system-tests assert client; soak runs reuse it
# (same NATS-side surface — seed/verify/soak/consistency).
build_assert() {
  echo ">> building assert client -> $ASSERT_BIN"
  ( cd "$REPO_ROOT" && go build -o "$ASSERT_BIN" ./tests/system/_lib/assert/ ) \
    || { echo ">> FAIL: could not build assert client" >&2; exit 1; }
}

# build_observer compiles the soak observer (host-side probe).
build_observer() {
  echo ">> building observer -> $OBSERVER_BIN"
  ( cd "$REPO_ROOT" && go build -o "$OBSERVER_BIN" ./tests/soak/_lib/observer/ ) \
    || { echo ">> FAIL: could not build observer" >&2; exit 1; }
}

# assert proxies to the compiled client.
assert() { "$ASSERT_BIN" "$@"; }

# observe proxies to the compiled observer.
observe() { "$OBSERVER_BIN" "$@"; }

# wait_http polls a URL until it answers or the timeout elapses.
wait_http() {
  local url="$1"
  local timeout="${2:-60}"
  local deadline=$((SECONDS + timeout))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      return 1
    fi
    sleep 1
  done
}

# results_dir creates a fresh subdirectory under SOAK_RESULTS_ROOT for this run
# and echoes the absolute path. Used by every scenario to anchor artefacts.
results_dir() {
  local stamp
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  local dir="$SOAK_RESULTS_ROOT/${PROJECT#foliage-soak-}-$stamp"
  mkdir -p "$dir"
  echo "$dir"
}

# dump_state captures logs + goroutine + heap pprof from every runtime service
# into $1 (a directory). Best-effort — missing pprof endpoints are tolerated
# (the soak compose files all expose :6060 via PPROF_ADDR, but a runtime might
# be paused/down).
dump_state() {
  local out="$1"
  mkdir -p "$out"
  echo ">> dumping state -> $out"
  dc logs --no-color >"$out/compose.log" 2>&1 || true
  dc ps -a >"$out/ps.txt" 2>&1 || true
  # The compose files for soak tests publish pprof on the host as :6060 (single
  # runtime) or :6060/:6061/:6062 (HA). Probe each in turn.
  for port in 6060 6061 6062; do
    if curl -fsS --max-time 5 "http://localhost:$port/debug/pprof/goroutine?debug=2" >"$out/goroutine-$port.txt" 2>/dev/null; then
      curl -fsS --max-time 10 "http://localhost:$port/debug/pprof/heap" >"$out/heap-$port.pprof" 2>/dev/null || true
    else
      rm -f "$out/goroutine-$port.txt"
    fi
  done
  for port in 9901 9902 9903; do
    curl -fsS --max-time 5 "http://localhost:$port/metrics" >"$out/metrics-$port.txt" 2>/dev/null || true
    [ -s "$out/metrics-$port.txt" ] || rm -f "$out/metrics-$port.txt"
  done
}

# fail captures state then exits non-zero. The EXIT trap still tears down.
fail() {
  echo ">> FAIL: $*" >&2
  if [ -n "${RUN_DIR:-}" ]; then
    dump_state "$RUN_DIR/dump-failure"
    echo ">> artefacts written to $RUN_DIR" >&2
  else
    dc logs --no-color >&2 2>&1 || true
  fi
  exit 1
}

# cleanup removes the project's containers and volumes.
cleanup() { dc down -v --remove-orphans >/dev/null 2>&1 || true; }

# install_trap wires cleanup to run on any exit (pass or fail) AND pre-cleans
# this project up front. See tests/system/_lib/compose.sh for the rationale.
install_trap() {
  trap cleanup EXIT INT TERM
  cleanup
}
