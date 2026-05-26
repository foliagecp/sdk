#!/usr/bin/env bash
#
# compose.sh — shared helpers for the docker-compose system tests under
# tests/system/<name>/run.sh. Source it from a run.sh that has already set:
#
#   PROJECT       docker compose project name (keeps resources isolated)
#   COMPOSE_FILE  path to that test's docker-compose.yaml
#
# It provides a thin `dc` wrapper, a one-time assert-client build, health-wait
# helpers, and a teardown trap so every test cleans up its volumes/containers
# regardless of outcome (and dumps logs on failure for debugging).

set -uo pipefail

SYS_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SYS_LIB_DIR/../../.." && pwd)"
ASSERT_BIN="${ASSERT_BIN:-/tmp/foliage-systest-assert}"

# dc runs docker compose scoped to this test's project + file.
dc() { docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }

# build_assert compiles the shared assertion client once per run.
build_assert() {
  echo ">> building assert client -> $ASSERT_BIN"
  ( cd "$REPO_ROOT" && go build -o "$ASSERT_BIN" ./tests/system/_lib/assert/ ) \
    || { echo ">> FAIL: could not build assert client" >&2; exit 1; }
}

# assert proxies to the compiled client (host -> exposed NATS port).
assert() { "$ASSERT_BIN" "$@"; }

# wait_http polls a URL until it answers or the timeout elapses.
#   wait_http <url> [timeout_sec]
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

# container_exit_code prints the exit code of a (stopped) compose service's
# container. Used to distinguish a clean SIGTERM exit (0) from a SIGKILL (137).
container_exit_code() {
  local svc="$1" cid
  cid="$(dc ps -aq "$svc" | head -n1)"
  [ -n "$cid" ] || { echo "no-container"; return 1; }
  docker inspect -f '{{.State.ExitCode}}' "$cid"
}

# fail dumps logs and exits non-zero. The EXIT trap still tears everything down.
fail() {
  echo ">> FAIL: $*" >&2
  echo ">> ---- docker compose logs ----" >&2
  dc logs --no-color >&2 2>&1 || true
  exit 1
}

# cleanup always removes the project's containers and volumes.
cleanup() { dc down -v --remove-orphans >/dev/null 2>&1 || true; }

# install_trap wires cleanup to run on any exit (pass or fail) AND pre-cleans
# this project up front. A prior run that was interrupted (Ctrl-C) or crashed
# can leave this project's NATS container running on a persisted volume; docker
# compose would then silently reuse it, so the test sees stale data (e.g.
# "vertex systest_node already exists") instead of a clean slate. Tearing the
# project down -v before `up` guarantees a fresh start every time.
install_trap() {
  trap cleanup EXIT INT TERM
  cleanup
}
