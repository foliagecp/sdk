#!/usr/bin/env bash
#
# compose.sh — shared helpers for the docker-compose perf scenarios under
# tests/perf/<name>/run.sh. Extends tests/system/_lib/compose.sh with:
#
#   - build_perfclient    — compile the perf CLI binary
#   - record_host_info    — capture git SHA / host / CPU / mem / OS into env
#                            vars consumed by perfclient (CSV row metadata)
#   - init_perf_csv       — make sure the per-run results CSV exists
#
# Each run.sh sources THIS file; it in turn sources the system harness, so
# every helper available to system tests (dc, wait_http, fail, cleanup,
# install_trap, assert) is available to perf tests too.

set -uo pipefail

PERF_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Re-use the system harness for dc / build_assert / wait_http / fail /
# cleanup / install_trap. REPO_ROOT and ASSERT_BIN are exported from there.
# shellcheck source=../../system/_lib/compose.sh
source "$PERF_LIB_DIR/../../system/_lib/compose.sh"

PERFCLIENT_BIN="${PERFCLIENT_BIN:-/tmp/foliage-perfclient}"
PERF_RESULTS_DIR="${PERF_RESULTS_DIR:-$REPO_ROOT/tests/perf/_results}"
# A single CSV per top-level run (perf runner sets PERF_CSV up front so all
# scenarios append to the same file). If it's not set, default to a
# timestamped file in _results/.
PERF_CSV="${PERF_CSV:-$PERF_RESULTS_DIR/perf-$(date -u +%Y%m%dT%H%M%SZ).csv}"
export PERF_CSV

build_perfclient() {
  echo ">> building perf client -> $PERFCLIENT_BIN"
  ( cd "$REPO_ROOT" && go build -o "$PERFCLIENT_BIN" ./tests/perf/_lib/perfclient/ ) \
    || { echo ">> FAIL: could not build perf client" >&2; exit 1; }
}
perfclient() { "$PERFCLIENT_BIN" "$@"; }

# record_host_info captures a fingerprint of the machine running the perf
# tests and exports it as env vars that perfclient writes into every CSV
# row. Without this, a CSV from yesterday on a laptop and one from today on
# a CI runner look the same — and they are not.
record_host_info() {
  PERF_GIT_SHA="$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  PERF_HOST="$(hostname)"
  PERF_OS="$(uname -s)"
  if [ "$PERF_OS" = "Darwin" ]; then
    PERF_CPU="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
    PERF_MEM_GB="$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 ))"
  else
    PERF_CPU="$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | sed 's/.*: //' || echo unknown)"
    local kb; kb="$(grep -m1 MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}')"
    PERF_MEM_GB="$(( ${kb:-0} / 1024 / 1024 ))"
  fi
  export PERF_GIT_SHA PERF_HOST PERF_OS PERF_CPU PERF_MEM_GB
  echo ">> host: $PERF_HOST ($PERF_OS, $PERF_CPU, ${PERF_MEM_GB}GB), git=$PERF_GIT_SHA"
}

init_perf_csv() {
  mkdir -p "$(dirname "$PERF_CSV")"
  # File is created lazily by perfclient on first append; nothing to do here
  # beyond ensuring the directory exists. We still announce the path so the
  # user knows where results are going.
  echo ">> results csv: $PERF_CSV"
}
