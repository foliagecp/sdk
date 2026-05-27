#!/usr/bin/env bash
#
# leak-hunt — reproduce cache-tombstone accumulation locally.
#
# Why this exists: tests/soak/steady-state-1h passes because it loops a
# bounded pool of object IDs — every Update hits the SAME node, so there is
# no fresh tombstone creation in the cache tree. Production stand 116
# reports heap_objects climbing 8M → 130M in 4h on a graph whose KV stays
# at ~258k entries. The hypothesis (cache-tombstone cascade not collapsing
# under load) needs a local reproducer, which is what this scenario is.
#
# Workload (assert leakhunt):
#   * 32 workers, every iteration creates a NEW unique vertex id and a chain
#     link from the previous one, then once we're past the rolling lag
#     window deletes the vertex created `lag` iterations ago.
#   * Live cardinality stabilises at workers*lag (~3200) but unique ids keep
#     flowing. Each ObjectDelete fans out to ~5 DeleteValue in ll_crud.go,
#     creating tombstones the cache must collapse.
#
# Observer is wired with drift checks DISABLED (max-mem-drift-bph=0,
# max-goroutine-drift-ph=0). The point is to see the curve in
# observer.csv, not to PASS/FAIL on it. The scenario always returns
# 0 (unless the workload itself errors out or the runtime stops responding
# entirely) — interpretation lives in the post-run CSV diff.
#
# Tunables:
#   SOAK_DURATION_MIN     duration in minutes      (default 30)
#   LEAK_WORKERS          concurrent workers       (default 32)
#   LEAK_LAG              rolling delete window    (default 100)

cd "$(dirname "$0")" || exit 1

# leak-hunt's own default is 30 min — long enough to see a clear ramp if
# the leak is real, short enough not to fill the disk with pprof artefacts.
# Set BEFORE sourcing compose.sh so its 60-min default doesn't win.
# SOAK_DURATION_MIN passed in from run-soak-tests.sh still overrides.
: "${SOAK_DURATION_MIN:=30}"
export SOAK_DURATION_MIN

# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-soak-leak"
COMPOSE_FILE="docker-compose.yaml"
WORKERS="${LEAK_WORKERS:-32}"
LAG="${LEAK_LAG:-100}"

RUN_DIR="$(results_dir)"
install_trap
build_assert
build_observer

echo ">> run dir: $RUN_DIR"
echo ">> duration ${SOAK_DURATION_MIN}m, workers $WORKERS, lag $LAG (live cardinality ~$((WORKERS * LAG)))"

echo ">> bringing up topology (building runtime image if needed)"
dc up -d --build || fail "compose up failed"

echo ">> waiting for NATS monitoring (:8222)"
wait_http "http://localhost:8222/healthz" 90 || fail "NATS did not become healthy"
echo ">> waiting for runtime readiness"
assert ping -wait 120 || fail "runtime never became ready"
echo ">> waiting for Prometheus :9901"
wait_http "http://localhost:9901/metrics" 60 || fail "runtime Prometheus endpoint never came up"

WORKLOAD_LOG="$RUN_DIR/workload.log"
(
  assert leakhunt -workers "$WORKERS" -duration "$SOAK_DURATION_SEC" \
                  -prefix leak -lag "$LAG" \
                  >"$WORKLOAD_LOG" 2>&1 || true
) &
WORKLOAD_PID=$!
echo ">> workload PID=$WORKLOAD_PID -> $WORKLOAD_LOG"

OBSERVER_CSV="$RUN_DIR/observer.csv"
echo ">> running observer for ${SOAK_DURATION_SEC}s -> $OBSERVER_CSV"
# Drift SLOs disabled (0) — leak-hunt is observation-only. We want the curve
# in the CSV, not a pass/fail on it. The max-stall stays at 30s as a sanity
# check (the runtime should keep serving probes even while accumulating
# memory; if it stops responding that IS a regression).
if ! observe \
      -nats "nats://nats:foliage@localhost:4222" \
      -prom "http://localhost:9901/metrics" \
      -csv "$OBSERVER_CSV" \
      -duration "${SOAK_DURATION_SEC}s" \
      -interval 10s \
      -max-stall 30s \
      -max-mem-drift-bph 0 \
      -max-goroutine-drift-ph 0; then
  kill "$WORKLOAD_PID" 2>/dev/null || true
  fail "observer reported SLO violation (see $OBSERVER_CSV)"
fi

kill "$WORKLOAD_PID" 2>/dev/null || true
wait "$WORKLOAD_PID" 2>/dev/null || true

echo ">> post-soak readiness"
assert ping -wait 30 || fail "runtime unresponsive after leakhunt ended"

echo ">> ---- leak-hunt summary ----"
# Quick on-the-fly summary so the operator sees the shape without having to
# open the CSV. First-vs-last 10% average of mem_alloc_bytes and
# heap_objects, plus the whole-run linear slope on heap_objects.
awk -F, '
  NR==2 {first_ts=$1}
  NR>1  {n++; ts[n]=$1; mem[n]=$5; obj[n]=$6; gor[n]=$7}
  END {
    if (n < 2) { print "  not enough samples to summarise"; exit }
    head_n = int(n*0.1); if (head_n < 2) head_n=2
    tail_n = head_n
    for (i=1;i<=head_n;i++) { head_mem+=mem[i]; head_obj+=obj[i] }
    head_mem/=head_n; head_obj/=head_n
    for (i=n-tail_n+1;i<=n;i++) { tail_mem+=mem[i]; tail_obj+=obj[i] }
    tail_mem/=tail_n; tail_obj/=tail_n
    # full-run regression on heap_objects per hour
    for (i=1;i<=n;i++) { x=ts[i]-ts[1]; sx+=x; sy+=obj[i]; sxx+=x*x; sxy+=x*obj[i] }
    slope=(n*sxy-sx*sy)/(n*sxx-sx*sx)  # objects/sec
    printf "  samples       : %d\n", n
    printf "  mem first 10%%  : %.0f MB    last 10%%: %.0f MB   delta: %+.0f MB\n", head_mem/1048576, tail_mem/1048576, (tail_mem-head_mem)/1048576
    printf "  heap_objects  : first 10%% %.0f   last 10%% %.0f   delta %+.0f  (slope %+.0f /hr)\n", head_obj, tail_obj, tail_obj-head_obj, slope*3600
    printf "  goroutines    : first 10%% %.0f   last 10%% %.0f\n", gor[1], gor[n]
  }
' "$OBSERVER_CSV"
echo

echo ">> leak-hunt: DONE (observation-only — see $OBSERVER_CSV)"
