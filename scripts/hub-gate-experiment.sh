#!/usr/bin/env bash
#
# hub-gate-experiment.sh — does the hub throughput check measure the code or
# the machine?
#
# Exports a git ref, builds its statefun/cache test binary in a Linux container
# and runs the ref's hub throughput check N times under each load profile, in a
# container limited to --cpus/--memory while a sibling container competes for
# the same CPUs:
#
#   idle    nothing else runs
#   steady  the sibling spins on --load-cpus cores for the whole profile
#   flap    the sibling spins for 3 s and rests for 3 s, over and over — a
#           transient neighbour, the kind that slows one window of a shared runner
#
# Which check runs depends on the ref:
#
#   up to v0.5.8  Test_Hub_ConcurrentThroughput, built with -race as in the full
#                 race run where it failed: one best-of-3 sample per mode, tree
#                 first, records within 1.25x of tree
#   later         Test_Hub_ConcurrentThroughputGate (-tags perf, no race
#                 detector): paired, interleaved samples, pairs taken while the
#                 machine changed are dropped, median of clean pairs within 1.25x
#
# Usage:
#   scripts/hub-gate-experiment.sh [--ref REF] [--runs N] [--profiles "idle steady flap"]
#                                  [--cpus N] [--memory SIZE] [--load-cpus N]
#
#   --ref        git ref to measure (default: HEAD)
#   --runs       runs per profile (default: 10)
#   --profiles   which profiles, in order (default: "idle steady flap")
#   --cpus       CPU limit of the measuring container (default: 4)
#   --memory     memory limit of the measuring container (default: 3g)
#   --load-cpus  cores the sibling spins on (default: 2)
#
# Examples:
#   scripts/hub-gate-experiment.sh --ref v0.5.8-dev03112026 --runs 20   # the old check
#   scripts/hub-gate-experiment.sh --ref HEAD --runs 10                 # the gate
#
# Needs docker and git. Raw output per profile goes to
# tests/perf/_results/hub-gate-<ref>-<profile>.log; the summary goes to stdout.
#
set -euo pipefail
cd "$(dirname "$0")/.."

REF="HEAD"
RUNS=10
PROFILES="idle steady flap"
CPUS=4
MEMORY=3g
LOAD_CPUS=2

while [ $# -gt 0 ]; do
  case "$1" in
    --ref)       REF="$2"; shift 2 ;;
    --runs)      RUNS="$2"; shift 2 ;;
    --profiles)  PROFILES="$2"; shift 2 ;;
    --cpus)      CPUS="$2"; shift 2 ;;
    --memory)    MEMORY="$2"; shift 2 ;;
    --load-cpus) LOAD_CPUS="$2"; shift 2 ;;
    -h|--help)   sed -n '2,46p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1 (see --help)"; exit 2 ;;
  esac
done

GO_VERSION=$(awk '$1 == "go" { split($2, v, "."); print v[1] "." v[2]; exit }' go.mod)
IMAGE="golang:${GO_VERSION}"
REF_NAME=$(echo "$REF" | tr '/' '_')
RESULTS="tests/perf/_results"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/hub-gate-XXXXXX")
LOAD="hub-gate-load-$$"
mkdir -p "$RESULTS" "$WORK/src" "$WORK/out"
cleanup() {
  docker rm -f "$LOAD" >/dev/null 2>&1 || true
  # The build container writes as root; remove its output through docker too.
  docker run --rm -v "$WORK:/work" "$IMAGE" rm -rf /work/src /work/out >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

git archive "$REF" | tar -x -C "$WORK/src"
if [ -f "$WORK/src/statefun/cache/hub_perf_test.go" ]; then
  KIND=gate
  BUILD="go test -tags perf -c -o /out/hub.test ./statefun/cache"
  RUN='^Test_Hub_ConcurrentThroughputGate$'
else
  KIND=old
  BUILD="go test -race -c -o /out/hub.test ./statefun/cache"
  RUN='^Test_Hub_'
fi
echo ">> ref $REF ($(git rev-parse --short "$REF")): $KIND check, $RUNS runs per profile, --cpus $CPUS --memory $MEMORY, sibling on $LOAD_CPUS cores"
echo ">> building in $IMAGE: $BUILD"
docker run --rm -v "$WORK/src:/src:ro" -w /src -v "$WORK/out:/out" \
  -v foliage-test-go-mod:/go/pkg/mod -v foliage-test-go-build:/root/.cache/go-build \
  -e GOTOOLCHAIN=local "$IMAGE" sh -c "$BUILD" >/dev/null

start_load() {
  local spin
  case "$1" in
    idle)   return 0 ;;
    steady) spin='while :; do :; done' ;;
    flap)   spin='while :; do end=$(( $(date +%s) + 3 )); while [ "$(date +%s)" -lt "$end" ]; do :; done; sleep 3; done' ;;
    *) echo "unknown profile: $1"; exit 2 ;;
  esac
  docker run -d --rm --name "$LOAD" --cpus "$LOAD_CPUS" "$IMAGE" \
    sh -c "for i in \$(seq 1 $LOAD_CPUS); do ( $spin ) & done; wait" >/dev/null
}

for profile in $PROFILES; do
  log="$RESULTS/hub-gate-${REF_NAME}-${profile}.log"
  echo ">> profile $profile -> $log"
  start_load "$profile"
  docker run --rm --cpus "$CPUS" --memory "$MEMORY" -v "$WORK/out:/out:ro" "$IMAGE" \
    sh -c "for i in \$(seq 1 $RUNS); do echo \"@@RUN \$i\"; /out/hub.test -test.run '$RUN' -test.count=1 -test.v 2>&1; done" > "$log" || true
  docker rm -f "$LOAD" >/dev/null 2>&1 || true
done

# Portable awk (mawk, BSD awk): no asort.
SORT_AWK='function sorted(a, n,   i, j, t) { for (i = 1; i < n; i++) for (j = i; j > 0 && a[j - 1] > a[j]; j--) { t = a[j]; a[j] = a[j - 1]; a[j - 1] = t } }'

echo "=================================================================="
printf "%-8s %5s %5s %6s %13s  %s\n" profile runs fail inconc "ratio p50/max" "notes"
for profile in $PROFILES; do
  log="$RESULTS/hub-gate-${REF_NAME}-${profile}.log"
  if [ "$KIND" = old ]; then
    # One ratio per run: records over tree, from the two lines the test logs.
    awk -v p="$profile" "$SORT_AWK"'
      /@@RUN/ { runs++ }
      /дерево:/ { for (i = 1; i <= NF; i++) if ($i ~ /ms$/) { sub(/ms$/, "", $i); tree = $i; break } }
      /записи:/ { for (i = 1; i <= NF; i++) if ($i ~ /ms$/) { sub(/ms$/, "", $i); r[n++] = $i / tree; break } }
      /--- FAIL: Test_Hub_ConcurrentThroughput / { fail++ }
      END {
        sorted(r, n)
        printf "%-8s %5d %5d %6s %6.2f/%-6.2f  one sample per mode, tree first\n", p, runs, fail, "-", r[int((n - 1) / 2)], r[n - 1]
      }' "$log"
  else
    awk -v p="$profile" "$SORT_AWK"'
      /@@RUN/ { runs++ }
      /чистых пар .* из .*медиана/ {
        for (i = 1; i <= NF; i++) {
          if ($i == "пар") clean += $(i + 1)
          if ($i == "медиана") { v = $(i + 1); sub(/,$/, "", v); m[n++] = v + 0 }
        }
      }
      /--- FAIL: Test_Hub_ConcurrentThroughputGate/ { fail++ }
      /INCONCLUSIVE/ { inconc++ }
      END {
        sorted(m, n)
        printf "%-8s %5d %5d %6d %6.2f/%-6.2f  median of clean pairs per run; %.1f clean pairs per decided run\n",
          p, runs, fail - inconc, inconc, (n ? m[int((n - 1) / 2)] : 0), (n ? m[n - 1] : 0), (n ? clean / n : 0)
      }' "$log"
  fi
done
echo "=================================================================="
