#!/usr/bin/env bash
#
# graceful-shutdown — verifies the runtime drains cleanly on a real OS SIGTERM.
#
# Why this needs a container (not `go test`): the drain path is triggered by a
# process-wide signal; sending one inside a `go test` binary also crashes the
# embedded NATS server, so it can only be observed against a real NATS with the
# runtime as its own OS process.
#
# Flow: up -> wait healthy -> seed a known graph -> SIGTERM the runtime ->
# assert it exits 0 (clean drain, not SIGKILL) within the grace window ->
# restart -> assert every committed object is still present (nothing lost).
#
# Exit 0 == pass. The EXIT trap always tears the project down.

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-systest-graceful"
COMPOSE_FILE="docker-compose.yaml"
N=300

install_trap
build_assert

echo ">> bringing up topology (building runtime image if needed)"
dc up -d --build || fail "compose up failed"

echo ">> waiting for NATS monitoring (:8222)"
wait_http "http://localhost:8222/healthz" 90 || fail "NATS did not become healthy"

echo ">> waiting for runtime readiness (CMDB ping)"
assert ping -wait 120 || fail "runtime never became ready"

echo ">> seeding $N objects"
assert seed -n "$N" || fail "seed failed"

echo ">> pre-shutdown verify"
assert verify -n "$N" || fail "pre-shutdown verify failed"

GRACE=90
echo ">> sending SIGTERM via 'compose stop' (grace ${GRACE}s)"
t0=$SECONDS
dc stop -t "$GRACE" runtime || fail "compose stop errored"
drain=$((SECONDS - t0))
echo ">> runtime stopped after ${drain}s"

code="$(container_exit_code runtime)"
echo ">> runtime exit code: $code"
[ "$code" = "0" ] || fail "runtime did not drain cleanly (exit $code; 137 == SIGKILL after grace == hang)"
[ "$drain" -lt "$GRACE" ] || fail "runtime took the full grace window (${drain}s) — did not drain, was killed"

echo ">> restarting runtime"
dc up -d runtime || fail "runtime restart failed"

echo ">> waiting for runtime readiness after restart"
assert ping -wait 120 || fail "runtime not ready after restart"

echo ">> durability verify (no committed op lost across shutdown+restart)"
assert verify -n "$N" || fail "durability verify failed — committed ops lost"

echo ">> graph-consistency check"
assert consistency -type systest_node -n "$N" || fail "post-restart consistency failed"

echo ">> graceful-shutdown: PASS"
