#!/usr/bin/env bash
#
# wal-restart-recovery — committed data survives an ungraceful crash.
#
# Unlike graceful-shutdown (clean SIGTERM drain), this sends SIGKILL: the
# runtime gets no chance to drain. On restart it must reconstruct the exact
# graph from the persisted startup snapshot + WAL in JetStream. This is the
# crash-recovery / durability guard.
#
# Flow: up -> wait healthy -> seed -> SIGKILL runtime -> restart -> assert all
# committed objects are present and the graph is consistent.
#
# Exit 0 == pass. The EXIT trap always tears the project down.

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-systest-wal-restart"
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

echo ">> pre-crash verify"
assert verify -n "$N" || fail "pre-crash verify failed"

echo ">> hard-killing runtime (SIGKILL, no drain)"
dc kill -s SIGKILL runtime || fail "compose kill errored"

code="$(container_exit_code runtime)"
echo ">> runtime exit code after SIGKILL: $code (expected 137)"

echo ">> restarting runtime"
dc up -d runtime || fail "runtime restart failed"

echo ">> waiting for runtime readiness after restart"
assert ping -wait 120 || fail "runtime not ready after restart"

echo ">> recovery verify (graph reconstructed from snapshot + WAL)"
assert verify -n "$N" || fail "recovery verify failed — committed data lost after crash"

echo ">> graph-consistency check"
assert consistency -type systest_node -n "$N" || fail "post-recovery consistency failed"

echo ">> wal-restart-recovery: PASS"
