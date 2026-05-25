#!/usr/bin/env bash
#
# nats-restart-recovery — the NATS node crashes and recovers its own store.
#
# wal-restart-recovery kills the runtime; this kills the STORAGE: SIGKILL the
# NATS container, then restart the SAME node on the SAME volume. The broker must
# recover its JetStream store from disk, and the still-running runtime must
# reconnect and keep serving with no data loss. This is single-node durability +
# self-recovery (cluster fault tolerance is nats-cluster-failover).
#
# Flow: up -> seed -> SIGKILL nats -> restart nats (same volume) -> assert the
# runtime reconnects, data intact, graph consistent. Exit 0 == pass.

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-systest-nats-restart"
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
assert verify -n "$N" || fail "pre-crash verify failed"

echo ">> hard-killing the NATS container (SIGKILL)"
dc kill -s SIGKILL nats || fail "compose kill nats errored"
code="$(container_exit_code nats)"
echo ">> nats exit code after SIGKILL: $code (expected 137)"

echo ">> restarting the SAME NATS node on the SAME volume"
dc up -d nats || fail "nats restart failed"
echo ">> waiting for NATS to recover its store and report healthy"
wait_http "http://localhost:8222/healthz" 120 || fail "NATS did not recover its store after crash"

echo ">> waiting for the runtime to reconnect and serve"
assert ping -wait 120 || fail "runtime did not resume after NATS crash+recovery (wedged?)"

echo ">> recovery verify (data survived the NATS crash, served via recovered store)"
assert verify -n "$N" || fail "data lost after NATS crash+recovery"

echo ">> graph-consistency check"
assert consistency -type systest_node -n "$N" || fail "post-recovery consistency failed"

echo ">> nats-restart-recovery: PASS"
