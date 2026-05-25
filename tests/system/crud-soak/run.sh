#!/usr/bin/env bash
#
# crud-soak — sustained concurrent CRUD against a real deployment.
#
# Many workers run mixed create/update/delete/link at high rate for a window.
# Afterwards the runtime must still be responsive and the graph must satisfy the
# consistency invariant (every enumerated object readable & typed). This is the
# load/stability guard complementing the in-process concurrency hunts; it also
# guards the 116-class failure where the runtime degrades and never recovers.
#
# Exit 0 == pass. The EXIT trap always tears the project down.
#
# Tunables via env: SOAK_WORKERS (default 8), SOAK_DURATION seconds (default 25).

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-systest-crud-soak"
COMPOSE_FILE="docker-compose.yaml"
WORKERS="${SOAK_WORKERS:-8}"
DURATION="${SOAK_DURATION:-25}"

install_trap
build_assert

echo ">> bringing up topology (building runtime image if needed)"
dc up -d --build || fail "compose up failed"

echo ">> waiting for NATS monitoring (:8222)"
wait_http "http://localhost:8222/healthz" 90 || fail "NATS did not become healthy"

echo ">> waiting for runtime readiness (CMDB ping)"
assert ping -wait 120 || fail "runtime never became ready"

echo ">> soak: $WORKERS workers for ${DURATION}s"
assert soak -workers "$WORKERS" -duration "$DURATION" || fail "soak reported operation errors"

echo ">> runtime must still be responsive after the load"
assert ping -wait 30 || fail "runtime unresponsive after soak (possible wedge/leak)"

echo ">> post-soak graph-consistency check (settle window tolerates index lag, not corruption)"
assert consistency -type systest_node -settle 15 || fail "post-soak consistency failed"

echo ">> crud-soak: PASS"
