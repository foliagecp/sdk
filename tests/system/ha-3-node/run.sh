#!/usr/bin/env bash
#
# ha-3-node — leadership/failover across three real runtime processes.
#
# Three runtimes share one domain in active/passive mode; exactly one holds the
# KV runtime lock and serves. We kill nodes one at a time and assert the system
# keeps serving and never loses data — the production analogue of the in-process
# statefun/ha_test.go, and the guard against the 116-class "goes passive and
# never returns" wedge.
#
# Note: a runtime exposes no external "am I active" probe, so we assert the
# externally observable invariants — service continuity through node kills and
# zero data loss — rather than peeking at each node's in-process flag (that is
# what ha_test.go covers directly). Killing two of three nodes guarantees the
# sole survivor must be (or become) the active and keep serving.
#
# Exit 0 == pass. The EXIT trap always tears the project down.

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-systest-ha3"
COMPOSE_FILE="docker-compose.yaml"
N=200
FAILOVER_WAIT=45   # KV lock TTL (~10s) + passive detect + subscribe + become active

install_trap
build_assert

echo ">> bringing up 1 NATS + 3 runtimes (building image if needed)"
dc up -d --build || fail "compose up failed"

echo ">> waiting for NATS monitoring (:8222)"
wait_http "http://localhost:8222/healthz" 90 || fail "NATS did not become healthy"

echo ">> waiting until one runtime is active and serving"
assert ping -wait 120 || fail "no runtime became active"

echo ">> seeding $N objects via the active"
assert seed -n "$N" || fail "seed failed"
assert verify -n "$N" || fail "initial verify failed"

echo ">> kill runtime-a; system must fail over and stay consistent"
dc kill runtime-a || fail "kill runtime-a errored"
assert ping -wait "$FAILOVER_WAIT" || fail "no failover after killing runtime-a (possible wedge)"
assert verify -n "$N" || fail "data lost after killing runtime-a"

echo ">> kill runtime-b; sole survivor (runtime-c) must serve"
dc kill runtime-b || fail "kill runtime-b errored"
assert ping -wait "$FAILOVER_WAIT" || fail "no failover after killing runtime-b (possible wedge)"
assert verify -n "$N" || fail "data lost after killing runtime-b"

echo ">> consistency on the surviving single node"
assert consistency -type systest_node -n "$N" || fail "consistency failed on survivor"

echo ">> restart runtime-a and runtime-b; they must rejoin as passives without disruption"
dc up -d runtime-a runtime-b || fail "rejoin start failed"
assert ping -wait "$FAILOVER_WAIT" || fail "service disrupted after nodes rejoined"
assert verify -n "$N" || fail "data lost after rejoin"

echo ">> ha-3-node: PASS"
