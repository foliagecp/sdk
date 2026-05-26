#!/usr/bin/env bash
#
# nats-cluster-failover — NATS cluster fault tolerance (R=3 redundancy).
#
# A 3-node JetStream cluster keeps a replica of every stream on every node and
# needs a 2/3 quorum. Killing ONE node must NOT stop the system: the surviving
# two have the data and quorum, so CRUD keeps working with no data loss; the
# killed node then rejoins and re-syncs. This is the "one of three falls and
# everything keeps working" HA the cluster is for (distinct from single-node
# store self-recovery, which is nats-restart-recovery).
#
# The assertion client connects to all three nodes (comma-separated) so it
# fails over with the cluster. Exit 0 == pass.

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-systest-nats-cluster"
COMPOSE_FILE="docker-compose.yaml"
N=200
# All three nodes, so the client fails over when one dies.
NATS3="nats://nats:foliage@localhost:4222,nats://nats:foliage@localhost:4223,nats://nats:foliage@localhost:4224"

install_trap
build_assert

echo ">> building cluster runtime image if needed"
dc build runtime || fail "image build failed"

echo ">> starting the 3-node NATS cluster + io FIRST (runtime must not start"
echo "   before the cluster can actually place R=3 streams)"
dc up -d nats1 nats2 nats3 io || fail "cluster up failed"

echo ">> waiting for all 3 cluster nodes to be JetStream-healthy (meta leader + current)"
for p in 8222 8223 8224; do
  wait_http "http://localhost:${p}/healthz" 120 || fail "NATS node on :${p} did not become healthy"
done

# Placement probe: /healthz only says a node is current — it does NOT prove the
# cluster can place a REPLICATED stream. Create+delete an R=3 test stream until
# it succeeds, so we start the runtime only once R=3 placement actually works.
echo ">> probing R=3 stream placement (cluster must be able to replicate)"
NURL_INT="nats://nats:foliage@nats1:4222"
probe_ok=no
probe_deadline=$((SECONDS + 150))
while [ "$SECONDS" -lt "$probe_deadline" ]; do
  if dc exec -T io sh -c "nats stream add __probe --subjects '__probe.>' --replicas 3 --defaults --server=$NURL_INT >/dev/null 2>&1 && nats stream rm -f __probe --server=$NURL_INT >/dev/null 2>&1"; then
    probe_ok=yes; break
  fi
  sleep 3
done
[ "$probe_ok" = "yes" ] || fail "cluster could not place an R=3 stream (cluster never stabilized)"
echo ">> R=3 placement works; starting the runtime"

echo ">> starting the runtime against the ready cluster"
dc up -d runtime || fail "runtime up failed"

echo ">> waiting for runtime readiness"
assert ping -nats "$NATS3" -wait 180 || fail "runtime never became ready on the cluster"

echo ">> seeding $N objects (replicated R=3)"
assert seed -nats "$NATS3" -n "$N" || fail "seed failed"
assert verify -nats "$NATS3" -n "$N" || fail "initial verify failed"

echo ">> killing one node (nats2); quorum 2/3 must keep the system serving"
dc kill nats2 || fail "kill nats2 errored"

echo ">> system must keep serving with a node down (no data loss)"
assert ping -nats "$NATS3" -wait 60 || fail "system stopped serving after losing one node (R=3 should tolerate it)"
assert verify -nats "$NATS3" -n "$N" || fail "data lost after losing one node"

echo ">> writing more while a node is down (must still commit on quorum)"
assert seed -nats "$NATS3" -n 50 -type systest_node2 -prefix down || fail "writes failed with one node down"

echo ">> restarting nats2; it must rejoin and re-sync"
dc up -d nats2 || fail "nats2 restart failed"
wait_http "http://localhost:8223/healthz" 120 || fail "nats2 did not rejoin"

echo ">> data intact after rejoin"
assert verify -nats "$NATS3" -n "$N" || fail "data lost after node rejoin"
assert consistency -nats "$NATS3" -type systest_node -n "$N" || fail "consistency failed after rejoin"

echo ">> nats-cluster-failover: PASS"
