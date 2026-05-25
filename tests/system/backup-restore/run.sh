#!/usr/bin/env bash
#
# backup-restore — JetStream backup/restore durability round-trip.
#
# Flow: up -> seed a known graph -> `nats account backup` -> stop runtime ->
# wipe ALL streams/KV -> `nats account restore` -> restart runtime -> assert the
# graph is exactly what was backed up. Proves a backup truly captures the graph
# and a restore brings it back from nothing.
#
# The backup/restore is driven via the `nats` CLI in the io (nats-box) container,
# the same mechanism samples/backup-barrier/foliage_mgr.sh uses. The seed is
# quiesced before backup so the snapshot is a clean point; the write-barrier that
# keeps a backup consistent under live writes is unit-tested separately.
#
# Exit 0 == pass. The EXIT trap always tears the project down.

cd "$(dirname "$0")" || exit 1
# shellcheck source=../_lib/compose.sh
source ../_lib/compose.sh

PROJECT="foliage-systest-backup-restore"
COMPOSE_FILE="docker-compose.yaml"
N=200
NURL="nats://nats:foliage@nats:4222"
BK="/tmp/foliage-backup"

# ioexec runs a shell snippet inside the nats-box container (has the nats CLI).
ioexec() { dc exec -T io sh -c "$1"; }

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
assert verify -n "$N" || fail "pre-backup verify failed"

echo ">> taking JetStream account backup"
ioexec "rm -rf $BK && mkdir -p $BK && nats account backup $BK --force --server=$NURL" \
  || fail "backup failed"

echo ">> stopping runtime (so the wipe/restore is not raced by it)"
dc kill runtime || fail "kill runtime errored"

echo ">> wiping ALL streams and KV buckets"
ioexec "for s in \$(nats stream ls -n --server=$NURL); do nats stream rm -f \"\$s\" --server=$NURL; done" \
  || fail "wipe failed"
remaining="$(ioexec "nats stream ls -n --server=$NURL" | tr -d '[:space:]')"
echo ">> streams remaining after wipe: '${remaining:-<none>}'"
[ -z "$remaining" ] || fail "wipe left streams behind: $remaining"

echo ">> restoring JetStream account from backup"
ioexec "nats account restore $BK --server=$NURL" || fail "restore failed"

echo ">> restarting runtime (reloads cache from the restored KV)"
dc up -d runtime || fail "runtime restart failed"
assert ping -wait 120 || fail "runtime not ready after restore"

echo ">> verify graph == backed-up snapshot"
assert verify -n "$N" || fail "restored graph does not match the backup"

echo ">> graph-consistency check"
assert consistency -type systest_node -n "$N" || fail "post-restore consistency failed"

echo ">> backup-restore: PASS"
