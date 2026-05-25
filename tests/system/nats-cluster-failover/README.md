# nats-cluster-failover

Verifies **NATS cluster fault tolerance through redundancy**: a real 3-node
JetStream cluster (RAFT, streams replicated R=3, each node its own volume) keeps
serving when **one** node is killed — quorum (2/3) holds and every stream has a
surviving replica — and the killed node then rejoins and re-syncs.

This is distinct from:
- `nats-restart-recovery` — a **single** node crashes and recovers its own store
  (durability, with an outage), and
- `ha-3-node` — three **runtime** instances fail over (active/passive), on one NATS.

## Scenario (`run.sh`)

1. Start the 3-node cluster first; wait until all nodes are JetStream-healthy.
2. Start one runtime in cluster mode (`NATS_CLUSTER_MODE=true`, `NATS_REPLICAS=3`).
3. Seed a known graph (replicated R=3); verify.
4. Kill one node (`nats2`); assert the system keeps serving and data is intact
   (quorum 2/3), and that new writes still commit with a node down.
5. Restart the node; assert it rejoins and data/consistency hold.

The assertion client connects to all three nodes (comma-separated) so it fails
over with the cluster.

## Opt-in: needs a stable multi-node environment

This test is **skipped by default**. A 3-node JetStream RAFT cluster needs enough
CPU to hold a steady leader/quorum; under constrained Docker (e.g. Docker Desktop
on a busy laptop) the cluster flaps during formation and the runtime can't reach
readiness (`NewDomain` JetStream operations time out / report "temporarily
unavailable"). It is reliable on a multi-core / multi-host CI.

Run it explicitly:

```sh
RUN_CLUSTER_TESTS=1 tests/system/nats-cluster-failover/run.sh
```

Related SDK hardening: startup now retries transient JetStream-unavailable errors
during stream/subscription creation (`retryTransientJS` in `statefun/runtime.go`),
so a runtime survives a brief cluster election rather than aborting. The deeper
`NewDomain` startup path is not yet covered by that retry.
