# nats-cluster-failover

Verifies **NATS cluster fault tolerance through redundancy**: a real 3-node
JetStream cluster (RAFT, streams replicated R=3, each node its own volume) keeps
serving when **one** node is killed — quorum (2/3) holds and every stream has a
surviving replica — and the killed node then rejoins and re-syncs.

Distinct from:
- `nats-restart-recovery` — a **single** node crashes and recovers its own store
  (durability, with an outage), and
- `ha-3-node` — three **runtime** instances fail over (active/passive), on one NATS.

## Scenario (`run.sh`)

1. Start the 3-node cluster + `io` first; wait until all nodes are
   JetStream-healthy, then **probe real R=3 stream placement** (create+delete a
   replicated test stream) so the runtime starts only once the cluster can
   actually replicate.
2. Start one runtime in cluster mode (`NATS_CLUSTER_MODE=true`, `NATS_REPLICAS=3`).
3. Seed a known graph (replicated R=3); verify.
4. Kill one node (`nats2`); assert the system keeps serving and data is intact
   (quorum 2/3), and that **new writes still commit with a node down**.
5. Restart the node; assert it rejoins and data/consistency hold.

The assertion client connects to all three nodes (comma-separated) so it fails
over with the cluster.

## SDK fixes this test surfaced

Getting this green uncovered two real startup bugs (both fixed):

1. **afterStart ran before the runtime was ready.** `cmdbSchemaPrepare` (creates
   the built-in `types`/`objects` roots via `runtime.Request`) was invoked before
   `isReady`, and `Request` rejects pre-ready calls. On fast single-node startup
   `isReady` wins the race; on a 3-node R=3 cluster (~30 subscriptions, slow to
   wire up) afterStart fired first and the schema init was silently dropped,
   leaving every CMDB op failing with `vertex hub/types does not exist`. Fixed by
   gating afterStart on `isReady` (`statefun/runtime.go`).
2. **Startup aborted on transient JetStream errors.** A runtime joining a still-
   electing cluster now retries (`retryStartupJS` across AccountInfo / KV-bucket /
   stream / consumer / subscription creation) instead of aborting.

Note: cluster startup is slower than single-node (forming ~30 R=3 stream/consumer
RAFT groups); the runtime retries transient errors up to
`JS_STARTUP_RETRY_TIMEOUT_SEC` (180 here).
