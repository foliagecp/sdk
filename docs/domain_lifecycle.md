# Domain, WAL and call diagnostics

A configured domain change does not migrate persisted SDK state. Startup checks
routing streams and consumers before creating application resources and reports
the resource, observed settings and expected settings on mismatch. Restore the
previous domain configuration to reuse data, or plan a migration. If old data
is disposable, stop all writers and manually reinitialize the affected storage
using the deployment procedure. Check shared resources and application-owned
snapshots and external databases separately. Domain mismatch handling never
resets, migrates, repairs or deletes domain data. No reset flag, function or
administrator command is shipped.

If another instance creates streams or consumers during validation, the SDK
repeats the complete read-only check within its existing startup budget. It
never accepts a partial inventory, rewrites a conflicting resource, or retries
a confirmed domain mismatch as if it were a transient count change. If no
complete snapshot can be obtained, startup fails with the timeout and inventory
diagnostic instead of serving an unvalidated configuration.

HA promotion renews the application-hook context under the same synchronization
used by shutdown cancellation. Old generations are canceled, and a late
promotion cannot create live application work after shutdown has begun.

## Dynamic function registration in HA

`RegisterDynamicFunctionType` may be called on both active and passive
runtimes after startup. A passive instance registers locally without taking
the function's single-instance lease or starting subscriptions. The active
instance records acquired leases for periodic refresh; a busy lease remains
pending and is retried on lifecycle ticks, including after its TTL expires.
The function starts serving once ownership is acquired, without requiring
another HA promotion. Registration still succeeds locally while a lease is
held elsewhere; this does not mean its subscriptions are already live.

Registration, lease updates, subscription changes and demotion share internal
synchronization. Slow stream creation does not expose a partially initialized
function or block HA promotion. Pending registration I/O does not block the
runtime lease refresher or postpone revoking a lost active role. Promotion
retains pending subscription work across ticks while registration holds the
lifecycle gate, including functions with no single-instance lease. A registration
that loses its active role parks its function instead of leaving it subscribed.
A failed registration releases its acquired
lease and removes the local entry. A failed deferred subscription is retried;
existing subscriptions are reused so retrying another function does not add
extra signal consumers or request responders. Local calls take a synchronized
registry snapshot during concurrent registration. Public registration and
call signatures, function configuration and message formats are unchanged.

The `Test_HA_DynamicRegistration_*` tests cover passive-first and concurrent
registration, busy and expired leases, refresh past the original TTL, failure
rollback, subscription retry, failover, promotion/shutdown during stream
creation and local requests during registration. They also run through the
HA system fixture in full, `--system-only` and `--race` modes.

## Call diagnostics

Call diagnostics preserve signatures, OpMsg/OpError layouts, JSON formats,
successful results, routing, execution order and existing timeout behavior.
Errors and logs identify the selected domain, function, object and transport.
The original NATS text and unwrap-able cause are preserved. No responders alone
cannot establish an invalid domain, missing function or missing object. Check
the ID domain prefix, application readiness and Request/Reply support. A timeout
does not prove that a write was not executed. No automatic write retry or
fallback to a different domain is performed.

Existing HTTP adapters retain their status mapping and may replace transport
error details with a fixed response; SDK logs still contain the diagnostic.
OpMsg conversion preserves text rather than the Go error chain.
Payloads, options, connection URLs and credentials are not logged.

Use `errors.Is(err, nats.ErrNoResponders)` or `errors.Is(err, nats.ErrTimeout)`
for transport classification. Direct equality with the original sentinel or
its entire error string does not match an error carrying additional context.

## WAL crash recovery

The committer applies all operations to KV and waits for their publish
acknowledgements, then creates an immutable applied receipt in the same bucket,
then acknowledges the WAL message. Receipt keys use the reserved prefix
`__wal_applied__.` and are excluded from the graph's `store.>` cache load.
A custom cache prefix must not overlap that reserved namespace.

A receipt identifies the WAL and KV stream creation times, bucket, consumer,
stream sequence, transaction ID, payload digest and original delivery ACK
address. It records the KV revisions of deletes before their tombstones can
be reclaimed. Receipts contain no operation values. Include these service keys
in backups: exporting only `store.*` is not a complete KV backup.

If a restored consumer still awaits an ACK for a missing WAL record, the SDK
validates that record's receipt and repeats its original ACK. It never deletes
or recreates the consumer. A missing, malformed, mismatched or expired receipt
returns an error matching `statefun.ErrWALRecovery`; the record is not silently
skipped. Legacy archives without receipts cannot use this automatic repair.
Preserve the backup and verify its consistency or restore a consistent backup;
clearing WAL or KV is not a recovery procedure.

The broker can also recover an older value after a delete marker was reclaimed.
At recovery, a recorded delete is repeated only if the current KV revision is
not newer than the proven delete. The write is conditional on the observed
revision, so a later legitimate PUT is preserved. Startup reloads the working
cache after replay and delete reconciliation, before serving functions.

Consumer delivery attempts are unlimited for the KV committer, with progress
heartbeats during a large apply or backup pause. Invalid transactions are kept
in WAL and reported as errors. Startup refuses to mark KV consistent while
matching records remain in the stream, even if consumer pending counters are
zero. A 30-second absence of apply/ACK progress fails recovery; it is not a
30-second total replay limit. Caller cancellation also interrupts startup.

Receipt maintenance runs every minute, retaining the latest 4096 receipts per
bucket and WAL generation and all receipts above the acknowledged floor.
Cleanup uses exact KV revisions and may retain more entries if interrupted or
unavailable. Before removing evidence it records a monotonic pruning boundary;
a retained old WAL body without its receipt below that boundary is refused,
not replayed over newer values. This is a bounded crash-recovery window, not
an unlimited transaction history or a guarantee against arbitrary broker-store
corruption. Recovery fails closed when the required evidence is unavailable.
Budget KV storage and message limits for receipts as well as application data;
receipts for delete batches include a key/revision pair for each delete.
Upgrade all SDK replicas serving the same bucket to obtain this protection;
older binaries do not write receipts. Public function signatures, application
payloads and request/reply formats are unchanged.

## Cache recovery and graph read repair

Before serving functions, the runtime drains pending WAL into KV and reloads
its working cache. Each completed KV load establishes a process-local time
boundary. Mutations stamped earlier than that boundary are refused even for
absent keys. Graph operations report a failed OpMsg instructing the caller to
retry with a new operation timestamp. The boundary advances again after an
HA promotion reload. It does not add per-key service records or change KV/WAL
payload formats. An unstamped request receives its timestamp when processed;
the SDK cannot infer its original creation time from an absent timestamp.

Pending changes appended while a WAL publish awaits its acknowledgement stay
pending until their own publication succeeds. A failed shutdown publication
retains those changes and does not close the cache's successful-flush signal.
Retries are bounded during shutdown; this does not make writes which never
reached WAL survive an abrupt process kill.

Vertex bodies are authoritative during read repair. A local type membership
pointing at a missing body is removed, never used to recreate that body or
park a nonexistent object. A surviving object body and its own current type
pointer can justify repairing its structural links. Repair preserves the body
and existing link metadata, and never chooses a type from a stale membership
on another vertex. Missing or malformed trash-edge provenance is reported as
an error rather than replaced with an invented deletion time or original type.
A parked object's current type takes precedence over an
old membership, so reading the old type cannot restore it to the live graph.
Local cache absence is not evidence that a remote vertex is missing.

A failed vertex deletion can leave its body and some links present. The call
reports failure; a subsequent explicit deletion cleans addressable remnants,
including links without bodies and vertices whose type link is already gone.
Deleting a bodyless object cleans links rather than moving it into the trash
can. Successful ordinary vertex deletion removes the body; `links_only` and
trash-can parking deliberately retain it. An isolated low-level vertex is
valid data: there is no blanket garbage collector deleting all unlinked
bodies, and no persistent background delete-intent mechanism is introduced.
A stale incoming reference must not delete a replacement edge which now
points to a different vertex.

## Validation

Run the focused regression checks with the race detector:

```sh
go test -race ./statefun ./clients/go/db ./internal/requestdiag \
  -run '^TestDomainState|^TestCallDiagnostics|^TestWrap|^Test_Request_AutoSelect_HonorsPerCallTimeout$' \
  -count=1
```

Run the complete suite, including Docker-backed integrations and system tests:

```sh
scripts/run-all-tests.sh
```

The regression cases cover a fresh custom domain, matching existing state,
stale routing streams and consumers, read failures, cache-ID suffix compatibility,
no responders, transport timeouts, actual shadow-object destinations and successful
local and remote calls. Refused startup preserves existing resources. Tests use
disposable local storage and never reset or migrate an existing installation.

WAL-focused regression checks:

```sh
go test -race ./statefun -run '^TestWALRecovery_|^TestWALKVBarrierTestSuite$' -count=1
bash tests/system/wal-ack-recovery/run.sh
```

The system fixture uses pinned NATS 2.12.4 in disposable local Docker containers
and separate SDK client processes. It creates real `SIGKILL` orphans without
editing store files, archives a cleanly stopped store, restores it in a new
container, checks KV/cache/DELETE/empty-index/request/new-write behavior and a
second restart, and verifies explicit refusal of a legacy orphan. It requires
a local Unix Docker endpoint, the Docker CLI, Go and tar. Six bounded attempts
are allowed to enter the broker's crash window; failure to exercise it is a
test failure, not a skipped or successful recovery check.
