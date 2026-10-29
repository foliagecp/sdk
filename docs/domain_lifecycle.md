# Domain state and call diagnostics

A configured domain change does not migrate persisted SDK state. Startup checks
routing streams and consumers before creating application resources and reports
the resource, observed settings and expected settings on mismatch. Restore the
previous domain configuration to reuse data, or plan a migration. If old data
is disposable, stop all writers and manually reinitialize the affected storage
using the deployment procedure. Check shared resources and application-owned
snapshots and external databases separately. The SDK never performs a reset,
repair or deletion. No reset flag, function or administrator command is shipped.

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
