# `_lib` — shared harness for docker-compose system tests

Reusable pieces every `tests/system/<name>/run.sh` builds on. Not a test
itself (no `run.sh`), so the runner skips it.

## `compose.sh`

Bash helpers, sourced by each `run.sh` after it sets `PROJECT` and
`COMPOSE_FILE`:

| Helper | Purpose |
|---|---|
| `dc …` | `docker compose -p $PROJECT -f $COMPOSE_FILE …` |
| `build_assert` | compile the assertion client once to `$ASSERT_BIN` (`/tmp/foliage-systest-assert`) |
| `assert …` | run the compiled client (host → exposed NATS) |
| `wait_http <url> [sec]` | poll until a URL answers (NATS `:8222/healthz`) |
| `container_exit_code <svc>` | exit code of a stopped service's container (0 == clean drain, 137 == SIGKILL) |
| `fail <msg>` | dump `docker compose logs` and exit non-zero |
| `install_trap` | tear the project down (`down -v`) on any exit |

## `assert/` — the assertion client

A small Go CLI (`package main`) that reuses the public `clients/go/db`
surface over NATS, so system-level assertions use the exact API consumers
use. Exit code 0 == pass, non-zero == fail, so it drops straight into shell
`if`/`||`.

It connects from the host to the runtime's exposed NATS port; the default
hub domain is `hub` (the effective domain of the `samples/simple` runtime
these tests reuse).

| Subcommand | What it does |
|---|---|
| `ping -wait N` | block until the runtime answers a CMDB request (readiness gate) |
| `seed -n N [-type T -prefix P]` | create a type + a deterministic chain of N objects/links |
| `verify -n N [-type T -prefix P]` | read every seeded object back; fail if any missing / body changed |
| `count -type T [-min M]` | print JPGQL type-enumeration count; optional lower-bound gate |
| `consistency -type T [-n N]` | enumerate a type and assert every member is readable & typed |
| `soak -workers W -duration S [-type T -prefix P]` | sustained concurrent CRUD; fail on any op error |

Shared connection flags: `-nats` (default
`nats://nats:foliage@localhost:4222`), `-domain` (default `hub`),
`-timeout` seconds (default 10).

## Runtime image

The system tests reuse the `samples/simple` binary (registers CRUD, JPGQL,
FPL, search, debug). Each `docker-compose.yaml` builds it as
`foliage-systest-runtime:latest` from `samples/simple/Dockerfile` with the
repo root as build context, so the image is built once and shared across the
test projects.
