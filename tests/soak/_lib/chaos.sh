#!/usr/bin/env bash
#
# chaos.sh — small library of fault-injection helpers used by the soak
# scenarios. They wrap `docker compose` actions on a specific service and
# echo timestamped event markers so the observer (and the post-mortem
# operator) can correlate stalls to the chaos that caused them.
#
# Source AFTER tests/soak/_lib/compose.sh so $PROJECT/$COMPOSE_FILE and the
# `dc` wrapper are already defined.
#
# Event log: every chaos primitive appends a line to $CHAOS_EVENTS_FILE (set
# by the scenario, usually $RUN_DIR/events.csv) of the form
#   <unix_ts>,<event>,<service>,<extra>
# The observer can ingest this to compute recovery latencies.

set -uo pipefail

# Sleep guard — `sleep` on Alpine variants of busybox doesn't accept floats,
# but docker compose's container is irrelevant; we run on the host, so plain
# `sleep` is fine. Keep this helper centralised so soak scripts call _sleep
# and it can be replaced later (e.g. for accelerated test mode).
_sleep() { sleep "$1"; }

# now_ts prints the current unix timestamp (seconds).
now_ts() { date -u +%s; }

# log_event appends one CSV row to the event log. Created by the first event.
log_event() {
  local ev="$1"; shift
  local svc="${1:-}"; shift || true
  local extra="${1:-}"
  local file="${CHAOS_EVENTS_FILE:-/dev/null}"
  if [ "$file" != "/dev/null" ] && [ ! -f "$file" ]; then
    echo "ts,event,service,extra" >"$file"
  fi
  printf '%s,%s,%s,%s\n' "$(now_ts)" "$ev" "$svc" "$extra" >>"$file"
}

# chaos_nats_stall pauses NATS for $1 seconds, then unpauses. This simulates
# the 116-class incident: the runtime sees its NATS connection stall (no acks,
# no reads), reacts (potentially `becomePassive`), and must recover when the
# broker returns.
#
# Records two events: nats_pause / nats_unpause. The interval between them is
# the stall duration; the interval from nats_unpause to the next successful
# ping is the recovery time (computed by the observer).
chaos_nats_stall() {
  local secs="${1:-15}"
  local svc="${2:-nats}"
  echo ">> chaos: pausing $svc for ${secs}s"
  log_event "nats_pause" "$svc" "${secs}s"
  dc pause "$svc" >/dev/null 2>&1 || { echo ">> chaos: pause failed" >&2; return 1; }
  _sleep "$secs"
  echo ">> chaos: unpausing $svc"
  dc unpause "$svc" >/dev/null 2>&1 || { echo ">> chaos: unpause failed" >&2; return 1; }
  log_event "nats_unpause" "$svc" "${secs}s"
}

# chaos_kill_restart SIGKILLs a service and brings it back up on the same
# volume. Used by ha-promotion-flap (kill the active runtime) and by recovery
# checks (kill NATS but persistent store survives).
#
# Records kill / restart events.
chaos_kill_restart() {
  local svc="$1"
  local pause="${2:-2}"
  echo ">> chaos: SIGKILL $svc"
  log_event "kill" "$svc" ""
  dc kill -s SIGKILL "$svc" >/dev/null 2>&1 || { echo ">> chaos: kill failed" >&2; return 1; }
  _sleep "$pause"
  echo ">> chaos: restarting $svc"
  dc up -d "$svc" >/dev/null 2>&1 || { echo ">> chaos: restart failed" >&2; return 1; }
  log_event "restart" "$svc" ""
}

# pick_random returns a random element from "$@". Used by the stall scenario
# to vary the pause duration between 5/15/30s so the recovery path is
# exercised at multiple TTL boundaries.
pick_random() {
  local n=$#
  if [ "$n" -eq 0 ]; then return 1; fi
  local idx=$(( RANDOM % n ))
  shift "$idx" 2>/dev/null || true
  echo "$1"
}
