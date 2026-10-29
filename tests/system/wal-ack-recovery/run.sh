#!/usr/bin/env bash
# Exercise the real NATS 2.12.4 crash window, archive/restore, proven recovery
# and refusal of legacy orphans. The fixture owns and removes its containers.
set -euo pipefail
cd "$(dirname "$0")/../../.."
probe_dir=$(mktemp -d "${TMPDIR:-/tmp}/foliage-wal-recovery-bin.XXXXXX")
trap 'rm -rf "$probe_dir"' EXIT

go build -o "$probe_dir/wal-recovery" ./tests/system/_lib/wal-recovery
"$probe_dir/wal-recovery" selftest
