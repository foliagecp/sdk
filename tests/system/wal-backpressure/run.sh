#!/usr/bin/env bash
# Exercise real JetStream writes with delayed PubAcks and the runtime's
# bounded async publisher. Included in full and --system-only runs.
set -euo pipefail
cd "$(dirname "$0")/../../.."
source scripts/_lib/linux-go-test.sh
race_args=""
if [ "${SDK_TEST_RACE:-0}" = 1 ]; then race_args="-race"; fi
sdk_go_test_in_linux -v -p 1 -count=1 $race_args -timeout=2m \
  ./statefun -run '^TestWALBackpressure_'
echo '>> wal-backpressure: PASS'
