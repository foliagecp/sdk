#!/usr/bin/env bash
# Reuse the synthetic WAL/cache/graph regression assertions inside Linux.
# This fixture deliberately needs no customer snapshots or Docker socket in
# the test container. It is included in both full and --system-only runs.
set -euo pipefail
cd "$(dirname "$0")/../../.."
source scripts/_lib/linux-go-test.sh
race_args=""
if [ "${SDK_TEST_RACE:-0}" = 1 ]; then race_args="-race"; fi
regressions='^Test_RecoveryFloor|^Test_ConditionalCreate|^Test_Shutdown_Retains|^Test_RootCause_RehydrateDoesNotResurrectDeleted$|^TestTrashCanTestSuite$|^TestCMDBClientContractTestSuite$/^Test_RootCause_Rejected|^TestLinkResolveTestSuite$'
sdk_go_test_in_linux -v -p 1 -count=1 $race_args -timeout=8m \
  ./statefun/cache ./embedded/graph/crud -run "$regressions"
echo '>> cache-graph-recovery: PASS'
