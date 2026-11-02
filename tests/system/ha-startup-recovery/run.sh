#!/usr/bin/env bash
# Exercise simultaneous domain validation and the HA runtime lifecycle against
# real local NATS brokers. Test cleanup stops runtimes before their brokers.
set -euo pipefail
cd "$(dirname "$0")/../../.."
source scripts/_lib/linux-go-test.sh
race_args=""
if [ "${SDK_TEST_RACE:-0}" = 1 ]; then race_args="-race"; fi
SDK_TEST_DOCKER_ACCESS=1 sdk_go_test_in_linux -v -p 1 -count=1 $race_args \
  -timeout=4m ./statefun -run '^TestDomainState_|^Test_HA_|^Test_ShutdownContext_'
echo '>> ha-startup-recovery: PASS'
