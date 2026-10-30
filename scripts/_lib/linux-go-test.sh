#!/usr/bin/env bash
# Run Go tests in Linux on the local Docker engine. This also provides a
# reproducible race-test environment on hosts with incompatible test brokers.
# Call from the repository root; arguments are passed unchanged to go test.
sdk_go_test_in_linux() (
  set -euo pipefail
  sdk_go_version=$(awk '$1 == "go" { split($2, v, "."); print v[1] "." v[2]; exit }' go.mod)
  [ -n "$sdk_go_version" ] || { echo "cannot read Go version from go.mod" >&2; exit 1; }
  sdk_docker_endpoint=${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}
  case "$sdk_docker_endpoint" in
    unix://*) sdk_docker_socket=${sdk_docker_endpoint#unix://} ;;
    *) echo "Linux test execution requires a local Unix Docker endpoint" >&2; exit 1 ;;
  esac
  sdk_test_container="foliage-systest-go-$$-${RANDOM}"
  trap 'docker rm -f "$sdk_test_container" >/dev/null 2>&1 || true' EXIT INT TERM
  sdk_docker_args=(--rm --name "$sdk_test_container"
    -v "$PWD:/src:${SDK_TEST_SOURCE_MODE:-ro}" -w /src
    -v foliage-test-go-mod:/go/pkg/mod
    -v foliage-test-go-build:/root/.cache/go-build
    -e GOTOOLCHAIN=local -e "CACHE_MODE=${CACHE_MODE:-}")
  if [ "${SDK_TEST_DOCKER_ACCESS:-0}" = 1 ]; then
    # testcontainers must reach sibling containers through the host's
    # published ports, not through this Go container's loopback address.
    sdk_docker_args+=(-v "$sdk_docker_socket:/var/run/docker.sock"
      -e DOCKER_HOST=unix:///var/run/docker.sock
      -e TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
      -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal)
    if [ "$(uname -s)" != Darwin ]; then
      sdk_docker_args+=(--add-host=host.docker.internal:host-gateway)
    fi
  fi
  echo ">> Linux Go ${sdk_go_version}: go test $*"
  docker run "${sdk_docker_args[@]}" "golang:${sdk_go_version}" go test "$@"
)
