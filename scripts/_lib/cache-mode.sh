# cache-mode.sh — one cache representation per run, named out loud.
#
# The cache holds the graph either as records or as a tree, and which one is
# active changes what the code under test actually is. A run therefore fixes
# ONE of them, says which at the top, and passes it to everything it starts —
# the Go tests in this process and the runtimes inside the compose files.
#
# The default is not written here. It is read from the SDK, so these scripts
# follow whatever the code ships as its default instead of restating it and
# drifting away from it.
#
# Precedence: --cache-mode wins, then CACHE_MODE already in the environment,
# then the SDK default. Whichever it was is printed, so a run always says where
# its mode came from.
#
# Source this, then:
#   cache_mode_resolve "$OVERRIDE"   # empty override => environment, else SDK
#   cache_mode_banner
# CACHE_MODE is exported by the first call; docker compose picks it up through
# ${CACHE_MODE} in the runtime services.

CACHE_MODE_SOURCE=""

cache_mode_default() {
  local m
  m="$(sed -n 's/^const defaultCacheMode = "\(.*\)"$/\1/p' statefun/cache/tiering.go)"
  if [ -z "$m" ]; then
    echo "cannot read defaultCacheMode from statefun/cache/tiering.go" >&2
    return 1
  fi
  printf '%s' "$m"
}

cache_mode_resolve() {
  local override="${1:-}"
  if [ -n "$override" ]; then
    CACHE_MODE="$override"
    CACHE_MODE_SOURCE="--cache-mode"
  elif [ -n "${CACHE_MODE:-}" ]; then
    CACHE_MODE_SOURCE="CACHE_MODE from the environment"
  else
    CACHE_MODE="$(cache_mode_default)" || return 1
    CACHE_MODE_SOURCE="SDK default, statefun/cache/tiering.go"
  fi
  case "$CACHE_MODE" in
    tree|records|zstd|zstd-dict) ;;
    *)
      echo "unknown cache mode: ${CACHE_MODE} (expected tree, records, zstd or zstd-dict)" >&2
      return 2
      ;;
  esac
  export CACHE_MODE
}

cache_mode_banner() {
  echo "=================================================================="
  echo "cache representation under test: ${CACHE_MODE}  (${CACHE_MODE_SOURCE})"
  echo "=================================================================="
}
