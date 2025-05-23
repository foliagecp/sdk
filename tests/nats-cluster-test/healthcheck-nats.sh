#!/bin/sh
# healthcheck-nats.sh

if [ -z "$NATS_URL" ]; then
  echo "NATS_URL is not set"
  exit 1
fi

nats --server="$NATS_URL" server check jetstream | grep -q "OK JetStream"