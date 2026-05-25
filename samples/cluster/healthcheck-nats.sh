#!/bin/sh
# healthcheck-nats.sh

if [ -z "$NATS_URL" ]; then
    echo "NATS_URL is not set"
    exit 1
fi

nats --server="$NATS_URL" server check jetstream | grep -q "OK JetStream"
if [ $? -eq 0 ]; then
    exit 0
else
    echo "JetStream check failed"
    exit 1
fi