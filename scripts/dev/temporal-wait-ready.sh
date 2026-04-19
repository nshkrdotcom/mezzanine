#!/usr/bin/env bash
set -euo pipefail

address="${TEMPORAL_ADDRESS:-127.0.0.1:7233}"
timeout="${TEMPORAL_READY_TIMEOUT:-45}"
deadline=$((SECONDS + timeout))

while true; do
  if TEMPORAL_ADDRESS="$address" temporal operator cluster health >/dev/null 2>&1; then
    printf 'Temporal healthy at %s\n' "$address"
    exit 0
  fi

  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "Temporal did not become healthy at $address within ${timeout}s" >&2
    ./scripts/dev/temporal-service.sh status >&2 || true
    exit 1
  fi

  sleep 1
done
