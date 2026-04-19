#!/usr/bin/env bash
set -euo pipefail

TEMPORAL_IP="${TEMPORAL_IP:-127.0.0.1}"
TEMPORAL_PORT="${TEMPORAL_PORT:-7233}"
TEMPORAL_UI_PORT="${TEMPORAL_UI_PORT:-8233}"
TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-default}"
TEMPORAL_DEV_DATA_DIR="${TEMPORAL_DEV_DATA_DIR:-$HOME/.local/share/temporal}"
TEMPORAL_DB_FILENAME="${TEMPORAL_DB_FILENAME:-$TEMPORAL_DEV_DATA_DIR/dev-server.db}"

mkdir -p "$TEMPORAL_DEV_DATA_DIR"

temporal_bin="${TEMPORAL_BIN:-}"
if [ -z "$temporal_bin" ]; then
  for candidate in /usr/local/bin/temporal /usr/bin/temporal "$(command -v temporal || true)"; do
    if [ -n "$candidate" ] && [ -x "$candidate" ]; then
      temporal_bin="$candidate"
      break
    fi
  done
fi

if [ -z "$temporal_bin" ]; then
  echo "Temporal CLI not found. Install it with dotfiles_private/linux/setup/install_temporal.sh." >&2
  exit 127
fi

exec "$temporal_bin" server start-dev \
  --ip "$TEMPORAL_IP" \
  --ui-ip "$TEMPORAL_IP" \
  --port "$TEMPORAL_PORT" \
  --ui-port "$TEMPORAL_UI_PORT" \
  --namespace "$TEMPORAL_NAMESPACE" \
  --db-filename "$TEMPORAL_DB_FILENAME"
