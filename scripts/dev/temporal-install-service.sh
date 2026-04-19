#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
service_name="mezzanine-temporal-dev.service"
legacy_service_name="temporal-dev.service"
current_user="$(id -un)"
current_group="$(id -gn)"

user_systemd_available() {
  systemctl --user status >/dev/null 2>&1
}

mkdir -p "$HOME/.local/share/temporal"

# Retire the previous jido_brainstorm-owned service name if present.
if systemctl list-unit-files "$legacy_service_name" --no-legend 2>/dev/null | grep -q "$legacy_service_name"; then
  sudo systemctl stop "$legacy_service_name" || true
  sudo systemctl disable "$legacy_service_name" || true
  sudo rm -f "/etc/systemd/system/$legacy_service_name"
  sudo systemctl daemon-reload
fi

if user_systemd_available; then
  unit_dir="$HOME/.config/systemd/user"
  unit_file="$unit_dir/$service_name"
  mkdir -p "$unit_dir"

  rm -f "$HOME/.config/systemd/user/$legacy_service_name"

  cat > "$unit_file" <<UNIT
[Unit]
Description=Mezzanine Temporal local dev server
After=network.target

[Service]
Type=simple
WorkingDirectory=$repo_root
Environment=TEMPORAL_ADDRESS=127.0.0.1:7233
Environment=TEMPORAL_NAMESPACE=default
Environment=TEMPORAL_IP=127.0.0.1
Environment=TEMPORAL_PORT=7233
Environment=TEMPORAL_UI_PORT=8233
Environment=TEMPORAL_DEV_DATA_DIR=%h/.local/share/temporal
ExecStart=$repo_root/scripts/dev/temporal-dev-start.sh
Restart=on-failure
RestartSec=2
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=default.target
UNIT

  systemctl --user daemon-reload
  systemctl --user enable "$service_name" >/dev/null
  printf 'Installed user service %s\n' "$unit_file"
else
  unit_file="/etc/systemd/system/$service_name"
  tmp_unit="$(mktemp)"
  trap 'rm -f "$tmp_unit"' EXIT

  cat > "$tmp_unit" <<UNIT
[Unit]
Description=Mezzanine Temporal local dev server
After=network.target

[Service]
Type=simple
User=$current_user
Group=$current_group
WorkingDirectory=$repo_root
Environment=HOME=$HOME
Environment=TEMPORAL_ADDRESS=127.0.0.1:7233
Environment=TEMPORAL_NAMESPACE=default
Environment=TEMPORAL_IP=127.0.0.1
Environment=TEMPORAL_PORT=7233
Environment=TEMPORAL_UI_PORT=8233
Environment=TEMPORAL_DEV_DATA_DIR=$HOME/.local/share/temporal
ExecStart=$repo_root/scripts/dev/temporal-dev-start.sh
Restart=on-failure
RestartSec=2
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
UNIT

  sudo install -m 0644 "$tmp_unit" "$unit_file"
  sudo systemctl daemon-reload
  sudo systemctl enable "$service_name" >/dev/null
  printf 'Installed system service %s running as %s\n' "$unit_file" "$current_user"
fi
