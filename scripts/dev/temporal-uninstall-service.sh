#!/usr/bin/env bash
set -euo pipefail

service_name="mezzanine-temporal-dev.service"
legacy_service_name="temporal-dev.service"

if systemctl --user status >/dev/null 2>&1; then
  systemctl --user stop "$service_name" || true
  systemctl --user disable "$service_name" || true
  systemctl --user stop "$legacy_service_name" || true
  systemctl --user disable "$legacy_service_name" || true
  rm -f "$HOME/.config/systemd/user/$service_name" "$HOME/.config/systemd/user/$legacy_service_name"
  systemctl --user daemon-reload
fi

for unit in "$service_name" "$legacy_service_name"; do
  if systemctl list-unit-files "$unit" --no-legend 2>/dev/null | grep -q "$unit"; then
    sudo systemctl stop "$unit" || true
    sudo systemctl disable "$unit" || true
    sudo rm -f "/etc/systemd/system/$unit"
    sudo systemctl daemon-reload
  fi
done

printf 'Removed Mezzanine Temporal dev services from available systemd managers\n'
