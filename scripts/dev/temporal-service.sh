#!/usr/bin/env bash
set -euo pipefail

action="${1:-status}"
service_name="mezzanine-temporal-dev.service"

user_systemd_available() {
  systemctl --user status >/dev/null 2>&1
}

user_unit_exists() {
  [ -f "$HOME/.config/systemd/user/$service_name" ] && user_systemd_available
}

system_unit_exists() {
  systemctl list-unit-files "$service_name" --no-legend 2>/dev/null | grep -q "$service_name"
}

if user_unit_exists; then
  case "$action" in
    start) systemctl --user start "$service_name" ;;
    stop) systemctl --user stop "$service_name" ;;
    restart) systemctl --user restart "$service_name" ;;
    status) systemctl --user --no-pager --lines=20 status "$service_name" ;;
    logs) journalctl --user -u "$service_name" -f ;;
    *) echo "Unsupported service action: $action" >&2; exit 64 ;;
  esac
elif system_unit_exists; then
  case "$action" in
    start) sudo systemctl start "$service_name" ;;
    stop) sudo systemctl stop "$service_name" ;;
    restart) sudo systemctl restart "$service_name" ;;
    status) systemctl --no-pager --lines=20 status "$service_name" ;;
    logs) journalctl -u "$service_name" -f ;;
    *) echo "Unsupported service action: $action" >&2; exit 64 ;;
  esac
else
  echo "Temporal service is not installed. Run: just temporal-install-service" >&2
  exit 69
fi
