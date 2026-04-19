set dotenv-load := true
set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

temporal_address := env_var_or_default("TEMPORAL_ADDRESS", "127.0.0.1:7233")
temporal_ui_url := env_var_or_default("TEMPORAL_UI_URL", "http://127.0.0.1:8233")

_default:
    @just --list

# Start the native local Mezzanine/Temporal developer substrate.
dev-up: temporal-start

# Stop the native local Mezzanine/Temporal developer substrate.
dev-down: temporal-stop

# Show local developer substrate status.
dev-status: temporal-status

# Tail local developer substrate logs.
dev-logs: temporal-logs

# Install or refresh the Mezzanine-owned Temporal systemd service.
temporal-install-service:
    ./scripts/dev/temporal-install-service.sh

# Start Temporal through the Mezzanine-owned systemd service.
temporal-start: temporal-install-service
    ./scripts/dev/temporal-service.sh start
    ./scripts/dev/temporal-wait-ready.sh

# Stop the Temporal systemd service.
temporal-stop:
    ./scripts/dev/temporal-service.sh stop || true

# Restart Temporal through the Mezzanine-owned systemd service.
temporal-restart: temporal-install-service
    ./scripts/dev/temporal-service.sh restart
    ./scripts/dev/temporal-wait-ready.sh

# Show service status and verify Temporal health.
temporal-status:
    @./scripts/dev/temporal-service.sh status || true
    @TEMPORAL_ADDRESS={{temporal_address}} temporal operator cluster health

# Tail Temporal service logs.
temporal-logs:
    ./scripts/dev/temporal-service.sh logs

# Open Temporal Web UI, or print the URL if no opener is available.
temporal-ui:
    @xdg-open {{temporal_ui_url}} >/dev/null 2>&1 || printf '%s\n' '{{temporal_ui_url}}'

# Refuse accidental destructive resets; use temporal-reset-confirm intentionally.
temporal-reset:
    @printf '%s\n' 'Refusing to reset persistent Temporal dev state by default.'
    @printf '%s\n' 'Run `just temporal-reset-confirm` only when losing local workflow history is acceptable.'

# Stop Temporal, remove persistent dev state, restart, and wait for health.
temporal-reset-confirm: temporal-install-service
    ./scripts/dev/temporal-service.sh stop || true
    rm -f "$HOME/.local/share/temporal/dev-server.db"
    ./scripts/dev/temporal-service.sh start
    ./scripts/dev/temporal-wait-ready.sh

# Disable and remove the Mezzanine-owned Temporal service.
temporal-uninstall-service:
    ./scripts/dev/temporal-uninstall-service.sh
