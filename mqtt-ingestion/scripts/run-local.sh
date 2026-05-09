#!/usr/bin/env bash
set -euo pipefail

# Local development launcher for MQTT ingestion service with env-only configuration.

export POCKETBASE_URL="${POCKETBASE_URL:-http://localhost:8090}"
export POCKETBASE_ADMIN_EMAIL="${POCKETBASE_ADMIN_EMAIL:-admin@example.com}"
export POCKETBASE_ADMIN_PASSWORD="${POCKETBASE_ADMIN_PASSWORD:-adminadmin}"

export MQTT_HOST="${MQTT_HOST:-localhost}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export MQTT_USER="${MQTT_USER:-}"
export MQTT_PASSWORD="${MQTT_PASSWORD:-}"
export MQTT_TOPICS="${MQTT_TOPICS:-bot1=bot1}"

export LOG_LEVEL="${LOG_LEVEL:-info}"

# Ensure virtual environment is activated
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PATH="$SCRIPT_DIR/.venv"

if [[ ! -d "$VENV_PATH" ]]; then
  echo "Creating Python virtual environment..."
  python -m venv "$VENV_PATH"
fi

source "$VENV_PATH/bin/activate"

# Ensure dependencies are installed
echo "Checking dependencies..."
pip install -q -r "$SCRIPT_DIR/requirements.txt"

echo "Starting MQTT ingestion service..."
cd "$SCRIPT_DIR"
exec python -m src.main
