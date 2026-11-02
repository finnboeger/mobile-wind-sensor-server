#!/usr/bin/env bash
set -euo pipefail

# Local development launcher for the SAP Sailing uplink service with env-only configuration.
# The SAP settings come from the sap-dashboard page (sap_settings) or from SAP_* variables set by the caller.

export POCKETBASE_URL="${POCKETBASE_URL:-http://localhost:8090}"
export POCKETBASE_ADMIN_EMAIL="${POCKETBASE_ADMIN_EMAIL:-admin@example.com}"
export POCKETBASE_ADMIN_PASSWORD="${POCKETBASE_ADMIN_PASSWORD:-adminadmin}"

export LOG_LEVEL="${LOG_LEVEL:-info}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PATH="$SCRIPT_DIR/.venv"
export SAP_STATE_FILE="${SAP_STATE_FILE:-$SCRIPT_DIR/.state/cursor.json}"

if [[ ! -d "$VENV_PATH" ]]; then
  echo "Creating Python virtual environment..."
  python3 -m venv "$VENV_PATH"
fi

source "$VENV_PATH/bin/activate"

echo "Checking dependencies..."
pip install -q -r "$SCRIPT_DIR/requirements.txt"

echo "Starting SAP Sailing uplink service..."
cd "$SCRIPT_DIR"
exec python -m src.main "$@"
