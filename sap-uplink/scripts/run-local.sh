#!/usr/bin/env bash
set -euo pipefail

# Local development launcher for the SAP Sailing uplink service with env-only configuration.
# SAP_URL, SAP_USER/SAP_PASSWORD (or SAP_BEARER_TOKEN) and SAP_SOURCES must be set by the caller.

export POCKETBASE_URL="${POCKETBASE_URL:-http://localhost:8090}"
export SAP_SOURCES="${SAP_SOURCES:-bot1=windbot1}"
export SAP_WIND_SOURCE_TYPE="${SAP_WIND_SOURCE_TYPE:-EXPEDITION}"

export LOG_LEVEL="${LOG_LEVEL:-info}"

# Ensure virtual environment is activated
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PATH="$SCRIPT_DIR/.venv"
export SAP_STATE_FILE="${SAP_STATE_FILE:-$SCRIPT_DIR/.state/cursor.json}"

if [[ ! -d "$VENV_PATH" ]]; then
  echo "Creating Python virtual environment..."
  python -m venv "$VENV_PATH"
fi

source "$VENV_PATH/bin/activate"

# Ensure dependencies are installed
echo "Checking dependencies..."
pip install -q -r "$SCRIPT_DIR/requirements.txt"

echo "Starting SAP Sailing uplink service..."
cd "$SCRIPT_DIR"
exec python -m src.main "$@"
