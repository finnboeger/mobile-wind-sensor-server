#!/usr/bin/env bash
set -euo pipefail

# Local development launcher for the SAP Sailing uplink service with env-only configuration.
# SAP_URL, SAP_BEARER_TOKEN and SAP_RACES (or SAP_ALL_RACES) must be set by the caller.

export POCKETBASE_URL="${POCKETBASE_URL:-http://localhost:8090}"

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
