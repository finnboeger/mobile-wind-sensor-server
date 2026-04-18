#!/usr/bin/env bash
set -euo pipefail

# Local development launcher for backend with env-only configuration.

export POCKETBASE_URL="${POCKETBASE_URL:-http://localhost:8090}"
export POCKETBASE_ADMIN_EMAIL="${POCKETBASE_ADMIN_EMAIL:-admin@example.com}"
export POCKETBASE_ADMIN_PASSWORD="${POCKETBASE_ADMIN_PASSWORD:-admin}"

export DEFAULT_TIMEFRAME_MINUTES="${DEFAULT_TIMEFRAME_MINUTES:-15}"
export DEFAULT_SMOOTHING_WINDOW_MINUTES="${DEFAULT_SMOOTHING_WINDOW_MINUTES:-1}"
export PORT="${PORT:-3000}"
export HOST="${HOST:-0.0.0.0}"
export LOG_LEVEL="${LOG_LEVEL:-info}"

exec npm run dev:backend
