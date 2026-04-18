#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PB_DIR="${PROJECT_ROOT}/pocketbase"

if ! command -v pocketbase >/dev/null 2>&1; then
  echo "Error: pocketbase is not installed or not in PATH."
  echo "Install it first, then re-run this script."
  echo "macOS (Homebrew): brew install pocketbase"
  exit 1
fi

mkdir -p "${PB_DIR}"

echo "Starting PocketBase with data directory: ${PB_DIR}"
exec pocketbase serve --dir "${PB_DIR}"
