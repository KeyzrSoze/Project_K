#!/usr/bin/env bash
set -uo pipefail

# Simple supervisor loop for running Discovery indefinitely.
# Activate your environment (e.g., conda env) before running this script.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables from .env if present.
# This enables toggles like PROJECT_K_TRADING_ENABLED without manual export.
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# ---- Canonical storage layout (override via env) ----
# Defaults are repo-relative for portability.
export PROJECT_K_ROOT="${PROJECT_K_ROOT:-$SCRIPT_DIR}"
export PROJECT_K_DB_PATH="${PROJECT_K_DB_PATH:-$PROJECT_K_ROOT/data/kalshi.db}"
# Optional: set this to a different file (e.g., $PROJECT_K_ROOT/data/obi.db) to reduce writer contention.
export PROJECT_K_OBI_DB_PATH="${PROJECT_K_OBI_DB_PATH:-$PROJECT_K_DB_PATH}"
# Default training output is repo-relative; make $PROJECT_K_ROOT/data/training a symlink to your external drive.
export PROJECT_K_TRAINING_DIR="${PROJECT_K_TRAINING_DIR:-$PROJECT_K_ROOT/data/training}"
export PROJECT_K_DB_READ_WORKERS="${PROJECT_K_DB_READ_WORKERS:-4}"

while true; do
  echo "[run_supervised] starting: $(date)  pwd=$(pwd)"
  python main.py
  code=$?

  if [ "${code}" -eq 2 ]; then
    echo "[run_supervised] main.py exited with code=2 (watchdog). restarting in 2s... $(date)"
    sleep 2
    continue
  fi

  echo "[run_supervised] main.py exited with code=${code}. exiting. $(date)"
  exit "${code}"
done
