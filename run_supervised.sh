#!/usr/bin/env bash
set -uo pipefail

# Simple supervisor loop for running Discovery indefinitely.
# Activate your environment (e.g., conda env) before running this script.

cd "$(dirname "$0")"

# ---- Canonical storage layout (override via env) ----
export PROJECT_K_ROOT="${PROJECT_K_ROOT:-$HOME/ev/Project_K}"
export PROJECT_K_DB_PATH="${PROJECT_K_DB_PATH:-$PROJECT_K_ROOT/data/kalshi.db}"
# Optional: set this to a different file (e.g., $PROJECT_K_ROOT/data/obi.db) to reduce writer contention.
export PROJECT_K_OBI_DB_PATH="${PROJECT_K_OBI_DB_PATH:-$PROJECT_K_DB_PATH}"
export PROJECT_K_TRAINING_DIR="${PROJECT_K_TRAINING_DIR:-/Volumes/external/ev/Project_K/data/training}"
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
