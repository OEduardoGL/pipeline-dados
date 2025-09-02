#!/usr/bin/env bash
set -euo pipefail

# Wrapper for spark-submit that automatically ships the local package to executors.
# Usage: scripts/spark.sh [spark-submit args] <job.py> [job args]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ZIP_PATH="$ROOT_DIR/pipe2.zip"

# Build or refresh the zip if missing or sources are newer
if [[ ! -f "$ZIP_PATH" ]]; then
  (cd "$ROOT_DIR/src" && zip -rq "$ZIP_PATH" pipe2)
else
  # If any file under src/pipe2 is newer than the zip, rebuild
  if find "$ROOT_DIR/src/pipe2" -type f -newer "$ZIP_PATH" | read; then
    (cd "$ROOT_DIR/src" && rm -f "$ZIP_PATH" && zip -rq "$ZIP_PATH" pipe2)
  fi
fi

# Prefer venv spark-submit if present
if [[ -x "$ROOT_DIR/.venv/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$ROOT_DIR/.venv/bin/spark-submit"
else
  SPARK_SUBMIT="spark-submit"
fi

exec "$SPARK_SUBMIT" --py-files "$ZIP_PATH" "$@"

