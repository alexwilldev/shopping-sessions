#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# PIPELINE selects the engine: pipeline_polars.py (default) or pipeline.py (DuckDB).
export POLARS_MAX_THREADS="${NCPU:-8}"
exec python3 "/app/pipeline/${PIPELINE:-pipeline_polars.py}"
