#!/bin/sh
# Native (no Docker) runner for run_fixtures.py: host_run.sh DATA_DIR OUTPUT_DIR. BIN overrides the binary.
set -eu
mkdir -p "$2"
DATA_DIR="$1" OUTPUT_DIR="$2" NCPU="${NCPU:-8}" exec "${BIN:-$(dirname "$0")/../rust/target/release/shopsess}" 2>/dev/null
