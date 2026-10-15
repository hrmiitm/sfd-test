#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec systemd-run --user --scope --quiet \
  -p MemoryMax=2G -p MemorySwapMax=0 -p CPUQuota=100% \
  -p TasksMax=128 -p OOMPolicy=kill \
  uv run --locked python scripts/analyze-results.py "$@"
