#!/usr/bin/env bash
set -euo pipefail
# Relative CLI paths are resolved inside this standalone project.
cd "$(dirname "$0")/.."
# Bound dependency setup and the complete benchmark/worker tree.
exec systemd-run --user --scope --quiet \
  -p MemoryMax=2G -p MemorySwapMax=0 -p CPUQuota=100% \
  -p TasksMax=128 -p OOMPolicy=kill \
  uv run --locked python -m agentic_search_only.parquet_agent "$@"
