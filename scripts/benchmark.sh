#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Bound the whole benchmark process tree, including native libraries/workers.
# Fail closed if a user systemd manager is unavailable; do not run unbounded.
exec systemd-run --user --scope --quiet \
  -p MemoryMax=2G -p MemorySwapMax=0 -p CPUQuota=100% \
  -p TasksMax=128 -p OOMPolicy=kill \
  uv run python -m sfd_2025.benchmark.run "$@"
