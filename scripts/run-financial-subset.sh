#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Exact 10-K / 10-Q only, all six months, separate from existing databases.
./scripts/run-local-pipeline.sh \
  --forms 10-K 10-Q --limit 0 \
  --work data/pipeline/financial-10k-10q "$@"
