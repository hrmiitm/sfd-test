#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/start-local-databases.sh
uv run python scripts/prepare-benchmark-sources.py
uv run sfd-pipeline all \
  --input data/pipeline/benchmark-fast/sources \
  --work data/pipeline/benchmark-fast/run \
  --limit 0 --chunk-tokens 2000 --chunk-chars 24000 \
  --embed-batch 64 --embed-workers 4 \
  --qdrant-url http://localhost:6333 "$@"
uv run python scripts/verify-local-databases.py --work data/pipeline/benchmark-fast/run
