#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/start-local-databases.sh
uv run python scripts/prepare-benchmark-additions.py
uv run sfd-pipeline prepare --input data/pipeline/benchmark-additions/sources --work data/pipeline/benchmark-additions/run --limit 0 --chunk-tokens 2000 --chunk-chars 24000
uv run sfd-pipeline embed --work data/pipeline/benchmark-additions/run --embed-batch 64 --embed-workers 4
uv run python scripts/append-prepared-documents.py
uv run python scripts/verify-local-databases.py --work data/pipeline/sample-1000

uv run python scripts/verify-benchmark-additions.py
