#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/start-local-databases.sh
uv run sfd-pipeline all --qdrant-url http://localhost:6333 "$@"
