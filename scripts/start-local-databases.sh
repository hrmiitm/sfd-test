#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# LanceDB is embedded: the ingestion command opens its local directory.
if docker container inspect sfd-qdrant >/dev/null 2>&1; then
  docker start sfd-qdrant >/dev/null
  docker update --restart unless-stopped sfd-qdrant >/dev/null
else
  mkdir -p data/qdrant-server
  docker run -d --name sfd-qdrant --restart unless-stopped \
    -p 127.0.0.1:6333:6333 \
    -v "$PWD/data/qdrant-server:/qdrant/storage" \
    qdrant/qdrant:v1.19.2
fi
printf 'Qdrant: http://localhost:6333 (persistent data/qdrant-server)\n'
printf 'LanceDB: embedded, created by sfd-pipeline in its work directory\n'
