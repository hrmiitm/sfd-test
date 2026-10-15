#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Annual/quarterly reports plus the five non-financial-form sources referenced
# by the existing questions.json benchmark. Does not alter any question files.
./scripts/run-financial-subset.sh \
  --work data/pipeline/financial-benchmark \
  --include-accessions \
  0000829224-25-000006 \
  0000950170-25-011901 \
  0001104659-25-001594 \
  0000077476-25-000008 \
  0000078003-25-000017 "$@"
