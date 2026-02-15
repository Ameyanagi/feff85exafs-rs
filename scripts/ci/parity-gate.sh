#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

MAX_ABS_DELTA="${MAX_ABS_DELTA:-8500}"
MAX_RMS_DELTA="${MAX_RMS_DELTA:-3400}"

cargo run -- parity report \
  --tests-root feff85exafs/tests \
  --working-root target/parity-report-ci \
  --max-abs-delta "$MAX_ABS_DELTA" \
  --max-rms-delta "$MAX_RMS_DELTA" \
  "$@"
