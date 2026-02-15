#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

BENCHMARK_ITERATIONS="${BENCHMARK_ITERATIONS:-1}"
BENCHMARK_WARMUP_ITERATIONS="${BENCHMARK_WARMUP_ITERATIONS:-0}"
BENCHMARK_OUTPUT_PATH="${BENCHMARK_OUTPUT_PATH:-target/benchmark-report-ci/benchmark-report.json}"
LEGACY_RUNNER="${LEGACY_RUNNER:-feff85exafs/bin/f85e}"

cargo run -- benchmark report \
  --tests-root feff85exafs/tests \
  --working-root target/benchmark-report-ci \
  --legacy-runner "$LEGACY_RUNNER" \
  --iterations "$BENCHMARK_ITERATIONS" \
  --warmup-iterations "$BENCHMARK_WARMUP_ITERATIONS" \
  --json-output "$BENCHMARK_OUTPUT_PATH" \
  "$@"
