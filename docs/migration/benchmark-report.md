# Benchmark Harness and CI Output

US-020 adds a benchmark command that measures modern Rust execution versus the
legacy `f85e` automation path across the baseline corpus.

## Benchmark Command

```sh
cargo run -- benchmark report
```

Defaults:

- `--tests-root feff85exafs/tests`
- `--working-root target/benchmark-report`
- `--legacy-runner feff85exafs/bin/f85e`
- `--iterations 1`
- `--warmup-iterations 0`
- `--json-output target/benchmark-report/benchmark-report.json`

Useful tuning:

- Increase `--iterations` to reduce timing noise.
- Use `--warmup-iterations` to amortize first-run filesystem/cache effects.

## Machine-Readable Report

The command always writes a JSON report to `--json-output` for CI trend tracking.

Top-level fields:

- `tests_root`, `working_root`, `legacy_runner`
- `iterations`, `warmup_iterations`
- `cases[]` with per-case Rust and legacy timing samples/statistics
- `summary` with aggregate mean totals and Rust/legacy ratio + speedup

Per-case IDs use:

- `<material>/<variant>`
- Example: `Copper/noSCF`

## CI Integration

Use `scripts/ci/benchmark-report.sh` in CI.

Example:

```sh
BENCHMARK_ITERATIONS=3 \
BENCHMARK_WARMUP_ITERATIONS=1 \
BENCHMARK_OUTPUT_PATH=target/benchmark-report-ci/report.json \
./scripts/ci/benchmark-report.sh
```
