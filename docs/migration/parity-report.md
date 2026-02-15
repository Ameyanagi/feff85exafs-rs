# Parity Report and CI Gate

US-019 adds a parity report command that executes the modern Rust core chain for
all baseline corpus cases and reports per-file numeric deltas against legacy
baseline outputs.

## Report Command

```sh
cargo run -- parity report
```

Defaults:

- `--tests-root feff85exafs/tests`
- `--working-root target/parity-report`

Per-file report rows are emitted for:

- `chi.dat`
- `xmu.dat`

Each row includes:

- `max_abs`: maximum absolute numeric delta
- `rms`: root-mean-square numeric delta
- `mean_abs`: mean absolute numeric delta
- `compared`: compared numeric value count
- `rows`: candidate/baseline numeric row counts

File IDs use this format and can be reused in allowlists:

- `<material>/<variant>/<file>`
- Example: `Copper/noSCF/chi.dat`

## CI Regression Gate

Enable failure on unapproved regressions by setting both thresholds:

```sh
cargo run -- parity report \
  --max-abs-delta 8500 \
  --max-rms-delta 3400
```

When either threshold is exceeded, the file is treated as a regression.
The command exits non-zero if any regression is unapproved.

Optional approvals:

- `--allow-regression <id>` (repeatable)
- `--allow-regression-file <path>` (repeatable; one ID per line, `#` comments supported)

For CI usage, use `scripts/ci/parity-gate.sh`.
