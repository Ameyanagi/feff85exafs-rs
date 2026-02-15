# Legacy-to-Modern Migration Guide

This guide maps legacy FEFF85EXAFS interfaces to the modern Rust CLI and API.
Use it when migrating automation away from shell/Python wrappers.

## Interface Mapping

| Legacy interface | Modern CLI | Modern Rust API | Notes |
| --- | --- | --- | --- |
| `feff85exafs/bin/f85e` in a working directory containing `feff.inp` | `cargo run -- workflow run --mode legacy --input <PATH>/feff.inp --working-root <RUN_DIR>` | `RunRequest { mode: RunMode::Legacy, operation: RunOperation::Workflow(WorkflowRunRequest { .. }) }` | CLI defaults `--legacy-runner feff85exafs/bin/f85e` in legacy mode. |
| `feff85exafs/bin/f85e` replacement for native Rust execution | `cargo run -- workflow run --mode modern --input <PATH>/feff.inp --working-root <RUN_DIR>` | `RunRequest { mode: RunMode::Modern, operation: RunOperation::Workflow(WorkflowRunRequest { .. }) }` | Runs native Rust stages and returns `chi.dat`/`xmu.dat` plus non-core artifacts. |
| `feff85exafs/bin/feff8l.sh` or `feff85exafs/bin/feff8l.py` wrapper invocation | Run `workflow run` per case from your own loop/orchestrator | Call `api::run(...)` per case from your own loop/orchestrator | Modern CLI intentionally avoids wrapper-specific flags and folder-scanning behavior. |
| Legacy baseline folders under `feff85exafs/tests/*/baseline/{noSCF,withSCF}` | `cargo run -- baseline <noscf|scf> [--mode <legacy|modern>]` | `RunRequest { operation: RunOperation::Baseline(BaselineRunRequest { .. }) }` | In `legacy` mode, baseline validation enforces canonical legacy filenames. |

## CLI Migration Examples

### Legacy-compatible workflow run

```sh
cargo run -- workflow run \
  --mode legacy \
  --input feff85exafs/tests/Copper/baseline/noSCF/feff.inp \
  --working-root target/workflow-legacy/copper-noscf \
  --legacy-runner feff85exafs/bin/f85e
```

### Modern native workflow run

```sh
cargo run -- workflow run \
  --mode modern \
  --input feff85exafs/tests/Copper/baseline/noSCF/feff.inp \
  --working-root target/workflow-modern/copper-noscf
```

### Baseline manifest generation

```sh
cargo run -- baseline noscf --mode modern --version v1
cargo run -- baseline scf --mode modern --version v1
```

## API Migration Examples

### In-memory request

```rust
use feff85exafs_core::api::{RunOperation, RunRequest, WorkflowRunRequest, run};
use feff85exafs_core::domain::RunMode;

let request = RunRequest {
    mode: RunMode::Modern,
    operation: RunOperation::Workflow(WorkflowRunRequest {
        input_path: "feff85exafs/tests/Copper/baseline/noSCF/feff.inp".to_string(),
        working_root: "target/workflow-modern/copper-noscf".to_string(),
        legacy_runner: None,
    }),
};

let result = run(request)?;
let workflow = result.workflow().expect("workflow result");
println!("chi: {}", workflow.chi_dat);
println!("xmu: {}", workflow.xmu_dat);
# Ok::<(), feff85exafs_errors::FeffError>(())
```

### File-based request (`run_from_file`)

`run-request.json`:

```json
{
  "mode": "modern",
  "operation": {
    "kind": "workflow",
    "input_path": "feff85exafs/tests/Copper/baseline/noSCF/feff.inp",
    "working_root": "target/workflow-modern/copper-noscf",
    "legacy_runner": null
  }
}
```

Then call `feff85exafs_core::api::run_from_file("run-request.json")`.

## Compatibility Mode Behavior

- `--mode legacy` preserves historical core-stage order: `rdinp -> pot -> xsph -> pathfinder -> genfmt -> ff2x`.
- Legacy workflow execution requires a legacy runner path in the API (`WorkflowRunRequest.legacy_runner`).
- CLI legacy workflow defaults to `feff85exafs/bin/f85e` if `--legacy-runner` is not provided.
- Legacy workflow success reports `chi.dat` and `xmu.dat`; non-core outputs are not returned (`sig2_dat`, `exchange_dat`, `fovrg_dat` are `None`).
- Baseline generation in legacy mode validates canonical legacy outputs: `feff.inp`, `files.dat`, `paths.dat`, `xmu.dat`, `chi.dat`, `f85e.log`, and at least one `feffNNNN.dat`.

## Known Differences vs Legacy Wrappers

- `workflow run` does not provide `feff8l.py`-style per-module skip flags (`--no-pot`, `--no-phases`, etc.).
- The modern workflow always executes the full native pipeline, including non-core stages (`debye`, `exch`, `fovrg`) after `ff2x`.
- `workflow run` resets `--working-root` for each invocation to keep outputs deterministic.
- `parity report` and `benchmark report` run in modern mode and compare against legacy baselines/runner; they do not expose a legacy execution mode switch.
- Legacy wrapper path assumptions still apply when using `f85e`: it expects repository-style `bin/` + `src/<MODULE>/` layout.

## Recommended Defaults

- Default to `--mode modern` for new integrations.
- Use `api::run(...)` or `api::run_with_config(RunConfigInput::InMemory { .. })` for programmatic integration.
- Keep run outputs in dedicated folders under `target/` (`target/workflow-run/...`, `target/parity-report`, `target/benchmark-report`).
- Gate regressions with parity thresholds in CI (`cargo run -- parity report --max-abs-delta ... --max-rms-delta ...`).
- Use legacy mode only for compatibility verification or phased migration cutovers.
