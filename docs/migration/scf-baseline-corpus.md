# withSCF Baseline Corpus Runner

US-004 extends the baseline corpus generator to produce deterministic `withSCF`
manifests using the same material set as the existing `noSCF` corpus.

## Command

```bash
cargo run -- baseline scf
```

Equivalent variant aliases:

- `cargo run -- baseline withscf`
- `cargo run -- baseline with-scf`

Optional flags:

- `--tests-root <path>`: source test-material root (default: `feff85exafs/tests`)
- `--output-root <path>`: artifact root (default: `docs/migration/baselines`)
- `--version <name>`: corpus version folder (default: `v1`)

## Environment Requirements

- Run from the repository root so default relative paths resolve correctly.
- Rust toolchain with Cargo available (`cargo --version` succeeds).
- Legacy baseline corpus present at
  `feff85exafs/tests/*/baseline/{noSCF,withSCF}`.
- The command enforces matching material coverage between `noSCF` and
  `withSCF`; it fails if either variant is missing a material.

## Output Layout

The command writes versioned artifacts to:

`docs/migration/baselines/withSCF/<version>/`

Artifacts include:

- `index.json`: corpus-level index
- `NNN-<material>-withscf.json`: per-material manifest files

Manifests are deterministic:

- materials are sorted lexicographically
- filenames use zero-padded ordering (`001-`, `002-`, ...)
- file entries are sorted by relative path
- SHA-256 hashes are recorded for each baseline file
