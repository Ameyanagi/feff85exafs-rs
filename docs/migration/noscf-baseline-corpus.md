# noSCF Baseline Corpus Runner

US-003 adds a deterministic corpus generator for noSCF baselines based on
existing legacy test materials in `feff85exafs/tests`.

## Command

```bash
cargo run -- baseline noscf
```

Optional flags:

- `--tests-root <path>`: source test-material root (default: `feff85exafs/tests`)
- `--output-root <path>`: artifact root (default: `docs/migration/baselines`)
- `--version <name>`: corpus version folder (default: `v1`)

## Output Layout

The command writes versioned artifacts to:

`docs/migration/baselines/noSCF/<version>/`

Artifacts include:

- `index.json`: corpus-level index
- `NNN-<material>-noscf.json`: per-material manifest files

Manifests are deterministic:

- materials are sorted lexicographically
- filenames use zero-padded ordering (`001-`, `002-`, ...)
- file entries are sorted by relative path
- SHA-256 hashes are recorded for each baseline file
