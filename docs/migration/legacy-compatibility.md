# Legacy Compatibility Integration Suite

## Covered Legacy Workflows

- The unchanged `feff85exafs/bin/f85e` script is executed in integration tests at `tests/legacy_automation_path.rs`.
- Tests mirror the historical script layout requirement (`bin/f85e` with sibling `src/<MODULE>/<executable>` modules).
- The suite runs representative corpus inputs for both execution variants:
  - `feff85exafs/tests/Copper/baseline/noSCF/feff.inp`
  - `feff85exafs/tests/Copper/baseline/withSCF/feff.inp`
- Assertions verify historical stage order (`rdinp -> pot -> xsph -> pathfinder -> genfmt -> ff2x`) and canonical output artifacts (`files.dat`, `paths.dat`, `xmu.dat`, `chi.dat`, `feff0001.dat`).

## Known Compatibility Exceptions

- Automated legacy workflow integration tests run only on Unix-like environments because legacy orchestration scripts are shell-based.
- Integration tests intentionally use mocked module executables under `src/` to validate unchanged script orchestration without requiring legacy Fortran builds.
- The `f85e` script still depends on repository-style path derivation (`bin/f85e` adjacent to `src/`); alternate installed layouts are not covered by automated compatibility tests.
- Legacy wrappers `feff8l.sh` and `feff8l.py` are not part of this automated compatibility suite and require separate/manual validation.
