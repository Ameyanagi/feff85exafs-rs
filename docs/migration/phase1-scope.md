# Phase 1 Core Scope and Release Gates

## Objective

Phase 1 is complete when the core FEFF85EXAFS workflow runs in modern Rust mode with reproducible parity against approved legacy baselines for both noSCF and SCF corpus cases.

This scope definition builds on `docs/migration/module-map.md`.

## Module Scope

### Core modules (must be natively implemented for Phase 1 completion)

- `RDINP`
- `POT`
- `XSPH`
- `PATH` (`pathfinder`)
- `GENFMT`
- `FF2X`

### Supporting modules (required as dependencies during Phase 1, but not independent release targets)

- `COMMON`
- `PAR`
- `MATH`
- `JSON`
- `ATOM`
- `FMS`
- `json-fortran` compatibility surface (until fully removed)

### Non-core modules (explicitly out of Phase 1 completion criteria)

- `DEBYE`
- `OPCONSAT`
- `feff6l`
- Full standalone ports of `EXCH` and `FOVRG` (only compatibility behavior needed by core stages is in scope during Phase 1)

## Core Workflow Release Gates

Each workflow must satisfy both **feature parity gates** and **test parity gates**.

| Core workflow | Feature parity gates | Test parity gates |
| --- | --- | --- |
| `RDINP` input ingestion | Parses all cards required by the approved Phase 1 corpus; preserves historical defaults required by downstream stages; returns typed errors for malformed/unsupported cards. | Unit tests cover required cards and error paths; corpus-driven golden tests verify equivalent intermediate outputs for noSCF and SCF cases. |
| `POT` potential generation | Consumes Rust `RDINP` output without legacy shell orchestration; produces artifacts needed by `XSPH`/`GENFMT`/`FF2X`; preserves required historical behavior in modern mode. | Stage integration tests run against corpus fixtures; parity checks against baseline artifacts pass using configured tolerances. |
| `XSPH` phase-shift workflow | Consumes `POT` artifacts from modern mode; emits expected phase/related outputs required by downstream stages. | Stage integration tests validate artifact presence and content; numeric parity checks pass within configured tolerances. |
| `PATH` pathfinder workflow | Generates scattering path outputs compatible with modern `GENFMT`; preserves required ordering/selection behavior for corpus cases. | Corpus tests compare generated path outputs with legacy baseline; regressions fail parity tests. |
| `GENFMT` formatting workflow | Produces formatted path/f-matrix artifacts consumable by `FF2X`; maintains required output contracts for Phase 1 workflows. | Stage tests compare generated artifacts to baseline references; parity failures block release gate. |
| `FF2X` final EXAFS workflow | Produces final core outputs (`chi`/`xmu` related artifacts) from modern upstream stages without mandatory legacy fallbacks. | End-stage parity tests verify outputs against baseline corpus for noSCF and SCF scenarios. |
| End-to-end core workflow (`rdinp -> pot -> xsph -> pathfinder -> genfmt -> ff2x`) | Modern mode executes all core stages natively in required order for both noSCF and SCF runs. | Full integration suite passes on approved corpus; parity report shows no unapproved regressions; typecheck and test suites are green. |

## Exit Criteria for Phase 1

Phase 1 release readiness requires all of the following:

- All core workflow gates above are passing.
- No required core stage depends on mandatory legacy binary execution in modern mode.
- Parity checks for approved noSCF and SCF corpus cases pass under documented tolerances.
- Project quality checks required by CI (typecheck/tests, and lint where configured) pass.
