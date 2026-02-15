# PRD: feff85exafs Rust Migration

## 1. Introduction/Overview

Migrate `feff85exafs` from the current Fortran/C-centric implementation to Rust so it can integrate directly with other Rust code while improving performance, reliability, and memory safety.

This migration will be phased:

- Phase 1: incremental migration (module-by-module) with production-ready core workflows.
- Phase 2: full Rust rewrite of all remaining modules.

Backward compatibility is required, but the product must also expose a modern API. The system will support both:

- Legacy-compatible mode for exact behavior/interface parity.
- Modern mode for improved developer ergonomics, with documented migration guidance.

## 2. Goals

- Provide a Rust-native library API for running feff85exafs workflows from other Rust applications.
- Preserve legacy behavior and interfaces via an explicit compatibility mode.
- Improve runtime performance for core workflows versus current baseline.
- Improve reliability (fewer runtime failures) and memory safety guarantees.
- Reach full module coverage in Rust after phased migration.

## 3. User Stories

### US-001: Define migration architecture and module map

**Description:** As a maintainer, I want a clear module migration map so the team can move incrementally without breaking core workflows.

**Acceptance Criteria:**

- [ ] A migration map lists all current modules and dependencies (including `rdinp`, `pot`, `xsph`, `pathfinder`, `genfmt`, `ff2x`, and supporting libraries).
- [ ] Each module is tagged as `core` or `non-core` for Phase 1 planning.
- [ ] The execution order and data handoff contracts between modules are documented.
- [ ] `cargo check` and lint pass.

### US-002: Build a baseline parity corpus

**Description:** As a developer, I want baseline outputs captured from the existing implementation so Rust results can be validated objectively.

**Acceptance Criteria:**

- [ ] Baseline outputs are generated for agreed reference materials/workflows using existing test inputs.
- [ ] The corpus includes both SCF and non-SCF variants where applicable.
- [ ] Stored baseline artifacts are versioned and reproducible.
- [ ] `cargo check` and lint pass.

### US-003: Create Rust workspace and shared domain model

**Description:** As a developer, I want a Rust workspace with explicit domain types so modules share validated, typed data structures.

**Acceptance Criteria:**

- [ ] Rust crates are created for core library logic and CLI/runtime entry points.
- [ ] Shared types are defined for input cards, atomic data, path data, and output tables.
- [ ] Error handling uses typed errors with actionable messages.
- [ ] `cargo check` and lint pass.

### US-004: Implement legacy-compatible orchestration mode

**Description:** As a legacy user, I want an option that preserves historical behavior so existing scripts and workflows continue to run.

**Acceptance Criteria:**

- [ ] A `legacy` compatibility mode is available from CLI and library API.
- [ ] In `legacy` mode, stage ordering, file names, and interfaces match current behavior.
- [ ] Existing automation/scripts run without required changes in compatibility mode.
- [ ] `cargo check` and lint pass.

### US-005: Implement modern Rust API mode

**Description:** As a Rust integrator, I want a modern typed API so I can call feff85exafs directly without shelling out to legacy binaries.

**Acceptance Criteria:**

- [ ] A Rust API exposes programmatic run configuration and execution without temporary script wrappers.
- [ ] API supports in-memory configuration plus file-based configuration.
- [ ] API errors return structured diagnostics (no string-only failures).
- [ ] A migration guide documents differences from the legacy interface.
- [ ] `cargo check` and lint pass.

### US-006: Port RDINP to Rust with parity checks

**Description:** As a developer, I want `RDINP` behavior in Rust so the pipeline can begin with native parsing and validated inputs.

**Acceptance Criteria:**

- [ ] Rust `RDINP` implementation parses accepted input variants used in existing test materials.
- [ ] Generated intermediate artifacts match legacy outputs within defined tolerance rules.
- [ ] Regression tests cover malformed input and expected error paths.
- [ ] `cargo check` and lint pass.

### US-007: Port POT to Rust with parity checks

**Description:** As a developer, I want `POT` in Rust so the core physics pipeline can run natively for core workflows.

**Acceptance Criteria:**

- [ ] Rust `POT` implementation produces parity-matching outputs for core benchmark cases.
- [ ] Differences versus baseline are reported automatically with numeric deltas.
- [ ] Failure paths are deterministic and reproducible.
- [ ] `cargo check` and lint pass.

### US-008: Port remaining core modules to Rust

**Description:** As a maintainer, I want `XSPH`, `pathfinder`, `GENFMT`, and `FF2X` ported so core workflows are fully native in Phase 1.

**Acceptance Criteria:**

- [ ] Core modules are migrated in Rust and integrated into the native execution pipeline.
- [ ] End-to-end core workflows pass parity checks against baseline corpus.
- [ ] Core workflows are production-ready (documented runbook + error handling + stable CLI/API contracts).
- [ ] `cargo check` and lint pass.

### US-009: Provide parity validation and reporting tooling

**Description:** As a maintainer, I want automated parity reports so behavior differences are visible and triaged quickly.

**Acceptance Criteria:**

- [ ] Automated tests compare key output files against baseline for each reference case.
- [ ] Reports classify differences as acceptable tolerance or regression.
- [ ] CI fails on unapproved regressions.
- [ ] `cargo check` and lint pass.

### US-010: Achieve Phase 1 release gate

**Description:** As a release owner, I want objective release gates so the Phase 1 migration is safe for production use.

**Acceptance Criteria:**

- [ ] Feature parity is met for all agreed Phase 1 core workflows.
- [ ] Test parity is met for all Phase 1 core workflows.
- [ ] Non-core workflows are explicitly routed through `legacy` mode and documented.
- [ ] `cargo check` and lint pass.

### US-011: Complete full Rust rewrite (Phase 2)

**Description:** As a maintainer, I want all remaining modules ported so the system no longer depends on legacy implementations.

**Acceptance Criteria:**

- [ ] All non-core modules are implemented in Rust.
- [ ] Legacy binary dependency is optional, not required for full functionality.
- [ ] End-to-end parity test suite passes for full workflow coverage.
- [ ] `cargo check` and lint pass.

## 4. Functional Requirements

- FR-1: The system must support a phased migration: incremental module migration first, full rewrite second.
- FR-2: The system must provide a Rust-native library API for integration into other Rust codebases.
- FR-3: The system must provide a legacy-compatible mode that preserves exact historical behavior and interface contracts.
- FR-4: The system must also provide a modern API mode with improved ergonomics and structured errors.
- FR-5: The system must allow selecting compatibility mode via both CLI and library API.
- FR-6: The system must preserve legacy file naming and output structure in legacy mode.
- FR-7: The system must define and maintain a versioned baseline output corpus for parity checks.
- FR-8: The system must run automated parity checks for each migrated module and for end-to-end workflows.
- FR-9: The system must define explicit numeric tolerance rules per output category.
- FR-10: The system must fail CI on unapproved parity regressions.
- FR-11: The system must migrate `RDINP` and `POT` as early Phase 1 modules.
- FR-12: The system must migrate `XSPH`, `pathfinder`, `GENFMT`, and `FF2X` to complete Phase 1 core workflows.
- FR-13: The system must route non-migrated modules through legacy mode during Phase 1 without changing user-visible contracts.
- FR-14: The system must provide deterministic, reproducible error behavior across runs.
- FR-15: The system must expose migration documentation that maps legacy interfaces to modern API usage.
- FR-16: The system must include benchmark tooling comparing Rust and legacy performance on the same test corpus.
- FR-17: The system must support full-Rust execution for all workflows by the end of Phase 2.
- FR-18: The system must keep legacy mode available as an option after Phase 2.

## 5. Non-Goals (Out of Scope)

- Adding new EXAFS physics models or changing scientific methodology during migration.
- Building a graphical UI.
- Replacing external downstream tools (for example, Larch-based analysis tooling).
- Large-scale input format redesign unrelated to compatibility or modern API needs.
- Hardware-specific acceleration (GPU/SIMD tuning) beyond baseline profiling-driven improvements.

## 6. Design Considerations (Optional)

- Prefer explicit, typed Rust interfaces over implicit file-contract-only APIs.
- Maintain a compatibility layer that mirrors current file-based workflow behavior.
- Keep mode selection obvious and explicit (for example, `--mode legacy` and `--mode modern`).
- Preserve deterministic output ordering to reduce noise in parity diffs.

## 7. Technical Considerations (Optional)

- Use Rust crate boundaries that separate domain logic, CLI/runtime orchestration, and compatibility adapters.
- Do not use `unsafe` Rust
- Establish stable numeric comparison rules (absolute/relative tolerance by output type).
- Reuse existing test materials and baselines from `feff85exafs/tests` as the initial verification corpus. The reference data should not be changed.
- Ensure CI runs: build, lint, parity tests, and benchmark smoke checks.

## 8. Success Metrics

- 100% parity pass rate for agreed Phase 1 core workflows on the baseline corpus.
- 100% pass rate for automated regression/parity test suite in CI for migrated scope.
- Measurable performance improvement for core workflows (target: >=20% lower median runtime vs legacy baseline on agreed benchmark set).
- Zero memory-unsafe defects in production code paths (no `unsafe` Rust).
- Full workflow support in pure Rust by end of Phase 2, with legacy mode still available.

## 9. Open Questions

- What exact numeric tolerances should be accepted for each output artifact type?
- Which workflows are officially classified as `core` for Phase 1 release gating?
- Should default mode be `legacy` initially (safest) or `modern` (new integrations first)?
- What benchmark environments (CPU/OS/compiler flags) will be the official source of performance truth?
- What deprecation timeline, if any, should apply to legacy mode after full migration?
