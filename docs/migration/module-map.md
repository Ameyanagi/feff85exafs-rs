# FEFF85EXAFS Legacy Module Map

## Source of truth

This inventory is based on the legacy build definitions in:

- `feff85exafs/src/*/Makefile`
- `feff85exafs/src/*/SConstruct`

Dependencies below are **direct build dependencies** (linked libraries, referenced objects, and explicit module/header coupling), not full transitive closures.

## Core execution order

`rdinp -> pot -> xsph -> pathfinder -> genfmt -> ff2x`

## Core stage handoff summary

- `rdinp`: reads `feff.inp`, validates cards, writes intermediate JSON/files used by downstream stages.
- `pot`: consumes `rdinp` outputs and computes potentials/phase inputs.
- `xsph`: consumes `pot` products and computes phase shifts.
- `pathfinder`: consumes shared inputs and writes scattering path definitions.
- `genfmt`: combines path data with phase/potential context and generates F-matrix/path artifacts.
- `ff2x`: consumes generated path artifacts and writes final EXAFS-oriented output files.

## Module inventory and direct dependencies

| Module | Primary artifacts | Core for Phase 1 | Direct dependencies |
| --- | --- | --- | --- |
| `json-fortran` | `libjsonfortran.a`, `json_module.mod` | Supporting | External JSON Fortran module (vendored in tree), no internal FEFF module dependency. |
| `PAR` | `libfeffpar.a` | Supporting | Shared `HEADERS/parallel.h` interface. |
| `COMMON` | `libfeffcom.a` | Supporting | Shared `HEADERS/*` include contracts. |
| `MATH` | `libfeffmath.a` | Supporting | `COMMON`, `PAR` (source-level helper usage in build metadata). |
| `JSON` | `libfeffjson.a` | Supporting | `json-fortran`, `HEADERS/*`, `RDINP/allinp.h` (via `json_xsect.f`). |
| `ATOM` | `libfeffatom.a` | Supporting | `COMMON`, `PAR`, `MATH`, `EXCH`. |
| `EXCH` | `libfeffexch.a` | Supporting | `COMMON`, `PAR`, `MATH` (source-level helper usage in build metadata). |
| `FOVRG` | `libfeffpha.a` | Supporting | Shared `HEADERS/*` include contracts. |
| `FMS` | `libfefffms.a` | Supporting | `COMMON`, `PAR`, `MATH` (used to support `pot`/`xsph`; standalone FMS binary is not part of feff85exafs flow). |
| `DEBYE` | `libfeffdw.a` | Non-core | `COMMON`, `MATH`, `PAR`. |
| `RDINP` | `rdinp` | Core | `json-fortran`, `COMMON`, `PAR`, `MATH`, `JSON`. |
| `POT` | `pot`, `libfeffint.a`, `libpotph`/`libfeffphases` wrappers | Core | `ATOM`, `FOVRG`, `COMMON`, `PAR`, `MATH`, `JSON`, `EXCH`, `FMS`, `json-fortran`; optional coupling to `OPCONSAT` in `libpotph` path. |
| `XSPH` | `xsph` | Core | `ATOM`, `FOVRG`, `FMS`, `POT`, `EXCH`, `COMMON`, `PAR`, `MATH`, `JSON`, `json-fortran`, plus object-level reuse from `FF2X/xscorr` and `POT/grids`. |
| `PATH` | `pathfinder` | Core | `json-fortran`, `COMMON`, `PAR`, `MATH`, `JSON`. |
| `GENFMT` | `genfmt`, `libfeffgenfmt.a`, `libonepath`, `libfeffpath` | Core | `json-fortran`, `DEBYE`, `COMMON`, `PAR`, `MATH`, `JSON`, plus object reuse from `FF2X` and `RDINP` for shared-library entry points. |
| `FF2X` | `ff2x` | Core | `json-fortran`, `DEBYE`, `COMMON`, `PAR`, `MATH`, `JSON`. |
| `OPCONSAT` | `opconsat`, `eps2exc`, `libfeffopconsat.a`, `libfeffloss` | Non-core | `JSON`, `json-fortran`, `COMMON`, `PAR`, `MATH`. |
| `feff6l` | `feff6l` monolithic executable | Non-core | Legacy monolith; compiles bundled sources from multiple modules directly rather than linking FEFF static libraries. |

## Notes for migration sequencing

- The Phase 1 core chain is the required minimal native pipeline: `RDINP`, `POT`, `XSPH`, `PATH`, `GENFMT`, `FF2X`.
- `POT` and `XSPH` are the highest-dependency core stages and should be treated as major integration points.
- `DEBYE`, `OPCONSAT`, and `feff6l` can remain in compatibility scope during core-stage migration.
