# Implementation Order for Open OpenSpec Changes

This document lists the current open changes under `openspec/changes/` (excluding `archive/`) in the recommended
implementation order, with notes on which workstreams can proceed in parallel.

## Overview of Open Changes

This list excludes changes that are already implemented and/or archived. At the time of writing, the following changes
remain open under `openspec/changes/`:

1. `fix-package-layout-critical-issues`
2. `refactor-module-layout-packages`
3. `consolidate-duckdb-cleanup-and-parallel-defaults`
4. `add-pyarrow-merge-aware-write`
5. `add-duckdb-merge-aware-write`
6. `add-pyarrow-dataset-handler`
7. `unify-dataset-handler-interface`
8. `refactor-modern-typing-fsspeckit`
9. `harden-gitlab-resource-management`

## Recommended Implementation Phases

### Phase 1 – Critical fixes and module layout refactor (high priority)

These changes address critical blocking issues and restructure module layout into package-based namespaces with shims and deprecations. They should land
early so that subsequent work targets the new layout.

1. **`fix-package-layout-critical-issues`**
   - Scope: Fix circular import, resolve layering violations, consolidate schema utilities, complete test migration, and add migration documentation.
   - Dependencies: None (critical unblocking work).
   - Parallelism: This must be done first as it blocks all other work due to circular import.

2. **`refactor-module-layout-packages`**
   - Scope: Introduce package-based layouts for `core.ext`, `core.filesystem`, `datasets.duckdb`, `datasets.pyarrow`, and `common.logging`, with backwards-compatible shim modules and deprecation warnings.
   - Dependencies: `fix-package-layout-critical-issues` (must land first to unblock functionality).
   - Parallelism: This is a touch-heavy change and should be done in a focused window to minimise conflicts with parallel work.

### Phase 2 – Core DuckDB cleanup and parallel defaults

3. **`consolidate-duckdb-cleanup-and-parallel-defaults`**
   - Scope: Deduplicate DuckDB cleanup helpers and clarify/joblib-related parallel execution defaults.
   - Depends on: Patterns established by the already-implemented core stability work (`stabilize-core-ext-io-helpers`) and both `fix-package-layout-critical-issues` and `refactor-module-layout-packages`.
   - Parallelism: Can follow immediately after layout fixes are complete, with care taken to update imports to the new package layout.

### Phase 2 – Merge-aware dataset writes

These changes add strategy-aware dataset write capabilities. They can be developed largely in parallel once the layout
refactor is complete so they target the new structure directly.

4. **`add-pyarrow-merge-aware-write`**
   - Scope: Add `strategy`/`key_columns` to `write_pyarrow_dataset` and expose convenience helpers (`insert_dataset`, etc.).
   - Depends on: Existing PyArrow merge helpers; should be implemented against the new `datasets.pyarrow` package layout.
   - Parallelism: Can be implemented in parallel with `consolidate-duckdb-cleanup-and-parallel-defaults` once layout fixes are in place.

5. **`add-duckdb-merge-aware-write`**
   - Scope: Add `strategy`/`key_columns` to `write_parquet_dataset` (DuckDB) and corresponding convenience helpers.
   - Depends on: Existing DuckDB merge helpers; conceptually aligned with `add-pyarrow-merge-aware-write`, and should target the new `datasets.duckdb` package layout.
   - Parallelism: Can be implemented in parallel with `add-pyarrow-merge-aware-write` as long as shared semantics are coordinated.

### Phase 3 – Handler parity and shared interface

These changes provide a symmetric handler UX across PyArrow and DuckDB and define a shared surface.

6. **`add-pyarrow-dataset-handler`**
   - Scope: Introduce `PyarrowDatasetIO` and `PyarrowDatasetHandler`, mirroring DuckDB's handler where feasible.
   - Depends on: `add-pyarrow-merge-aware-write` (so that handler can rely on merge-aware writes) and new `datasets.pyarrow` package layout.
   - Parallelism: Should follow completion of `add-pyarrow-merge-aware-write`; can overlap with late-stage work on `add-duckdb-merge-aware-write`.

7. **`unify-dataset-handler-interface`**
   - Scope: Define/document a shared handler surface (and optional protocol) across DuckDB and PyArrow handlers.
   - Depends on: Both handlers being in place (`DuckDBDatasetIO`/`DuckDBParquetHandler` existing, `PyarrowDatasetIO`/`PyarrowDatasetHandler` added).
   - Parallelism: Finalise after `add-pyarrow-dataset-handler`; primarily documentation/type-level work and can be done while other code is stabilising.

### Phase 4 – Cross-cutting typing refactor

8. **`refactor-modern-typing-fsspeckit`**
   - Scope: Convert codebase to modern typing conventions (PEP 604 unions, built-in generics), and remove legacy `typing.Union`/`Optional`/`List`/`Dict` usage.
   - Depends on: All major behavioural and structural changes above being merged, to minimise churn and merge conflicts (especially after layout refactor).
   - Parallelism: Best done after other feature changes are stabilised; can be executed as a focused, mechanical pass.

### Phase 5 – Post-feature stability hardening

9. **`harden-gitlab-resource-management`**
   - Scope: Add session resource cleanup, pagination limits, and input validation to GitLab filesystem to prevent resource leaks and infinite loops.
   - Depends on: All major feature changes being complete and stable; this is a stability hardening change that should be implemented after all functional work.
   - Parallelism: Can be done independently as it affects a specific filesystem implementation; lowest priority as it addresses production stability rather than new features.

## Parallelisation Summary

- **Safe to implement in parallel:**
  - `add-pyarrow-merge-aware-write` ↔ `add-duckdb-merge-aware-write` (after layout fixes are complete).
  - `unify-dataset-handler-interface` can be done alongside final polish once both handlers exist.

- **Should be sequenced:**
  - `fix-package-layout-critical-issues` first (critical unblocking work).
  - `refactor-module-layout-packages` after critical fixes (depends on unblocked functionality).
  - `consolidate-duckdb-cleanup-and-parallel-defaults` after layout fixes (so it targets new layout).
  - `add-pyarrow-dataset-handler` after `add-pyarrow-merge-aware-write`.
  - `unify-dataset-handler-interface` after both handler implementations.
  - `refactor-modern-typing-fsspeckit` last, after behavioural work.
  - `harden-gitlab-resource-management` after all other changes, as final stability hardening.
