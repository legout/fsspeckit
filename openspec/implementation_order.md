# Implementation Order for Open OpenSpec Changes

This document lists the current open changes under `openspec/changes/` (excluding `archive/`) in the recommended
implementation order, with notes on which workstreams can proceed in parallel.

## Overview of Open Changes

This list excludes changes that are already implemented and/or archived. At the time of writing, the following changes
remain open under `openspec/changes/`:

1. `refactor-module-layout-packages`
2. `consolidate-duckdb-cleanup-and-parallel-defaults`
3. `add-pyarrow-merge-aware-write`
4. `add-duckdb-merge-aware-write`
5. `add-pyarrow-dataset-handler`
6. `unify-dataset-handler-interface`
7. `refactor-modern-typing-fsspeckit`
8. `harden-gitlab-resource-management`

## Recommended Implementation Phases

### Phase 1 – Module layout refactor (high priority)

These changes restructure the module layout into package-based namespaces with shims and deprecations. They should land
early so that subsequent work targets the new layout.

1. **`refactor-module-layout-packages`**
   - Scope: Introduce package-based layouts for `core.ext`, `core.filesystem`, `datasets.duckdb`, `datasets.pyarrow`, and `common.logging`, with backwards-compatible shim modules and deprecation warnings.
   - Dependencies: None (but benefits from the already-implemented core stability changes).
   - Parallelism: This is a touch-heavy change and should be done in a focused window to minimise conflicts with parallel work.

### Phase 2 – Core DuckDB cleanup and parallel defaults

2. **`consolidate-duckdb-cleanup-and-parallel-defaults`**
   - Scope: Deduplicate DuckDB cleanup helpers and clarify/joblib-related parallel execution defaults.
   - Depends on: Patterns established by the already-implemented core stability work (`stabilize-core-ext-io-helpers`).
   - Parallelism: Can follow immediately after `refactor-module-layout-packages`, with care taken to update imports to the new package layout.

### Phase 2 – Merge-aware dataset writes

These changes add strategy-aware dataset write capabilities. They can be developed largely in parallel once the layout
refactor is complete so they target the new structure directly.

3. **`add-pyarrow-merge-aware-write`**
   - Scope: Add `strategy`/`key_columns` to `write_pyarrow_dataset` and expose convenience helpers (`insert_dataset`, etc.).
   - Depends on: Existing PyArrow merge helpers; should be implemented against the new `datasets.pyarrow` package layout.
   - Parallelism: Can be implemented in parallel with `consolidate-duckdb-cleanup-and-parallel-defaults` once `refactor-module-layout-packages` is in place.

4. **`add-duckdb-merge-aware-write`**
   - Scope: Add `strategy`/`key_columns` to `write_parquet_dataset` (DuckDB) and corresponding convenience helpers.
   - Depends on: Existing DuckDB merge helpers; conceptually aligned with `add-pyarrow-merge-aware-write`, and should target the new `datasets.duckdb` package layout.
   - Parallelism: Can be implemented in parallel with `add-pyarrow-merge-aware-write` as long as shared semantics are coordinated.

### Phase 3 – Handler parity and shared interface

These changes provide a symmetric handler UX across PyArrow and DuckDB and define a shared surface.

5. **`add-pyarrow-dataset-handler`**
   - Scope: Introduce `PyarrowDatasetIO` and `PyarrowDatasetHandler`, mirroring DuckDB’s handler where feasible.
   - Depends on: `add-pyarrow-merge-aware-write` (so the handler can rely on merge-aware writes) and the new `datasets.pyarrow` package layout.
   - Parallelism: Should follow completion of `add-pyarrow-merge-aware-write`; can overlap with late-stage work on `add-duckdb-merge-aware-write`.

6. **`unify-dataset-handler-interface`**
   - Scope: Define/document a shared handler surface (and optional protocol) across DuckDB and PyArrow handlers.
   - Depends on: Both handlers being in place (`DuckDBDatasetIO`/`DuckDBParquetHandler` existing, `PyarrowDatasetIO`/`PyarrowDatasetHandler` added).
   - Parallelism: Finalise after `add-pyarrow-dataset-handler`; primarily documentation/type-level work and can be done while other code is stabilising.

### Phase 4 – Cross-cutting typing refactor

7. **`refactor-modern-typing-fsspeckit`**
   - Scope: Convert codebase to modern typing conventions (PEP 604 unions, built-in generics), and remove legacy `typing.Union`/`Optional`/`List`/`Dict` usage.
   - Depends on: All major behavioural and structural changes above being merged, to minimise churn and merge conflicts (especially after layout refactor).
   - Parallelism: Best done after other feature changes are stabilised; can be executed as a focused, mechanical pass.

### Phase 5 – Post-feature stability hardening

8. **`harden-gitlab-resource-management`**
   - Scope: Add session resource cleanup, pagination limits, and input validation to GitLab filesystem to prevent resource leaks and infinite loops.
   - Depends on: All major feature changes being complete and stable; this is a stability hardening change that should be implemented after all functional work.
   - Parallelism: Can be done independently as it affects a specific filesystem implementation; lowest priority as it addresses production stability rather than new features.

## Parallelisation Summary

- **Safe to implement in parallel:**
  - `add-pyarrow-merge-aware-write` ↔ `add-duckdb-merge-aware-write` (after the layout refactor is complete).
  - `unify-dataset-handler-interface` can be done alongside final polish once both handlers exist.

- **Should be sequenced:**
  - `refactor-module-layout-packages` as early as practical, before new feature work that depends on layout.
  - `consolidate-duckdb-cleanup-and-parallel-defaults` after `refactor-module-layout-packages` (so it targets the new layout).
  - `add-pyarrow-dataset-handler` after `add-pyarrow-merge-aware-write`.
  - `unify-dataset-handler-interface` after both handler implementations.
  - `refactor-modern-typing-fsspeckit` last, after behavioural work.
  - `harden-gitlab-resource-management` after all other changes, as final stability hardening.
