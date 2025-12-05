## Why

Several modules in `fsspeckit` have grown into large, multi-purpose “god files”:

- `src/fsspeckit/core/ext.py` (~2k lines) bundles JSON/CSV/Parquet IO helpers, Polars/PyArrow integration, and filesystem monkey-patching.
- `src/fsspeckit/datasets/pyarrow.py` (~2k lines) mixes type inference, schema unification, merge/maintenance helpers, and dataset-level operations.
- `src/fsspeckit/datasets/duckdb.py` (~1.3k lines) combines connection management, IO, merge/maintenance helpers, and dataset writing.
- `src/fsspeckit/core/filesystem.py` (~1k lines) handles protocol parsing, path normalisation, DirFileSystem wrapping, cache filesystem behaviour, and more.

This concentration of responsibilities makes it harder to:

- Understand the code and reason about changes.
- Reuse smaller units of functionality in isolation.
- Keep import layering and optional dependency boundaries clean.

## What Changes

- Decompose large modules into smaller, domain-focused submodules, while preserving the public API and import paths:
  - Split `core.ext` into format-focused and registration modules (e.g., JSON, CSV, Parquet helpers and a thin wiring layer).
  - Split `datasets.pyarrow` into:
    - Schema/type inference and unification.
    - Dataset merge/maintenance helpers.
  - Split `datasets.duckdb` into:
    - Connection/handle management.
    - Dataset IO and maintenance.
  - Factor out path-manipulation and cache-specific logic from `core.filesystem` into smaller helpers, leaving the main module focused on high-level filesystem construction and delegation.
- Add thin “public entrypoint” modules (or `__init__` re-exports) so that existing imports continue to work unchanged.

## Impact

- **Behaviour:**
  - No change to the public APIs or expected semantics; this is a structural refactor only.
  - Internal call graphs become simpler and better aligned with the domain-driven package layout defined in the architecture spec.

- **Maintainability:**
  - Smaller modules with single responsibilities make it easier to review and test changes.
  - Better separation between IO helpers, schema logic, dataset operations, and filesystem wiring reduces the risk of cyclical dependencies.

- **Specs affected:**
  - `project-architecture` (detailed module organisation within the existing domain packages).

