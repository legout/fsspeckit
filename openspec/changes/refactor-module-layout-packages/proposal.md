# Change: Refactor module layout to package-based structure with shims

## Why

The current layout uses many flat, underscored modules (e.g. `ext_csv.py`, `ext_dataset.py`, `filesystem_paths.py`,
`duckdb_dataset.py`, `pyarrow_dataset.py`, `logging_config.py`). This is functionally correct but:

- Makes navigation and discoverability harder than necessary.
- Hides natural groupings (e.g. all `ext` helpers, all filesystem helpers, all DuckDB/PyArrow dataset helpers).
- Forces users to remember slightly awkward names instead of idiomatic package paths.

Moving to package-based layouts like `core/ext/csv.py`, `core/filesystem/paths.py`, `datasets/duckdb/dataset.py`,
`datasets/pyarrow/dataset.py`, and `common/logging/config.py` will:

- Better reflect the conceptual structure of the project.
- Simplify mental models and IDE navigation.
- Align with common Python packaging conventions.

This change SHALL use shims and deprecation warnings rather than breaking existing import paths.

## What Changes

- Introduce package-based layouts for key domains:
  - `fsspeckit.core.ext` → `core/ext/__init__.py`, `core/ext/csv.py`, `core/ext/json.py`, `core/ext/parquet.py`,
    `core/ext/dataset.py`, `core/ext/io.py`, `core/ext/register.py`.
  - `fsspeckit.core.filesystem` → `core/filesystem/__init__.py` (factory and high-level APIs),
    `core/filesystem/paths.py`, `core/filesystem/cache.py`, and optionally `core/filesystem/gitlab.py`.
  - `fsspeckit.datasets.duckdb` → `datasets/duckdb/__init__.py`, `datasets/duckdb/dataset.py`,
    `datasets/duckdb/connection.py`, `datasets/duckdb/helpers.py`.
  - `fsspeckit.datasets.pyarrow` → `datasets/pyarrow/__init__.py`, `datasets/pyarrow/dataset.py`,
    `datasets/pyarrow/schema.py`.
  - `fsspeckit.common.logging` → `common/logging/__init__.py`, `common/logging/config.py`.
- Leave existing flat modules in place as **shim modules** that:
  - Import from the new package locations and re-export public names.
  - Optionally emit `DeprecationWarning` with guidance on new import paths.
- Update internal imports across the repository to use the new package-based paths.
- Update documentation and generated API references to prefer the new paths, while documenting legacy imports.

## Impact

- Improves code organisation and UX without breaking existing user imports.
- Introduces a deprecation path for legacy module names.
- Touches many modules and docs, so it should be done in a focused pass.

