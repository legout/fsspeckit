## ADDED Requirements

### Requirement: Package-based layout for core domains

Core domains SHALL use package-based layouts rather than flat, underscored modules to reflect their structure.

#### Scenario: Core ext helpers live under `core/ext/`
- **WHEN** inspecting the `fsspeckit.core` package
- **THEN** ext helpers SHALL reside under `fsspeckit.core.ext` as a package
- **AND** modules such as CSV, JSON, Parquet, dataset, and IO helpers SHALL be implemented as `ext/csv.py`, `ext/json.py`, `ext/parquet.py`, `ext/dataset.py`, `ext/io.py`, and so on.

#### Scenario: Filesystem helpers live under `core/filesystem/`
- **WHEN** inspecting filesystem-related helpers
- **THEN** path and cache helpers SHALL reside under `fsspeckit.core.filesystem` as a package (e.g. `filesystem/paths.py`, `filesystem/cache.py`)
- **AND** high-level filesystem factories SHALL be exposed from the package’s `__init__.py`.

#### Scenario: Dataset backends use backend-specific packages
- **WHEN** inspecting dataset helpers
- **THEN** DuckDB helpers SHALL reside under `fsspeckit.datasets.duckdb` as a package
- **AND** PyArrow helpers SHALL reside under `fsspeckit.datasets.pyarrow` as a package.

#### Scenario: Logging helpers live under `common/logging/`
- **WHEN** inspecting logging utilities
- **THEN** core logging APIs and configuration SHALL reside under `fsspeckit.common.logging` as a package
- **AND** configuration helpers SHALL be in a dedicated submodule (e.g. `common/logging/config.py`).

### Requirement: Backwards-compatible shim modules

Legacy flat modules SHALL remain as backwards-compatible shims for at least one stable release, with a documented deprecation path.

#### Scenario: Legacy imports continue to work
- **WHEN** existing code imports from modules such as `ext_csv`, `filesystem_paths`, `duckdb_dataset`, `pyarrow_dataset`, or `logging_config`
- **THEN** those imports SHALL continue to succeed
- **AND** the modules SHALL re-export the same public names from the new package-based implementations.

#### Scenario: Deprecation is communicated clearly
- **WHEN** a legacy shim module is imported
- **THEN** it MAY emit a `DeprecationWarning` that explains the new import path
- **AND** the documentation SHALL describe the migration path away from shim modules.

