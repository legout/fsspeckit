# project-architecture Specification

## Purpose
TBD - created by archiving change refactor-package-layout-fsspeckit. Update Purpose after archive.
## Requirements
### Requirement: Domain-Driven Package Layout

The system SHALL organize the `fsspeckit` Python package into clear domain
packages that reflect how users interact with the library: filesystem core,
storage configuration, dataset operations, SQL helpers, and common utilities.

#### Scenario: Core, storage, datasets, sql, and common packages exist

- **WHEN** a developer inspects the `src/fsspeckit` directory
- **THEN** they find top-level packages for `core`, `storage_options`,
  `datasets`, `sql`, `common`, and `utils`
- **AND** each package name corresponds to a well-defined domain (filesystem
  core, storage configuration, dataset operations, SQL filters, shared
  helpers, and backwards-compatible utilities).

### Requirement: Core Filesystem and Planning Layer

The `fsspeckit.core` package SHALL own filesystem factories and backend-neutral
planning logic, without depending on dataset or SQL-specific modules.

#### Scenario: Filesystem API centralized in core.filesystem

- **WHEN** a user imports filesystem primitives
  (e.g. `filesystem`, `get_filesystem`, `DirFileSystem`,
  `MonitoredSimpleCacheFileSystem`, `GitLabFileSystem`)
- **THEN** these symbols are defined in `fsspeckit.core.filesystem`
- **AND** they are re-exported from `fsspeckit.core.__init__` for convenience.

#### Scenario: Backend-neutral merge and maintenance remain in core

- **WHEN** DuckDB or PyArrow dataset helpers perform merge or maintenance
  operations
- **THEN** they delegate strategy semantics and planning to
  `fsspeckit.core.merge` and `fsspeckit.core.maintenance`
- **AND** no duplicate merge/maintenance logic is implemented in backend
  packages.

### Requirement: Dataset Operations Package

Dataset-level operations SHALL live under `fsspeckit.datasets`, grouping
backend-specific helpers (DuckDB, PyArrow) behind a consistent package.

#### Scenario: DuckDB dataset helpers live under datasets

- **WHEN** a user looks for DuckDB-based parquet dataset helpers
- **THEN** they can import `DuckDBParquetHandler` and related dataset
  maintenance functions from `fsspeckit.datasets.duckdb`
- **AND** these helpers internally use `fsspeckit.core.merge` and
  `fsspeckit.core.maintenance` for planning.

#### Scenario: PyArrow dataset helpers live under datasets

- **WHEN** a user looks for PyArrow-based parquet dataset helpers
- **THEN** they can import merge and maintenance helpers
  (e.g. `merge_parquet_dataset_pyarrow`,
  `compact_parquet_dataset_pyarrow`,
  `optimize_parquet_dataset_pyarrow`) from
  `fsspeckit.datasets.pyarrow`
- **AND** these helpers internally use `fsspeckit.core.merge` and
  `fsspeckit.core.maintenance` for planning.

### Requirement: SQL Helpers Package

SQL-to-filter translation helpers SHALL live under `fsspeckit.sql`, separate
from generic utilities and dataset packages.

#### Scenario: SQL filter helpers live under sql.filters

- **WHEN** a user needs to convert SQL-like filter expressions into PyArrow
  or Polars expressions
- **THEN** they can import functions like `sql2pyarrow_filter` and
  `sql2polars_filter` from `fsspeckit.sql.filters`
- **AND** these helpers do not depend on dataset backends.

### Requirement: Common Utilities Package

The system SHALL place cross-cutting utilities (datetime parsing, logging,
parallelism, Polars helpers, type conversion) under `fsspeckit.common` and
these utilities SHALL be safe to use across all domains.

#### Scenario: Shared helpers live under common

- **WHEN** a developer needs datetime parsing, logging setup, parallel
  execution, Polars helpers, or type conversion utilities
- **THEN** they can import these helpers from `fsspeckit.common.*`
- **AND** these modules do not import from `datasets` or `sql`, preventing
  circular dependencies.

### Requirement: Backwards-Compatible Utils Façade

The `fsspeckit.utils` package SHALL act as a backwards-compatible façade that
re-exports public names from `datasets`, `sql`, and `common`, rather than
being the primary home for new implementation code.

#### Scenario: Existing utils imports remain valid

- **WHEN** existing user code imports public helpers via
  `from fsspeckit.utils import DuckDBParquetHandler`,
  `run_parallel`, `opt_dtype_pl`, or `sql2pyarrow_filter`
- **THEN** these imports continue to succeed
- **AND** the imported objects are now defined in the appropriate domain
  packages (`datasets`, `sql`, or `common`) and re-exported through
  `fsspeckit.utils`.

#### Scenario: New implementation code avoids utils

- **WHEN** new features are added to the library
- **THEN** their implementation modules live under the appropriate domain
  package (`datasets`, `sql`, `common`, `core`, or `storage_options`)
- **AND** `fsspeckit.utils` only exposes thin re-exports for selected public
  helpers as needed for backwards compatibility.

### Requirement: Import Layering and Dependencies

The package SHALL enforce a simple import layering model to keep backend-
neutral logic independent from higher-level helpers and avoid cycles.

#### Scenario: Core does not depend on datasets or sql

- **WHEN** imports in `fsspeckit.core.*` are analyzed
- **THEN** they only target the standard library, third-party dependencies,
  `fsspeckit.storage_options`, and `fsspeckit.common`
- **AND** no `core` module imports from `fsspeckit.datasets`,
  `fsspeckit.sql`, or `fsspeckit.utils`.

#### Scenario: Datasets depend on core and common but not utils

- **WHEN** imports in `fsspeckit.datasets.*` are analyzed
- **THEN** they may import from `fsspeckit.core`, `fsspeckit.storage_options`,
  `fsspeckit.sql`, and `fsspeckit.common`
- **AND** they do not import from `fsspeckit.utils`.

#### Scenario: Sql helpers depend only on common

- **WHEN** imports in `fsspeckit.sql.*` are analyzed
- **THEN** they may import from `fsspeckit.common` and third-party libraries
- **AND** they do not import from `fsspeckit.datasets` or `fsspeckit.utils`.

### Requirement: Optional dependencies are declared consistently

The project SHALL declare optional dependency groups in a way that:

- Is valid TOML under the `[project.optional-dependencies]` table.
- Uses extras names that correspond to documented installation commands (e.g. `pip install fsspeckit[datasets]`).

#### Scenario: Installing extras for datasets and SQL
- **WHEN** a caller runs `pip install fsspeckit[datasets]` or `pip install fsspeckit[sql]`
- **THEN** the declared extras SHALL resolve cleanly
- **AND** the resulting environment SHALL contain the packages required for the documented dataset and SQL features.

### Requirement: Version information is safely initialised

The project SHALL expose a `__version__` attribute on the top-level package that is robust to common development and deployment workflows.

#### Scenario: Version resolution in an installed environment
- **WHEN** the package is installed in a standard environment
- **AND** a caller imports `fsspeckit` and accesses `fsspeckit.__version__`
- **THEN** the version SHALL reflect the installed distribution version.

#### Scenario: Version resolution in a source-only development environment
- **WHEN** a developer imports `fsspeckit` from a working copy that has not been installed
- **THEN** the import SHALL NOT fail solely because `importlib.metadata` cannot find an installed distribution
- **AND** `fsspeckit.__version__` SHALL be set to a safe default string that clearly indicates a non-installed/development state.

### Requirement: Helper APIs align with test expectations

Helper functions that are covered by tests SHALL preserve the semantics and key error messages asserted in those tests, except where a clear, documented behavioural change is explicitly approved.

#### Scenario: `run_parallel` supports generator input
- **WHEN** a caller passes a generator as the primary iterable argument to `run_parallel`
- **THEN** the function SHALL treat the generator as a valid iterable, materialising it as needed
- **AND** SHALL produce results consistent with passing an equivalent list.

#### Scenario: Error messages are stable for key error cases
- **WHEN** `run_parallel` raises due to mismatched iterable lengths or missing iterable arguments
- **THEN** the error messages SHALL include wording that satisfies the existing tests’ `match=` assertions
- **AND** any changes to these messages SHALL be accompanied by corresponding spec/test updates.

### Requirement: Avoid mutable defaults and dead code in core helpers

Core helper functions SHALL avoid mutable default arguments and unreachable code branches that cannot be exercised.

#### Scenario: No mutable default arguments in core helpers
- **WHEN** reviewing core helper function definitions
- **THEN** default values for parameters SHALL be immutable or `None`
- **AND** any required mutable objects SHALL be created inside the function body.

#### Scenario: Unreachable code eliminated
- **WHEN** analysing core helper functions for control flow
- **THEN** there SHALL be no branches that can never be executed in practice
- **AND** no branches SHALL refer to variables that are not guaranteed to be defined along all paths.

### Requirement: Backwards-Compatible Utils Façade (extended)

The `fsspeckit.utils` package SHALL continue to act as a backwards-compatible façade while explicitly defining which imports are supported and how they map to domain packages.

#### Scenario: Legacy utils imports remain supported
- **WHEN** existing user code imports helpers via `fsspeckit.utils` (including common patterns used in tests)
- **THEN** those imports SHALL continue to succeed and refer to the same objects as the canonical implementation in `datasets`, `sql`, or `common`.

#### Scenario: Shim modules preserve deeper import paths
- **WHEN** code imports names via deeper paths such as `fsspeckit.utils.misc.Progress`
- **THEN** small shim modules under `fsspeckit.utils` SHALL re-export those names from the canonical locations
- **AND** behaviour SHALL remain consistent across at least one major version to allow migration.

#### Scenario: New code avoids utils implementation
- **WHEN** new features are added to the library
- **THEN** their implementation modules SHALL live under the appropriate domain packages
- **AND** any exposure through `fsspeckit.utils` SHALL be limited to re-exports.

### Requirement: Error handling follows consistent, non-silencing patterns

Core code paths SHALL avoid silently swallowing unexpected exceptions and SHALL use consistent, explicit error handling patterns.

#### Scenario: No bare `except Exception` in core flows
- **WHEN** core modules (`core`, `datasets`, `storage_options`) are inspected
- **THEN** they SHALL NOT contain bare `except Exception:` blocks that simply `pass` or suppress errors without logging
- **AND** any broad exception handlers SHALL either:
  - Narrow the types caught to expected exceptions, or
  - Log the error using the project’s logging facilities and re-raise.

#### Scenario: Resource cleanup is granular and observable
- **WHEN** cleanup logic (e.g. unregistering DuckDB tables, closing resources) is executed
- **THEN** each resource SHALL be handled individually so that failures are visible in logs
- **AND** a failure to clean up one resource SHALL NOT silently mask issues or prevent attempts to clean up others.

### Requirement: Logging is used instead of `print` for error reporting

Core modules SHALL rely on the logging utilities provided by `fsspeckit.common.logging` for error reporting and diagnostics.

#### Scenario: Errors are logged, not printed
- **WHEN** a non-trivial error is handled in core code
- **THEN** the error details SHALL be recorded using a logger obtained via `get_logger`
- **AND** `print(...)` SHALL NOT be used to signal errors or warnings in these paths.

### Requirement: DuckDB dataset writes expose merge-aware UX

The DuckDB dataset write API SHALL offer merge-aware entry points so that common strategies do not require separate staging steps.

#### Scenario: One-step merge via write helper
- **WHEN** a caller writes in-memory data to a parquet dataset using the DuckDB path
- **AND** specifies a merge strategy and key columns where applicable
- **THEN** the library SHALL perform the merge in a single call
- **AND** SHALL expose shortcut methods whose names mirror the supported strategies for parity with the PyArrow path.

### Requirement: PyArrow dataset handler parity with DuckDB

The project SHALL provide a class-based PyArrow dataset handler that aligns with the DuckDB handler for a consistent user experience.

#### Scenario: Class-based PyArrow dataset API
- **WHEN** a user prefers a class-based API for PyArrow parquet datasets
- **THEN** they SHALL instantiate `PyarrowDatasetIO` or `PyarrowDatasetHandler` to access read/write/merge/maintenance helpers
- **AND** method names SHALL mirror the DuckDB handler where supported.

#### Scenario: Lazy optional dependency imports for PyArrow handler
- **WHEN** importing the PyArrow handler without PyArrow installed
- **THEN** the import SHALL succeed
- **AND** an `ImportError` SHALL only be raised when PyArrow-dependent methods are invoked, with guidance on the required extras.

### Requirement: PyArrow dataset writes expose merge-aware UX

The PyArrow dataset write API SHALL offer merge-aware entry points so that common strategies do not require separate staging steps.

#### Scenario: One-step merge via write helper
- **WHEN** a caller writes in-memory data to a parquet dataset using the PyArrow path
- **AND** specifies a merge strategy and key columns where applicable
- **THEN** the library SHALL perform the merge in a single call
- **AND** SHALL expose shortcut methods whose names mirror the supported strategies for parity with DuckDB.

### Requirement: Core filesystem path handling is explicit and deterministic

Core filesystem factories SHALL handle local vs remote paths explicitly and derive base directories and cache hints in a
deterministic, easy-to-reason-about way.

#### Scenario: Local paths are normalised consistently
- **WHEN** a caller passes a local file or directory path to `fsspeckit.core.filesystem.filesystem()`
- **THEN** the factory SHALL normalise the path using the dedicated helpers in `fsspeckit.core.filesystem_paths`
- **AND** the resulting `DirFileSystem` or base filesystem SHALL have a clear and predictable root.

#### Scenario: Cache path hints are derived from normalised roots
- **WHEN** a caller enables caching on a filesystem created from a local path
- **THEN** the cache storage hint SHALL be derived from the normalised dataset root or base directory
- **AND** SHALL NOT depend on ambiguous “file vs directory” heuristics.

### Requirement: Git-based filesystems handle deep paths and large trees

Git-based filesystem implementations SHALL correctly handle URL encoding, pagination, and timeouts when accessing remote
repositories.

#### Scenario: GitLab filesystem encodes paths and paginates listings
- **WHEN** a caller uses `GitLabFileSystem.ls()` on a repository with nested paths or more than one page of results
- **THEN** the filesystem SHALL URL-encode project identifiers and paths as needed
- **AND** SHALL follow GitLab pagination headers to return a complete listing for the requested path
- **AND** SHALL use bounded HTTP requests with a sensible timeout.

### Requirement: Modern typing conventions across modules

The project SHALL use modern Python typing conventions consistently across all modules.

#### Scenario: PEP 604 unions are used instead of typing.Union
- **WHEN** reviewing type annotations in core, common, datasets, storage_options, sql, and utils modules
- **THEN** union types are expressed using `X | Y` and `X | None`
- **AND** `typing.Union` and `typing.Optional` are not used in new or updated code.

#### Scenario: Built-in generics replace typing.List and typing.Dict
- **WHEN** reviewing collection type annotations
- **THEN** list and dict types use `list[T]` and `dict[K, V]`
- **AND** `typing.List` and `typing.Dict` are not used in new or updated code.

#### Scenario: Optional dependency types respect lazy-import rules
- **WHEN** optional-dependency types such as Polars, Pandas, PyArrow, DuckDB, sqlglot, or orjson are used in annotations
- **THEN** those types are imported only under `if TYPE_CHECKING:` or referenced via shared helpers in `fsspeckit.common.optional`
- **AND** importing modules does not require those optional packages at runtime.

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

### Requirement: Shared dataset handler surface

Dataset handlers across backends SHALL share a clearly documented core surface (method names and key parameters) to
provide a consistent user experience.

#### Scenario: Common method naming across handlers
- **WHEN** comparing DuckDB and PyArrow dataset handlers
- **THEN** core operations (read, write, merge, compact/optimize where supported) SHALL use consistent method names and parameter conventions
- **AND** any backend-specific differences SHALL be explicitly documented.

#### Scenario: Optional protocol for tooling
- **WHEN** static analysis or editor tooling is used
- **THEN** a minimal protocol or type annotation MAY be provided to describe the shared handler surface
- **AND** handlers SHALL satisfy this protocol for the overlapping capabilities.

