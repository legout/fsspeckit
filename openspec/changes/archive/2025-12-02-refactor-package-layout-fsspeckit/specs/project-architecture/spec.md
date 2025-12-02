# Capability: Project Architecture and Package Layout

## ADDED Requirements

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
