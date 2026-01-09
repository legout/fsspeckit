## MODIFIED Requirements

### Requirement: Document DuckDBDatasetIO.write_dataset() and merge() Parameters

DuckDB dataset handler documentation SHALL document the `write_dataset()` and `merge()` methods with their parameters, including backend-specific features like `use_merge`.

#### Scenario: Users understand write_dataset parameters

- **WHEN** a user reads DuckDB dataset handler documentation
- **THEN** they see complete parameter list for `DuckDBDatasetIO.write_dataset()`
- **AND** documentation includes `mode` ("append" | "overwrite"), `compression`, `max_rows_per_file`, `row_group_size`, `partition_by`, etc.
- **AND** backend-specific features (like `use_threads` in `write_parquet`) are documented separately

#### Scenario: Users understand merge() strategy and parameters

- **WHEN** a user reads DuckDB merge documentation
- **THEN** they see `DuckDBDatasetIO.merge()` method signature with `strategy` parameter ("insert" | "update" | "upsert")
- **AND** documentation describes `key_columns` requirement and composite key usage
- **AND** backend-specific `use_merge` parameter is documented (DuckDB 1.4.0+ MERGE SQL vs UNION ALL fallback)
- **AND** examples show correct handler class usage (not removed `DuckDBParquetHandler`)

#### Scenario: Users understand DuckDB filter validation

- **WHEN** a user reads DuckDB `read_parquet()` documentation
- **THEN** documentation states `filters` parameter is `str | None` (SQL WHERE clause string only)
- **AND** runtime validation error is documented (TypeError for non-string filters)
- **AND** examples show correct filter usage (e.g., `filters="id > 100 AND category = 'A'"`)

#### Scenario: Users understand DuckDB backend-specific knobs

- **WHEN** a user reads DuckDB handler documentation
- **THEN** backend-specific parameters are clearly marked:
  - `use_merge`: DuckDB-only, controls MERGE SQL vs UNION ALL (auto-detect + manual override)
  - `use_threads`: DuckDB-only, in `write_parquet()` method only
- **AND** docs contrast these with PyArrow backend (which has different knobs like streaming/memory controls)
- **AND** documentation points to `docs/dataset-handlers.md` for full backend differences matrix

### Requirement: Document DuckDB Convenience Functions

DuckDB-specific convenience functions SHALL be documented with their signatures and behavior, including `collect_dataset_stats_duckdb()` and `compact_parquet_dataset_duckdb()`.

#### Scenario: Users find DuckDB helper functions

- **WHEN** a user searches for DuckDB-specific utilities
- **THEN** documentation lists `collect_dataset_stats_duckdb()`, `compact_parquet_dataset_duckdb()`, and `compact_parquet_dataset_duckdb()`
- **AND** each function has clear parameter documentation
- **AND** examples show how to use these functions (not the handler methods)
- **AND** relationship between top-level functions and handler methods is clear

### Requirement: DuckDB Connection Management

DuckDB dataset handler documentation SHALL describe the connection setup pattern using `DuckDBConnection` and `create_duckdb_connection()`.

#### Scenario: Users understand DuckDB connection setup

- **WHEN** a user reads DuckDB dataset documentation
- **THEN** they see examples of creating `DuckDBConnection` instance
- **AND** documentation shows passing connection to `DuckDBDatasetIO` constructor
- **AND** examples show using `create_duckdb_connection()` for simple cases
- **AND** documentation explains filesystem registration via connection (for cloud storage)

### Requirement: DuckDB Handler Matches BaseDatasetHandler Protocol

DuckDB dataset handler documentation SHALL verify that all required methods (`write_dataset`, `merge`, `compact_parquet_dataset`, `optimize_parquet_dataset`, `read_parquet`) are present with matching signatures.

#### Scenario: Users see consistent DuckDB handler interface

- **WHEN** a user compares DuckDB handler with PyArrow handler
- **THEN** both have same core method signatures (`write_dataset`, `merge`, etc.)
- **AND** DuckDB-specific extensions (like `use_merge`) are allowed per pragmatic backend extension approach
- **AND** documentation points to `BaseDatasetHandler` protocol for common surface
