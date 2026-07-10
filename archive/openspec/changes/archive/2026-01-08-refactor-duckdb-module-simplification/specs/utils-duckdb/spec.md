## MODIFIED Requirements

### Requirement: Filesystem Registration in DuckDB
The system SHALL register fsspec filesystem instances in DuckDB connections to enable operations on remote storage systems. This is managed by `DuckDBConnection`.

#### Scenario: Register filesystem via DuckDBConnection
- **WHEN** `DuckDBConnection` is initialized with a filesystem
- **THEN** it SHALL register the filesystem with the active DuckDB connection
- **AND** DuckDB SHALL be able to access paths using the registered filesystem

### Requirement: SQL Query Execution
The system SHALL provide an `execute_sql` method on `DuckDBConnection` that executes SQL queries using the managed DuckDB connection and returns results.

#### Scenario: Execute SQL query via connection manager
- **WHEN** user calls `connection.execute_sql("SELECT * FROM ...")`
- **THEN** method executes query using the managed DuckDB connection
- **AND** returns results as a DuckDB result object (which can fetch Arrow tables)

### Requirement: Context Manager Support
`DuckDBConnection` SHALL implement the context manager protocol for automatic resource cleanup and connection management.

#### Scenario: Use DuckDBConnection with statement
- **WHEN** user uses `with DuckDBConnection(fs) as conn:`
- **THEN** connection is automatically closed on exit
- **AND** resources are properly cleaned up

### Requirement: Parquet Dataset Compaction
The system SHALL provide a `compact_parquet_dataset_duckdb` function that consolidates small parquet files using DuckDB's native SQL `COPY` command for efficient, streaming data movement.

#### Scenario: Compact dataset using DuckDB SQL
- **WHEN** `compact_parquet_dataset_duckdb` is called
- **THEN** it SHALL use DuckDB SQL `COPY (SELECT * FROM parquet_scan(...)) TO ...`
- **AND** it SHALL support recompression during compaction
- **AND** it SHALL preserve all data and schema

### Requirement: Parquet Dataset Deduplication Maintenance (DuckDB)
The system SHALL provide a `deduplicate_parquet_dataset` method that removes duplicate rows from an existing dataset using DuckDB's native SQL capabilities (`DISTINCT` or `DISTINCT ON`).

#### Scenario: Deduplicate dataset using DuckDB SQL
- **WHEN** `deduplicate_parquet_dataset` is called
- **THEN** it SHALL use DuckDB SQL to identify and remove duplicates
- **AND** it SHALL support ordering to specify which record to keep when duplicates are found

## REMOVED Requirements

### Requirement: DuckDB Parquet Handler Initialization
**Reason**: `DuckDBParquetHandler` is a legacy wrapper that has been removed. Use `DuckDBConnection` and `DuckDBDatasetIO` directly.

### Requirement: Write Parquet Dataset with Unique Filenames
**Reason**: This requirement was tied to the legacy `write_parquet_dataset` method. Standardized `write_dataset` and `merge` now handle this.

### Requirement: Dataset Write Mode - Overwrite
**Reason**: Legacy `write_parquet_dataset` requirement. Use `DuckDBDatasetIO.write_dataset(mode="overwrite")`.

### Requirement: Dataset Write Mode - Append
**Reason**: Legacy `write_parquet_dataset` requirement. Use `DuckDBDatasetIO.write_dataset(mode="append")`.

### Requirement: Dataset Write Validation
**Reason**: Tied to legacy API. New APIs have their own validation.

### Requirement: Dataset Write Performance
**Reason**: Tied to legacy API. New implementations focus on DuckDB native performance.

### Requirement: Unique Filename Generation
**Reason**: Tied to legacy API.

### Requirement: Dataset Write with Compression
**Reason**: Tied to legacy API.

### Requirement: Dataset Read Compatibility
**Reason**: Tied to legacy API.
