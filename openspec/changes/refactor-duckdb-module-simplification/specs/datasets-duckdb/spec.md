## MODIFIED Requirements

### Requirement: Document DuckDB convenience helper functions
All DuckDB convenience helper functions SHALL be documented with clear usage examples.

#### Scenario: DuckDB helper function documentation
- **WHEN** a user searches for DuckDB merge functionality
- **THEN** documentation SHALL include `DuckDBDatasetIO.merge`, `collect_dataset_stats_duckdb`, and `compact_parquet_dataset_duckdb`
- **AND** each function SHALL have clear parameter documentation
- **AND** each function SHALL include practical usage examples
- **AND** each function SHALL document key column requirements

### Requirement: DuckDB Dataset Merge Implementation
`DuckDBDatasetIO.merge` SHALL implement merge strategies (insert, update, upsert) using DuckDB's native SQL engine for record matching and joins, while preserving atomic file replacement.

#### Scenario: SQL-based merge execution
- **WHEN** `DuckDBDatasetIO.merge` is called with a PyArrow Table
- **THEN** it SHALL register the table as a temporary view in DuckDB
- **AND** it SHALL use SQL joins/exists clauses to identify records for insertion/update
- **AND** it SHALL use `COPY ... TO ...` for efficient data writing

## REMOVED Requirements

### Requirement: DuckDB Parquet Handler Initialization
**Reason**: `DuckDBParquetHandler` was a deprecated legacy wrapper.

### Requirement: Document DuckDB merge-aware write parameters
**Reason**: The legacy `write_parquet_dataset` API has been removed.
