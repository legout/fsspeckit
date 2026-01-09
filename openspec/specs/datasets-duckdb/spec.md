# datasets-duckdb Specification

## Purpose
TBD - created by archiving change add-duckdb-merge-aware-write-docs. Update Purpose after archive.
## Requirements
### Requirement: Document DuckDB merge-aware write parameters

`write_parquet_dataset` API documentation SHALL include new merge strategy parameters and their behavior for DuckDB backends.

#### Scenario: DuckDB strategy parameter documentation
- **WHEN** a user views `write_parquet_dataset` API documentation for DuckDB
- **THEN** documentation SHALL include `strategy` parameter with all valid options ('insert', 'upsert', 'update', 'full_merge', 'deduplicate')
- **AND** SHALL describe DuckDB-specific behavior for each strategy
- **AND** SHALL document any requirements (like key_columns for relational strategies)

#### Scenario: DuckDB key columns parameter documentation
- **WHEN** a user views `write_parquet_dataset` API documentation for DuckDB
- **THEN** documentation SHALL include `key_columns` parameter
- **AND** SHALL explain its purpose for DuckDB merge operations
- **AND** SHALL document which strategies require it
- **AND** SHALL show examples of single and composite key usage

### Requirement: Document DuckDB convenience helper functions
All DuckDB convenience helper functions SHALL be documented with clear usage examples.

#### Scenario: DuckDB helper function documentation
- **WHEN** a user searches for DuckDB merge functionality
- **THEN** documentation SHALL include `DuckDBDatasetIO.merge`, `collect_dataset_stats_duckdb`, and `compact_parquet_dataset_duckdb`
- **AND** each function SHALL have clear parameter documentation
- **AND** each function SHALL include practical usage examples
- **AND** each function SHALL document key column requirements

