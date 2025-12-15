## ADDED Requirements

### Requirement: Dataset Write Default Mode - Append
The system SHALL default `write_parquet_dataset(..., mode=...)` to `mode="append"` when `mode` is not provided.

#### Scenario: Default append on repeated writes
- **GIVEN** a dataset directory already contains parquet files
- **WHEN** user calls `handler.write_parquet_dataset(table, path)` twice without specifying `mode`
- **THEN** the second call SHALL write additional parquet file(s) with unique names
- **AND** SHALL preserve the existing parquet files
- **AND** reading the dataset SHALL return combined rows from all parquet files

### Requirement: Mode and Strategy Compatibility (DuckDB)
The system SHALL validate `mode` and reject incompatible combinations with merge `strategy`.

#### Scenario: Reject append with rewrite strategies
- **WHEN** user calls `handler.write_parquet_dataset(table, path, mode="append", strategy="upsert")`
- **OR** uses `strategy="update"|"full_merge"|"deduplicate"`
- **THEN** the method SHALL raise `ValueError` indicating that `mode="append"` is not supported for the chosen strategy

### Requirement: Insert Strategy Supports Append-Only Writes (DuckDB)
When `strategy="insert"` and `mode="append"`, the system SHALL avoid rewriting existing parquet files and SHALL write only newly insertable rows to new parquet file(s).

#### Scenario: Insert + append writes only new keys
- **GIVEN** a target dataset exists with key `id=1`
- **AND** user provides a source table with keys `id=1` and `id=2`
- **WHEN** user calls `handler.write_parquet_dataset(source, path, strategy="insert", key_columns=["id"], mode="append")`
- **THEN** the system SHALL write parquet file(s) containing only rows for `id=2`
- **AND** SHALL NOT delete or rewrite existing parquet files
