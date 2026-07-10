## ADDED Requirements

### Requirement: DuckDB dataset writes support merge strategies

`write_parquet_dataset` SHALL accept optional merge strategy arguments and apply them when provided.

#### Scenario: Strategy-aware write
- **WHEN** a caller passes `strategy` (one of `insert`, `upsert`, `update`, `full_merge`, `deduplicate`) and `key_columns` (when required)
- **THEN** `write_parquet_dataset` SHALL apply the corresponding merge semantics instead of a plain write
- **AND** behaviour without `strategy` remains unchanged.

#### Scenario: Convenience helpers
- **WHEN** a caller invokes `insert_dataset`, `upsert_dataset`, `update_dataset`, or `deduplicate_dataset` on DuckDB dataset helpers
- **THEN** these helpers SHALL delegate to `write_parquet_dataset` with the appropriate `strategy`
- **AND** SHALL validate that required `key_columns` are provided for key-based strategies.
