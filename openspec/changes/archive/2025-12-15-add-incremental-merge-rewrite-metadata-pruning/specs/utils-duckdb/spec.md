## ADDED Requirements

### Requirement: Incremental Merge Rewrite Mode (DuckDB)
The system SHALL support an opt-in incremental rewrite mode for merge-aware dataset operations.

#### Scenario: Incremental UPSERT rewrites only affected files
- **GIVEN** a target dataset directory with many parquet files
- **AND** the source contains updates for keys that may appear in only a subset of files
- **AND** user calls `handler.write_parquet_dataset(source, path, strategy="upsert", key_columns=["id"], rewrite_mode="incremental")`
- **THEN** the system SHALL preserve all parquet files that cannot contain any of the updated keys
- **AND** SHALL rewrite only the affected parquet files into new parquet file(s)
- **AND** SHALL write additional new parquet file(s) for newly inserted rows
- **AND** the resulting dataset SHALL reflect correct UPSERT semantics when read

#### Scenario: Incremental UPDATE never writes inserts
- **GIVEN** a target dataset exists
- **AND** user calls `handler.write_parquet_dataset(source, path, strategy="update", key_columns=["id"], rewrite_mode="incremental")`
- **THEN** the system SHALL rewrite only files that might contain keys from the source
- **AND** SHALL NOT write any rows for keys not present in the target (no inserts)

### Requirement: Conservative Metadata Pruning (DuckDB)
Incremental rewrite pruning SHALL be conservative to preserve correctness.

#### Scenario: Unknown file membership treated as affected
- **WHEN** parquet metadata cannot prove that a parquet file is free of any source keys
- **THEN** the system SHALL treat that file as affected for incremental rewrite purposes
- **AND** MAY rewrite more files than strictly necessary
- **AND** SHALL NOT skip rewriting a file that contains keys needing updates

### Requirement: Incremental Rewrite Not Supported for Full Sync Strategies (DuckDB)
The system SHALL reject incremental rewrite mode for strategies that require full dataset rewrite.

#### Scenario: Reject incremental full_merge
- **WHEN** user calls `handler.write_parquet_dataset(source, path, strategy="full_merge", rewrite_mode="incremental")`
- **THEN** the method SHALL raise `ValueError` indicating incremental rewrite is not supported for `full_merge`

#### Scenario: Reject incremental deduplicate
- **WHEN** user calls `handler.write_parquet_dataset(source, path, strategy="deduplicate", rewrite_mode="incremental")`
- **THEN** the method SHALL raise `ValueError` indicating incremental rewrite is not supported for `deduplicate`

