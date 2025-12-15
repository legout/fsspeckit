## ADDED Requirements

### Requirement: Incremental Merge Rewrite Mode (PyArrow)
The system SHALL support an opt-in incremental rewrite mode for merge-aware dataset operations in the PyArrow handler.

#### Scenario: Incremental UPSERT preserves unaffected parquet files
- **GIVEN** a target dataset directory with many parquet files
- **AND** user calls `io.write_parquet_dataset(source, path, strategy="upsert", key_columns=["id"], rewrite_mode="incremental")`
- **THEN** the system SHALL preserve all parquet files that cannot contain any of the updated keys
- **AND** SHALL rewrite only affected parquet files into new parquet file(s)
- **AND** SHALL write additional new parquet file(s) for inserted rows

### Requirement: Conservative Metadata Pruning (PyArrow)
Incremental rewrite pruning SHALL be conservative to preserve correctness.

#### Scenario: Unknown file membership treated as affected
- **WHEN** parquet metadata cannot prove that a parquet file is free of any source keys
- **THEN** the system SHALL treat that file as affected for incremental rewrite purposes

### Requirement: Incremental Rewrite Not Supported for Full Sync Strategies (PyArrow)
The system SHALL reject incremental rewrite mode for strategies that require full dataset rewrite.

#### Scenario: Reject incremental full_merge
- **WHEN** user calls `io.write_parquet_dataset(source, path, strategy="full_merge", rewrite_mode="incremental")`
- **THEN** the method SHALL raise `ValueError` indicating incremental rewrite is not supported for `full_merge`

