## ADDED Requirements

### Requirement: Parquet Dataset Deduplication Maintenance (PyArrow)
The system SHALL provide `deduplicate_parquet_dataset` to deduplicate an existing parquet dataset directory using PyArrow and fsspec.

#### Scenario: Deduplicate by key columns
- **GIVEN** a dataset contains duplicate keys under `key_columns=["id"]`
- **WHEN** user calls `io.deduplicate_parquet_dataset(path, key_columns=["id"])`
- **THEN** the dataset SHALL be rewritten so that only one row per key remains
- **AND** the resulting dataset SHALL remain readable by `read_parquet`

#### Scenario: Dry run returns plan only
- **WHEN** user calls `io.deduplicate_parquet_dataset(path, key_columns=["id"], dry_run=True)`
- **THEN** the method SHALL NOT write or delete any files
- **AND** SHALL return statistics including `before_file_count` and an estimated `after_file_count`

### Requirement: Optimize Supports Optional Deduplication Step (PyArrow)
The system SHALL allow callers to request deduplication during `optimize_parquet_dataset`.

#### Scenario: Optimize performs deduplication when requested
- **GIVEN** a dataset contains duplicate keys under `key_columns=["id"]`
- **WHEN** user calls `io.optimize_parquet_dataset(path, deduplicate_key_columns=["id"])`
- **THEN** the optimized output SHALL not contain duplicate keys for `id`
- **AND** optimization statistics SHALL indicate that deduplication was performed

