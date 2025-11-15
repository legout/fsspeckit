# Capability: PyArrow Parquet Dataset Maintenance

## ADDED Requirements

### Requirement: PyArrow Parquet Dataset Compaction

The system SHALL provide a `compact_parquet_dataset_pyarrow` helper that
consolidates small parquet files in a dataset directory into fewer larger files
using only PyArrow and fsspec.

#### Scenario: Compaction by target file size and rows

- **WHEN** user calls  
  `compact_parquet_dataset_pyarrow(path, target_mb_per_file=128)` or  
  `compact_parquet_dataset_pyarrow(path, target_rows_per_file=1_000_000)`
- **THEN** the helper groups input files into compaction groups such that output
  files approximately respect the provided thresholds
- **AND** the total row count across the dataset remains unchanged
- **AND** the schema is preserved.

#### Scenario: Dry-run plan

- **WHEN** `dry_run=True` is passed
- **THEN** the helper SHALL NOT create or delete any files
- **AND** SHALL return a stats object with `before_file_count`,
  `after_file_count`, `before_total_bytes`, `after_total_bytes`,
  `compacted_file_count`, `rewritten_bytes`, `compression_codec`, `dry_run`,
  and `planned_groups` describing which files would be compacted together.

### Requirement: PyArrow Parquet Dataset Z-Order Optimization

The system SHALL provide an `optimize_parquet_dataset_pyarrow` helper that
rewrites a parquet dataset ordered by a user-provided list of clustering
columns, approximating z-order style locality, using only PyArrow and fsspec.

#### Scenario: Optimize with clustering and compaction

- **WHEN** user calls  
  `optimize_parquet_dataset_pyarrow(path, zorder_columns=[\"user_id\", \"event_date\"], target_mb_per_file=256)`
- **THEN** the helper reads data in a streaming fashion, orders rows by the
  given columns (NULLs last) within each output file, and writes
  `optimized-*.parquet` files
- **AND** the resulting files approximate the requested size thresholds
- **AND** the returned stats include `before_file_count`, `after_file_count`,
  `zorder_columns`, `compacted_file_count`, `compression_codec`, and `dry_run`.

### Requirement: Maintenance Validation, Safety, and Memory Constraints

Maintenance helpers SHALL validate inputs, support dry-run, and avoid full
dataset materialization.

#### Scenario: Invalid thresholds

- **WHEN** user provides `target_mb_per_file <= 0` or
  `target_rows_per_file <= 0`
- **THEN** the helper SHALL raise `ValueError` with a clear message and SHALL
  not attempt any reads or writes.

#### Scenario: Non-existent path or no files matching filter

- **WHEN** the dataset path does not exist or the `partition_filter` excludes
  all parquet files
- **THEN** the helper SHALL raise `FileNotFoundError` indicating that no
  matching parquet files were found under the path.

#### Scenario: Streaming per-group processing

- **WHEN** compaction or optimization is run on a dataset with many files
- **THEN** the implementation SHALL:
  - Discover and group files using metadata and per-file row counts.
  - Read only the files in each group when processing that group.
  - Avoid calling `dataset.to_table()` or otherwise loading all files into a
    single in-memory `Table`.

#### Scenario: Partition-limited maintenance

- **WHEN** `partition_filter` is provided (e.g. `\"date=2025-11-04\"`)
- **THEN** only files under matching partition prefixes SHALL be considered for
  compaction or optimization
- **AND** other partitions SHALL remain untouched.

