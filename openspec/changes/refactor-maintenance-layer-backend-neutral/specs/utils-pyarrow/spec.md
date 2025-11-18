## MODIFIED Requirements

### Requirement: Backend-Neutral PyArrow Parquet Dataset Compaction
The system SHALL provide a `compact_parquet_dataset_pyarrow` helper that consolidates small parquet files in a dataset directory into fewer larger files using only PyArrow and fsspec, while leveraging shared backend-neutral maintenance planning for discovery and grouping.

#### Scenario: Compaction by target file size and rows
- **WHEN** user calls
  `compact_parquet_dataset_pyarrow(path, target_mb_per_file=128)` or
  `compact_parquet_dataset_pyarrow(path, target_rows_per_file=1_000_000)`
- **THEN** shared discovery logic finds parquet files and shared grouping algorithm groups them such that output files approximately respect the provided thresholds
- **AND** the total row count across the dataset remains unchanged
- **AND** the schema is preserved
- **AND** PyArrow execution focuses on streaming group processing rather than planning

#### Scenario: Dry-run plan with shared planning
- **WHEN** `dry_run=True` is passed
- **THEN** the helper SHALL NOT create or delete any files
- **AND** shared planning provides grouping information
- **AND** SHALL return a canonical stats object with `before_file_count`,
  `after_file_count`, `before_total_bytes`, `after_total_bytes`,
  `compacted_file_count`, `rewritten_bytes`, `compression_codec`, `dry_run`,
  and `planned_groups` describing which files would be compacted together
- **AND** stats structure matches DuckDB backend exactly

#### Scenario: Streaming per-group processing with shared groups
- **WHEN** compaction is run on a dataset with many files
- **THEN** shared discovery and grouping provides file groups
- **AND** the implementation SHALL:
  - Read only the files in each group when processing that group via PyArrow
  - Avoid calling `dataset.to_table()` or otherwise loading all files into a single in-memory `Table`
  - Execute streaming writes per group using fsspec

### Requirement: Backend-Neutral PyArrow Parquet Dataset Z-Order Optimization
The system SHALL provide an `optimize_parquet_dataset_pyarrow` helper that rewrites a parquet dataset ordered by a user-provided list of clustering columns, approximating z-order style locality, using only PyArrow and fsspec with shared backend-neutral planning.

#### Scenario: Optimize with clustering and compaction using shared planning
- **WHEN** user calls
  `optimize_parquet_dataset_pyarrow(path, zorder_columns=["user_id", "event_date"], target_mb_per_file=256)`
- **THEN** shared discovery finds files and shared validation confirms z-order columns exist
- **AND** shared planning determines grouping strategy that avoids full dataset materialization
- **AND** PyArrow execution reads data in a per-group streaming fashion, orders rows by the given columns (NULLs last) within each output file, and writes `optimized-*.parquet` files
- **AND** the resulting files approximate the requested size thresholds
- **AND** the returned canonical stats include `before_file_count`, `after_file_count`,
  `zorder_columns`, `compacted_file_count`, `compression_codec`, `dry_run`

#### Scenario: Z-order validation using shared logic
- **WHEN** user calls `optimize_parquet_dataset_pyarrow` with invalid `zorder_columns`
- **THEN** shared validation raises ValueError listing available columns
- **AND** no PyArrow execution is attempted

### Requirement: Enhanced Backend-Neutral Maintenance Constraints
Maintenance helpers SHALL validate inputs, support dry-run, and avoid full dataset materialization using shared planning while preserving PyArrow-specific streaming execution.

#### Scenario: Invalid thresholds with shared validation
- **WHEN** user provides `target_mb_per_file <= 0` or
  `target_rows_per_file <= 0`
- **THEN** shared validation SHALL raise `ValueError` with a clear message and SHALL
  not attempt any reads or writes via PyArrow

#### Scenario: Non-existent path or no files matching filter with shared discovery
- **WHEN** the dataset path does not exist or the `partition_filter` excludes
  all parquet files
- **THEN** shared discovery SHALL raise `FileNotFoundError` indicating that no
  matching parquet files were found under the path
- **AND** no PyArrow operations are attempted

#### Scenario: Streaming per-group processing with shared grouping
- **WHEN** compaction or optimization is run on a dataset with many files
- **THEN** shared planning provides file groups to avoid full materialization
- **AND** the PyArrow implementation SHALL:
  - Use shared discovery and grouping to get file lists
  - Read only the files in each group when processing that group
  - Avoid calling `dataset.to_table()` or otherwise loading all files into a
    single in-memory `Table`
  - Process groups sequentially using PyArrow streaming capabilities

#### Scenario: Partition-limited maintenance with shared filtering
- **WHEN** `partition_filter` is provided (e.g. `"date=2025-11-04"`)
- **THEN** shared discovery SHALL consider only files under matching partition prefixes
- **AND** PyArrow execution operates on filtered file list from shared planner
- **AND** other partitions SHALL remain untouched

#### Scenario: Canonical statistics structure
- **WHEN** maintenance operations complete
- **THEN** returned stats follow canonical structure matching DuckDB backend exactly
- **AND** include all required keys: `before_file_count`, `after_file_count`, `before_total_bytes`, `after_total_bytes`, `compacted_file_count`, `rewritten_bytes`, `compression_codec`, `dry_run`
- **AND** optionally include `zorder_columns` for optimization or `planned_groups` for dry-run scenarios