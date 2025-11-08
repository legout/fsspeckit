## ADDED Requirements

### Requirement: Parquet Dataset Compaction
The system SHALL provide a `compact_parquet_dataset` method that consolidates small parquet files in a dataset directory into fewer larger files according to configured thresholds while preserving all rows and schema.

#### Scenario: Compact dataset by target file size
- **WHEN** user calls `handler.compact_parquet_dataset(path, target_mb_per_file=128)`
- **THEN** method groups existing small files so output files are approximately 128MB
- **AND** total row count remains unchanged
- **AND** number of parquet files decreases

#### Scenario: Compact dataset by target rows per file
- **WHEN** user calls `handler.compact_parquet_dataset(path, target_rows_per_file=1_000_000)`
- **THEN** method rewrites dataset so each output file has <= 1,000,000 rows
- **AND** schema is preserved across all files

#### Scenario: Dry run compaction plan
- **WHEN** user calls `handler.compact_parquet_dataset(path, target_mb_per_file=256, dry_run=True)`
- **THEN** method returns planned file groups and estimated output file count
- **AND** existing files are not modified

#### Scenario: Recompression during compaction
- **WHEN** user calls `handler.compact_parquet_dataset(path, target_mb_per_file=128, compression="zstd")`
- **THEN** rewritten parquet files use zstd compression
- **AND** all data is preserved

#### Scenario: Partition-limited compaction
- **WHEN** user calls `handler.compact_parquet_dataset(path, partition_filter=["date=2025-11-04"])`
- **THEN** only files under matching partition paths are considered
- **AND** other partitions remain unchanged

#### Scenario: No-op when already optimized
- **WHEN** dataset has few large files above threshold
- **AND** user calls `compact_parquet_dataset` with same threshold
- **THEN** method performs no rewrite
- **AND** returns statistics indicating zero files compacted

### Requirement: Parquet Dataset Z-Order Optimization
The system SHALL provide an `optimize_parquet_dataset` method that rewrites a dataset ordering rows by a multi-column clustering key (z-order approximation) improving locality for selective predicate queries.

#### Scenario: Optimize dataset by z-order columns
- **WHEN** user calls `handler.optimize_parquet_dataset(path, zorder_columns=["customer_id", "event_date"])`
- **THEN** method reads dataset, orders rows by interleaving sort approximation on provided columns
- **AND** writes optimized files back (overwrite semantics)
- **AND** returns statistics including file count change

#### Scenario: Optimize with simultaneous compaction
- **WHEN** user calls `handler.optimize_parquet_dataset(path, zorder_columns=["user_id"], target_mb_per_file=256)`
- **THEN** method applies z-order ordering and file consolidation in a single rewrite
- **AND** resulting files approximate 256MB each

#### Scenario: Optimize dry run
- **WHEN** user calls `handler.optimize_parquet_dataset(path, zorder_columns=["user_id"], dry_run=True)`
- **THEN** method returns planned ordering and estimated file grouping
- **AND** dataset files are unchanged

#### Scenario: Invalid z-order column
- **WHEN** user calls `handler.optimize_parquet_dataset(path, zorder_columns=["missing_col"])`
- **THEN** method raises ValueError listing available columns

#### Scenario: Optimize already ordered dataset
- **WHEN** dataset is already clustered on given z-order columns
- **AND** user calls optimize again
- **THEN** method detects minimal reorder benefit and may no-op
- **AND** returns statistics indicating zero files rewritten

### Requirement: Maintenance Operation Statistics
The system SHALL return structured statistics objects from maintenance operations containing counts and size metrics.

#### Scenario: Compaction statistics
- **WHEN** user compacts dataset
- **THEN** return stats with keys: `before_file_count`, `after_file_count`, `before_total_bytes`, `after_total_bytes`, `compacted_file_count`, `rewritten_bytes`, `compression_codec`

#### Scenario: Optimization statistics
- **WHEN** user optimizes dataset
- **THEN** return stats with keys: `before_file_count`, `after_file_count`, `zorder_columns`, `compacted_file_count`, `dry_run`, `compression_codec`

### Requirement: Maintenance Validation and Safety
The system SHALL validate inputs and support dry-run safety for all maintenance operations.

#### Scenario: Invalid thresholds
- **WHEN** user provides `target_mb_per_file <= 0` or `target_rows_per_file <= 0`
- **THEN** method raises ValueError with clear message

#### Scenario: Dry run returns plan only
- **WHEN** dry_run=True is passed
- **THEN** no files are written or deleted
- **AND** plan includes proposed output file structure

#### Scenario: Non-existent dataset path
- **WHEN** user calls maintenance on path that does not exist
- **THEN** method raises FileNotFoundError with clear message
