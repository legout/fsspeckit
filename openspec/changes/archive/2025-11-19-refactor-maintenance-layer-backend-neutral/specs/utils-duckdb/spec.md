## MODIFIED Requirements

### Requirement: Backend-Neutral Parquet Dataset Compaction
The system SHALL provide a `compact_parquet_dataset` method that consolidates small parquet files in a dataset directory into fewer larger files according to configured thresholds while preserving all rows and schema, using shared backend-neutral maintenance planning for discovery and grouping.

#### Scenario: Compact dataset by target file size
- **WHEN** user calls `handler.compact_parquet_dataset(path, target_mb_per_file=128)`
- **THEN** method uses shared discovery to find parquet files and groups them using backend-neutral planning so output files are approximately 128MB
- **AND** total row count remains unchanged
- **AND** number of parquet files decreases
- **AND** method executes group rewrites via DuckDB while using shared grouping logic

#### Scenario: Compact dataset by target rows per file
- **WHEN** user calls `handler.compact_parquet_dataset(path, target_rows_per_file=1_000_000)`
- **THEN** method uses shared grouping algorithm to rewrite dataset so each output file has <= 1,000,000 rows
- **AND** schema is preserved across all files
- **AND** DuckDB backend focuses on execution rather than planning

#### Scenario: Dry run compaction plan
- **WHEN** user calls `handler.compact_parquet_dataset(path, target_mb_per_file=256, dry_run=True)`
- **THEN** method uses shared planning to return planned file groups and estimated output file count
- **AND** existing files are not modified
- **AND** returned stats follow canonical structure shared across backends

#### Scenario: Recompression during compaction
- **WHEN** user calls `handler.compact_parquet_dataset(path, target_mb_per_file=128, compression="zstd")`
- **THEN** rewritten parquet files use zstd compression via DuckDB execution
- **AND** all data is preserved
- **AND** compression codec is reflected in shared stats structure

#### Scenario: Partition-limited compaction
- **WHEN** user calls `handler.compact_parquet_dataset(path, partition_filter=["date=2025-11-04"])`
- **THEN** shared discovery logic considers only files under matching partition paths
- **AND** other partitions remain unchanged
- **AND** DuckDB execution operates on filtered file list from shared planner

#### Scenario: No-op when already optimized
- **WHEN** dataset has few large files above threshold
- **AND** user calls `compact_parquet_dataset` with same threshold
- **THEN** shared grouping determines no compaction needed
- **AND** method performs no rewrite
- **AND** returns canonical statistics indicating zero files compacted

### Requirement: Backend-Neutral Parquet Dataset Z-Order Optimization
The system SHALL provide an `optimize_parquet_dataset` method that rewrites a dataset ordering rows by a multi-column clustering key (z-order approximation) improving locality for selective predicate queries, using shared backend-neutral planning for discovery, validation, and grouping.

#### Scenario: Optimize dataset by z-order columns
- **WHEN** user calls `handler.optimize_parquet_dataset(path, zorder_columns=["customer_id", "event_date"])`
- **THEN** shared discovery and validation finds files and validates column existence
- **AND** shared planning determines grouping strategy
- **AND** DuckDB backend orders rows by interleaving sort approximation on provided columns
- **AND** writes optimized files back (overwrite semantics)
- **AND** returns canonical statistics including file count change

#### Scenario: Optimize with simultaneous compaction
- **WHEN** user calls `handler.optimize_parquet_dataset(path, zorder_columns=["user_id"], target_mb_per_file=256)`
- **THEN** shared planning combines z-order validation with compaction grouping
- **AND** DuckDB backend applies z-order ordering and file consolidation in a single rewrite
- **AND** resulting files approximate 256MB each

#### Scenario: Optimize dry run
- **WHEN** user calls `handler.optimize_parquet_dataset(path, zorder_columns=["user_id"], dry_run=True)`
- **THEN** shared validation and planning returns proposed ordering and estimated file grouping
- **AND** dataset files are unchanged
- **AND** stats follow canonical structure shared across backends

#### Scenario: Invalid z-order column
- **WHEN** user calls `handler.optimize_parquet_dataset(path, zorder_columns=["missing_col"])`
- **THEN** shared validation raises ValueError listing available columns
- **AND** no DuckDB execution is attempted

#### Scenario: Optimize already ordered dataset
- **WHEN** dataset is already clustered on given z-order columns
- **AND** user calls optimize again
- **THEN** shared planning determines minimal benefit
- **AND** method may no-op
- **AND** returns canonical statistics indicating zero files rewritten

### Requirement: Canonical Maintenance Operation Statistics
The system SHALL return structured statistics objects from maintenance operations containing counts and size metrics following a canonical structure shared across all backends.

#### Scenario: Compaction statistics
- **WHEN** user compacts dataset
- **THEN** return canonical stats with keys: `before_file_count`, `after_file_count`, `before_total_bytes`, `after_total_bytes`, `compacted_file_count`, `rewritten_bytes`, `compression_codec`, `dry_run`
- **AND** stats structure matches PyArrow backend exactly

#### Scenario: Optimization statistics
- **WHEN** user optimizes dataset
- **THEN** return canonical stats with keys: `before_file_count`, `after_file_count`, `before_total_bytes`, `after_total_bytes`, `compacted_file_count`, `rewritten_bytes`, `compression_codec`, `dry_run`, `zorder_columns`
- **AND** stats structure matches PyArrow backend exactly

#### Scenario: Statistics with planning details
- **WHEN** dry_run=True or planning information is requested
- **THEN** canonical stats include optional `planned_groups` key describing file groupings
- **AND** structure matches across DuckDB and PyArrow backends

### Requirement: Backend-Neutral Maintenance Validation and Safety
The system SHALL validate inputs and support dry-run safety for all maintenance operations using shared validation logic while preserving DuckDB-specific execution behavior.

#### Scenario: Invalid thresholds
- **WHEN** user provides `target_mb_per_file <= 0` or `target_rows_per_file <= 0`
- **THEN** shared validation raises ValueError with clear message
- **AND** no DuckDB execution is attempted

#### Scenario: Dry run returns plan only
- **WHEN** dry_run=True is passed
- **THEN** no files are written or deleted via DuckDB
- **AND** shared planning provides proposed output file structure
- **AND** stats follow canonical format

#### Scenario: Non-existent dataset path
- **WHEN** user calls maintenance on path that does not exist
- **THEN** shared discovery raises FileNotFoundError with clear message
- **AND** no DuckDB operations are attempted

#### Scenario: Shared file discovery behavior
- **WHEN** maintenance operations are called with partition filters
- **THEN** shared discovery logic applies filters consistently across backends
- **AND** DuckDB execution receives filtered file list from shared planner