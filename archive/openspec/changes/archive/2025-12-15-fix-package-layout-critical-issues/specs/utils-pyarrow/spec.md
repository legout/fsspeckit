# utils-pyarrow Specification

## Purpose
Defines PyArrow-specific dataset operations, schema utilities, and optimization helpers for parquet datasets.
## Requirements
### Requirement: Merge Parquet Dataset with Strategies (PyArrow)

The system SHALL provide a `merge_parquet_dataset_pyarrow` helper that merges a
source table or parquet dataset into a target parquet dataset directory using
PyArrow only, with configurable merge strategies.

#### Scenario: UPSERT with single key column

- **WHEN** user calls  
  `merge_parquet_dataset_pyarrow(source, target_path, key_columns=["id"], strategy="upsert")`
- **AND** `source` contains ids [1, 2, 3] where id=1,2 exist in the target
  dataset
- **THEN** the helper rewrites the target dataset so that rows for id=1,2 are
  updated with source values, id=3 is inserted, and all other rows are
  preserved.

#### Scenario: INSERT / UPDATE / FULL_MERGE / DEDUPLICATE

- **WHEN** the caller selects `strategy="insert"|"update"|"full_merge"|"deduplicate"`
- **THEN** the behavior for inserts, updates, deletes, and deduplication SHALL
  match the semantics defined by the DuckDB merge capability:
  - INSERT: insert only rows not present in the target.
  - UPDATE: update only rows already present; ignore new ones.
  - FULL_MERGE: insert new, update matching rows, and delete rows missing from
    the source (full sync).
  - DEDUPLICATE: deduplicate the source on `key_columns` (keeping the preferred
    record according to `dedup_order_by`) and then perform an UPSERT-style
    merge.

### Requirement: Merge Statistics Reporting (PyArrow)

The helper SHALL return a statistics dictionary with the following keys:
`"inserted"`, `"updated"`, `"deleted"`, and `"total"` representing the number
of inserted, updated, deleted, and final total rows in the merged dataset.

#### Scenario: Stats reflect merge operations

- **WHEN** a merge inserts 5 new rows, updates 10 existing rows, and deletes 3
  rows
- **THEN** the returned stats SHALL be
  `{"inserted": 5, "updated": 10, "deleted": 3, "total": <final_count>}`
- **AND** `total` SHALL equal `previous_total + inserted - deleted`.

### Requirement: Key Column and Schema Validation (PyArrow)

The helper SHALL validate that the requested key columns exist and are safe to
use for joining, and that the source and target schemas are compatible.

#### Scenario: Missing key columns or NULL keys

- **WHEN** any column in `key_columns` is missing from the source or target
  schemas
- **OR** contains NULL values in either source or target
- **THEN** the helper SHALL raise `ValueError` with an informative message
  indicating the problematic key(s)
- **AND** no merge output SHALL be written.

#### Scenario: Schema incompatibility

- **WHEN** source and target columns with the same name have incompatible types
  according to `utils-pyarrow` schema compatibility rules
- **THEN** the helper SHALL raise a descriptive error (e.g. `TypeError`) instead
  of performing a merge.

### Requirement: Filtered, Batch-Oriented Merge Execution

The helper SHALL avoid full in-memory materialization of large datasets and MUST
use PyArrow dataset scanners and compute filters to restrict what is loaded into
memory.

#### Scenario: Key-filtered scanning on large target

- **WHEN** the target dataset has many files (e.g. tens of millions of rows)
- **AND** `merge_parquet_dataset_pyarrow` is invoked
- **THEN** the implementation MUST:
  - Process the source in batches.
  - For each batch, construct a filter on the target such as
    `pc.field("id").is_in(batch_keys)` using `pyarrow.compute`.
  - Use `pyarrow.dataset.Scanner` or equivalent to read only rows matching that
    filter from the target.
  - Avoid calling unfiltered `dataset.to_table()` on the entire target dataset.

#### Scenario: Partition-limited merge

- **WHEN** the source is partitioned (e.g. `/staging/updates/date=2025-11-15`)
- **THEN** the implementation SHOULD take advantage of partition information
  (where available) to further restrict the target scan to relevant partitions
  before applying the key-based filter.

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

### Requirement: Shared Merge Core Integration

merge_parquet_dataset_pyarrow SHALL use shared validation and statistics from fsspeckit.core.merge.

#### Scenario: Backend-neutral validation usage
- WHEN calling merge_parquet_dataset_pyarrow
- THEN function calls normalize_keys() for key column normalization
- AND function calls validate_keys_and_schema() for shared validation
- AND function calls compute_merge_stats() for canonical statistics

### Requirement: Canonical Key Validation

merge_parquet_dataset_pyarrow SHALL perform canonical key validation using backend-neutral helpers.

#### Scenario: Key normalization
- WHEN user provides key_columns as string or list
- THEN function calls normalize_keys() to convert to list format
- AND returns normalized list for internal use

#### Scenario: Key existence validation
- WHEN source table and target dataset schemas are available
- THEN function validates all key columns exist in both schemas
- AND raises ValueError with clear message for missing keys

#### Scenario: NULL key detection with filtered scanners
- WHEN processing merge data
- THEN function uses filtered scanners to detect NULL values in key columns
- AND raises ValueError before processing if NULL keys found
- AND uses scanner filtering to avoid full dataset materialization for NULL checking

### Requirement: Canonical Strategy Semantics

All merge strategies SHALL follow canonical semantics defined in backend-neutral core.

#### Scenario: UPSERT strategy behavior
- WHEN strategy="upsert" and key exists in target
- THEN existing target row is updated with source row data
- AND statistics.count updated incremented

#### Scenario: UPSERT strategy new keys
- WHEN strategy="upsert" and key does not exist in target
- THEN source row is inserted as new row
- AND statistics.count inserted incremented

#### Scenario: INSERT strategy behavior
- WHEN strategy="insert" and key exists in target
- THEN source row is ignored (no update)
- AND statistics.count updated unchanged

#### Scenario: INSERT strategy new keys
- WHEN strategy="insert" and key does not exist in target
- THEN source row is inserted as new row
- AND statistics.count inserted incremented

#### Scenario: UPDATE strategy behavior
- WHEN strategy="update" and key exists in target
- THEN existing target row is updated with source row data
- AND statistics.count updated incremented

#### Scenario: UPDATE strategy new keys
- WHEN strategy="update" and key does not exist in target
- THEN source row is ignored (no insert)
- AND statistics.count updated unchanged

#### Scenario: FULL_MERGE strategy behavior
- WHEN strategy="full_merge"
- THEN perform insert for new keys, update for existing keys, and delete for target-only keys
- AND all three statistics (inserted, updated, deleted) can be non-zero

#### Scenario: FULL_MERGE empty source
- WHEN strategy="full_merge" and source is empty
- THEN all target rows are deleted
- AND statistics.count deleted equals original target row count
- AND statistics.count inserted and updated are zero

#### Scenario: DEDUPLICATE strategy behavior
- WHEN strategy="deduplicate"
- THEN deduplicate source data first (keep highest values from dedup_order_by)
- AND then apply UPSERT semantics to deduplicated source
- AND statistics reflect operations on deduplicated data

### Requirement: Streaming Scanner Requirements

merge_parquet_dataset_pyarrow SHALL use filtered scanners as primary mechanism.

#### Scenario: Key-based filtered scanning
- WHEN matching source keys to target dataset
- THEN build filter expressions around key column values
- AND use scanner.filter() to limit target data to relevant rows
- AND process only target rows that could match source keys

#### Scenario: Batch streaming processing
- WHEN processing target dataset
- THEN use scanner.to_batches() with batch_rows parameter
- AND process each batch independently in memory
- AND build output incrementally without full dataset materialization

#### Scenario: Avoid full dataset materialization
- WHEN processing merge operations
- THEN avoid calling dataset.to_table() on full target dataset
- AND use filtered scanners for all target data access
- AND maintain memory usage bounded by batch size

### Requirement: Per-Group Processing

merge operations SHALL process data in streaming batches.

#### Scenario: Batch row processing
- WHEN processing target batches via scanner.to_batches()
- THEN process each batch to identify insert/update/delete actions
- AND build output table for each batch
- AND accumulate statistics across batches

#### Scenario: Batch size control
- WHEN batch_rows parameter is specified
- THEN limit each batch to at most batch_rows rows
- AND adjust memory usage accordingly
- AND ensure deterministic processing regardless of batch size

#### Scenario: Source row handling
- WHEN processing all target batches
- THEN append remaining source rows that should be inserted
- AND handle cases where source keys had no target matches
- AND ensure final dataset includes all appropriate source rows

### Requirement: Enhanced Schema Compatibility

merge operations SHALL use shared schema compatibility helpers.

#### Scenario: Schema unification
- WHEN source and target schemas have compatible types
- THEN use unify_schemas() from shared core for type promotion
- AND apply consistent type casting across both backends
- AND preserve existing casting behavior for compatible schemas

#### Scenario: Type promotion
- WHEN schemas have different but compatible types (e.g., int32 vs int64)
- THEN promote to widest compatible type
- AND maintain data integrity during merge
- AND apply consistent promotion rules

#### Scenario: Schema incompatibility
- WHEN source and target schemas have incompatible types
- THEN raise ValueError with clear schema mismatch message
- AND identify incompatible column names or types
- AND suggest resolution approaches when possible

### Requirement: Canonical Statistics Structure

merge_parquet_dataset_pyarrow SHALL return canonical statistics structure.

#### Scenario: Statistics format
- WHEN merge operation completes successfully
- THEN return dict with keys: "inserted", "updated", "deleted", "total"
- AND all values are non-negative integers
- AND "total" equals final target dataset row count

#### Scenario: Statistics calculation
- WHEN calculating merge statistics
- THEN count actual rows inserted, updated, deleted during operation
- AND accumulate statistics incrementally during batch processing
- AND ensure statistics sum to expected totals

#### Scenario: Empty source statistics
- WHEN source table is empty
- THEN return {"inserted": 0, "updated": 0, "deleted": target_row_count, "total": 0}
- IF strategy="full_merge" and target exists
- AND target is removed completely

### Requirement: Edge Case Alignment

Edge case behavior SHALL match shared canonical definitions.

#### Scenario: Empty target + UPSERT/INSERT
- WHEN target dataset does not exist and strategy is "upsert" or "insert"
- THEN all source rows are inserted into new dataset
- AND statistics.deleted = 0
- AND statistics.total = source row count

#### Scenario: Empty target + UPDATE
- WHEN target dataset does not exist and strategy is "update"
- THEN zero rows are updated
- AND statistics.updated = 0
- AND no dataset is created (consistent no-op behavior)

#### Scenario: Filtered scanner NULL key detection
- WHEN checking for NULL keys in target dataset
- THEN use scanner.filter() with IS NULL conditions on key columns
- AND detect NULL values without full dataset materialization
- AND raise appropriate error before processing begins

#### Scenario: Deduplication ordering
- WHEN strategy="deduplicate" and dedup_order_by is specified
- THEN keep records with highest values in dedup_order_by columns
- AND apply sorting before deduplication
- AND maintain consistent ordering with DuckDB backend

### Requirement: PyArrow helpers reuse shared schema and partition logic

PyArrow-based dataset helpers SHALL delegate schema compatibility and partition handling decisions to shared helper modules instead of maintaining separate implementations.

#### Scenario: Schema unification uses shared helper
- **WHEN** PyArrow-based helpers need to unify schemas across multiple tables or datasets
- **THEN** they SHALL call the shared schema helper (e.g. `common.schema`) for unification
- **AND** SHALL not embed duplicate unification logic locally.

#### Scenario: Partition handling uses shared helper
- **WHEN** PyArrow-based helpers need to reason about partitioned paths
- **THEN** they SHALL use the canonical partition helper (e.g. `common.partitions`)
- **AND** the semantics SHALL match the behaviour documented by that helper.

### Requirement: Parquet helpers return consistent PyArrow tables

The system SHALL ensure that helpers which read Parquet data and expose a PyArrow interface:

- Always return `pyarrow.Table` objects when declared to do so.
- Use native PyArrow types for any additional columns that they add (for example, `file_path`).

#### Scenario: Parquet `include_file_path` uses a PyArrow string array
- **WHEN** a caller reads Parquet data via a helper that returns a `pyarrow.Table`
- **AND** passes `include_file_path=True`
- **THEN** the resulting table SHALL include a `file_path` column
- **AND** that column SHALL be a PyArrow string array whose length matches the number of rows in the table.

#### Scenario: Threading parameter does not change semantics
- **WHEN** a caller reads JSON or CSV data via helpers that support a `use_threads` parameter
- **AND** calls the helper with the same paths and arguments but different values for `use_threads`
- **THEN** the resulting data SHALL be semantically equivalent (same records)
- **AND** the `use_threads` parameter SHALL only affect the execution strategy (parallel vs sequential), not the content.

### Requirement: PyArrow helpers use specific exception types

PyArrow dataset helpers and utilities SHALL surface specific PyArrow exception
types for expected failure modes and SHALL preserve the original exception type
and message when re-raising.

#### Scenario: Invalid schema uses ArrowInvalid
- **WHEN** a merge, compaction, or optimization helper encounters an invalid
  schema or data condition (e.g. incompatible column types or missing fields)
- **THEN** it SHALL raise or propagate a `pyarrow.ArrowInvalid` (or a more
  specific PyArrow error) instead of a generic `Exception`
- **AND** the error message SHALL include operation context (helper name, path,
  and a short description of the schema issue).

#### Scenario: I/O failures use ArrowIOError
- **WHEN** a helper fails to read or write parquet data due to file system or
  network issues
- **THEN** it SHALL raise or propagate a `pyarrow.ArrowIOError`
- **AND** the message SHALL include the affected path or dataset identifier
  and a brief description of the I/O failure.

#### Scenario: Type and indexing errors use typed exceptions
- **WHEN** a helper performs type conversions or positional access that fails
- **THEN** it SHALL raise `pyarrow.ArrowTypeError`, `pyarrow.ArrowKeyError`,
  or `pyarrow.ArrowIndexError` as appropriate
- **AND** callers MAY rely on these specific types to handle failures in
  a targeted way.

### Requirement: PyArrow error handling is logged and non-silencing

PyArrow helpers SHALL avoid silently swallowing unexpected exceptions and SHALL
log error details using the project logging utilities before propagating the
error.

#### Scenario: Unexpected errors are logged and re-raised
- **WHEN** an unexpected exception is raised inside a PyArrow helper
- **THEN** the helper SHALL log the error (including operation name and
  relevant path or dataset identifiers) using a module-level logger
- **AND** it SHALL re-raise the exception (or a more specific wrapper) rather
  than returning partial results or silently continuing.

#### Scenario: Cleanup helpers log individual failures
- **WHEN** a cleanup helper is used to release PyArrow-related resources
  (datasets, scanners, temporary files)
- **THEN** it SHALL handle each resource individually so that a failure to
  clean up one resource does not prevent attempts on others
- **AND** each cleanup failure SHALL be logged with context instead of being
  ignored.

## MODIFIED Requirements

### Requirement: PyArrow helpers reuse shared schema and partition logic (MODIFIED)

PyArrow-based dataset helpers SHALL delegate schema compatibility and partition handling decisions to shared helper modules instead of maintaining separate implementations.

#### Scenario: Schema utilities moved to common (MODIFIED)
- **WHEN** PyArrow-based helpers need schema operations (`cast_schema`, `opt_dtype`, `unify_schemas`, `convert_large_types_to_normal`)
- **THEN** they SHALL import these functions from `fsspeckit.common.schema`
- **AND** SHALL NOT maintain duplicate implementations in `datasets.pyarrow.schema`
- **AND** SHALL re-export these functions from `datasets.pyarrow` for backwards compatibility

#### Scenario: Core modules use common schema utilities (MODIFIED)
- **WHEN** `core.ext.parquet` needs schema operations
- **THEN** it SHALL import from `fsspeckit.common.schema` (not from `datasets`)
- **AND** this maintains architectural rule that core depends on common but not on datasets

## REMOVED Requirements

### Requirement: Local Schema Utilities in PyArrow Package (REMOVED)

Schema utility functions SHALL be implemented locally in `datasets.pyarrow.schema` for PyArrow-specific operations.

#### Scenario: Local schema implementation
- **WHEN** PyArrow helpers need schema operations
- **THEN** they SHALL use local implementations in `datasets.pyarrow.schema`
- **AND** these implementations SHALL be optimized for PyArrow usage patterns

**REASONING**: This requirement is removed to eliminate code duplication and establish `common.schema` as canonical location for schema utilities across all backends.

