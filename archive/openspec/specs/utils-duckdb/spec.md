# utils-duckdb Specification

## Purpose
TBD - created by archiving change add-duckdb-dataset-write. Update Purpose after archive.
## Requirements
### Requirement: Filesystem Registration in DuckDB
The system SHALL register fsspec filesystem instances in DuckDB connections to enable operations on remote storage systems. This is managed by `DuckDBConnection`.

#### Scenario: Register filesystem via DuckDBConnection
- **WHEN** `DuckDBConnection` is initialized with a filesystem
- **THEN** it SHALL register the filesystem with the active DuckDB connection
- **AND** DuckDB SHALL be able to access paths using the registered filesystem

### Requirement: Read Parquet Files and Datasets

The system SHALL provide a `read_parquet` method that reads parquet files or directories containing parquet files and returns PyArrow tables.

#### Scenario: Read single parquet file

- **WHEN** user calls `handler.read_parquet("/path/to/file.parquet")`
- **THEN** method returns PyArrow table with all data from the file

#### Scenario: Read parquet dataset directory

- **WHEN** user calls `handler.read_parquet("/path/to/dataset/")`
- **THEN** method reads all parquet files in directory and subdirectories
- **AND** returns combined PyArrow table with all data

#### Scenario: Read with column selection

- **WHEN** user calls `handler.read_parquet(path, columns=["col1", "col2"])`
- **THEN** method returns PyArrow table containing only specified columns
- **AND** improves performance by reading only required columns

#### Scenario: Read from remote storage

- **WHEN** user provides remote path like "s3://bucket/data.parquet"
- **AND** handler has appropriate storage options configured
- **THEN** method reads parquet data from remote location using registered filesystem

### Requirement: Write Parquet Files

The system SHALL provide a `write_parquet` method that writes PyArrow tables to parquet format with configurable compression.

#### Scenario: Write parquet file with default compression

- **WHEN** user calls `handler.write_parquet(table, "/path/to/output.parquet")`
- **THEN** method writes PyArrow table to parquet file
- **AND** creates parent directories if they don't exist

#### Scenario: Write with custom compression

- **WHEN** user calls `handler.write_parquet(table, path, compression="gzip")`
- **THEN** method writes parquet file with specified compression codec
- **AND** supports codecs: "snappy", "gzip", "lz4", "zstd", "brotli"

#### Scenario: Write to remote storage

- **WHEN** user provides remote path like "s3://bucket/output.parquet"
- **AND** handler has appropriate storage options configured
- **THEN** method writes parquet data to remote location using registered filesystem

#### Scenario: Write to nested directory

- **WHEN** user provides path with multiple nested directories
- **AND** parent directories don't exist
- **THEN** method creates all necessary parent directories
- **AND** writes parquet file successfully

### Requirement: SQL Query Execution
The system SHALL provide an `execute_sql` method on `DuckDBConnection` that executes SQL queries using the managed DuckDB connection and returns results.

#### Scenario: Execute SQL query via connection manager
- **WHEN** user calls `connection.execute_sql("SELECT * FROM ...")`
- **THEN** method executes query using the managed DuckDB connection
- **AND** returns results as a DuckDB result object (which can fetch Arrow tables)

### Requirement: Context Manager Support
`DuckDBConnection` SHALL implement the context manager protocol for automatic resource cleanup and connection management.

#### Scenario: Use DuckDBConnection with statement
- **WHEN** user uses `with DuckDBConnection(fs) as conn:`
- **THEN** connection is automatically closed on exit
- **AND** resources are properly cleaned up

### Requirement: Type Safety and Documentation

The system SHALL provide complete type hints for all public methods and comprehensive Google-style docstrings with usage examples.

#### Scenario: Type hints for all methods

- **WHEN** developer uses handler in type-checked code
- **THEN** all method signatures have complete type annotations
- **AND** mypy validates types correctly

#### Scenario: Comprehensive docstrings

- **WHEN** developer reads method documentation
- **THEN** each method has Google-style docstring
- **AND** docstring includes description, arguments, returns, and usage examples

### Requirement: Error Handling

The system SHALL provide clear error messages for common failure scenarios with specific exception types that allow callers to distinguish between different error conditions.

#### Scenario: Invalid path error

- **WHEN** user provides non-existent path to read_parquet
- **THEN** method raises FileNotFoundError with clear message indicating file or directory not found
- **AND** error message includes the problematic path

#### Scenario: Invalid storage options error

- **WHEN** user provides storage options with missing credentials for remote storage
- **THEN** method raises clear exception indicating authentication failure
- **AND** error preserves original authentication error details

#### Scenario: SQL execution error

- **WHEN** SQL query has syntax error or references invalid columns
- **THEN** execute_sql raises exception with DuckDB error message
- **AND** original DuckDB error type and message are preserved

### Requirement: Merge Parquet Dataset with UPSERT Strategy

The system SHALL provide a `merge_parquet_dataset` method that merges source data into target dataset using UPSERT strategy (insert new records, update existing records based on key columns).

#### Scenario: UPSERT with single key column

- **WHEN** user calls `handler.merge_parquet_dataset(source, target, key_columns=["id"], strategy="upsert")`
- **AND** source contains records with ids [1, 2, 3] (id=1,2 exist in target, id=3 is new)
- **THEN** method updates existing records (id=1,2) with new values from source
- **AND** inserts new record (id=3) into target
- **AND** preserves other target records not in source

#### Scenario: UPSERT with composite key

- **WHEN** user merges with composite key `key_columns=["customer_id", "order_date"]`
- **AND** source has records matching on both columns
- **THEN** method updates records where both key columns match
- **AND** inserts records where key combination doesn't exist
- **AND** treats partial key matches as different records

#### Scenario: UPSERT from PyArrow table source

- **WHEN** source is PyArrow table with new/updated records
- **THEN** method performs UPSERT using table data
- **AND** returns merge statistics with inserted and updated counts

#### Scenario: UPSERT from parquet path source

- **WHEN** source is path to parquet dataset (e.g., "/staging/updates/")
- **THEN** method reads source dataset and performs UPSERT
- **AND** handles source dataset with multiple files correctly

### Requirement: Merge Parquet Dataset with INSERT Strategy

The system SHALL support INSERT strategy that adds only new records from source, ignoring records that already exist in target.

#### Scenario: INSERT only new records

- **WHEN** user calls `merge_parquet_dataset(source, target, key_columns=["id"], strategy="insert")`
- **AND** source contains ids [1, 2, 3] where id=1,2 exist in target
- **THEN** method inserts only id=3 (new record)
- **AND** preserves existing target records unchanged
- **AND** ignores source records with matching keys

#### Scenario: INSERT with no new records

- **WHEN** all source records exist in target (based on key columns)
- **THEN** method completes without error
- **AND** target dataset remains unchanged
- **AND** returns statistics showing zero inserted records

#### Scenario: INSERT all records as new

- **WHEN** no source records exist in target
- **THEN** method inserts all source records
- **AND** returns statistics showing all records inserted

### Requirement: Merge Parquet Dataset with UPDATE Strategy

The system SHALL support UPDATE strategy that updates only existing records, ignoring new records from source.

#### Scenario: UPDATE only existing records

- **WHEN** user calls `merge_parquet_dataset(source, target, key_columns=["id"], strategy="update")`
- **AND** source contains ids [1, 2, 3] where id=1,2 exist in target
- **THEN** method updates existing records (id=1,2) with source values
- **AND** ignores new record (id=3)
- **AND** preserves other target records unchanged

#### Scenario: UPDATE with no matching records

- **WHEN** no source records exist in target (all are new)
- **THEN** method completes without error
- **AND** target dataset remains unchanged
- **AND** returns statistics showing zero updated records

#### Scenario: UPDATE all matching records

- **WHEN** all source records exist in target
- **THEN** method updates all source records in target
- **AND** returns statistics showing all records updated

### Requirement: Merge Parquet Dataset with FULL_MERGE Strategy

The system SHALL support FULL_MERGE strategy that inserts new records, updates existing records, and deletes records missing from source (synchronization).

#### Scenario: FULL_MERGE with inserts, updates, and deletes

- **WHEN** user calls `merge_parquet_dataset(source, target, key_columns=["id"], strategy="full_merge")`
- **AND** source has ids [2, 3, 4]
- **AND** target has ids [1, 2, 3]
- **THEN** method updates id=2,3 with source values
- **AND** inserts new id=4
- **AND** deletes id=1 (missing from source)

#### Scenario: FULL_MERGE replaces entire dataset

- **WHEN** target has records not in source
- **THEN** method removes all target records not in source
- **AND** final dataset matches source exactly
- **AND** returns statistics showing inserted, updated, and deleted counts

#### Scenario: FULL_MERGE with empty source

- **WHEN** source dataset is empty
- **THEN** method deletes all records from target
- **AND** target becomes empty dataset
- **AND** returns statistics showing all records deleted

### Requirement: Merge Parquet Dataset with DEDUPLICATE Strategy

The system SHALL support DEDUPLICATE strategy that removes duplicates from source before performing UPSERT merge.

#### Scenario: DEDUPLICATE with duplicate source records

- **WHEN** user calls `merge_parquet_dataset(source, target, key_columns=["id"], strategy="deduplicate")`
- **AND** source has duplicate ids [1, 1, 2] with different values
- **THEN** method removes duplicates from source first
- **AND** keeps last occurrence of each duplicate (by default)
- **AND** performs UPSERT with deduplicated source

#### Scenario: DEDUPLICATE with custom sort order

- **WHEN** user specifies `dedup_order_by=["timestamp"]` parameter
- **AND** source has duplicates with different timestamps
- **THEN** method keeps record with highest timestamp value
- **AND** removes other duplicates
- **AND** performs UPSERT with deduplicated result

#### Scenario: DEDUPLICATE with no duplicates

- **WHEN** source has no duplicate keys
- **THEN** method performs standard UPSERT (no deduplication needed)
- **AND** behavior identical to UPSERT strategy

### Requirement: Merge Statistics Reporting

The system SHALL return merge statistics indicating number of records inserted, updated, deleted, and total records in merged dataset.

#### Scenario: Return statistics for UPSERT

- **WHEN** merge completes successfully
- **THEN** method returns dictionary with statistics
- **AND** includes "inserted" count (new records added)
- **AND** includes "updated" count (existing records modified)
- **AND** includes "deleted" count (0 for UPSERT)
- **AND** includes "total" count (final dataset size)

#### Scenario: Statistics reflect actual operations

- **WHEN** 5 records inserted, 10 updated, 3 deleted
- **THEN** statistics show {"inserted": 5, "updated": 10, "deleted": 3, "total": <count>}
- **AND** total equals previous_total + inserted - deleted

#### Scenario: Statistics for INSERT strategy

- **WHEN** using INSERT strategy
- **THEN** statistics show only inserted count
- **AND** updated count is 0
- **AND** deleted count is 0

### Requirement: Key Column Validation

The system SHALL validate that key columns exist in both source and target datasets and contain no NULL values.

#### Scenario: Missing key column in source

- **WHEN** key_columns includes column not in source dataset
- **THEN** method raises ValueError with clear error message
- **AND** indicates which column is missing
- **AND** no merge operation is performed

#### Scenario: Missing key column in target

- **WHEN** key_columns includes column not in target dataset
- **THEN** method raises ValueError with clear error message
- **AND** indicates which column is missing
- **AND** no merge operation is performed

#### Scenario: NULL values in key columns

- **WHEN** source or target has NULL values in key columns
- **THEN** method raises ValueError indicating NULL keys not allowed
- **AND** suggests filtering or filling NULL values first
- **AND** no merge operation is performed

#### Scenario: Valid key columns

- **WHEN** all key columns exist in both source and target
- **AND** no NULL values in key columns
- **THEN** method proceeds with merge operation
- **AND** uses specified columns for record matching

### Requirement: Schema Compatibility Validation

The system SHALL validate that source and target datasets have compatible schemas before merging.

#### Scenario: Matching schemas

- **WHEN** source and target have identical column names and types
- **THEN** method proceeds with merge
- **AND** no schema errors are raised

#### Scenario: Schema mismatch - missing column

- **WHEN** source has column not in target (or vice versa)
- **THEN** method raises ValueError indicating schema mismatch
- **AND** lists columns that don't match
- **AND** no merge operation is performed

#### Scenario: Schema mismatch - type incompatibility

- **WHEN** source and target have same column names but different types
- **THEN** method raises TypeError indicating incompatible types
- **AND** shows column name and type difference
- **AND** no merge operation is performed

### Requirement: Merge with Compression

The system SHALL support configurable compression for merged dataset output.

#### Scenario: Merge with custom compression

- **WHEN** user specifies `compression="gzip"` parameter
- **THEN** method writes merged dataset with gzip compression
- **AND** all output files use specified compression

#### Scenario: Merge with default compression

- **WHEN** user doesn't specify compression parameter
- **THEN** method uses default snappy compression
- **AND** merged dataset written with snappy compression

### Requirement: Merge Error Handling

The system SHALL provide clear error messages for invalid merge operations and handle errors gracefully.

#### Scenario: Invalid strategy value

- **WHEN** user provides strategy not in valid set
- **THEN** method raises ValueError listing valid strategies
- **AND** no merge operation is performed

#### Scenario: Empty target dataset

- **WHEN** target dataset path doesn't exist or is empty
- **AND** strategy is UPSERT or INSERT
- **THEN** method creates new dataset with source data
- **AND** treats as initial data load

#### Scenario: Empty target with UPDATE strategy

- **WHEN** target dataset is empty
- **AND** strategy is UPDATE
- **THEN** method completes with no changes
- **AND** returns statistics showing zero updates

#### Scenario: Merge failure mid-operation

- **WHEN** merge operation fails during processing
- **THEN** method raises exception with clear error message
- **AND** target dataset remains in original state (atomic operation)
- **AND** no partial merge is left behind

### Requirement: Merge Performance Optimization

The system SHALL optimize merge operations for performance using DuckDB's analytical query engine.

#### Scenario: Efficient key matching

- **WHEN** merging datasets with key columns
- **THEN** method uses DuckDB joins for key matching
- **AND** leverages DuckDB query optimization
- **AND** completes merge in reasonable time for large datasets

#### Scenario: Memory-efficient merge

- **WHEN** merging large datasets
- **THEN** method uses DuckDB's larger-than-memory capabilities
- **AND** doesn't require entire dataset in Python memory
- **AND** handles datasets larger than available RAM

### Requirement: Merge Documentation and Examples

The system SHALL provide comprehensive documentation and examples for all merge strategies.

#### Scenario: Docstring completeness

- **WHEN** user accesses method documentation
- **THEN** docstring includes all parameters
- **AND** explains each merge strategy with examples
- **AND** shows expected behavior for each strategy

#### Scenario: Example availability

- **WHEN** user looks for merge examples
- **THEN** example scripts demonstrate each strategy
- **AND** show common use cases (CDC, incremental loads, deduplication)
- **AND** include performance best practices

### Requirement: Parquet Dataset Compaction
The system SHALL provide a `compact_parquet_dataset_duckdb` function that consolidates small parquet files using DuckDB's native SQL `COPY` command for efficient, streaming data movement.

#### Scenario: Compact dataset using DuckDB SQL
- **WHEN** `compact_parquet_dataset_duckdb` is called
- **THEN** it SHALL use DuckDB SQL `COPY (SELECT * FROM parquet_scan(...)) TO ...`
- **AND** it SHALL support recompression during compaction
- **AND** it SHALL preserve all data and schema

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

The system SHALL validate inputs and support dry-run safety for all maintenance operations with consistent error handling.

#### Scenario: Invalid thresholds

- **WHEN** user provides `target_mb_per_file <= 0` or `target_rows_per_file <= 0`
- **THEN** method raises ValueError with clear message indicating minimum valid values
- **AND** error message includes the invalid threshold values

#### Scenario: Dry run returns plan only

- **WHEN** dry_run=True is passed
- **THEN** no files are written or deleted
- **AND** plan includes proposed output file structure

#### Scenario: Non-existent dataset path

- **WHEN** user calls maintenance on path that does not exist
- **THEN** method raises FileNotFoundError with clear message indicating dataset path not found
- **AND** error message includes the problematic path

#### Scenario: No parquet files found

- **WHEN** maintenance operation finds no parquet files matching criteria (including partition filters)
- **THEN** method raises FileNotFoundError with clear message indicating no parquet files found
- **AND** error message specifies the search criteria that yielded no results

### Requirement: Shared Merge Core Integration

DuckDBParquetHandler.merge_parquet_dataset SHALL use shared validation and statistics from fsspeckit.core.merge.

#### Scenario: Backend-neutral validation usage
- WHEN calling merge_parquet_dataset
- THEN function calls normalize_keys() for key column normalization
- AND function calls validate_keys_and_schema() for shared validation
- AND function calls compute_merge_stats() for canonical statistics

### Requirement: Canonical Key Validation

merge_parquet_dataset SHALL perform canonical key validation using backend-neutral helpers.

#### Scenario: Key normalization
- WHEN user provides key_columns as string or list
- THEN function calls normalize_keys() to convert to list format
- AND returns normalized list for internal use

#### Scenario: Key existence validation
- WHEN source or target schemas are available
- THEN function validates all key columns exist in both schemas
- AND raises ValueError with clear message for missing keys

#### Scenario: NULL key detection
- WHEN processing merge data
- THEN function checks for NULL values in key columns
- AND raises ValueError before processing if NULL keys found
- AND error message identifies which key columns contain NULL values

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

### Requirement: Streaming Execution Requirements

merge_parquet_dataset SHALL process target datasets in streaming fashion.

#### Scenario: Avoid full dataset materialization
- WHEN processing target dataset for merge
- THEN use parquet_scan with SQL filters instead of read_parquet(target_path)
- AND process data in SQL queries without Python materialization
- AND memory usage remains bounded relative to batch size

#### Scenario: Key-based filtering
- WHEN matching source keys to target dataset
- THEN construct SQL WHERE clauses using key column values
- AND only read target rows with matching keys
- AND minimize I/O by reading only relevant data

#### Scenario: Batch processing
- WHEN processing large target datasets
- THEN execute merge in batches if target exceeds reasonable size
- AND maintain atomicity across batch boundaries
- AND process each batch independently

### Requirement: Canonical Statistics Structure

merge_parquet_dataset SHALL return canonical statistics structure.

#### Scenario: Statistics format
- WHEN merge operation completes successfully
- THEN return dict with keys: "inserted", "updated", "deleted", "total"
- AND all values are non-negative integers
- AND "total" equals final target dataset row count

#### Scenario: Statistics calculation
- WHEN calculating merge statistics
- THEN count actual rows inserted, updated, deleted during operation
- AND avoid heuristic estimates or approximations
- AND ensure statistics sum to expected totals

#### Scenario: Empty source statistics
- WHEN source table is empty
- THEN return {"inserted": 0, "updated": 0, "deleted": target_row_count, "total": 0}
- IF strategy="full_merge" and target exists
- AND target is removed completely

### Requirement: Atomicity and Error Handling

merge operations SHALL maintain dataset atomicity.

#### Scenario: Atomic write operation
- WHEN writing merged dataset
- THEN write to temporary directory first
- AND swap with original directory only after successful write
- AND preserve original dataset if merge fails mid-operation

#### Scenario: Empty target handling
- WHEN target dataset does not exist
- THEN create new dataset with source data
- AND behavior consistent across all merge strategies
- AND provide clear status in returned statistics

#### Scenario: Schema incompatibility
- WHEN source and target schemas are incompatible
- THEN raise ValueError with clear schema mismatch message
- AND identify incompatible column names or types
- AND suggest resolution approaches when possible

### Requirement: Edge Case Alignment

Edge case behavior SHALL match shared canonical definitions.

#### Scenario: Empty target + UPSERT/INSERT
- WHEN target dataset is empty and strategy is "upsert" or "insert"
- THEN all source rows are inserted
- AND statistics.deleted = 0
- AND statistics.total = source row count

#### Scenario: Empty target + UPDATE
- WHEN target dataset is empty and strategy is "update"
- THEN zero rows are updated
- AND statistics.updated = 0
- AND statistics.total = 0 (no change)

#### Scenario: Schema compatibility
- WHEN source and target schemas have compatible types
- THEN use unify_schemas() for type promotion
- AND apply consistent type casting
- AND preserve data integrity during merge

#### Scenario: Invalid strategy
- WHEN strategy parameter is not in valid strategies list
- THEN raise ValueError with list of valid strategies
- AND include current invalid value in error message

### Requirement: DuckDB helpers reuse shared schema and partition logic

DuckDB-based helpers SHALL reuse the same shared schema and partition helpers as the PyArrow backend to ensure consistent behaviour.

#### Scenario: Shared schema compatibility rules across backends
- **WHEN** DuckDB helpers need to check or reconcile schemas
- **THEN** they SHALL call the shared schema helper
- **AND** SHALL honour the same compatibility rules as the PyArrow backend.

#### Scenario: Shared partition semantics across backends
- **WHEN** DuckDB helpers operate on partitioned datasets
- **THEN** they SHALL interpret partition paths according to the shared partition helper’s semantics
- **AND** SHALL not diverge from the behaviour used by PyArrow helpers.

### Requirement: DuckDB helpers use specific exception types

DuckDB-based dataset helpers SHALL surface specific DuckDB exception types for
common failure scenarios and SHALL preserve the original DuckDB error type and
message when propagating failures.

#### Scenario: Invalid SQL uses InvalidInputException
- **WHEN** a helper executes DuckDB SQL with invalid syntax or references
  missing columns or tables
- **THEN** it SHALL raise or propagate `duckdb.InvalidInputException`
  (or a more specific DuckDB exception) rather than a generic `Exception`
- **AND** the error message SHALL include the operation name and a summary of
  the SQL problem.

#### Scenario: Operational failures use OperationalException
- **WHEN** a helper encounters database operation failures (e.g. constraint
  violations, transaction errors)
- **THEN** it SHALL raise or propagate `duckdb.OperationalException`
- **AND** the original DuckDB message SHALL be preserved in the raised error.

#### Scenario: Catalog and I/O issues use dedicated types
- **WHEN** a helper fails because of catalog issues (missing table/view) or
  file I/O problems for parquet data
- **THEN** it SHALL raise or propagate `duckdb.CatalogException` or
  `duckdb.IOException` respectively
- **AND** the error message SHALL include the affected table name or path.

### Requirement: DuckDB cleanup helpers are granular and logged

DuckDB-related cleanup helpers (including table unregistration) SHALL handle
each resource individually and SHALL log failures with sufficient context while
still attempting to clean up remaining resources.

#### Scenario: Table unregistration logs failures but continues
- **WHEN** a cleanup helper unregisters multiple DuckDB tables or views
- **THEN** it SHALL attempt to unregister each table individually
- **AND** failures for one table SHALL be logged with table identifier and
  underlying exception
- **AND** failures SHALL NOT prevent attempts to clean up remaining tables.

#### Scenario: Catch-all handlers log and re-raise
- **WHEN** a DuckDB helper needs a catch-all exception handler
- **THEN** the handler SHALL log the unexpected exception with context
  (operation, table/path) using the project logger
- **AND** it SHALL re-raise the exception instead of silently swallowing it.

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

### Requirement: DuckDB helpers share cleanup behaviour

DuckDB helper modules SHALL share a single canonical implementation for unregistering DuckDB tables safely.

#### Scenario: DuckDB cleanup uses a central helper
- **WHEN** cleanup code in DuckDB-related modules unregisters DuckDB tables
- **THEN** it SHALL call a shared `_unregister_duckdb_table_safely` helper from a canonical DuckDB helpers module
- **AND** no module SHALL maintain its own divergent copy of this logic.

### Requirement: Parquet Dataset Deduplication Maintenance (DuckDB)
The system SHALL provide a `deduplicate_parquet_dataset` method that removes duplicate rows from an existing dataset using DuckDB's native SQL capabilities (`DISTINCT` or `DISTINCT ON`).

#### Scenario: Deduplicate dataset using DuckDB SQL
- **WHEN** `deduplicate_parquet_dataset` is called
- **THEN** it SHALL use DuckDB SQL to identify and remove duplicates
- **AND** it SHALL support ordering to specify which record to keep when duplicates are found

### Requirement: Optimize Supports Optional Deduplication Step (DuckDB)
The system SHALL allow callers to request deduplication during `optimize_parquet_dataset`.

#### Scenario: Optimize performs deduplication when requested
- **GIVEN** a dataset contains duplicate keys under `key_columns=["id"]`
- **WHEN** user calls `handler.optimize_parquet_dataset(path, deduplicate_key_columns=["id"])`
- **THEN** the optimized output SHALL not contain duplicate keys for `id`
- **AND** optimization statistics SHALL indicate that deduplication was performed

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

### Requirement: `mode` is Ignored When `strategy` is Provided (DuckDB)
The system SHALL treat `strategy` as the primary control for `write_parquet_dataset` behavior and SHALL ignore `mode` when `strategy` is not `None`.

#### Scenario: `mode="append"` does not affect upsert
- **WHEN** user calls `handler.write_parquet_dataset(table, path, strategy="upsert", key_columns=["id"], mode="append")`
- **THEN** the method SHALL perform the UPSERT semantics
- **AND** SHALL NOT raise an error due to the presence of `mode`

#### Scenario: `mode="overwrite"` does not affect update
- **WHEN** user calls `handler.write_parquet_dataset(table, path, strategy="update", key_columns=["id"], mode="overwrite")`
- **THEN** the method SHALL perform the UPDATE semantics
- **AND** SHALL NOT raise an error due to the presence of `mode`

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

