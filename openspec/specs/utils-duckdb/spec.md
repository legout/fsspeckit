# utils-duckdb Specification

## Purpose
TBD - created by archiving change add-duckdb-dataset-write. Update Purpose after archive.
## Requirements
### Requirement: Write Parquet Dataset with Unique Filenames

The system SHALL provide a `write_parquet_dataset` method that writes PyArrow tables to parquet dataset directories with automatically generated unique filenames using a UUID-based mechanism.

#### Scenario: Write dataset with default UUID filenames

- **WHEN** user calls `handler.write_parquet_dataset(table, "/path/to/dataset/")`
- **THEN** method creates dataset directory if it doesn't exist
- **AND** writes parquet file with UUID-based filename (e.g., "part-a1b2c3d4.parquet")
- **AND** file contains all data from the table

#### Scenario: Write large table split into multiple files

- **WHEN** user calls `handler.write_parquet_dataset(table, path, max_rows_per_file=1000)`
- **AND** table has more than 1000 rows
- **THEN** method splits table into multiple files with ~1000 rows each
- **AND** each file has unique UUID-based filename
- **AND** reading the dataset returns all original data

#### Scenario: Write with custom basename template

- **WHEN** user calls `handler.write_parquet_dataset(table, path, basename_template="data_{}.parquet")`
- **THEN** method generates filenames using the template with UUID-based unique identifiers
- **AND** files are named like "data_a1b2c3d4.parquet", "data_e5f6g7h8.parquet", etc.
- **AND** each filename contains a unique UUID token ensuring no collisions

#### Scenario: Write empty table to dataset

- **WHEN** user calls `handler.write_parquet_dataset(empty_table, path)`
- **THEN** method creates at least one file with the schema
- **AND** file contains zero rows but preserves column structure
- **AND** filename follows UUID-based unique naming pattern

### Requirement: Dataset Write Mode - Overwrite

The system SHALL support `mode="overwrite"` to replace existing dataset contents with new data.

#### Scenario: Overwrite existing dataset

- **WHEN** dataset directory contains existing parquet files
- **AND** user calls `handler.write_parquet_dataset(table, path, mode="overwrite")`
- **THEN** method deletes all existing parquet files in the directory
- **AND** writes new parquet file(s) with new data
- **AND** only new data is present in the dataset

#### Scenario: Overwrite non-existent dataset

- **WHEN** dataset directory does not exist
- **AND** user calls `handler.write_parquet_dataset(table, path, mode="overwrite")`
- **THEN** method creates directory and writes data
- **AND** behaves same as initial write

#### Scenario: Overwrite preserves non-parquet files

- **WHEN** dataset directory contains non-parquet files (e.g., "_metadata", ".txt")
- **AND** user calls `handler.write_parquet_dataset(table, path, mode="overwrite")`
- **THEN** method only deletes files matching parquet pattern
- **AND** preserves non-parquet files

### Requirement: Dataset Write Mode - Append

The system SHALL support `mode="append"` to add new data files to existing dataset without modifying existing files.

#### Scenario: Append to existing dataset

- **WHEN** dataset directory contains existing parquet files
- **AND** user calls `handler.write_parquet_dataset(table, path, mode="append")`
- **THEN** method writes new parquet file(s) with unique names
- **AND** preserves all existing parquet files
- **AND** reading dataset returns combined data from old and new files

#### Scenario: Append to empty directory

- **WHEN** dataset directory exists but is empty
- **AND** user calls `handler.write_parquet_dataset(table, path, mode="append")`
- **THEN** method writes new data files
- **AND** behaves same as initial write

#### Scenario: Append to non-existent dataset

- **WHEN** dataset directory does not exist
- **AND** user calls `handler.write_parquet_dataset(table, path, mode="append")`
- **THEN** method creates directory and writes data
- **AND** behaves same as initial write

#### Scenario: Append multiple times

- **WHEN** user calls `write_parquet_dataset` multiple times with `mode="append"`
- **THEN** each call adds new files with unique names
- **AND** no filename collisions occur
- **AND** all data is preserved and readable

### Requirement: Dataset Write Validation

The system SHALL validate inputs and provide clear error messages for invalid dataset write operations with specific exception types.

#### Scenario: Invalid mode error

- **WHEN** user provides invalid mode value (not "overwrite" or "append")
- **THEN** method raises ValueError with clear message listing valid modes
- **AND** error message includes the invalid value that was provided

#### Scenario: Invalid max_rows_per_file error

- **WHEN** user provides max_rows_per_file <= 0
- **THEN** method raises ValueError indicating minimum value must be > 0
- **AND** error message includes the invalid value that was provided

#### Scenario: Path is file not directory error

- **WHEN** user provides path to existing file (not directory) for dataset operations
- **THEN** method raises ValueError or NotADirectoryError with clear message indicating path must be directory
- **AND** error message includes the problematic path

#### Scenario: Remote storage write permission error

- **WHEN** user attempts to write to remote storage without write permissions
- **THEN** method raises exception with clear authentication/permission error message
- **AND** original permission error details are preserved

### Requirement: Dataset Write Performance

The system SHALL optimize dataset write operations for performance and efficiency.

#### Scenario: Parallel file writes

- **WHEN** writing large table split into multiple files
- **THEN** method leverages DuckDB's parallel execution where possible
- **AND** writes multiple files efficiently

#### Scenario: Memory efficient splitting

- **WHEN** table is split using max_rows_per_file
- **THEN** method streams data without loading entire table into memory multiple times
- **AND** memory usage remains reasonable for large datasets

### Requirement: Unique Filename Generation

The system SHALL generate unique filenames using UUID-based identifiers that avoid collisions across multiple writes without requiring sequential numbering or timestamp-based ordering.

#### Scenario: UUID-based filename uniqueness

- **WHEN** method generates filenames using default UUID strategy
- **THEN** filenames are globally unique using short UUID tokens
- **AND** format is "part-{uuid}.parquet" where uuid is an 8-character UUID fragment
- **AND** no collision management or sequential state is required

#### Scenario: Custom template with UUID insertion

- **WHEN** method uses basename_template with {} placeholder
- **THEN** {} is replaced with a unique UUID token
- **AND** template structure is preserved (e.g., "data_{}.parquet" → "data_a1b2c3d4.parquet")
- **AND** each call generates different UUID tokens

#### Scenario: Filename collision prevention

- **WHEN** multiple concurrent writes to same dataset occur
- **THEN** UUID-based generation ensures no collisions
- **AND** each write produces unique filenames without coordination
- **AND** no sequential state or timestamp ordering is required

### Requirement: Dataset Write with Compression

The system SHALL support configurable compression for dataset writes.

#### Scenario: Write dataset with custom compression

- **WHEN** user calls `handler.write_parquet_dataset(table, path, compression="gzip")`
- **THEN** all files in dataset use gzip compression
- **AND** compression applies to all files uniformly

#### Scenario: Write dataset with different compression per file

- **WHEN** writing multiple files with max_rows_per_file
- **THEN** all files use same compression codec specified
- **AND** dataset maintains consistent compression across files

### Requirement: Dataset Read Compatibility

The system SHALL ensure datasets written by `write_parquet_dataset` are readable by existing `read_parquet` method.

#### Scenario: Read written dataset

- **WHEN** dataset is written using `write_parquet_dataset`
- **AND** user calls `read_parquet(dataset_path)`
- **THEN** method reads all files in dataset
- **AND** returns complete table with all data
- **AND** schema matches original table schema

#### Scenario: Read appended dataset

- **WHEN** multiple writes with `mode="append"` create multiple files
- **AND** user calls `read_parquet(dataset_path)`
- **THEN** method reads all files including appended ones
- **AND** returns complete table with all data from all writes

### Requirement: DuckDB Parquet Handler Initialization

The system SHALL provide a `DuckDBParquetHandler` class that can be initialized with either a storage options object or an existing filesystem instance to enable parquet operations with DuckDB.

#### Scenario: Initialize with storage options

- **WHEN** user creates `DuckDBParquetHandler(storage_options=AwsStorageOptions(...))`
- **THEN** handler creates filesystem from storage options and registers it in DuckDB connection

#### Scenario: Initialize with filesystem instance

- **WHEN** user creates `DuckDBParquetHandler(filesystem=fs)`
- **THEN** handler uses provided filesystem and registers it in DuckDB connection

#### Scenario: Initialize with default filesystem

- **WHEN** user creates `DuckDBParquetHandler()` without parameters
- **THEN** handler creates default local filesystem for operations

### Requirement: Filesystem Registration in DuckDB

The system SHALL register fsspec filesystem instances in DuckDB connections using `.register_filesystem(fs)` to enable operations on remote storage systems.

#### Scenario: Register S3 filesystem

- **WHEN** handler is initialized with S3 storage options
- **THEN** S3 filesystem is registered in DuckDB connection via `.register_filesystem(fs)`
- **AND** DuckDB can access S3 paths using the registered filesystem

#### Scenario: Register local filesystem

- **WHEN** handler is initialized with local storage options or no options
- **THEN** local filesystem is registered in DuckDB connection
- **AND** DuckDB can access local paths

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

The system SHALL provide an `execute_sql` method that executes SQL queries on parquet files using DuckDB and returns results as PyArrow tables.

#### Scenario: Execute SQL query on parquet file

- **WHEN** user calls `handler.execute_sql("SELECT * FROM parquet_scan('file.parquet') WHERE col > 10")`
- **THEN** method executes query using DuckDB
- **AND** returns PyArrow table with query results

#### Scenario: Execute parameterized query

- **WHEN** user calls `handler.execute_sql(query, parameters=[value1, value2])`
- **AND** query contains parameter placeholders (`?`)
- **THEN** method safely binds parameters to query
- **AND** executes parameterized query
- **AND** returns PyArrow table with results

#### Scenario: Execute aggregation query

- **WHEN** user executes SQL with GROUP BY, aggregate functions, or window functions
- **THEN** method returns PyArrow table with aggregated results
- **AND** leverages DuckDB's analytical query capabilities

#### Scenario: Execute query on remote parquet

- **WHEN** query references remote parquet path (s3://, gs://, etc.)
- **AND** filesystem is registered
- **THEN** method executes query on remote data
- **AND** returns PyArrow table with results

### Requirement: Context Manager Support

The system SHALL implement context manager protocol for automatic resource cleanup and connection management.

#### Scenario: Use with statement

- **WHEN** user creates handler with `with DuckDBParquetHandler() as handler:`
- **THEN** handler initializes DuckDB connection on enter
- **AND** automatically closes connection on exit
- **AND** resources are properly cleaned up even if exceptions occur

#### Scenario: Manual resource management

- **WHEN** user creates handler without context manager
- **THEN** handler still functions correctly
- **AND** user can manually close connection if needed

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

