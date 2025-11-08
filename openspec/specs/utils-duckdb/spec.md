# utils-duckdb Specification

## Purpose
TBD - created by archiving change add-duckdb-dataset-write. Update Purpose after archive.
## Requirements
### Requirement: Write Parquet Dataset with Unique Filenames

The system SHALL provide a `write_parquet_dataset` method that writes PyArrow tables to parquet dataset directories with automatically generated unique filenames.

#### Scenario: Write dataset with default UUID filenames

- **WHEN** user calls `handler.write_parquet_dataset(table, "/path/to/dataset/")`
- **THEN** method creates dataset directory if it doesn't exist
- **AND** writes parquet file with UUID-based filename (e.g., "part-1a2b3c4d.parquet")
- **AND** file contains all data from the table

#### Scenario: Write large table split into multiple files

- **WHEN** user calls `handler.write_parquet_dataset(table, path, max_rows_per_file=1000)`
- **AND** table has more than 1000 rows
- **THEN** method splits table into multiple files with ~1000 rows each
- **AND** each file has unique filename
- **AND** reading the dataset returns all original data

#### Scenario: Write with custom basename template

- **WHEN** user calls `handler.write_parquet_dataset(table, path, basename_template="data_{}.parquet")`
- **THEN** method generates filenames using the template with unique identifiers
- **AND** files are named like "data_001.parquet", "data_002.parquet", etc.

#### Scenario: Write empty table to dataset

- **WHEN** user calls `handler.write_parquet_dataset(empty_table, path)`
- **THEN** method creates at least one file with the schema
- **AND** file contains zero rows but preserves column structure

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

The system SHALL validate inputs and provide clear error messages for invalid dataset write operations.

#### Scenario: Invalid mode error

- **WHEN** user provides invalid mode value (not "overwrite" or "append")
- **THEN** method raises ValueError with clear message listing valid modes

#### Scenario: Invalid max_rows_per_file error

- **WHEN** user provides max_rows_per_file <= 0
- **THEN** method raises ValueError indicating minimum value must be > 0

#### Scenario: Path is file not directory error

- **WHEN** user provides path to existing file (not directory)
- **THEN** method raises clear error indicating path must be directory

#### Scenario: Remote storage write permission error

- **WHEN** user attempts to write to remote storage without write permissions
- **THEN** method raises exception with clear authentication/permission error message

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

The system SHALL generate unique filenames that avoid collisions across multiple writes.

#### Scenario: UUID-based filename uniqueness

- **WHEN** method generates filenames using default UUID strategy
- **THEN** filenames are globally unique
- **AND** format is "part-{uuid}.parquet"

#### Scenario: Timestamp-based filename option

- **WHEN** method uses timestamp-based naming strategy
- **THEN** filenames include high-resolution timestamp
- **AND** format is "part-{timestamp}.parquet"
- **AND** filenames sort chronologically

#### Scenario: Sequential filename option

- **WHEN** method uses sequential naming with template
- **AND** template is "data_{}.parquet"
- **THEN** filenames are numbered sequentially starting from 0
- **AND** format is "data_000.parquet", "data_001.parquet", etc.

#### Scenario: Filename collision prevention

- **WHEN** multiple concurrent writes to same dataset
- **THEN** filename generation ensures no collisions
- **AND** each write produces unique filenames

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

The system SHALL provide clear error messages for common failure scenarios.

#### Scenario: Invalid path error

- **WHEN** user provides non-existent path to read_parquet
- **THEN** method raises clear exception indicating file not found

#### Scenario: Invalid storage options error

- **WHEN** user provides storage options with missing credentials for remote storage
- **THEN** method raises clear exception indicating authentication failure

#### Scenario: SQL execution error

- **WHEN** SQL query has syntax error or references invalid columns
- **THEN** execute_sql raises exception with DuckDB error message

