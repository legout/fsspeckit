# Change Proposal: Add Dataset Write Support to DuckDB Handler

## Why

The current DuckDB parquet handler only supports writing to single parquet files. Real-world data workflows often require writing to parquet datasets - directory structures containing multiple parquet files. This enables:

- **Incremental data updates**: Append new data files to existing datasets without rewriting everything
- **Large dataset handling**: Split large tables into multiple smaller files for better parallelism and memory management
- **Data versioning**: Add timestamped files to track data changes over time
- **Distributed processing**: Multiple processes can write to the same dataset concurrently

Additionally, users need control over write semantics:
- **Overwrite mode**: Replace entire dataset with new data
- **Append mode**: Add new files to existing dataset without modifying existing files

## What Changes

- Add `write_parquet_dataset` method to `DuckDBParquetHandler` class
- Support automatic generation of unique filenames (UUID, timestamp, or sequential)
- Implement `mode="overwrite"` to clear dataset directory before writing
- Implement `mode="append"` to add files to existing dataset
- Support `max_rows_per_file` parameter to control file size
- Support `partition_cols` parameter for hive-style partitioning (optional)
- Add `basename_template` parameter for custom filename patterns

## Impact

- Affected specs: `utils-duckdb` (modify existing capability)
- Affected code:
  - Modified: `src/fsspeckit/utils/duckdb.py` (add new method)
  - Tests: `tests/test_utils/test_duckdb.py` (add new tests)
  - Examples: `examples/duckdb/` (add dataset write examples)
- Dependencies: No new dependencies required
- No breaking changes (additive feature)
- Extends existing `DuckDBParquetHandler` functionality
