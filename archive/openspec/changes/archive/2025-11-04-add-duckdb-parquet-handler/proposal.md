# Change Proposal: Add DuckDB Parquet Handler

## Why

fsspeckit needs a high-performance, SQL-capable interface for reading and writing parquet datasets across multiple storage backends. DuckDB provides excellent parquet support with SQL analytics capabilities, and integrating it with fsspec filesystems enables seamless operations on local and remote (S3, GCS, Azure) storage systems.

## What Changes

- Add `DuckDBParquetHandler` class to `fsspeckit.utils.duckdb` module
- Provide methods for reading and writing parquet files and datasets
- Support SQL query execution on parquet data
- Enable fsspec filesystem registration in DuckDB connections for remote storage access
- Support initialization from storage options or existing filesystem instances
- Implement context manager protocol for automatic resource cleanup
- Support configurable compression codecs for parquet writes
- Enable column selection for efficient reads

## Impact

- Affected specs: `utils-duckdb` (new capability)
- Affected code:
  - New file: `src/fsspeckit/utils/duckdb.py`
  - Modified: `src/fsspeckit/utils/__init__.py` (export already present, needs implementation)
  - Tests: `tests/test_utils/test_duckdb.py` (new)
  - Documentation: examples already exist in `examples/duckdb/`
- Dependencies: `duckdb>=1.0.0` (already in pyproject.toml)
- No breaking changes
