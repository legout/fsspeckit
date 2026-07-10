# Implementation Tasks

## 1. Implementation

- [x] 1.1 Create `src/fsspeckit/utils/duckdb.py` module with `DuckDBParquetHandler` class
- [x] 1.2 Implement `__init__` method with storage_options and filesystem parameters
- [x] 1.3 Implement `_register_filesystem` method to register fsspec filesystem in DuckDB
- [x] 1.4 Implement `read_parquet` method with path and optional columns parameter
- [x] 1.5 Implement `write_parquet` method with table, path, and compression parameters
- [x] 1.6 Implement `execute_sql` method with query and optional parameters
- [x] 1.7 Implement context manager methods (`__enter__`, `__exit__`)
- [x] 1.8 Add type hints and Google-style docstrings with examples

## 2. Testing

- [x] 2.1 Create `tests/test_utils/test_duckdb.py` with test fixtures
- [x] 2.2 Write tests for local filesystem operations
- [x] 2.3 Write tests for read_parquet with single file and dataset directory
- [x] 2.4 Write tests for write_parquet with different compression codecs
- [x] 2.5 Write tests for execute_sql with queries and parameters
- [x] 2.6 Write tests for column selection in read_parquet
- [x] 2.7 Write tests for context manager usage
- [x] 2.8 Write tests for storage_options initialization
- [x] 2.9 Write tests for filesystem instance initialization
- [x] 2.10 Ensure test coverage meets 80% threshold

## 3. Documentation

- [x] 3.1 Verify examples in `examples/duckdb/` work with implementation
- [x] 3.2 Add module docstring to `duckdb.py`
- [x] 3.3 Ensure all public methods have comprehensive docstrings

## 4. Validation

- [x] 4.1 Run `pytest tests/test_utils/test_duckdb.py -v`
- [x] 4.2 Run `ruff check src/fsspeckit/utils/duckdb.py`
- [x] 4.3 Run `mypy src/fsspeckit/utils/duckdb.py`
- [x] 4.4 Verify test coverage: `pytest --cov=fsspeckit.utils.duckdb --cov-report=term-missing`
- [x] 4.5 Run example scripts to validate functionality
