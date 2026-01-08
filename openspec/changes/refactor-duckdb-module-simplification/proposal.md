# Change: Simplify DuckDB Module Implementation

## Why
The DuckDB integration in `src/fsspeckit/datasets/duckdb/` has accumulated legacy code and over-complex implementations:
- `dataset.py` is bloated with legacy API stubs that only raise `NotImplementedError`.
- The `merge` implementation relies heavily on PyArrow in-memory joins, which is inefficient compared to DuckDB's native SQL engine.
- Maintenance operations like compaction and deduplication move data through PyArrow buffers instead of using DuckDB's streaming `COPY` capabilities.
- The `DuckDBParquetHandler` wrapper adds unnecessary complexity and indirection.

## What Changes
- **Redesign Merge Engine**: Transition `DuckDBDatasetIO.merge` to a fully SQL-driven implementation using DuckDB joins and `COPY` commands.
- **Optimize Maintenance**: Refactor `compact_parquet_dataset_duckdb` and `deduplicate_parquet_dataset` to use DuckDB's native SQL for data movement and transformation.
- **API Cleanup**:
    - Remove the deprecated `DuckDBParquetHandler` class.
    - Remove all `NotImplementedError` stubs for legacy APIs (`write_parquet_dataset`, etc.).
- **Consolidate Connection Management**: Standardize all operations to use the `DuckDBConnection` manager for lifecycle and filesystem registration.

## Impact
- Affected specs: `datasets-duckdb`, `utils-duckdb`
- Affected code: `src/fsspeckit/datasets/duckdb/`
- **BREAKING**: `DuckDBParquetHandler` is removed. Users should use `DuckDBConnection` and `DuckDBDatasetIO` directly.
- **BREAKING**: Legacy methods in `DuckDBDatasetIO` (e.g., `write_parquet_dataset`) are removed.
- **Performance**: Significant reduction in memory usage for merge and maintenance operations due to streaming execution in DuckDB.
