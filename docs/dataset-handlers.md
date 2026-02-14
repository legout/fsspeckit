# Dataset Handler Interface

This document describes the shared interface for dataset handlers across different backends (DuckDB, PyArrow, etc.).

## Overview

Dataset handlers provide a consistent API for reading, writing, and maintaining parquet datasets, regardless of the underlying backend. This allows users to switch between backends with minimal code changes while taking advantage of backend-specific optimizations.

## Shared Interface

All dataset handlers implement the `DatasetHandler` protocol, which defines the following core operations:

### Core Methods

#### `write_dataset()`
Write a parquet dataset with explicit mode configuration.

**Signature:**
```python
def write_dataset(
    data: pa.Table | list[pa.Table],
    path: str,
    *,
    mode: Literal["append"] | Literal["overwrite"] = "append",
    basename_template: str | None = None,
    schema: pa.Schema | None = None,
    partition_by: str | list[str] | None = None,
    compression: str | None = "snappy",
    max_rows_per_file: int | None = 5_000_000,
    row_group_size: int | None = 500_000,
    **kwargs: Any,
) -> WriteDatasetResult
```

**Parameters:**
- `data`: PyArrow table or list of tables to write
- `path`: Output directory path
- `mode`: Write mode - `"append"` (default) or `"overwrite"`
- `basename_template`: Template for file names
- `schema`: Optional schema to enforce
- `partition_by`: Column(s) to partition by
- `compression`: Compression codec
- `max_rows_per_file`: Maximum rows per file
- `row_group_size`: Rows per row group

#### `merge()`
Perform incremental merge operations on existing datasets.

**Signature:**
```python
def merge(
    data: pa.Table | list[pa.Table],
    path: str,
    strategy: Literal["insert"] | Literal["update"] | Literal["upsert"],
    key_columns: list[str] | str,
    *,
    partition_columns: list[str] | str | None = None,
    schema: pa.Schema | None = None,
    compression: str | None = "snappy",
    max_rows_per_file: int | None = 5_000_000,
    row_group_size: int | None = 500_000,
    merge_chunk_size_rows: int = 100_000,
    enable_streaming_merge: bool = True,
    merge_max_memory_mb: int = 1024,
    merge_max_process_memory_mb: int | None = None,
    merge_min_system_available_mb: int = 512,
    merge_progress_callback: Callable[[int, int], None] | None = None,
    use_merge: bool | None = None,
    **kwargs: Any,
) -> MergeResult
```

**Parameters:**
- `data`: PyArrow table or list of tables to merge
- `path`: Existing dataset directory path
- `strategy`: Merge strategy:
  - `'insert'`: Only insert new records
  - `'update'`: Only update existing records
  - `'upsert'`: Insert or update existing records
- `key_columns`: Column(s) used as merge keys
- `partition_columns`: Columns that must not change for existing keys
- `schema`: Optional schema to enforce
- `compression`: Compression codec
- `max_rows_per_file`: Maximum rows per file
- `row_group_size`: Rows per row group
- `merge_chunk_size_rows`: Chunk size for streaming merges
- `enable_streaming_merge`: Toggle streaming merge path
- `merge_max_memory_mb`: Max PyArrow memory for merge
- `merge_max_process_memory_mb`: Optional max process RSS
- `merge_min_system_available_mb`: Minimum system available memory
- `merge_progress_callback`: Optional progress callback
- `use_merge`: Reserved for backward compatibility (ignored by DuckDB and PyArrow backends)

#### `compact_parquet_dataset()`
Compact a parquet dataset by combining small files.

**Signature:**
```python
def compact_parquet_dataset(
    path: str,
    *,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    **kwargs: Any,
) -> dict[str, Any]
```

#### `optimize_parquet_dataset()`
Optimize a parquet dataset through compaction and maintenance.

**Signature:**
```python
def optimize_parquet_dataset(
    path: str,
    *,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    deduplicate_key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    verbose: bool = False,
    **kwargs: Any,
) -> dict[str, Any]
```

## Backend Comparison

### DuckDB Dataset Handler (`DuckDBDatasetIO`)

**Class-based interface** that provides high-performance parquet operations using DuckDB's engine.

**Strengths:**
- Excellent SQL-based merging capabilities
- Fast merge operations using DuckDB's query optimizer
- Efficient for large-scale dataset operations
- Rich SQL syntax for complex merge strategies

**Backend-specific features:**
- `use_merge` is reserved for backward compatibility and ignored
- SQL WHERE clause filters for `read_parquet`

**Example usage:**
```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)

io.write_dataset(data, "/path/to/dataset/", mode="append")
result = io.merge(
    data,
    "/path/to/dataset/",
    strategy="upsert",
    key_columns=["id"],
)
```

### PyArrow Dataset Handler (`PyarrowDatasetIO`)

**Class-based interface** using PyArrow's native parquet engine with API symmetry to DuckDB.

**Strengths:**
- Direct PyArrow integration with schema enforcement
- Streaming merge controls for memory-efficient operations
- Partitioning support with predicate pushdown
- Compatibility with the PyArrow ecosystem

**Example usage:**
```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

io.write_dataset(data, "/path/to/dataset/", mode="append")
result = io.merge(
    data,
    "/path/to/dataset/",
    strategy="upsert",
    key_columns=["id"],
    enable_streaming_merge=True,
)
```

## Backend-Specific Differences

| Feature | DuckDB | PyArrow |
|----------|--------|----------|
| **Filters** | SQL WHERE clause strings (`str`) only | PyArrow expressions, DNF tuples, or SQL-like strings (converted to expressions) |
| **Merge backend** | `use_merge` reserved for backward compatibility (ignored) | Streaming/in-memory merge knobs (merge_chunk_size_rows, enable_streaming_merge, merge_max_memory_mb, etc.) |
| **Write backend** | `use_threads` parameter for write_parquet | `use_threads` accepted but ignored |
| **Optimization features** | SQL-based query optimization | Adaptive key tracking + streaming merge controls |
| **Best for** | Complex merge logic, very large datasets, SQL workflows | Partitioned datasets, predicate pushdown, memory-constrained environments |

**Note:** These differences reflect backend-specific optimizations rather than incompatibilities. Both backends provide the same core API surface (`write_dataset`, `merge`, `compact_parquet_dataset`, `optimize_parquet_dataset`) with identical shared parameters.

## Type Safety

Both handlers implement the `DatasetHandler` protocol, which allows static analysis tools to provide better autocomplete and type checking:

```python
from fsspeckit.datasets.interfaces import DatasetHandler
from fsspeckit.datasets.duckdb import DuckDBDatasetIO

def process_dataset(handler: DatasetHandler, data: pa.Table) -> None:
    handler.write_dataset(data, "output/")
    handler.merge(data, "output/", strategy="upsert", key_columns=["id"])
```

## Implementation Notes

- All handlers share core merge invariants defined in `fsspeckit.core.incremental`
- Validation logic is centralized in `fsspeckit.datasets.base`
- Result types are canonical (`WriteDatasetResult`, `MergeResult`)
