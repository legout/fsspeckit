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
    max_rows_per_file: int | None = None,
    row_group_size: int | None = None,
    **kwargs: Any,
) -> Any
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
    *,
    strategy: Literal["insert"] | Literal["update"] | Literal["upsert"],
    key_columns: list[str] | str,
    basename_template: str | None = None,
    schema: pa.Schema | None = None,
    partition_by: str | list[str] | None = None,
    compression: str | None = "snappy",
    max_rows_per_file: int | None = None,
    row_group_size: int | None = None,
    **kwargs: Any,
) -> Any
```

**Parameters:**
- `data`: PyArrow table or list of tables to merge
- `path`: Existing dataset directory path
- `strategy`: Merge strategy:
  - `'insert'`: Only insert new records
  - `'update'`: Only update existing records  
  - `'upsert'`: Insert or update existing records
- `key_columns`: Column(s) used as merge keys
- `basename_template`: Template for file names
- `schema`: Optional schema to enforce
- `partition_by`: Column(s) to partition by
- `compression`: Compression codec
- `max_rows_per_file`: Maximum rows per file
- `row_group_size`: Rows per row group
- `key_columns`: Key columns for merge operations (required for relational strategies)

**Returns:** Backend-specific result (e.g., MergeStats for merge operations)

**Returns:** Backend-specific result containing merge statistics

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

**Parameters:**
- `path`: Dataset path
- `target_mb_per_file`: Target size per file in MB
- `target_rows_per_file`: Target rows per file
- `partition_filter`: Optional partition filters
- `compression`: Compression codec for output
- `dry_run`: Whether to perform a dry run (return plan without executing)
- `verbose`: Print progress information

**Returns:** Dictionary containing compaction statistics and metadata

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
    verbose: bool = False,
    **kwargs: Any,
) -> dict[str, Any]
```

**Parameters:**
- `path`: Dataset path
- `target_mb_per_file`: Target size per file in MB
- `target_rows_per_file`: Target rows per file
- `partition_filter`: Optional partition filters
- `compression`: Compression codec for output
- `verbose`: Print progress information

**Returns:** Dictionary containing optimization statistics and metadata

## Backend Comparison

### DuckDB Dataset Handler (`DuckDBDatasetIO`)

**Class-based interface** that provides high-performance parquet operations using DuckDB's engine.

**Strengths:**
- Excellent SQL-based merging capabilities
- Fast merge operations using DuckDB's query optimizer
- Efficient for large-scale dataset operations
- Rich SQL syntax for complex merge strategies

**Backend-specific features:**
- SQL-based merge operations with complex WHERE clauses
- Parallel read/write operations
- In-memory processing for small datasets

**Example usage:**
```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)

# Standard write (append mode by default)
io.write_dataset(data, "/path/to/dataset/")

# Overwrite write
io.write_dataset(data, "/path/to/dataset/", mode="overwrite")

# Merge operation
stats = io.merge(
    data,
    "/path/to/dataset/",
    strategy="upsert",
    key_columns=["id"]
)
```

### PyArrow Dataset Handler (Function-based)

**Function-based interface** (monkey-patched to filesystem objects) using PyArrow's native parquet engine.

**Strengths:**
- Direct PyArrow integration
- Schema enforcement and validation
- Partition discovery and pruning
- Predicate pushdown for efficient querying
- Memory-efficient streaming operations

**Backend-specific features:**
- Direct PyArrow table integration
- Advanced partitioning support
- File-level metadata optimization
- Compatibility with PyArrow ecosystem

**Example usage:**
```python
from fsspec import LocalFileSystem

fs = LocalFileSystem()

# Standard write (append mode by default)
fs.write_dataset(data, "/path/to/dataset/")

# Overwrite write
fs.write_dataset(data, "/path/to/dataset/", mode="overwrite")

# Merge operation
fs.merge(
    data,
    "/path/to/dataset/",
    strategy="upsert",
    key_columns=["id"]
)
```

### PyArrow Dataset Handler (Class-based)

**Class-based interface** using PyArrow's native parquet engine with API symmetry to DuckDB.

**Strengths:**
- Class-based API consistent with DuckDB handler
- Direct PyArrow integration with schema enforcement
- All merge strategies (INSERT, UPSERT, UPDATE, FULL_MERGE, DEDUPLICATE)
- Memory-efficient operations with in-memory merging
- Context manager support for resource management

**Backend-specific features:**
- In-memory merge operations for all strategies
- Advanced partitioning and compression support
- File-level metadata optimization
- Compatibility with PyArrow ecosystem

**Example usage:**
```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO, PyarrowDatasetHandler

# Class-based approach
io = PyarrowDatasetIO()
io.write_dataset(data, "/path/to/dataset/")
io.merge(data, "/path/to/dataset/", strategy="upsert", key_columns=["id"])

# Handler wrapper approach
with PyarrowDatasetHandler() as handler:
    handler.write_dataset(data, "/path/to/dataset/")
    handler.merge(data, "/path/to/dataset/", strategy="upsert", key_columns=["id"])

# Read operations
table = io.read_parquet("/path/to/dataset/", columns=["id", "name"])

# Maintenance operations
stats = io.compact_parquet_dataset("/path/to/dataset/", target_mb_per_file=64)
result = io.optimize_parquet_dataset("/path/to/dataset/", compression="zstd")
```

## Merge Strategies

Use the explicit `merge()` method with defined strategies:

- `'insert'` - Insert-only operations
- `'upsert'` - Insert-or-update operations  
- `'update'` - Update-only operations

## Backend-Specific Notes

### DuckDB
- Requires a `DuckDBConnection` instance
- Merge operations use SQL-based optimization
- Best for complex merge logic and large datasets
- Returns `MergeStats` objects for merge operations

### PyArrow
- Integrated with `AbstractFileSystem` instances
- Merge operations use PyArrow's native table operations
- Best for streaming operations and memory efficiency
- Returns `MergeStats` objects for merge operations

## Choosing a Backend

**Use DuckDB when:**
- You need complex SQL-based merge logic
- Working with very large datasets
- Need maximum merge performance
- Prefer class-based APIs

**Use PyArrow when:**
- Already using PyArrow in your workflow
- Need schema enforcement and validation
- Working with partitioned datasets
- Prefer class-based APIs with DuckDB-like ergonomics
- Need predicate pushdown and query optimization
- Want memory-efficient operations with in-memory merging

### Choosing Your PyArrow Approach

**Use Function-based PyArrow when:**
- You prefer filesystem-integrated APIs
- Already using fsspec monkey-patched methods
- Want minimal code changes from existing PyArrow workflows
- Need simple, direct method calls on filesystem objects

**Use Class-based PyArrow when:**
- You want API consistency with DuckDB handler
- Need context manager support for resource management
- Prefer object-oriented patterns with method chaining
- Want explicit separation between I/O and merge operations
- Need advanced maintenance operations (compact, optimize)

### Migration Between PyArrow Approaches

**From Function-based to Class-based:**
```python
# Function-based approach
from fsspec import LocalFileSystem
fs = LocalFileSystem()
fs.write_dataset(data, "/path/to/dataset/")
fs.merge(data, "/path/to/dataset/", strategy="upsert", key_columns=["id"])

# Equivalent class-based approach
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
io = PyarrowDatasetIO()
io.write_dataset(data, "/path/to/dataset/")
io.merge(data, "/path/to/dataset/", strategy="upsert", key_columns=["id"])
```

**From Class-based to Function-based:**
```python
# Class-based approach
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
io = PyarrowDatasetIO()
table = io.read_parquet("/path/to/dataset/")

# Equivalent function-based approach
from fsspec import LocalFileSystem
fs = LocalFileSystem()
table = fs.read_pyarrow_dataset("/path/to/dataset/")
```

**Key Differences:**
- Function-based: Methods are attached to filesystem objects
- Class-based: Methods are attached to dedicated handler objects
- Both approaches support the same merge strategies and parameters
- Choose based on your preferred coding style and project requirements

## Type Safety

Both handlers implement the `DatasetHandler` protocol, which allows static analysis tools to provide better autocomplete and type checking:

```python
from fsspeckit.datasets.interfaces import DatasetHandler
from fsspeckit.datasets.duckdb import DuckDBDatasetIO

def process_dataset(handler: DatasetHandler, data: pa.Table) -> None:
    # Static analysis knows handler has write_dataset and merge methods
    handler.write_dataset(data, "output/")
    handler.merge(data, "output/", strategy="upsert", key_columns=["id"])
```

## Protocol Definition

The `DatasetHandler` protocol is defined in `fsspeckit.datasets.interfaces` and uses Python's `typing.Protocol` to enable structural subtyping. This means:

- No explicit inheritance required
- Both class-based and function-based implementations are supported
- Type checkers verify compatibility at compile time
- Runtime checking via `isinstance()` is not applicable (use Protocol checking instead)

## Implementation Notes

- All handlers share the same core merge strategies defined in `fsspeckit.core.merge`
- Validation logic is shared in `fsspeckit.core.merge` and `fsspeckit.core.maintenance`
- Backend-specific optimizations are applied within each handler
- The protocol ensures consistent behavior across backends while allowing for backend-specific extensions

## Backend-Specific Differences

| Feature | DuckDB | PyArrow |
|----------|--------|----------|
| **Filters** | SQL WHERE clause strings (`str`) only | PyArrow compute expressions, DNF tuples, or SQL-like strings (converted to expressions) |
| **Merge backend** | `use_merge` parameter (DuckDB 1.4.0+ MERGE SQL vs UNION ALL fallback) | Streaming/in-memory merge knobs (merge_chunk_size_rows, enable_streaming_merge, merge_max_memory_mb, etc.) |
| **Write backend** | `use_threads` parameter (write_parquet only) | No threading control (PyArrow handles internally) |
| **Optimization features** | SQL-based query optimization | Advanced deduplication with AdaptiveKeyTracker, vectorized multi-key processing |
| **Best for** | Complex merge logic, very large datasets, SQL-based workflows | Partitioned datasets, predicate pushdown, memory-constrained environments |

**Note:** These differences reflect backend-specific optimizations rather than incompatibilities. Both backends provide the same core API surface (write_dataset, merge, compact_parquet_dataset, optimize_parquet_dataset) with identical shared parameters.