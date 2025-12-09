# Dataset Handler Interface

This document describes the shared interface for dataset handlers across different backends (DuckDB, PyArrow, etc.).

## Overview

Dataset handlers provide a consistent API for reading, writing, and maintaining parquet datasets, regardless of the underlying backend. This allows users to switch between backends with minimal code changes while taking advantage of backend-specific optimizations.

## Shared Interface

All dataset handlers implement the `DatasetHandler` protocol, which defines the following core operations:

### Core Methods

#### `write_parquet_dataset()`
Write a parquet dataset with optional merge strategies.

**Signature:**
```python
def write_parquet_dataset(
    data: pa.Table | list[pa.Table],
    path: str,
    *,
    basename_template: str | None = None,
    schema: pa.Schema | None = None,
    partition_by: str | list[str] | None = None,
    compression: str | None = "snappy",
    max_rows_per_file: int | None = None,
    row_group_size: int | None = None,
    strategy: MergeStrategy | None = None,
    key_columns: list[str] | str | None = None,
    **kwargs: Any,
) -> Any
```

**Parameters:**
- `data`: PyArrow table or list of tables to write
- `path`: Output directory path
- `basename_template`: Template for file names
- `schema`: Optional schema to enforce
- `partition_by`: Column(s) to partition by
- `compression`: Compression codec
- `max_rows_per_file`: Maximum rows per file
- `row_group_size`: Rows per row group
- `strategy`: Optional merge strategy:
  - `'insert'`: Only insert new records
  - `'upsert'`: Insert or update existing records
  - `'update'`: Only update existing records
  - `'full_merge'`: Full replacement with source
  - `'deduplicate'`: Remove duplicates
- `key_columns`: Key columns for merge operations (required for relational strategies)

**Returns:** Backend-specific result (e.g., MergeStats for merge operations)

#### `merge_parquet_dataset()`
Merge multiple parquet datasets.

**Signature:**
```python
def merge_parquet_dataset(
    sources: list[str],
    output_path: str,
    *,
    target: str | None = None,
    strategy: MergeStrategy = "deduplicate",
    key_columns: list[str] | str | None = None,
    compression: str | None = None,
    verbose: bool = False,
    **kwargs: Any,
) -> Any
```

**Parameters:**
- `sources`: List of source dataset paths
- `output_path`: Path for merged output
- `target`: Target dataset path (for upsert/update strategies)
- `strategy`: Merge strategy to use
- `key_columns`: Key columns for merging
- `compression`: Output compression codec
- `verbose`: Print progress information

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

# Standard write
io.write_parquet_dataset(data, "/path/to/dataset/")

# Merge-aware write
stats = io.write_parquet_dataset(
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

# Standard write
fs.write_pyarrow_dataset(data, "/path/to/dataset/")

# Merge-aware write
fs.write_pyarrow_dataset(
    data,
    "/path/to/dataset/",
    strategy="upsert",
    key_columns=["id"]
)

# Convenience methods
fs.upsert_dataset(data, "/path/to/dataset/", key_columns=["id"])
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
io.write_parquet_dataset(data, "/path/to/dataset/", strategy="upsert", key_columns=["id"])

# Handler wrapper approach
with PyarrowDatasetHandler() as handler:
    handler.upsert_dataset(data, "/path/to/dataset/", key_columns=["id"])

# Read operations
table = io.read_parquet("/path/to/dataset/", columns=["id", "name"])

# Maintenance operations
stats = io.compact_parquet_dataset("/path/to/dataset/", target_mb_per_file=64)
result = io.optimize_parquet_dataset("/path/to/dataset/", compression="zstd")
```

## Convenience Methods

Both backends provide convenience methods for common merge strategies:

- `insert_dataset()` - Insert-only operations
- `upsert_dataset()` - Insert-or-update operations
- `update_dataset()` - Update-only operations
- `deduplicate_dataset()` - Deduplication operations

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
fs.write_pyarrow_dataset(data, "/path/to/dataset/", strategy="upsert", key_columns=["id"])

# Equivalent class-based approach
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
io = PyarrowDatasetIO()
io.write_parquet_dataset(data, "/path/to/dataset/", strategy="upsert", key_columns=["id"])
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
    # Static analysis knows handler has write_parquet_dataset method
    handler.write_parquet_dataset(data, "output/")
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
