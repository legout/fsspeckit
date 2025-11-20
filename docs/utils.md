# Utilities Reference

This page documents utilities available in fsspeckit. The utilities are organized into domain packages for better discoverability and maintainability.

> **Package Layout Overview**: fsspeckit is organized into domain packages - see [Architecture](architecture.md) for details.
> **Backwards Compatibility**: All imports from `fsspeckit.utils` continue to work unchanged.

## Cross-Cutting Utilities (`fsspeckit.common`)

### Logging

#### `setup_logging()`

Configure logging throughout your application with loguru:

```python
from fsspeckit.common.logging import setup_logging

# Basic setup
setup_logging()

# With custom level and format
setup_logging(level="DEBUG", format_string="{time} | {level} | {message}")

# Control logging via environment variable
# export fsspeckit_LOG_LEVEL=DEBUG
```

**Environment Variables:**
- `fsspeckit_LOG_LEVEL` - Set the logging level (default: INFO)

### Parallel Processing

#### `run_parallel()`

Execute a function across multiple inputs using parallel threads with optional progress bar:

```python
from fsspeckit.common.misc import run_parallel

def process_file(path, multiplier=1):
    return len(path) * multiplier

results = run_parallel(
    process_file,
    ["/path1", "/path2", "/path3"],
    multiplier=2,
    n_jobs=4,
    verbose=True,  # Show progress bar
    backend="threading"
)
```

**Parameters:**
- `func` - Function to apply to each item
- `items` - List of inputs to process
- `n_jobs` - Number of parallel threads (default: CPU count)
- `backend` - Parallel backend: "threading" or "process" (default: "threading")

### File System Operations

#### File Synchronization

```python
from fsspeckit.common.misc import sync_files, sync_dir

# Sync individual files
sync_files(
    source_paths=["/data/file1.txt", "/data/file2.txt"],
    fs_target=filesystem("s3://bucket/"),
    verbose=True
)

# Sync directories recursively
sync_dir(
    source_dir="/data/",
    fs_target=filesystem("s3://bucket/"),
    pattern="*.parquet",
    delete=True
)
```

#### Path Utilities

```python
from fsspeckit.common.misc import get_partitions_from_path

# Extract partition information from paths
partitions = get_partitions_from_path("/data/year=2023/month=01/file.parquet")
# Returns: {'year': '2023', 'month': '01'}
```

### Type Conversion

#### DataFrame Conversion

```python
from fsspeckit.common.types import dict_to_dataframe, to_pyarrow_table

# Convert dict to DataFrame
data = {"col1": [1, 2, 3], "col2": [4, 5, 6]}
df = dict_to_dataframe(data)

# Convert to PyArrow table
table = to_pyarrow_table(df)
```

#### DateTime Utilities

```python
from fsspeckit.common.datetime import timestamp_from_string, get_timestamp_column

# Parse timestamp strings
ts = timestamp_from_string("2023-01-15 10:30:00")
ts_with_tz = timestamp_from_string("2023-01-15T10:30:00Z", tz="UTC")

# Find timestamp column in DataFrame
timestamp_cols = get_timestamp_column(df)
```

#### Polars Optimization

```python
from fsspeckit.common.polars import opt_dtype_pl

# Optimize DataFrame data types
df_optimized = opt_dtype_pl(df, shrink_numerics=True)

# Optimize specific columns
df_optimized = opt_dtype_pl(df, columns=["numeric_col"])
```

## Dataset Operations (`fsspeckit.datasets`)

### DuckDB Dataset Handler

#### `DuckDBParquetHandler`

High-performance parquet dataset operations using DuckDB:

```python
from fsspeckit.datasets import DuckDBParquetHandler

with DuckDBParquetHandler() as handler:
    # Dataset maintenance operations
    handler.compact_parquet_dataset(
        path="/data/events/",
        target_rows_per_file=500_000
    )

    # Data analytics
    handler.optimize_parquet_dataset(
        path="/data/events/",
        zorder_columns=["user_id", "timestamp"]
    )
```

### PyArrow Dataset Helpers

#### Schema Management

```python
from fsspeckit.datasets import cast_schema, convert_large_types_to_normal

# Convert to standard schema
standard_schema = convert_large_types_normal(original_schema)

# Cast table to schema
table_casted = cast_schema(table, target_schema)
```

#### Data Type Optimization

```python
from fsspeckit.datasets import opt_dtype_pa

# Optimize PyArrow table data types
table_optimized = opt_dtype_pa(table)
```

#### Dataset Merging

```python
from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow

# Merge multiple datasets
merge_parquet_pyarrow(
    dataset_paths=["/data/part1/", "/data/part2/"],
    target_path="/data/merged/",
    key_columns=["id"]
)
```

## SQL Filtering (`fsspeckit.sql`)

### SQL to Expression Translation

#### `sql2pyarrow_filter()`

Convert SQL WHERE clauses to PyArrow filter expressions:

```python
from fsspeckit.sql.filters import sql2pyarrow_filter

import pyarrow as pa

schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("timestamp", pa.timestamp("us")),
    ("value", pa.float64())
])

# Convert SQL to PyArrow filter
filter_expr = sql2pyarrow_filter("name = 'test' AND value > 100", schema)
```

#### `sql2polars_filter()`

Convert SQL WHERE clauses to Polars expressions:

```python
from fsspeckit.sql.filters import sql2polars_filter

import polars as pl

# Convert SQL to Polars filter
filter_expr = sql2polars_filter("name == 'test' AND value > 100")
```

## Backwards Compatibility (`fsspeckit.utils`)

The `fsspeckit.utils` module provides a backwards-compatible façade that re-exports selected helpers from the domain packages.

### Legacy Import Examples

```python
# These imports continue to work for backwards compatibility
from fsspeckit.utils import (
    setup_logging,
    run_parallel,
    DuckDBParquetHandler,
    sql2pyarrow_filter,
    dict_to_dataframe,
    to_pyarrow_table,
    opt_dtype_pl,
    opt_dtype_pa
)
```

### Migration Recommendation

For new code, prefer importing directly from domain packages:

```python
# Recommended (new code)
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.common.logging import setup_logging
from fsspeckit.sql.filters import sql2pyarrow_filter

# Legacy (existing code - still works)
from fsspeckit.utils import DuckDBParquetHandler, setup_logging, sql2pyarrow_filter
```

For detailed migration instructions, see the [Migration Guide](https://github.com/legout/fsspeckit/blob/main/MIGRATION_GUIDE.md).