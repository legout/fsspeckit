# Utils Module

The `fsspeckit.utils` module provides a collection of utility functions that simplify common tasks such as logging, parallel processing, data type conversions, and schema transformations.

## Logging

### `setup_logging`

Configure logging throughout your application with loguru:

```python
from fsspeckit.utils import setup_logging

# Basic setup
setup_logging()

# With custom level and format
setup_logging(level="DEBUG", format_string="{time} | {level} | {message}")

# Control logging via environment variable
# export fsspeckit_LOG_LEVEL=DEBUG
```

**Environment Variables:**
- `fsspeckit_LOG_LEVEL` - Set the logging level (default: INFO)

## Parallel Processing

### `run_parallel`

Execute a function across multiple inputs using parallel threads with optional progress bar:

```python
from fsspeckit.utils import run_parallel

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
- `items` - List of items to process
- `n_jobs` - Number of parallel jobs (default: 1)
- `verbose` - Show progress bar (default: False)
- `backend` - Parallel backend ('threading' or 'loky')
- `**kwargs` - Additional keyword arguments passed to func

## File Synchronization

### `sync_files`

Synchronize files from source to destination, supporting efficient server-side copy when both paths are on the same filesystem:

```python
from fsspeckit.utils import sync_files

# Copy files with optional filtering
synced = sync_files(
    fs,
    source_paths=["/source/file1.txt", "/source/file2.txt"],
    target_path="/destination/",
    overwrite=True,
    verbose=True
)
```

### `sync_dir`

Recursively sync directories between filesystems:

```python
from fsspeckit.utils import sync_dir

# Sync entire directory
sync_dir(
    fs_source,
    source_path="/source/data/",
    fs_target,
    target_path="/backup/data/",
    overwrite=False,
    verbose=True
)
```

**Performance Note:** When source and target are on the same filesystem, `sync_dir` performs server-side copy for improved performance.

## Partitioning Utilities

### `get_partitions_from_path`

Extract partition information from a file path in Hive-style partition format:

```python
from fsspeckit.utils import get_partitions_from_path

# Extract partitions from path like "year=2023/month=10/day=15/data.parquet"
partitions = get_partitions_from_path("/data/year=2023/month=10/day=15/data.parquet")
# Returns: {"year": "2023", "month": "10", "day": "15"}
```

### `path_to_glob`

Convert a path with partition placeholders to a glob pattern:

```python
from fsspeckit.utils import path_to_glob

# Convert partition path to glob pattern
pattern = path_to_glob("/data/year=*/month=*/day=*/data.parquet")
# Returns: "/data/year=*/month=*/day=*/data.parquet"
```

## Type Conversion

### `dict_to_dataframe`

Convert dictionaries or lists of dictionaries to Polars DataFrame:

```python
from fsspeckit.utils import dict_to_dataframe

# Single dict
data = {"col1": [1, 2, 3], "col2": [4, 5, 6]}
df = dict_to_dataframe(data)

# List of dicts (records format)
records = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
]
df = dict_to_dataframe(records)
```

### `to_pyarrow_table`

Convert various data types to PyArrow Table:

```python
from fsspeckit.utils import to_pyarrow_table

# From Polars DataFrame
table = to_pyarrow_table(polars_df)

# From Pandas DataFrame
table = to_pyarrow_table(pandas_df)

# From dictionary
data = {"col1": [1, 2, 3], "col2": [4, 5, 6]}
table = to_pyarrow_table(data)

# From list of dicts
records = [{"a": 1}, {"a": 2}]
table = to_pyarrow_table(records)
```

## Datetime Utilities

### `timestamp_from_string`

Parse timestamp strings using standard library (zoneinfo-aware):

```python
from fsspeckit.utils import timestamp_from_string
from datetime import datetime

# Parse ISO format
ts = timestamp_from_string("2023-10-15T10:30:00")

# Parse with timezone
ts = timestamp_from_string("2023-10-15T10:30:00+02:00")

# Returns: datetime object
```

### `get_timedelta_str`

Get a human-readable time difference string:

```python
from fsspeckit.utils import get_timedelta_str
from datetime import datetime

start = datetime(2023, 1, 1)
end = datetime(2023, 1, 5, 12, 30, 45)

diff_str = get_timedelta_str(start, end)
# Returns: "4 days 12:30:45" (or similar format)
```

## Data Type Optimization

### Polars Data Type Optimization

#### `opt_dtype`

Automatically optimize Polars column data types to reduce memory usage:

```python
from fsspeckit.utils import opt_dtype_pl
import polars as pl

# Optimize a single column
df = pl.DataFrame({"id": [1, 2, 3], "count": [100, 200, 300]})
optimized = opt_dtype_pl(df)

# Or use as DataFrame extension:
df_opt = df.opt_dtype  # Custom extension method
```

You can tune `sample_size` and `sample_method` when calling `opt_dtype_pl` (or `df.opt_dtype`) to keep the regex heuristics bounded. The defaults (1024 rows, `sample_method="first"`) provide a good balance between inference accuracy and memory usage, and the inferred schema is based solely on the sampled rows before the full column is cast.

**Optimizations include:**
- Int64 → Int32 when range fits
- Float64 → Float32 when precision allows
- Large string → small string
- Categorical encoding for repetitive strings

#### `opt_dtype_pa`

PyArrow equivalent for type optimization:

```python
from fsspeckit.utils import opt_dtype_pa

# Optimize PyArrow table
table = pa.table({"id": [1, 2, 3], "count": [100, 200, 300]})
optimized = opt_dtype_pa(table)
```

The PyArrow helper exposes the same `sample_size`/`sample_method` knobs so you can limit the inference subset before the full table is cast, with the schema derived entirely from the sampled values.

## Schema Utilities

### `cast_schema`

Unify schemas across multiple tables/dataframes:

```python
from fsspeckit.utils import cast_schema

# Cast one schema to match another
target_schema = table1.schema
cast_table2 = cast_schema(table2, target_schema)
```

### `convert_large_types_to_normal`

Convert large_string/large_binary to normal string/binary types:

```python
from fsspeckit.utils import convert_large_types_to_normal

# Convert large types in PyArrow table
table = convert_large_types_to_normal(table)

# Useful for compatibility with systems that don't support large types
```

## SQL-to-Expression Conversion

### `sql2pyarrow_filter`

Convert SQL WHERE clause to PyArrow filter expression:

```python
from fsspeckit.utils import sql2pyarrow_filter
import pyarrow as pa

# Define schema
schema = pa.schema([
    ("age", pa.int32()),
    ("name", pa.string()),
    ("date", pa.timestamp("us"))
])

# Create filter from SQL
expr = sql2pyarrow_filter(
    "age > 25 AND name = 'Alice'",
    schema
)

# Apply to dataset
filtered = dataset.to_table(filter=expr)
```

### `sql2polars_filter`

Convert SQL WHERE clause to Polars filter expression:

```python
from fsspeckit.utils import sql2polars_filter

# Create filter expression
expr = sql2polars_filter("age > 25 AND status = 'active'")

# Apply to DataFrame
filtered_df = df.filter(expr)
```

**Supported SQL syntax:**
- Comparison operators: `>`, `<`, `>=`, `<=`, `=`, `!=`
- Logical operators: `AND`, `OR`, `NOT`
- In operator: `IN (val1, val2)`
- Between operator: `BETWEEN x AND y`
- Null checks: `IS NULL`, `IS NOT NULL`

## DuckDB Parquet Handler

### `DuckDBParquetHandler`

High-performance parquet dataset operations using DuckDB with fsspec integration for local and remote storage.

#### Basic Usage

```python
from fsspeckit.utils import DuckDBParquetHandler
import pyarrow as pa

# Create sample data
table = pa.table({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})

# Write and read parquet files
with DuckDBParquetHandler() as handler:
    handler.write_parquet(table, "/tmp/data.parquet")
    result = handler.read_parquet("/tmp/data.parquet")
```

#### Dataset Write Operations

Write to parquet datasets with automatic unique filenames:

```python
# Basic dataset write
with DuckDBParquetHandler() as handler:
    handler.write_parquet_dataset(table, "/data/sales/")
    # Creates: /data/sales/part-a1b2c3d4.parquet
```

**Append Mode (Incremental Updates)**

```python
# Day 1 - initial load
handler.write_parquet_dataset(batch1, "/data/sales/", mode="append")

# Day 2 - add new data (preserves Day 1 file)
handler.write_parquet_dataset(batch2, "/data/sales/", mode="append")

# Read combined dataset
all_data = handler.read_parquet("/data/sales/")
```

**Overwrite Mode (Replace Dataset)**

```python
# Replace entire dataset
handler.write_parquet_dataset(
    new_data,
    "/data/sales/",
    mode="overwrite"  # Deletes existing parquet files
)
```

**Split Large Tables**

```python
# Split into multiple files with max 100k rows each
handler.write_parquet_dataset(
    large_table,
    "/data/sales/",
    max_rows_per_file=100000
)
```

**Custom Filename Templates**

```python
# Custom filename pattern
handler.write_parquet_dataset(
    table,
    "/data/sales/",
    basename_template="sales_{}.parquet"
)
# Creates: sales_a1b2c3d4.parquet
```

#### Dataset Merge Operations

Intelligently merge parquet datasets with multiple strategies for CDC, incremental loads, and synchronization:

**Basic UPSERT (Change Data Capture)**

```python
with DuckDBParquetHandler() as handler:
    # Initial data
    initial = pa.table({
        'customer_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'balance': [100, 200, 300]
    })
    handler.write_parquet_dataset(initial, "/data/customers/")
    
    # CDC changes: Update customer 2, add customer 4
    changes = pa.table({
        'customer_id': [2, 4],
        'name': ['Bob', 'Diana'],
        'balance': [250, 400]
    })
    
    # Merge with UPSERT strategy
    stats = handler.merge_parquet_dataset(
        source=changes,
        target_path="/data/customers/",
        key_columns="customer_id",
        strategy="upsert"
    )
    
    print(stats)
    # {'inserted': 1, 'updated': 1, 'deleted': 0, 'total': 4}
```

**Available Merge Strategies**

1. **UPSERT** - Insert new records and update existing ones (default for CDC)
   ```python
   # Best for: Change data capture, general updates
   handler.merge_parquet_dataset(
       source=changes,
       target_path="/data/",
       key_columns="id",
       strategy="upsert"
   )
   ```

2. **INSERT** - Add only new records, ignore existing (append-only)
   ```python
   # Best for: Event logs, audit trails, immutable data
   handler.merge_parquet_dataset(
       source=new_events,
       target_path="/data/events/",
       key_columns="event_id",
       strategy="insert"
   )
   ```

3. **UPDATE** - Update only existing records, ignore new ones
   ```python
   # Best for: Dimension table updates (SCD Type 1)
   handler.merge_parquet_dataset(
       source=updates,
       target_path="/data/products/",
       key_columns="product_id",
       strategy="update"
   )
   ```

4. **FULL_MERGE** - Complete synchronization (insert, update, delete)
   ```python
   # Best for: Full snapshots, complete syncs
   handler.merge_parquet_dataset(
       source=fresh_snapshot,
       target_path="/data/inventory/",
       key_columns="sku",
       strategy="full_merge"
   )
   ```

5. **DEDUPLICATE** - Remove duplicates, keeping most recent
   ```python
   # Best for: Deduplication, handling duplicate records
   handler.merge_parquet_dataset(
       source=data_with_duplicates,
       target_path="/data/transactions/",
       key_columns="transaction_id",
       strategy="deduplicate",
       dedup_order_by=["timestamp"]  # Keep highest timestamp
   )
   ```

**Composite Key Support**

Merge on multiple columns for complex uniqueness requirements:

```python
# Merge on user + date combination
handler.merge_parquet_dataset(
    source=daily_metrics,
    target_path="/data/user_metrics/",
    key_columns=["user_id", "date"],  # Composite key
    strategy="upsert"
)
```

**Source from Path or Table**

```python
# Source as PyArrow table
handler.merge_parquet_dataset(
    source=pa_table,
    target_path="/data/target/",
    key_columns="id",
    strategy="upsert"
)

# Source as path to dataset
handler.merge_parquet_dataset(
    source="/data/source/",  # Path to parquet dataset
    target_path="/data/target/",
    key_columns="id",
    strategy="upsert"
)
```

**Merge with Compression**

```python
# Merge and recompress with better algorithm
handler.merge_parquet_dataset(
    source=new_data,
    target_path="/data/",
    key_columns="id",
    strategy="upsert",
    compression="zstd"  # High compression ratio
)
```

**Merge Statistics**

All merge operations return detailed statistics:

```python
stats = handler.merge_parquet_dataset(...)

print(f"Inserted: {stats['inserted']}")  # New records added
print(f"Updated: {stats['updated']}")    # Existing records updated
print(f"Deleted: {stats['deleted']}")    # Records removed (FULL_MERGE only)
print(f"Total: {stats['total']}")        # Final record count
```

**Validation and Error Handling**

The merge operation performs comprehensive validation:

```python
try:
    handler.merge_parquet_dataset(
        source=data,
        target_path="/data/",
        key_columns="id",
        strategy="upsert"
    )
except ValueError as e:
    # Handles: Missing key columns, NULL keys, schema mismatches
    print(f"Merge validation failed: {e}")
except TypeError as e:
    # Handles: Column type mismatches
    print(f"Type error: {e}")
```

**Best Practices for Merging**

1. **Choose the right strategy** - UPSERT for CDC, INSERT for events, UPDATE for dimensions
2. **Use composite keys** - When uniqueness depends on multiple columns
3. **Validate schemas first** - Ensure source and target schemas match
4. **Monitor statistics** - Track inserted/updated/deleted counts
5. **Handle NULL keys** - Key columns must not contain NULL values
6. **Test with small datasets** - Verify merge logic before production
7. **Use DEDUPLICATE** - Clean data before merging if duplicates exist
8. **Optimize with QUALIFY** - DuckDB's QUALIFY clause optimizes deduplication

**Performance Characteristics**

| Strategy | Time Complexity | Best For | Deletes Data |
|----------|----------------|----------|--------------|
| UPSERT | O(n + m) | CDC, general updates | No |
| INSERT | O(n + m) | Append-only loads | No |
| UPDATE | O(n + m) | Dimension updates | No |
| FULL_MERGE | O(n + m) | Full synchronization | Yes |
| DEDUPLICATE | O(n + m) | Duplicate removal | Yes |

*n = target rows, m = source rows*

See `examples/duckdb/duckdb_merge_example.py` for comprehensive examples of all strategies.

#### SQL Query Execution

Execute SQL queries on parquet data:

```python
with DuckDBParquetHandler() as handler:
    handler.write_parquet(table, "/tmp/data.parquet")
    
    # Simple query
    result = handler.execute_sql(
        "SELECT * FROM parquet_scan('/tmp/data.parquet') WHERE id > 1"
    )
    
    # Parameterized query
    result = handler.execute_sql(
        "SELECT * FROM parquet_scan('/tmp/data.parquet') WHERE id BETWEEN ? AND ?",
        parameters=[1, 3]
    )
    
    # Aggregation
    result = handler.execute_sql("""
        SELECT name, COUNT(*) as count
        FROM parquet_scan('/tmp/data.parquet')
        GROUP BY name
    """)
```

#### Remote Storage Integration

Works seamlessly with S3, GCS, Azure through fsspec:

```python
from fsspeckit.storage_options import AwsStorageOptions

options = AwsStorageOptions(
    access_key_id="YOUR_KEY",
    secret_access_key="YOUR_SECRET",
    region="us-east-1"
)

with DuckDBParquetHandler(storage_options=options) as handler:
    # Write to S3
    handler.write_parquet_dataset(table, "s3://bucket/data/")
    
    # Read from S3
    result = handler.read_parquet("s3://bucket/data/")
    
    # Query S3 data
    result = handler.execute_sql(
        "SELECT * FROM parquet_scan('s3://bucket/data/*.parquet')"
    )
```

#### Key Features

- **Unique Filenames**: UUID-based generation prevents collisions
- **Write Modes**: Append (default) or overwrite
- **File Splitting**: Control file size with `max_rows_per_file`
- **Compression**: Support for snappy, gzip, zstd, lz4, brotli
- **Column Selection**: Read only needed columns for efficiency
- **SQL Analytics**: Full DuckDB SQL capabilities on parquet data

#### Best Practices

1. **Use append mode for incremental updates** - Safer default, no accidental data loss
2. **Keep files reasonably sized** - 10-100 MB per file using `max_rows_per_file`
3. **Organize hierarchically** - Use year/month/day directory structure
4. **Choose appropriate compression** - 'snappy' for speed, 'zstd' for better compression
5. **Periodic compaction** - Consolidate many small files when using append mode

See `examples/duckdb/duckdb_dataset_write_example.py` for comprehensive examples.

## Dependency Checking

### `check_optional_dependency`

Verify that optional dependencies are installed:

```python
from fsspeckit.utils import check_optional_dependency

# Check for a dependency
try:
    check_optional_dependency("polars")
except ImportError as e:
    print(f"Optional dependency missing: {e}")
```

## Filesystem Comparison

### `check_fs_identical`

Compare two filesystems to verify they contain identical data:

```python
from fsspeckit.utils import check_fs_identical

# Compare local directories
fs1 = filesystem("/path1")
fs2 = filesystem("/path2")

identical = check_fs_identical(fs1, "/data", fs2, "/data")
```

## Polars DataFrame Extensions

When using fsspeckit with Polars, additional methods are automatically added to DataFrames:

```python
import polars as pl
from fsspeckit import filesystem

df = pl.DataFrame({
    "date": ["2023-01-01", "2023-02-15"],
    "category": ["A", "B"],
    "value": [100, 200]
})

# Access optimized dtypes
df_opt = df.opt_dtype

# Create partition columns from date
df_with_parts = df.with_datepart_columns("date")

# Drop columns with all null values
df_clean = df.drop_null_columns()
```
