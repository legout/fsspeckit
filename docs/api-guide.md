# API Guide

This guide provides a comprehensive overview of the fsspeckit public API and how to use its main components.

## Core Filesystem Factory

### `filesystem()`

The main entry point for creating configured filesystems:

```python
from fsspeckit import filesystem

# Basic local filesystem
fs = filesystem("file")

# S3 with caching
fs = filesystem("s3://my-bucket/", cached=True)

# With storage options
fs = filesystem("s3", storage_options={"region": "us-west-2"})

# With base filesystem (DirFileSystem hierarchy)
fs = filesystem("/data", dirfs=True, base_fs=parent_fs)
```

**Parameters:**
- `protocol_or_path` - Protocol (e.g., 's3', 'gs') or path
- `storage_options` - Dict or StorageOptions object
- `cached` - Enable caching (default: False)
- `cache_storage` - Cache directory location
- `verbose` - Log cache operations (default: False)
- `dirfs` - Wrap in DirFileSystem (default: True)
- `base_fs` - Parent DirFileSystem for hierarchy
- `use_listings_cache` - Use listings cache (default: True)
- `skip_instance_cache` - Skip instance cache (default: False)
- `**kwargs` - Protocol-specific options

## Storage Options Classes

Storage options provide structured configuration for different providers:

```python
from fsspeckit.storage_options import (
    AwsStorageOptions,
    GcsStorageOptions,
    AzureStorageOptions,
    GitHubStorageOptions,
    GitLabStorageOptions,
    LocalStorageOptions
)

# AWS S3
aws = AwsStorageOptions(
    region="us-east-1",
    access_key_id="key",
    secret_access_key="secret"
)
fs = aws.to_filesystem()

# Google Cloud Storage
gcs = GcsStorageOptions(
    project="my-project",
    token="path/to/service-account.json"
)
fs = gcs.to_filesystem()

# Azure
azure = AzureStorageOptions(
    account_name="myaccount",
    account_key="key..."
)
fs = azure.to_filesystem()

# GitHub Repository
github = GitHubStorageOptions(
    org="microsoft",
    repo="vscode",
    token="ghp_xxx"
)
fs = github.to_filesystem()

# GitLab Repository
gitlab = GitLabStorageOptions(
    project_id=12345,
    token="glpat_xxx"
)
fs = gitlab.to_filesystem()

# Local
local = LocalStorageOptions(auto_mkdir=True)
fs = local.to_filesystem()
```

### StorageOptions Factories

Create storage options from various sources:

```python
from fsspeckit.storage_options import (
    from_dict,
    from_env,
    merge_storage_options,
    infer_protocol_from_uri,
    storage_options_from_uri
)

# From dictionary
opts = from_dict("s3", {"region": "us-west-2"})

# From environment variables
opts = AwsStorageOptions.from_env()

# Merge multiple options
merged = merge_storage_options(opts1, opts2, overwrite=True)

# Infer protocol from URI
protocol = infer_protocol_from_uri("s3://bucket/path")

# Create from URI
opts = storage_options_from_uri("s3://my-bucket/data")
```

## Extended I/O Operations

fsspeckit adds rich I/O methods to all fsspec filesystems through monkey-patching.

### Reading Operations

#### JSON Operations

```python
from fsspeckit import filesystem

fs = filesystem(".")

# Read single JSON file
data = fs.read_json_file("data.json")  # Returns dict
df = fs.read_json_file("data.json", as_dataframe=True)  # Returns Polars DF

# Read multiple JSON files with batching
for batch in fs.read_json("data/*.json", batch_size=5):
    # Process batch
    pass

# Read JSON Lines format
df = fs.read_json("data/lines.jsonl", as_dataframe=True)

# With threading
df = fs.read_json("data/*.json", use_threads=True, num_threads=4)

# Include source file path
df = fs.read_json("data/*.json", include_file_path=True)
```

#### CSV Operations

```python
# Read single CSV
df = fs.read_csv_file("data.csv")

# Read multiple CSV files
df = fs.read_csv("data/*.csv", concat=True)

# Batch reading
for batch in fs.read_csv("data/*.csv", batch_size=10):
    pass

# Optimize data types
df = fs.read_csv("data/*.csv", opt_dtypes=True)

# With parallelism
df = fs.read_csv("data/*.csv", use_threads=True)
```

#### Parquet Operations

```python
# Read single Parquet file
table = fs.read_parquet_file("data.parquet")

# Read multiple with schema unification
table = fs.read_parquet("data/*.parquet", concat=True)

# Batch reading
for batch in fs.read_parquet("data/*.parquet", batch_size=20):
    pass

# With partitioning support
table = fs.read_parquet("partitioned_data/**/*.parquet", concat=True)

# Include file path column
table = fs.read_parquet("data/*.parquet", include_file_path=True)
```

#### Universal Reader

```python
# Auto-detect format from file extension
df = fs.read_files("data/mixed/*", format="auto")

# Explicit format
df = fs.read_files("data/*.csv", format="csv")

# Control result type
df_polars = fs.read_files("data/*.parquet", as_dataframe=True)
table_arrow = fs.read_files("data/*.parquet", as_dataframe=False)
```

### Dataset Operations

```python
# Create PyArrow dataset
dataset = fs.pyarrow_dataset("data/")

# Optimized for Parquet with metadata
dataset = fs.pyarrow_parquet_dataset("partitioned_data/")

# Query dataset with filtering
filtered = dataset.to_table(filter=pyarrow.compute.greater(dataset.column("age"), 25))

# Schema inspection
print(dataset.schema)

# Statistics and metadata
print(dataset.count_rows())
```

### Writing Operations

#### Parquet Writing

```python
import polars as pl

# From Polars DataFrame
df = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
fs.write_parquet(df, "output.parquet")

# From Pandas
import pandas as pd
df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
fs.write_parquet(df, "output.parquet")

# Compression
fs.write_parquet(df, "output.parquet", compression="zstd")

# Append mode
fs.write_parquet(df, "output.parquet", mode="append")
```

#### CSV Writing

```python
# Write DataFrame
df.write_csv("output.csv")

# Append to existing file
fs.write_csv(new_df, "output.csv", mode="append")
```

#### JSON Writing

```python
# Write DataFrame to JSON
df.write_json("output.json")

# JSON Lines format
fs.write_json(df, "output.jsonl", format="json_lines")

# Include file path metadata
fs.write_json(df, "output.json", include_file_path=True)
```

#### PyArrow Dataset Writing

```python
import pyarrow as pa

# Write partitioned dataset
table = pa.table({"year": [2023, 2023, 2024], "value": [10, 20, 30]})
fs.write_pyarrow_dataset(
    data=table,
    path="partitioned_data",
    partition_by=["year"],
    format="parquet",
    compression="zstd"
)

# Result structure: partitioned_data/year=2023/...parquet files
```

### Cache Management

```python
# Clear all caches
fs.clear_cache()

# Check cache size
size = fs.get_cache_size()

# Sync cache (ensure data is written)
fs.sync_cache()
```

## Helper Utilities

> **Note**: fsspeckit utilities are organized into domain packages. See [Architecture](architecture.md) for details.

### Parallel Processing

```python
from fsspeckit.common.misc import run_parallel

def process(item):
    return len(item)

results = run_parallel(
    process,
    ["item1", "item2", "item3"],
    n_jobs=4,
    verbose=True
)
```

### Type Conversions

```python
from fsspeckit.common.types import (
    dict_to_dataframe,
    to_pyarrow_table
)

# Dict to DataFrame
df = dict_to_dataframe({"col1": [1, 2], "col2": [3, 4]})

# Any to PyArrow
table = to_pyarrow_table(df)
```

### Data Type Optimization

```python
import polars as pl
from fsspeckit.common import opt_dtype_pl

# Optimize DataFrame
df = pl.read_csv("data.csv")
df_opt = opt_dtype_pl(df)

# Or use extension
df_opt = df.opt_dtype
```

### SQL Filtering

```python
from fsspeckit.sql.filters import sql2pyarrow_filter
import pyarrow as pa

schema = pa.schema([("age", pa.int32()), ("name", pa.string())])
expr = sql2pyarrow_filter("age > 25 AND name = 'Alice'", schema)
filtered_table = dataset.to_table(filter=expr)
```

### File Synchronization

```python
from fsspeckit.common.misc import sync_dir

# Sync directories
sync_dir(
    fs_source, "/source/",
    fs_target, "/target/",
    overwrite=False
)
```

## Filesystem Classes

### Custom Implementations

#### `GitLabFileSystem`

Read-only filesystem for GitLab repositories:

```python
from fsspeckit.core import filesystem

fs = filesystem(
    "gitlab",
    storage_options={
        "project_name": "group/project",
        "token": "glpat_xxx",
        "ref": "main"
    }
)

# List files
files = fs.ls("/")

# Read file
content = fs.cat("README.md")
```

#### `MonitoredSimpleCacheFileSystem`

Enhanced cache with logging and monitoring:

```python
# Automatically used when cached=True
fs = filesystem("s3", cached=True, verbose=True)

# Monitor cache operations
fs.sync_cache()
size = fs.get_cache_size()
```

## Working with DirFileSystem

```python
from fsspeckit import filesystem

# Create DirFileSystem wrapper
fs = filesystem("/data", dirfs=True)

# Access files within the base directory
fs.ls("/subdir")

# Create hierarchical filesystem (base_fs parameter)
parent_fs = filesystem("/datasets", dirfs=True)
child_fs = filesystem("/datasets/project1", dirfs=True, base_fs=parent_fs)

# Files are accessible only within the base directory
# Attempting to escape raises an error
```

## Configuration Methods

All storage option classes provide conversion methods:

```python
opts = AwsStorageOptions(...)

# Convert to fsspec kwargs
kwargs = opts.to_fsspec_kwargs()

# Convert to filesystem
fs = opts.to_filesystem()

# Convert to object store kwargs (for deltalake, etc.)
obj_store_kwargs = opts.to_object_store_kwargs()

# Convert to YAML
yaml_str = opts.to_yaml()

# Load from YAML
opts = AwsStorageOptions.from_yaml(yaml_str)

# Convert to environment variables
env = opts.to_env()

# Load from environment
opts = AwsStorageOptions.from_env()
```

## Error Handling

```python
from fsspeckit import filesystem
from fsspeckit.storage_options import AwsStorageOptions

try:
    fs = filesystem("s3", storage_options={"region": "invalid"})
    data = fs.cat("s3://bucket/file.txt")
except Exception as e:
    print(f"Error: {e}")

# Check optional dependencies
from fsspeckit.common.misc import check_optional_dependency
try:
    check_optional_dependency("deltalake")
except ImportError:
    print("Install with: pip install deltalake")
```

## Best Practices

1. **Use StorageOptions** - Type-safe and consistent configuration
2. **Enable Caching** - For remote filesystems, always consider caching
3. **Batch Processing** - Use batch_size for large datasets
4. **Parallel I/O** - Enable threading for multi-file operations
5. **Type Optimization** - Use opt_dtypes=True to reduce memory
6. **Error Handling** - Wrap filesystem operations in try/except
7. **Import from Domain Packages** - Use `fsspeckit.datasets`, `fsspeckit.common`, etc. for better discoverability

## Optional Dependencies

`fsspeckit` uses **lazy imports** to keep installations lightweight. Core functionality works without any optional dependencies.

### Checking for Optional Dependencies

```python
# The import succeeds even without optional dependencies
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.common.polars import opt_dtype
from fsspeckit.sql.filters import sql2pyarrow_filter

# Dependencies are only required when actually using the features
try:
    # This requires pyarrow to be installed
    from fsspeckit.datasets.pyarrow import optimize_parquet_dataset_pyarrow
    optimize_parquet_dataset_pyarrow("path/to/dataset")
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install pyarrow")
```

### Feature-to-Dependency Mapping

| Feature | Required Package | Install Command |
|---------|-----------------|-----------------|
| `fsspeckit.datasets.pyarrow` | `pyarrow>=10.0.0` | `pip install pyarrow` |
| `fsspeckit.datasets.DuckDBParquetHandler` | `duckdb>=0.9.0` | `pip install duckdb` |
| `fsspeckit.common.polars` | `polars>=0.19.0` | `pip install polars` |
| `fsspeckit.sql.filters` | `sqlglot>=20.0.0`, `pyarrow` | `pip install sqlglot pyarrow` |
| Fast JSON in `core.ext` | `orjson>=3.8.0` | `pip install orjson` |

### Error Messages

When an optional dependency is missing, you'll see a clear error message:

```
ImportError: The 'duckdb' package is required for DuckDBParquetHandler.
Install it with: pip install duckdb
```

### Installing All Optional Dependencies

```bash
# Install all data processing libraries
pip install pyarrow polars duckdb sqlglot orjson

# Or use specific combinations as needed
pip install pyarrow duckdb  # For DuckDB + PyArrow workflows
pip install polars sqlglot  # For Polars + SQL filtering
```

## See Also

- [Advanced Usage](advanced.md) - In-depth guides and patterns
- [Utils Module](utils.md) - Utility functions reference
- [Examples](examples.md) - Runnable example scripts
- [Architecture](architecture.md) - Design and implementation details
