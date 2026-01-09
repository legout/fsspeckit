# API Guide

This guide provides a capability-oriented overview of fsspeckit's public API. For detailed method signatures and parameters, see the [generated API documentation](../api/index.md).

> **Package Structure Note:** fsspeckit has been refactored to use a package-based structure. While legacy import paths still work, new code should use the improved package structure for better organization and discoverability.

## Core Capabilities

### Filesystem Factory

Create configured filesystems with protocol inference and path safety.

**Capability**: Configure storage  
**Functions**: `filesystem()`  
**API Reference**: [fsspeckit.core.filesystem](../api/fsspeckit.core.filesystem.md)  
**How-to Guides**: [Work with Filesystems](../how-to/work-with-filesystems.md), [Configure Cloud Storage](../how-to/configure-cloud-storage.md)

### Storage Options

Structured configuration for cloud and Git providers.

**Capability**: Configure storage  
**Classes**: `AwsStorageOptions`, `GcsStorageOptions`, `AzureStorageOptions`, `GitHubStorageOptions`, `GitLabStorageOptions`  
**API Reference**: [fsspeckit.storage_options](../api/fsspeckit.storage_options.base.md)  
**How-to Guides**: [Configure Cloud Storage](../how-to/configure-cloud-storage.md)

## Data Processing Capabilities

### Dataset Operations

High-performance dataset operations with DuckDB and PyArrow.

**Capability**: Process datasets  
**Classes**: `DuckDBDatasetIO`, `PyarrowDatasetIO`, `DuckDBDatasetHandler`, `PyarrowDatasetHandler`  
**Methods**: 
- `write_dataset(mode='append'|'overwrite')` - Write datasets with metadata tracking
- `merge(strategy='insert'|'update'|'upsert', key_columns=...)` - Incremental merge operations
- `compact_parquet_dataset()`, `optimize_parquet_dataset()` - Dataset maintenance

**Return Types**: `WriteDatasetResult`, `MergeResult`  
**API Reference**: [fsspeckit.datasets](../api/fsspeckit.datasets.md)  
**How-to Guides**: [Read and Write Datasets](../how-to/read-and-write-datasets.md), [Merge Datasets](../how-to/merge-datasets.md)

### Extended I/O

Enhanced file reading and writing capabilities.

**Capability**: Read/write files  
**Methods**: `read_json()`, `read_csv()`, `read_parquet()`, `write_json()`, `write_csv()`, `write_parquet()`  
**API Reference**: [fsspeckit.core.ext](../api/fsspeckit.core.ext.md)  
**How-to Guides**: [Read and Write Datasets](../how-to/read-and-write-datasets.md)

### SQL Filter Translation

Convert SQL WHERE clauses to framework-specific expressions.

**Capability**: Filter data with SQL  
**Functions**: `sql2pyarrow_filter()`, `sql2polars_filter()`  
**API Reference**: [fsspeckit.sql.filters](../api/fsspeckit.sql.filters.md)  
**How-to Guides**: [Use SQL Filters](../how-to/use-sql-filters.md)

## Utility Capabilities

### Parallel Processing

Execute functions across multiple inputs with progress tracking.

**Capability**: Process data in parallel  
**Function**: `run_parallel()`  
**API Reference**: [fsspeckit.common.misc](../api/fsspeckit.common.md)  
**How-to Guides**: [Optimize Performance](../how-to/optimize-performance.md)

### File Synchronization

Synchronize files and directories between storage backends.

**Capability**: Sync files  
**Functions**: `sync_files()`, `sync_dir()`  
**API Reference**: [fsspeckit.common.misc](../api/fsspeckit.common.md)  
**How-to Guides**: [Sync and Manage Files](../how-to/sync-and-manage-files.md)

### Type Conversion

Convert between different data formats and optimize data types.

**Capability**: Convert and optimize data  
**Functions**: `dict_to_dataframe()`, `to_pyarrow_table()`, `convert_large_types_to_normal()`  
**API Reference**: [fsspeckit.common.types](../api/fsspeckit.common.md)  
**How-to Guides**: [Read and Write Datasets](../how-to/read-and-write-datasets.md)

## Domain Package Organization

fsspeckit is organized into domain-specific packages for better discoverability:

### Core Package (`fsspeckit.core`)

Foundation layer providing filesystem APIs and path safety.

- **Filesystem Creation**: Enhanced `filesystem()` function with protocol inference
- **Path Safety**: `DirFileSystem` wrapper for secure directory confinement
- **Extended I/O**: Rich file reading/writing methods
- **Base Classes**: Enhanced filesystem base classes

### Storage Options (`fsspeckit.storage_options`)

Configuration layer for cloud and Git providers.

- **Provider Classes**: Structured configuration for AWS, GCP, Azure, GitHub, GitLab
- **Factory Functions**: Environment-based and URI-based configuration
- **Conversion Methods**: Serialize to/from YAML, environment variables

### Datasets (`fsspeckit.datasets`)

Data processing layer for large-scale operations.

- **DuckDB Handler**: High-performance parquet operations with SQL integration
- **PyArrow Handler**: Memory-efficient PyArrow operations with merge support
- **Write Operations**: `write_dataset()` for append/overwrite with `WriteDatasetResult` tracking
- **Merge Operations**: `merge()` for incremental updates with `MergeResult` tracking
- **Schema Management**: Type conversion and schema evolution

### SQL (`fsspeckit.sql`)

Query translation layer for cross-framework compatibility.

- **Filter Translation**: SQL to PyArrow and Polars expressions
- **Schema Awareness**: Type-aware filter generation
- **Cross-Framework**: Consistent querying across data backends

### Common (`fsspeckit.common`)

Shared utilities layer used across all domains.

- **Parallel Processing**: Concurrent execution with progress tracking
- **Type Conversion**: Format conversion and optimization
- **File Operations**: Synchronization and path utilities
- **Security**: Path validation and credential scrubbing

## Usage Patterns

### Basic Workflow

```python
# 1. Configure storage
from fsspeckit import filesystem
from fsspeckit.storage_options import storage_options_from_env

options = storage_options_from_env("s3")
fs = filesystem("s3", storage_options=options.to_dict())

# 2. Process data (DuckDB)
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, DuckDBDatasetHandler
import polars as pl

io = DuckDBDatasetIO()
data = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})

# Write dataset
result = io.write_dataset(data, "s3://bucket/dataset/", mode="append")
print(f"Wrote {result.total_rows} rows")

# Merge data
updates = pl.DataFrame({"id": [2, 3], "value": ["updated", "c"]})
merge_result = io.merge(
    data=updates,
    path="s3://bucket/dataset/",
    strategy="upsert",
    key_columns=["id"]
)
print(f"Inserted: {merge_result.inserted}, Updated: {merge_result.updated}")

# SQL operations with handler
handler = DuckDBDatasetHandler()
result = handler.execute_sql("SELECT * FROM parquet_scan('s3://bucket/dataset/')")

# 2. Process data (PyArrow)
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO, PyarrowDatasetHandler
import pyarrow as pa

io = PyarrowDatasetIO()
data = pa.table({"id": [1, 2], "value": ["a", "b"]})

# Write dataset
result = io.write_dataset(data, "s3://bucket/dataset/", mode="append")
print(f"Wrote {result.total_rows} rows across {len(result.files)} files")

# Merge data
updates = pa.table({"id": [2, 3], "value": ["updated", "c"]})
merge_result = io.merge(
    data=updates,
    path="s3://bucket/dataset/",
    strategy="upsert",
    key_columns=["id"]
)
print(f"Inserted: {merge_result.inserted}, Updated: {merge_result.updated}")

# Maintenance operations with handler
with PyarrowDatasetHandler() as handler:
    stats = handler.compact_parquet_dataset("s3://bucket/dataset/", target_mb_per_file=64)

# 3. Optimize performance
from fsspeckit.common.misc import run_parallel

results = run_parallel(process_file, file_list, max_workers=4)
```

### Advanced Workflow

```python
# 1. Multi-cloud configuration
from fsspeckit import AwsStorageOptions, GcsStorageOptions

aws_fs = AwsStorageOptions(region="us-east-1").to_filesystem()
gcs_fs = GcsStorageOptions(project="my-project").to_filesystem()

# 2. Cross-framework filtering
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

pyarrow_filter = sql2pyarrow_filter("value > 100", schema)
polars_filter = sql2polars_filter("value > 100", schema)

# 3. Dataset optimization
from fsspeckit.datasets import optimize_parquet_dataset_pyarrow

optimize_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/data/",
    z_order_columns=["category", "timestamp"],
    target_file_size="256MB"
)
```

## Migration from Utils

The `fsspeckit.utils` module provides backwards compatibility. For new code, use domain packages:

| Legacy Import | Domain Package | Recommended Import |
|---------------|----------------|-------------------|
| `from fsspeckit.utils import run_parallel` | Common | `from fsspeckit.common.misc import run_parallel` |
| `from fsspeckit.utils import DuckDBParquetHandler` | Datasets | `from fsspeckit.datasets import DuckDBParquetHandler` |
| `from fsspeckit.utils import sql2pyarrow_filter` | SQL | `from fsspeckit.sql.filters import sql2pyarrow_filter` |
| `from fsspeckit.utils import AwsStorageOptions` | Storage Options | `from fsspeckit.storage_options import AwsStorageOptions` |
| `from fsspeckit.utils import dict_to_dataframe` | Common | `from fsspeckit.common.types import dict_to_dataframe` |

For information on migration from older versions, refer to the project release notes.

## Error Handling

fsspeckit uses consistent exception types:

- **`ValueError`**: Configuration and validation errors
- **`FileNotFoundError`**: Missing resources
- **`PermissionError`**: Access control issues
- **`ImportError`**: Missing optional dependencies

### Error Handling Pattern

```python
from fsspeckit import AwsStorageOptions, filesystem

try:
    # Configure storage
    options = AwsStorageOptions(region="us-east-1", ...)
    fs = options.to_filesystem()
    
    # Use filesystem
    files = fs.ls("s3://bucket/")
    
except ValueError as e:
    # Configuration/validation errors
    print(f"Configuration error: {e}")
except FileNotFoundError as e:
    # Missing resources
    print(f"Resource not found: {e}")
except PermissionError as e:
    # Access control issues
    print(f"Access denied: {e}")
except ImportError as e:
    # Missing optional dependencies
    print(f"Missing dependency: {e}")
```

## Optional Dependencies

fsspeckit uses lazy imports for optional dependencies:

| Feature | Required Package | Install Command |
|---------|-----------------|-----------------|
| Dataset operations | `duckdb>=1.4.0` | `pip install duckdb` |
| PyArrow operations | `pyarrow>=10.0.0` | `pip install pyarrow` |
| Polars support | `polars>=0.19.0` | `pip install polars` |
| SQL filtering | `sqlglot>=20.0.0` | `pip install sqlglot` |
| Fast JSON | `orjson>=3.8.0` | `pip install orjson` |

### Dependency Management

```python
# Imports work even without dependencies
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.sql.filters import sql2pyarrow_filter

# Dependencies are required when actually using features
try:
    handler = DuckDBParquetHandler()
    handler.write_parquet_dataset(data, "path/")
except ImportError as e:
    print(f"Install with: pip install duckdb")
```

## Performance Considerations

### Caching

Enable caching for remote filesystems:

```python
fs = filesystem("s3://bucket/", cached=True, cache_storage="/fast/cache")
```

### Parallel Processing

Use parallel execution for I/O bound operations:

```python
from fsspeckit.common.misc import run_parallel

results = run_parallel(process_func, data_list, max_workers=8)
```

### Batch Operations

Process large datasets in batches:

```python
for batch in fs.read_parquet("data/*.parquet", batch_size="100MB"):
    process_batch(batch)
```

## Security Features

### Path Safety

Filesystems are wrapped in `DirFileSystem` by default for security:

```python
# All operations confined to specified directory
fs = filesystem("/data/", dirfs=True)
```

### Credential Scrubbing

Prevent credential leakage in logs:

```python
from fsspeckit.common.security import scrub_credentials

error_msg = f"Failed: access_key=AKIAIOSFODNN7EXAMPLE"
safe_msg = scrub_credentials(error_msg)
# Output: "Failed: access_key=[REDACTED]"
```

### Input Validation

Validate user inputs for security:

```python
from fsspeckit.common.security import validate_path, validate_columns

safe_path = validate_path(user_path, base_dir="/data/allowed")
safe_columns = validate_columns(user_columns, valid_columns=schema_columns)
```

## API Reference Links

For detailed method signatures and parameters:

- [Core APIs](../api/fsspeckit.core.base.md)
- [Storage Options](../api/fsspeckit.storage_options.base.md)
- [Dataset Operations](../api/fsspeckit.datasets.md)
- [SQL Filtering](../api/fsspeckit.sql.filters.md)
- [Common Utilities](../api/fsspeckit.common.md)

## Related Documentation

- [How-to Guides](../how-to/index.md) - Task-oriented recipes
- [Tutorials](../tutorials/index.md) - Step-by-step learning
- [Explanation](../explanation/index.md) - Conceptual understanding
