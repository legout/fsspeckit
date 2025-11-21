# Advanced Usage

`fsspeckit` extends the capabilities of `fsspec` to provide enhanced filesystem utilities, storage option configurations, and cross-framework SQL filter translation. This section covers advanced features and configurations for getting the most out of the library.

## Domain Package Organization

The refactored `fsspeckit` architecture organizes functionality into domain-specific packages, making it easier to discover and use the right tools for each task.

### Choosing the Right Package

For optimal experience, import from the appropriate domain package:

- **Dataset Operations**: Use `fsspeckit.datasets` for DuckDB and PyArrow dataset operations
- **SQL Filtering**: Use `fsspeckit.sql` for SQL-to-filter translation
- **Storage Configuration**: Use `fsspeckit.storage_options` for cloud and Git provider setup
- **General Utilities**: Use `fsspeckit.common` for logging, parallel processing, type conversion
- **Backwards Compatibility**: `fsspeckit.utils` continues to work for existing code

### Import Examples by Use Case

```python
# Dataset operations (recommended)
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow

# SQL filtering
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

# Storage configuration
from fsspeckit.storage_options import AwsStorageOptions, storage_options_from_env

# Common utilities
from fsspeckit.common.misc import run_parallel
from fsspeckit.common.types import convert_large_types_to_normal

# Filesystem creation
from fsspeckit.core.filesystem import filesystem

# Backwards compatible (legacy)
from fsspeckit.utils import DuckDBParquetHandler  # Still works
```

## Enhanced Filesystem Creation

The `fsspeckit.core.filesystem.filesystem` function provides a centralized way to create fsspec filesystem objects with protocol inference and validation.

### Protocol Inference

```python
from fsspeckit.core.filesystem import filesystem

# Auto-detect protocol from URI
fs = filesystem("s3://bucket/path")  # Automatically detects S3
fs = filesystem("gs://bucket/path")  # Automatically detects GCS
fs = filesystem("az://container/path")  # Automatically detects Azure
fs = filesystem("github://owner/repo")  # Automatically detects GitHub
```

### Storage Options Integration

```python
from fsspeckit.storage_options import AwsStorageOptions
from fsspeckit.core.filesystem import filesystem

# Configure S3 using structured options
aws_opts = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY"
)

# Create filesystem with structured options
fs = filesystem("s3", storage_options=aws_opts.to_dict())
```

## Advanced Dataset Operations

### DuckDB Parquet Handler

The `DuckDBParquetHandler` provides high-performance dataset operations with atomic guarantees and fsspec integration.

```python
from fsspeckit.datasets import DuckDBParquetHandler
import polars as pl

# Initialize handler with storage options
storage_options = {"key": "value", "secret": "secret"}
handler = DuckDBParquetHandler(storage_options=storage_options)

# Sample data
data = pl.DataFrame({
    "id": [1, 2, 3, 4],
    "value": ["A", "B", "C", "D"],
    "category": ["X", "Y", "X", "Y"]
})

# Write dataset with atomic guarantees
handler.write_parquet_dataset(data, "s3://bucket/dataset/")

# Execute SQL with fsspec integration
result = handler.execute_sql("""
    SELECT category, COUNT(*) as count, SUM(id) as total_id
    FROM parquet_scan('s3://bucket/dataset/')
    GROUP BY category
""")

print(result)
```

### PyArrow Dataset Operations

```python
import pyarrow as pa
import pyarrow.parquet as pq
from fsspeckit.datasets.pyarrow import (
    merge_parquet_dataset_pyarrow,
    optimize_parquet_dataset_pyarrow,
    compact_parquet_dataset_pyarrow
)

# Merge multiple parquet datasets
merge_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/dataset/",
    output_path="s3://bucket/merged/",
    merge_strategy="schema_evolution"
)

# Optimize dataset with Z-ordering
optimize_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/dataset/",
    z_order_columns=["category", "timestamp"],
    target_file_size="256MB"
)

# Compact small files
compact_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/dataset/",
    target_file_size="128MB"
)
```

## SQL Filter Translation

The `fsspeckit.sql` package provides cross-framework SQL-to-filter translation, enabling consistent querying across PyArrow and Polars.

### PyArrow Filter Translation

```python
import pyarrow as pa
import pyarrow.compute as pc
from fsspeckit.sql.filters import sql2pyarrow_filter

# Define schema
schema = pa.schema([
    ("id", pa.int64()),
    ("value", pa.string()),
    ("timestamp", pa.timestamp("us"))
])

# Convert SQL to PyArrow filter expression
sql_filter = "id > 100 AND value IN ('A', 'B', 'C')"
pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

# Apply filter to dataset
dataset = pq.ParquetDataset("data.parquet")
filtered_table = dataset.to_table(filter=pyarrow_filter)
```

### Polars Filter Translation

```python
import polars as pl
from fsspeckit.sql.filters import sql2polars_filter

# Define schema
schema = pl.Schema({
    "id": pl.Int64,
    "value": pl.String,
    "timestamp": pl.Datetime
})

# Convert SQL to Polars filter expression
sql_filter = "value LIKE 'prefix%' AND timestamp >= '2023-01-01'"
polars_filter = sql2polars_filter(sql_filter, schema)

# Apply filter to DataFrame
df = pl.read_parquet("data.parquet")
filtered_df = df.filter(polars_filter)
```

## Storage Options Management

### Environment-Based Configuration

Load storage configurations directly from environment variables for production deployments.

```python
from fsspeckit.storage_options import storage_options_from_env

# Load AWS options from environment variables
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
aws_options = storage_options_from_env("s3")

# Load GCS options from environment variables
# GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_CLOUD_PROJECT
gcs_options = storage_options_from_env("gs")

# Create filesystems
aws_fs = filesystem("s3", storage_options=aws_options.to_dict())
gcs_fs = filesystem("gs", storage_options=gcs_options.to_dict())
```

### URI-Based Configuration

```python
from fsspeckit.storage_options import storage_options_from_uri

# Extract storage options from URI
s3_options = storage_options_from_uri("s3://bucket/path?region=us-east-1")
gs_options = storage_options_from_uri("gs://bucket/path")

# Use extracted options
fs = filesystem(s3_options.protocol, storage_options=s3_options.to_dict())
```

### Provider-Specific Configuration

#### AWS S3 Configuration

```python
from fsspeckit.storage_options import AwsStorageOptions

aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY",
    endpoint_url=None,  # Use default endpoint
    allow_http=False,  # Enforce HTTPS
    assume_role_arn=None  # Optional role assumption
)

fs = aws_options.to_filesystem()
```

#### Google Cloud Storage Configuration

```python
from fsspeckit.storage_options import GcsStorageOptions

gcs_options = GcsStorageOptions(
    project="your-gcp-project",
    token="path/to/service-account.json",  # or None for default credentials
    endpoint_override=None  # Use default endpoint
)

fs = gcs_options.to_filesystem()
```

#### Azure Blob Storage Configuration

```python
from fsspeckit.storage_options import AzureStorageOptions

azure_options = AzureStorageOptions(
    account_name="yourstorageaccount",
    account_key="YOUR_ACCOUNT_KEY",
    connection_string=None,  # Alternative to account_name/account_key
    sas_token=None  # Optional SAS token
)

fs = azure_options.to_filesystem()
```

#### GitHub Configuration

```python
from fsspeckit.storage_options import GitHubStorageOptions

github_options = GitHubStorageOptions(
    token="github_pat_YOUR_TOKEN",
    default_branch="main"
)

fs = github_options.to_filesystem()
```

## Common Utilities

### Parallel Processing

```python
from fsspeckit.common.misc import run_parallel

def process_file(file_path):
    # Process individual file
    return processed_data

file_list = ["file1.parquet", "file2.parquet", "file3.parquet"]

# Process files in parallel
results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=4,  # Optional: specify worker count
    progress=True   # Show progress bar
)
```

### Type Conversion

```python
import pyarrow as pa
from fsspeckit.common.types import convert_large_types_to_normal, dict_to_dataframe

# Convert large string types to normal strings
table = pa.Table.from_pydict({"large_strings": ["value1", "value2"]})
normalized_table = convert_large_types_to_normal(table)

# Convert dictionaries to DataFrames
data = {"id": [1, 2, 3], "value": ["A", "B", "C"]}
df = dict_to_dataframe(data, library="polars")
```

### Directory Synchronization

```python
from fsspeckit.core.filesystem import filesystem
from fsspeckit.common.misc import sync_dir

# Create filesystems for source and destination
src_fs = filesystem("s3", storage_options=src_options)
dst_fs = filesystem("az", storage_options=dst_options)

# Synchronize directories
sync_dir(
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="s3://source-bucket/data/",
    dst_path="az://dest-container/data/",
    progress=True
)
```

## Performance Optimization

### Efficient Dataset Reading

```python
import pyarrow as pa
from fsspeckit.core.filesystem import filesystem

# Create filesystem with optimal settings
fs = filesystem("s3", storage_options=storage_options)

# Read dataset with filtering and projection
dataset = pq.ParquetDataset(
    "s3://bucket/large-dataset/",
    filesystem=fs,
    filters=[("category", "=", "important")],  # Pushdown filtering
    read_columns=["id", "value", "timestamp"]  # Column projection
)

# Load only needed data
table = dataset.read()
```

### Batch Processing for Large Datasets

```python
from fsspeckit.datasets.pyarrow import process_dataset_in_batches

def process_batch(batch_table):
    """Process individual batch"""
    # Your processing logic here
    return processed_batch

# Process large dataset in batches
process_dataset_in_batches(
    dataset_path="s3://bucket/huge-dataset/",
    batch_size="100MB",
    process_func=process_batch,
    max_workers=4
)
```

## Error Handling and Validation

### Input Validation

```python
from fsspeckit.core.merge import validate_merge_inputs

# Validate inputs before merge operations
errors = validate_merge_inputs(
    input_paths=["s3://bucket/part1/", "s3://bucket/part2/"],
    schema=expected_schema
)

if errors:
    print(f"Validation errors: {errors}")
    # Handle validation errors
else:
    # Proceed with merge operation
    pass
```

### Robust File Operations

```python
from fsspeckit.core.filesystem import DirFileSystem
import tempfile

# Create safe filesystem that restricts operations to specific directory
safe_fs = DirFileSystem(
    fs=base_fs,
    path="/allowed/directory"  # Operations confined to this directory
)

# Use safe filesystem for operations
try:
    with safe_fs.open("/allowed/directory/file.txt", "r") as f:
        content = f.read()
except FileNotFoundError:
    print("File not found in allowed directory")
except PermissionError:
    print("Operation outside allowed directory")
```

## Migration Guide

### From fsspec-utils to fsspeckit

**Step 1: Update Imports**
```python
# Old imports
from fsspec_utils import run_parallel, storage_options_from_env

# New imports
from fsspeckit.common import run_parallel
from fsspeckit.storage_options import storage_options_from_env
```

**Step 2: Update Filesystem Creation**
```python
# Old method
import fsspec
fs = fsspec.filesystem("s3", **storage_options)

# New method
from fsspeckit.core.filesystem import filesystem
fs = filesystem("s3", storage_options=storage_options)
```

**Step 3: Update Dataset Operations**
```python
# Old method
from fsspec_utils import write_parquet_dataset

# New method
from fsspeckit.datasets import DuckDBParquetHandler
handler = DuckDBParquetHandler(storage_options=storage_options)
handler.write_parquet_dataset(data, path)
```

## Best Practices

1. **Use Domain-Specific Imports**: Import from `fsspeckit.datasets`, `fsspeckit.storage_options`, etc. instead of the utils façade for better type hints and clearer code.

2. **Environment-Based Configuration**: Use `storage_options_from_env()` in production to load credentials from environment variables.

3. **Protocol Inference**: Let the `filesystem()` function auto-detect protocols from URIs when possible.

4. **Type Safety**: Use the structured `StorageOptions` classes instead of raw dictionaries for better validation and IDE support.

5. **Error Handling**: Always wrap filesystem operations in try-except blocks to handle network errors and permission issues gracefully.

6. **Performance**: Use column projection and row filtering when working with large datasets to minimize data transfer.

7. **Testing**: Use `LocalStorageOptions` and `DirFileSystem` for creating safe, isolated test environments.