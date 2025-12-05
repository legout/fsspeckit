# Quickstart

This guide will help you get started with `fsspeckit` by demonstrating the key features for working with various storage backends and data processing frameworks.

## Installation

Install `fsspeckit` with the dependencies you need:

```bash
# Basic installation
pip install fsspeckit

# With cloud storage support
pip install "fsspeckit[aws,gcp,azure]"

# With all optional dependencies for data processing
pip install "fsspeckit[aws,gcp,azure]" duckdb pyarrow polars sqlglot
```

## Domain Package Structure

`fsspeckit` is organized into domain-specific packages. Import from the appropriate package for your use case:

```python
# Filesystem creation and core functionality
from fsspeckit.core.filesystem import filesystem

# Storage configuration
from fsspeckit.storage_options import AwsStorageOptions, storage_options_from_env

# Dataset operations
from fsspeckit.datasets import DuckDBParquetHandler

# SQL filter translation
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

# Common utilities
from fsspeckit.common.misc import run_parallel
from fsspeckit.common.types import dict_to_dataframe

# Backwards compatibility (legacy)
from fsspeckit.utils import DuckDBParquetHandler  # Still works
```

## Basic Usage: Local Filesystem

```python
from fsspeckit.core.filesystem import filesystem
import os

# Create a local filesystem
# Note: filesystem() wraps the filesystem in DirFileSystem by default (dirfs=True)
# for path safety, confining all operations to the specified directory
fs = filesystem("file")

# Define a directory path
local_dir = "./my_data/"
os.makedirs(local_dir, exist_ok=True)

# Create and write a file
with fs.open(f"{local_dir}example.txt", "w") as f:
    f.write("Hello, fsspeckit!")

# Read the file
with fs.open(f"{local_dir}example.txt", "r") as f:
    content = f.read()
print(f"Content: {content}")

# List files in directory
files = fs.ls(local_dir)
print(f"Files: {files}")
```

**Path Safety:** The `filesystem()` function wraps filesystems in `DirFileSystem` by default (`dirfs=True`), which confines all operations to the specified directory path. This prevents accidental access to paths outside the intended directory.

## Storage Options Configuration

### Environment-Based Configuration

```python
from fsspeckit.storage_options import storage_options_from_env
from fsspeckit.core.filesystem import filesystem

# Set environment variables (or set them in your environment)
import os
os.environ["AWS_ACCESS_KEY_ID"] = "your_access_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "your_secret_key"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# Load AWS options from environment
aws_options = storage_options_from_env("s3")
fs = filesystem("s3", storage_options=aws_options.to_dict())

print(f"Created S3 filesystem in region: {aws_options.region}")
```

### Manual Configuration

```python
from fsspeckit.storage_options import AwsStorageOptions, GcsStorageOptions

# Configure AWS S3
aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY"
)

# Configure Google Cloud Storage
gcs_options = GcsStorageOptions(
    project="your-gcp-project",
    token="path/to/service-account.json"
)

# Create filesystems
aws_fs = aws_options.to_filesystem()
gcs_fs = gcs_options.to_filesystem()
```

## Protocol Inference

The `filesystem()` function can automatically detect protocols from URIs:

```python
from fsspeckit.core.filesystem import filesystem

# Auto-detect protocols
s3_fs = filesystem("s3://bucket/path")      # S3
gcs_fs = filesystem("gs://bucket/path")      # Google Cloud Storage
az_fs = filesystem("az://container/path")    # Azure Blob Storage
github_fs = filesystem("github://owner/repo") # GitHub

# All work with the same interface
for name, fs in [("S3", s3_fs), ("GCS", gcs_fs)]:
    try:
        files = fs.ls("/")
        print(f"{name} files: {len(files)}")
    except Exception as e:
        print(f"{name} error: {e}")
```

## Dataset Operations

### DuckDB Parquet Handler

```python
from fsspeckit.datasets import DuckDBParquetHandler
import polars as pl

# Initialize handler with storage options
storage_options = {"key": "value", "secret": "secret"}
handler = DuckDBParquetHandler(storage_options=storage_options)

# Create sample data
data = pl.DataFrame({
    "id": [1, 2, 3, 4],
    "category": ["A", "B", "A", "B"],
    "value": [10.5, 20.3, 15.7, 25.1]
})

# Write dataset
handler.write_parquet_dataset(data, "s3://bucket/my-dataset/")

# Execute SQL queries
result = handler.execute_sql("""
    SELECT category, COUNT(*) as count, AVG(value) as avg_value
    FROM parquet_scan('s3://bucket/my-dataset/')
    GROUP BY category
""")

print(result)
```

### PyArrow Dataset Operations

```python
from fsspeckit.datasets.pyarrow import optimize_parquet_dataset_pyarrow

# Optimize large datasets
optimize_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/large-dataset/",
    z_order_columns=["category", "timestamp"],
    target_file_size="256MB"
)
```

## SQL Filter Translation

Convert SQL WHERE clauses to framework-specific filter expressions:

```python
import pyarrow as pa
import polars as pl
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

# Define schemas
pyarrow_schema = pa.schema([
    ("id", pa.int64()),
    ("category", pa.string()),
    ("value", pa.float64())
])

polars_schema = pl.Schema({
    "id": pl.Int64,
    "category": pl.String,
    "value": pl.Float64
})

# SQL filter
sql_filter = "category IN ('A', 'B') AND value > 15.0"

# Convert to PyArrow filter
pyarrow_filter = sql2pyarrow_filter(sql_filter, pyarrow_schema)
print(f"PyArrow filter: {pyarrow_filter}")

# Convert to Polars filter
polars_filter = sql2polars_filter(sql_filter, polars_schema)
print(f"Polars filter: {polars_filter}")

# Apply filters to data
import pyarrow.parquet as pq
dataset = pq.ParquetDataset("data.parquet")
filtered_table = dataset.to_table(filter=pyarrow_filter)

df = pl.read_parquet("data.parquet")
filtered_df = df.filter(polars_filter)
```

## Common Utilities

### Parallel Processing

```python
from fsspeckit.common.misc import run_parallel

def process_file(file_path):
    # Process individual file
    return f"Processed {file_path}"

# Process files in parallel
file_list = ["file1.parquet", "file2.parquet", "file3.parquet"]
results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=4,
    progress=True
)

print(results)
```

### Type Conversion

```python
from fsspeckit.common.types import dict_to_dataframe, convert_large_types_to_normal
import pyarrow as pa

# Convert dictionaries to DataFrames
data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}

# To Polars DataFrame
pl_df = dict_to_dataframe(data, library="polars")
print(f"Polars DataFrame:\n{pl_df}")

# To Pandas DataFrame
pd_df = dict_to_dataframe(data, library="pandas")
print(f"Pandas DataFrame:\n{pd_df}")

# Convert large string types to normal strings
large_table = pa.Table.from_pydict({
    "text": pa.array(["value1", "value2"], type=pa.large_string())
})
normal_table = convert_large_types_to_normal(large_table)
print(f"Original schema: {large_table.schema}")
print(f"Normal schema: {normal_table.schema}")
```

## Safe File Operations

### Directory-Constrained Filesystem

```python
from fsspeckit.core.filesystem import DirFileSystem, filesystem

# Create base filesystem
base_fs = filesystem("file")

# Create safe filesystem confined to specific directory
safe_fs = DirFileSystem(fs=base_fs, path="/allowed/directory")

try:
    # This works - within allowed directory
    with safe_fs.open("/allowed/directory/file.txt", "w") as f:
        f.write("Safe content")

    # This fails - outside allowed directory
    safe_fs.open("/etc/passwd", "r")  # Raises ValueError/PermissionError

except (ValueError, PermissionError) as e:
    print(f"Security check worked: {e}")
```

## Error Handling

Always wrap filesystem operations in try-except blocks:

```python
from fsspeckit.core.filesystem import filesystem
from fsspeckit.storage_options import AwsStorageOptions

try:
    # Try to create filesystem
    storage_options = AwsStorageOptions(
        region="us-east-1",
        access_key_id="invalid_key",
        secret_access_key="invalid_secret"
    )
    fs = storage_options.to_filesystem()

    # Try to use it
    files = fs.ls("s3://bucket/")

except Exception as e:
    print(f"Operation failed: {e}")

    # Fall back to local filesystem
    fs = filesystem("file")
    print("Fell back to local filesystem")
```

## Next Steps

### Explore More Features

- **Advanced Usage**: Read the [Advanced Usage](advanced.md) guide for complex scenarios
- **Examples**: Check the [Examples](examples.md) guide for practical code samples
- **API Reference**: Browse the [API Reference](api/index.md) for detailed documentation

### Common Use Cases

1. **Cloud Data Processing**: Use `storage_options_from_env()` for production deployments
2. **Dataset Operations**: Use `DuckDBParquetHandler` for large-scale parquet operations
3. **SQL Filtering**: Use `sql2pyarrow_filter()` and `sql2polars_filter()` for cross-framework compatibility
4. **Safe Operations**: Use `DirFileSystem` for security-critical applications
5. **Performance**: Use `run_parallel()` for concurrent file processing

### Production Tips

1. **Use Domain Packages**: Import from `fsspeckit.datasets`, `fsspeckit.storage_options`, etc. instead of utils
2. **Environment Configuration**: Load credentials from environment variables in production
3. **Error Handling**: Always wrap remote filesystem operations in try-except blocks
4. **Type Safety**: Use structured `StorageOptions` classes instead of raw dictionaries
5. **Testing**: Use `LocalStorageOptions` and `DirFileSystem` for isolated test environments

For more detailed information, explore the other sections of the documentation.