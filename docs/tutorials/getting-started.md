# Getting Started

This tutorial will walk you through your first steps with `fsspeckit`. You'll learn how to install the library, work with local and cloud storage, and perform basic dataset operations.

## Prerequisites

- Python 3.11 or higher
- Basic familiarity with Python and data concepts

## Installation

First, install `fsspeckit` with the dependencies you need:

```bash
# Basic installation
pip install fsspeckit

# With cloud storage support
pip install "fsspeckit[aws,gcp,azure]"

# With all optional dependencies for data processing
pip install "fsspeckit[aws,gcp,azure]" duckdb pyarrow polars sqlglot
```

For detailed installation instructions, see the [Installation Guide](../installation.md).

## Your First Local Filesystem

Let's start by creating a local filesystem and performing basic operations:

```python
from fsspeckit import filesystem
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

## Working with Cloud Storage

Now let's configure cloud storage. We'll use environment variables for credentials:

```python
from fsspeckit import filesystem
from fsspeckit.storage_options import storage_options_from_env

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

You can also configure storage manually:

```python
from fsspeckit import AwsStorageOptions

# Configure AWS S3
aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY"
)

# Create filesystem
aws_fs = aws_options.to_filesystem()
```

## Protocol Inference

The `filesystem()` function can automatically detect protocols from URIs:

```python
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

## Your First Dataset Operation

Let's perform basic dataset operations using both DuckDB and PyArrow handlers:

### DuckDB Approach

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, DuckDBDatasetHandler
import polars as pl

# Initialize I/O handler
io = DuckDBDatasetIO()

# Create sample data
data = pl.DataFrame({
    "id": [1, 2, 3, 4],
    "category": ["A", "B", "A", "B"],
    "value": [10.5, 20.3, 15.7, 25.1]
})

# Write dataset
result = io.write_dataset(data, "s3://bucket/my-dataset/", mode="append")
print(f"Wrote {result.total_rows} rows using {result.backend} backend")

# Use handler wrapper for SQL operations
handler = DuckDBDatasetHandler()
query_result = handler.execute_sql("""
    SELECT category, COUNT(*) as count, AVG(value) as avg_value
    FROM parquet_scan('s3://bucket/my-dataset/')
    GROUP BY category
""")

print(query_result)
```

### PyArrow Approach

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO, PyarrowDatasetHandler
import pyarrow as pa

# Initialize I/O handler
io = PyarrowDatasetIO()

# Create sample data
data = pa.table({
    "id": [1, 2, 3, 4],
    "category": ["A", "B", "A", "B"],
    "value": [10.5, 20.3, 15.7, 25.1]
})

# Write dataset
result = io.write_dataset(data, "s3://bucket/my-dataset/", mode="append")
print(f"Wrote {result.total_rows} rows across {len(result.files)} files")
print(f"Backend: {result.backend}, Mode: {result.mode}")

# Update existing records using merge
updates = pa.table({
    "id": [2, 4],
    "category": ["B", "B"],
    "value": [99.9, 88.8]
})
merge_result = io.merge(
    data=updates,
    path="s3://bucket/my-dataset/",
    strategy="upsert",
    key_columns=["id"]
)
print(f"Inserted: {merge_result.inserted}, Updated: {merge_result.updated}")

# Handler wrapper approach for reading and maintenance
with PyarrowDatasetHandler() as handler:
    # Read dataset with column selection
    table = handler.read_parquet("s3://bucket/my-dataset/", columns=["category", "value"])
    print(f"Read {table.num_rows} rows with columns: {table.column_names}")
    
    # Compact dataset
    stats = handler.compact_parquet_dataset("s3://bucket/my-dataset/", target_mb_per_file=64)
    print(f"Compaction stats: {stats}")
```

## Domain Package Structure

`fsspeckit` is organized into domain-specific packages. Import from the appropriate package for your use case:

```python
# Filesystem creation and core functionality
from fsspeckit import filesystem

# Storage configuration
from fsspeckit import AwsStorageOptions, GcsStorageOptions
from fsspeckit.storage_options import storage_options_from_env

# Dataset operations - I/O handlers
from fsspeckit.datasets import DuckDBDatasetIO, DuckDBDatasetHandler
from fsspeckit.datasets import PyarrowDatasetIO, PyarrowDatasetHandler

# SQL filter translation
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

# Common utilities
from fsspeckit.common.misc import run_parallel
from fsspeckit.common.types import dict_to_dataframe
```

## Next Steps

Congratulations! You've completed the basic fsspeckit tutorial. Here are some recommended next steps:

### Explore More Features

- **How-to Guides**: Dive into specific tasks with our [How-to Guides](../how-to/index.md)
- **API Reference**: Browse the [API Reference](../reference/api-guide.md) for detailed documentation
- **Architecture & Concepts**: Understand the design principles in [Architecture & Concepts](../explanation/index.md)

### Common Use Cases

1. **Cloud Data Processing**: Use `storage_options_from_env()` for production deployments
2. **Dataset Operations**: Use `DuckDBDatasetIO` for SQL-based operations or `PyarrowDatasetIO` for PyArrow operations
3. **Merge Operations**: Use `merge()` for incremental updates with `insert`, `update`, or `upsert` strategies
4. **SQL Filtering**: Use `sql2pyarrow_filter()` and `sql2polars_filter()` for cross-framework compatibility
5. **Safe Operations**: Use `DirFileSystem` for security-critical applications
6. **Performance**: Use `run_parallel()` for concurrent file processing

### Production Tips

1. **Use Domain Packages**: Import from `fsspeckit.datasets`, `fsspeckit.storage_options`, etc. instead of utils
2. **Environment Configuration**: Load credentials from environment variables in production
3. **Error Handling**: Always wrap remote filesystem operations in try-except blocks
4. **Type Safety**: Use structured `StorageOptions` classes instead of raw dictionaries
5. **Testing**: Use `LocalStorageOptions` and `DirFileSystem` for isolated test environments

For more detailed information, explore the other sections of the documentation.