# Examples Guide

This page provides practical examples of using `fsspeckit` for real-world data processing tasks. All examples use the domain package structure and demonstrate the current implemented features.

## Quick Start Examples

### Basic Filesystem Creation

```python
from fsspeckit.core.filesystem import filesystem

# Auto-detect protocol from URI
fs = filesystem("s3://bucket/path")  # S3
fs = filesystem("gs://bucket/path")  # Google Cloud Storage
fs = filesystem("az://container/path")  # Azure Blob Storage

# Local filesystem with specific options
fs = filesystem("file", auto_mkdir=True)
```

### Storage Options Configuration

```python
from fsspeckit.storage_options import (
    AwsStorageOptions,
    GcsStorageOptions,
    AzureStorageOptions,
    storage_options_from_env
)

# Configure AWS from environment variables
aws_options = storage_options_from_env("s3")
fs = filesystem("s3", storage_options=aws_options.to_dict())

# Manual configuration
aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY"
)
fs = aws_options.to_filesystem()
```

## Dataset Operations Examples

### DuckDB Parquet Handler

```python
from fsspeckit.datasets import DuckDBParquetHandler
import polars as pl

# Initialize with storage options
storage_options = {"key": "value", "secret": "secret"}
handler = DuckDBParquetHandler(storage_options=storage_options)

# Create sample data
data = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "B", "A"],
    "value": [10.5, 20.3, 15.7, 25.1, 12.8],
    "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
})

# Write dataset
handler.write_parquet_dataset(data, "s3://bucket/my-dataset/")

# Execute SQL queries
result = handler.execute_sql("""
    SELECT
        category,
        COUNT(*) as count,
        AVG(value) as avg_value,
        SUM(value) as total_value
    FROM parquet_scan('s3://bucket/my-dataset/')
    GROUP BY category
    ORDER BY category
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
from fsspeckit.core.filesystem import filesystem

# Create filesystem
fs = filesystem("s3", storage_options=storage_options)

# Merge multiple parquet datasets
merge_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/source-datasets/",
    output_path="s3://bucket/merged-dataset/",
    merge_strategy="schema_evolution"
)

# Optimize dataset with Z-ordering
optimize_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/large-dataset/",
    z_order_columns=["category", "timestamp"],
    target_file_size="256MB"
)

# Compact small files
compact_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/fragmented-dataset/",
    target_file_size="128MB"
)
```

## SQL Filter Translation Examples

### Cross-Framept SQL Filtering

```python
import pyarrow as pa
import pyarrow.compute as pc
import polars as pl
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

# Define schema
schema = pa.schema([
    ("id", pa.int64()),
    ("value", pa.string()),
    ("category", pa.string()),
    ("timestamp", pa.timestamp("us")),
    ("amount", pa.float64())
])

# SQL filter examples
sql_filters = [
    "id > 100 AND category IN ('A', 'B', 'C')",
    "value LIKE 'prefix%' AND amount > 1000.0",
    "timestamp >= '2023-01-01' AND timestamp <= '2023-12-31'",
    "category = 'IMPORTANT' AND (amount BETWEEN 100 AND 1000)"
]

# Convert to PyArrow filters
for sql_filter in sql_filters:
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)
    print(f"SQL: {sql_filter}")
    print(f"PyArrow: {pyarrow_filter}")
    print()

# Convert to Polars filters
polars_schema = pl.Schema({
    "id": pl.Int64,
    "value": pl.String,
    "category": pl.String,
    "timestamp": pl.Datetime,
    "amount": pl.Float64
})

for sql_filter in sql_filters:
    polars_filter = sql2polars_filter(sql_filter, polars_schema)
    print(f"SQL: {sql_filter}")
    print(f"Polars: {polars_filter}")
    print()
```

### Practical Dataset Filtering

```python
import pandas as pd
import pyarrow.parquet as pq
from fsspeckit.sql.filters import sql2pyarrow_filter

# Load a dataset
dataset = pq.ParquetDataset("s3://bucket/large-dataset/")
table = dataset.to_table()

# Define your schema
schema = table.schema

# Use SQL to create filters
sql_conditions = [
    "category = 'HIGH_PRIORITY'",
    "amount > 50000",
    "timestamp >= '2023-06-01'",
    "status IN ('ACTIVE', 'PENDING')"
]

# Apply filters incrementally
filtered_data = table
for condition in sql_conditions:
    filter_expr = sql2pyarrow_filter(condition, schema)
    filtered_data = filtered_data.filter(filter_expr)

print(f"Original rows: {len(table)}")
print(f"Filtered rows: {len(filtered_data)}")
```

## Storage Options Examples

### Environment-Based Configuration

```python
from fsspeckit.storage_options import storage_options_from_env
from fsspeckit.core.filesystem import filesystem
import os

# Set environment variables (in production, these would be set externally)
os.environ["AWS_ACCESS_KEY_ID"] = "your_access_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "your_secret_key"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# Load configuration from environment
aws_options = storage_options_from_env("s3")
print(f"Loaded AWS region: {aws_options.region}")

# Create filesystem
fs = filesystem("s3", storage_options=aws_options.to_dict())

# Use the filesystem
files = fs.ls("s3://your-bucket/")
print(files)
```

### Multi-Cloud Configuration

```python
from fsspeckit.storage_options import (
    AwsStorageOptions,
    GcsStorageOptions,
    AzureStorageOptions
)

# AWS configuration
aws_config = AwsStorageOptions(
    region="us-west-2",
    access_key_id="aws_key",
    secret_access_key="aws_secret"
)

# Google Cloud configuration
gcs_config = GcsStorageOptions(
    project="gcp-project",
    token="path/to/service-account.json"
)

# Azure configuration
azure_config = AzureStorageOptions(
    account_name="storageaccount",
    account_key="azure_key"
)

# Create filesystems for each provider
aws_fs = aws_config.to_filesystem()
gcs_fs = gcs_config.to_filesystem()
azure_fs = azure_config.to_filesystem()

# Use them interchangeably
for provider, fs in [("AWS", aws_fs), ("GCS", gcs_fs), ("Azure", azure_fs)]:
    print(f"{provider} filesystem: {type(fs).__name__}")
```

### URI-Based Configuration

```python
from fsspeckit.storage_options import storage_options_from_uri
from fsspeckit.core.filesystem import filesystem

# Extract storage options from URIs
uris = [
    "s3://bucket/path?region=us-east-1&endpoint_url=https://s3.amazonaws.com",
    "gs://bucket/path?project=my-gcp-project",
    "az://container/path?account_name=mystorageaccount"
]

for uri in uris:
    options = storage_options_from_uri(uri)
    fs = filesystem(options.protocol, storage_options=options.to_dict())
    print(f"URI: {uri}")
    print(f"Protocol: {options.protocol}")
    print(f"Filesystem: {type(fs).__name__}")
    print()
```

## Common Utilities Examples

### Parallel Processing

```python
from fsspeckit.common.misc import run_parallel
import time

def process_file(file_path):
    """Simulate processing a file"""
    print(f"Processing {file_path}...")
    time.sleep(0.1)  # Simulate work
    return f"Processed {file_path}"

# List of files to process
file_list = [f"file_{i}.parquet" for i in range(10)]

# Process files in parallel
results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=4,
    progress=True
)

print("\nResults:")
for result in results:
    print(result)
```

### Type Conversion and Utilities

```python
import pyarrow as pa
import polars as pl
from fsspeckit.common.types import (
    convert_large_types_to_normal,
    dict_to_dataframe,
    to_pyarrow_table
)

# Convert large string types to normal
large_string_table = pa.Table.from_pydict({
    "text": pa.array(["value1", "value2", "value3"], type=pa.large_string())
})

print(f"Original schema: {large_string_table.schema}")
normal_table = convert_large_types_to_normal(large_string_table)
print(f"Normal schema: {normal_table.schema}")

# Convert dictionaries to DataFrames
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [95.5, 87.3, 92.1]
}

# Convert to Polars DataFrame
pl_df = dict_to_dataframe(data, library="polars")
print(f"Polars DataFrame:\n{pl_df}")

# Convert to Pandas DataFrame
pd_df = dict_to_dataframe(data, library="pandas")
print(f"Pandas DataFrame:\n{pd_df}")

# Convert to PyArrow Table
arrow_table = to_pyarrow_table(data)
print(f"PyArrow Table:\n{arrow_table}")
```

### Directory Operations

```python
from fsspeckit.core.filesystem import filesystem, DirFileSystem
from fsspeckit.common.misc import sync_dir, extract_partitions
import tempfile
import shutil

# Create temporary directories for testing
src_dir = tempfile.mkdtemp()
dst_dir = tempfile.mkdtemp()

# Create base filesystems
src_fs = filesystem("file")
dst_fs = filesystem("file")

# Create safe filesystems that restrict operations
safe_src = DirFileSystem(fs=src_fs, path=src_dir)
safe_dst = DirFileSystem(fs=dst_fs, path=dst_dir)

# Create some test files
with safe_src.open("test1.txt", "w") as f:
    f.write("Content 1")
with safe_src.open("test2.txt", "w") as f:
    f.write("Content 2")

# List files
print("Source files:", safe_src.ls("/"))

# Synchronize directories
sync_dir(
    src_fs=safe_src,
    dst_fs=safe_dst,
    src_path="/",
    dst_path="/",
    progress=True
)

# List destination files
print("Destination files:", safe_dst.ls("/"))

# Extract partition information
paths = [
    "data/year=2023/month=01/day=15/file.parquet",
    "data/year=2023/month=01/day=16/file.parquet",
    "data/year=2023/month=02/day=01/file.parquet"
]

for path in paths:
    partitions = extract_partitions(path)
    print(f"Path: {path}")
    print(f"Partitions: {partitions}")

# Cleanup
shutil.rmtree(src_dir)
shutil.rmtree(dst_dir)
```

## Error Handling Examples

### Robust File Operations

```python
from fsspeckit.core.filesystem import filesystem, DirFileSystem
from fsspeckit.storage_options import AwsStorageOptions
import os

def safe_file_operations():
    """Demonstrate safe file operations with error handling"""

    try:
        # Create filesystem with error handling
        storage_options = AwsStorageOptions(
            region="us-east-1",
            access_key_id="invalid_key",  # This will cause authentication error
            secret_access_key="invalid_secret"
        )

        fs = storage_options.to_filesystem()

        # Try to list files (will fail)
        files = fs.ls("s3://bucket/")

    except Exception as e:
        print(f"Expected authentication error: {e}")

        # Fall back to local filesystem
        fs = filesystem("file")
        print("Fell back to local filesystem")

    try:
        # Create a safe filesystem that restricts operations
        safe_dir = "/tmp/fsspeckit_safe"
        os.makedirs(safe_dir, exist_ok=True)

        safe_fs = DirFileSystem(fs=fs, path=safe_dir)

        # This works
        with safe_fs.open("test.txt", "w") as f:
            f.write("Safe content")

        # This should fail (attempting to access outside safe directory)
        try:
            safe_fs.open("../../../etc/passwd", "r")
        except (ValueError, PermissionError) as e:
            print(f"Security check worked: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")

safe_file_operations()
```

## Performance Optimization Examples

### Efficient Large Dataset Processing

```python
import pyarrow as pa
import pyarrow.parquet as pq
from fsspeckit.core.filesystem import filesystem
from fsspeckit.datasets.pyarrow import process_dataset_in_batches

def process_batch_efficiently():
    """Show how to process large datasets efficiently"""

    # Create filesystem with optimal settings
    fs = filesystem("s3", storage_options=storage_options)

    # Define batch processing function
    def process_batch(batch_table):
        """Process individual batch efficiently"""
        # Example: calculate statistics
        total_rows = len(batch_table)
        if "amount" in batch_table.column_names:
            total_amount = batch_table.column("amount").to_pandas().sum()
            return {"rows": total_rows, "total_amount": total_amount}
        return {"rows": total_rows}

    # Process dataset in batches
    dataset_path = "s3://bucket/large-dataset/"

    print("Processing large dataset in batches...")
    for i, result in enumerate(process_dataset_in_batches(
        dataset_path=dataset_path,
        batch_size="100MB",
        process_func=process_batch,
        max_workers=4
    )):
        print(f"Batch {i+1}: {result}")

# Uncomment to run (requires actual dataset)
# process_batch_efficiently()
```

## Running Examples

### Prerequisites

```bash
# Install with all dependencies
pip install "fsspeckit[aws,gcp,azure]"

# For SQL filter translation
pip install sqlglot

# For dataset operations
pip install duckdb pyarrow polars
```

### Environment Setup

```bash
# Set AWS credentials (replace with your actual values)
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_DEFAULT_REGION="us-east-1"

# Set GCS credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GOOGLE_CLOUD_PROJECT="your-gcp-project"

# Set Azure credentials
export AZURE_STORAGE_ACCOUNT="your_storage_account"
export AZURE_STORAGE_KEY="your_storage_key"
```

### Testing Examples

```python
# Test individual components
from fsspeckit.storage_options import storage_options_from_env
from fsspeckit.core.filesystem import filesystem

# Test filesystem creation
try:
    fs = filesystem("file")  # Local filesystem should always work
    print("✓ Local filesystem created successfully")
except Exception as e:
    print(f"✗ Local filesystem failed: {e}")

# Test storage options loading
try:
    options = storage_options_from_env("s3")  # May fail if no env vars set
    print("✓ AWS storage options loaded")
except Exception as e:
    print(f"⚠ AWS storage options not available: {e}")

# Test SQL filter translation
from fsspeckit.sql.filters import sql2pyarrow_filter
import pyarrow as pa

try:
    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    filter_expr = sql2pyarrow_filter("id > 100", schema)
    print("✓ SQL filter translation working")
except Exception as e:
    print(f"✗ SQL filter translation failed: {e}")
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Install with `pip install "fsspeckit[aws,gcp,azure]"`
2. **Authentication**: Set environment variables for cloud providers
3. **Dependencies**: Install optional dependencies: `pip install duckdb pyarrow polars sqlglot`
4. **Network**: Check connectivity to cloud services
5. **Permissions**: Verify IAM roles and access policies

### Getting Help

- Check the [API Reference](api/fsspeckit.core.filesystem.md) for detailed method documentation
- Review the [Advanced Usage](advanced.md) guide for complex scenarios
- Examine the source code in the `src/fsspeckit/` directory for implementation details