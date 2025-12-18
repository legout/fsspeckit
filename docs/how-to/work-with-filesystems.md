# Work with Filesystems

This guide covers how to create and use filesystems with fsspeckit, including path safety, caching, and advanced filesystem operations.

## Creating Filesystems

### Basic Local Filesystem

```python
from fsspeckit import filesystem

# Create local filesystem
fs = filesystem("file")

# Create with auto-mkdir
fs = filesystem("file", auto_mkdir=True)
```

### Cloud Filesystems

```python
# Auto-detect protocol from URI
s3_fs = filesystem("s3://bucket/path")      # S3
gcs_fs = filesystem("gs://bucket/path")      # Google Cloud Storage
az_fs = filesystem("az://container/path")    # Azure Blob Storage
github_fs = filesystem("github://owner/repo") # GitHub

# Manual protocol specification
s3_fs = filesystem("s3", storage_options={"region": "us-east-1"})
```

### With Storage Options

```python
from fsspeckit import AwsStorageOptions

aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY"
)

fs = filesystem("s3", storage_options=aws_options.to_dict())
```

## Path Safety with DirFileSystem

fsspeckit wraps filesystems in `DirFileSystem` by default for enhanced security.

### Basic Path Safety

```python
from fsspeckit import filesystem
from fsspeckit.core.filesystem import DirFileSystem

# Default behavior: wrapped in DirFileSystem
fs = filesystem("/data", dirfs=True)

# All operations confined to /data directory
fs.ls("/subdir")  # Works
fs.open("/data/file.txt", "r")  # Works

# Attempting to escape fails
try:
    fs.open("../../../etc/passwd", "r")
except (ValueError, PermissionError) as e:
    print(f"Security check worked: {e}")
```

### Manual DirFileSystem Creation

```python
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

### Hierarchical Filesystems

```python
# Create parent filesystem
parent_fs = filesystem("/datasets", dirfs=True)

# Create child filesystem with parent as base
child_fs = filesystem("/datasets/project1", dirfs=True, base_fs=parent_fs)

# Files are accessible only within the base directory
# Relative paths are resolved relative to parent filesystem's base directory
```

## Caching

Caching improves performance for remote filesystems by storing frequently accessed data locally.

### Enable Caching

```python
# Enable caching with default settings
fs = filesystem("s3://bucket/", cached=True)

# Enable with custom cache directory
fs = filesystem("s3://bucket/", cached=True, cache_storage="/tmp/cache")

# Enable with verbose logging
fs = filesystem("s3://bucket/", cached=True, verbose=True)
```

### Cache Management

```python
# Clear all caches
fs.clear_cache()

# Sync cache (ensure data is written)
fs.sync_cache()

# Check cache size (for MonitoredSimpleCacheFileSystem)
if hasattr(fs, 'cache_size'):
    size = fs.cache_size
    print(f"Cache size: {size}")
```

### Cache Best Practices

```python
# Good: Enable caching for remote filesystems
remote_fs = filesystem("s3://data/", cached=True)

# Not necessary: Local filesystems don't need caching
local_fs = filesystem("file")  # cached=False by default

# Use with large files
fs = filesystem("s3://large-datasets/", cached=True, cache_storage="/ssd/cache")
```

## Basic File Operations

### Reading Files

```python
# Read text file
with fs.open("data.txt", "r") as f:
    content = f.read()

# Read binary file
with fs.open("data.bin", "rb") as f:
    data = f.read()

# Read first few bytes
with fs.open("large_file.txt", "r") as f:
    header = f.read(1024)
```

### Writing Files

```python
# Write text file
with fs.open("output.txt", "w") as f:
    f.write("Hello, World!")

# Write binary file
with fs.open("output.bin", "wb") as f:
    f.write(binary_data)

# Append to file
with fs.open("log.txt", "a") as f:
    f.write("New log entry\n")
```

### Directory Operations

```python
# List files
files = fs.ls("/path/to/directory")
print(f"Files: {files}")

# List with details
files = fs.ls("/path/", detail=True)
for file_info in files:
    print(f"{file_info['name']}: {file_info['size']} bytes")

# Create directory
fs.makedirs("/new/directory", exist_ok=True)

# Check if path exists
exists = fs.exists("/path/to/file")
is_dir = fs.isdir("/path/to/directory")
is_file = fs.isfile("/path/to/file")

# Get file info
info = fs.info("/path/to/file")
print(f"Size: {info['size']}, Modified: {info['modified']}")
```

## Extended I/O Operations

fsspeckit adds rich I/O methods to all fsspec filesystems.

### JSON Operations

```python
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

### CSV Operations

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

### Parquet Operations

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

### Universal Reader

```python
# Auto-detect format from file extension
df = fs.read_files("data/mixed/*", format="auto")

# Explicit format
df = fs.read_files("data/*.csv", format="csv")

# Control result type
df_polars = fs.read_files("data/*.parquet", as_dataframe=True)
table_arrow = fs.read_files("data/*.parquet", as_dataframe=False)
```

## Writing Operations

### DataFrame Writing

```python
import polars as pl

# Create DataFrame
df = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})

# Write Parquet
fs.write_parquet(df, "output.parquet")

# Write CSV
fs.write_csv(df, "output.csv")

# Write JSON
fs.write_json(df, "output.json")
```

### Dataset Writing

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
```

## Error Handling

Always wrap filesystem operations in try-except blocks:

```python
from fsspeckit import AwsStorageOptions

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

## Performance Tips

### Use Caching for Remote Storage

```python
# Good: Enable caching for remote filesystems
remote_fs = filesystem("s3://data/", cached=True)

# Configure cache size for large datasets
fs = filesystem("s3://large-data/", cached=True, cache_storage="/fast/ssd/cache")
```

### Batch Operations

```python
# Good: Batch file operations
for batch in fs.read_json("data/*.json", batch_size=100):
    process_batch(batch)

# Good: Use threading for multiple files
df = fs.read_csv("data/*.csv", use_threads=True, num_threads=4)
```

### Column Projection

```python
# Good: Read only needed columns
df = fs.read_parquet("large_dataset.parquet", columns=["id", "name"])
```

## Best Practices

1. **Use DirFileSystem**: Always use path-safe filesystems for security
2. **Enable Caching**: Use caching for remote filesystems
3. **Error Handling**: Wrap operations in try-except blocks
4. **Batch Processing**: Use batch operations for large datasets
5. **Environment Configuration**: Load credentials from environment variables

For more information on dataset operations, see [Read and Write Datasets](read-and-write-datasets.md).