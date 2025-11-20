# Advanced Usage

`fsspeckit` extends the capabilities of `fsspec` to provide a more robust and feature-rich experience for handling diverse file systems and data formats. This section delves into advanced features, configurations, and performance tips to help you get the most out of the library.

## Domain Package Organization

The refactored `fsspeckit` architecture organizes functionality into domain-specific packages, making it easier to discover and use the right tools for each task. This design improves maintainability and provides clearer separation of concerns.

### Choosing the Right Package

For optimal experience, import from the appropriate domain package:

- **Dataset Operations**: Use `fsspeckit.datasets` for DuckDB and PyArrow dataset operations
- **SQL Filtering**: Use `fsspeckit.sql` for SQL-to-filter translation
- **General Utilities**: Use `fsspeckit.common` for logging, parallel processing, type conversion
- **Backwards Compatibility**: `fsspeckit.utils` continues to work for existing code

### Import Examples by Use Case

```python
# Dataset operations (recommended)
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow

# SQL filtering
from fsspeckit.sql.filters import sql2pyarrow_filter

# Common utilities
from fsspeckit.common.logging import setup_logging
from fsspeckit.common.misc import run_parallel

# Backwards compatible (legacy)
from fsspeckit.utils import DuckDBParquetHandler  # Still works
```

## Unified Filesystem Creation with `filesystem`

The `fsspeckit.core.filesystem` function offers a centralized and enhanced way to instantiate `fsspec` filesystem objects. It supports:

-   **Intelligent Caching**: Automatically wraps filesystems with `MonitoredSimpleCacheFileSystem` for improved performance and verbose logging of cache operations.
-   **Structured Storage Options**: Integrates seamlessly with `fsspeckit.storage_options` classes, allowing for type-safe and organized configuration of cloud and Git-based storage.
-   **Protocol Inference**: Can infer the filesystem protocol directly from a URI or path, reducing boilerplate.

**Example: Cached S3 Filesystem with Structured Options**

```python
from fsspeckit import filesystem  # Top-level import for convenience
from fsspeckit.storage_options import AwsStorageOptions

# Configure S3 options using the structured class
s3_opts = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY"
)

# Create a cached S3 filesystem using the 'filesystem' helper
fs = filesystem(
    "s3",
    storage_options=s3_opts,
    cached=True,
    cache_storage="/tmp/s3_cache", # Optional: specify cache directory
    verbose=True # Enable verbose cache logging
)

# Use the filesystem as usual
print(fs.ls("s3://your-bucket/"))
```

### Detailed Caching for Improved Performance

`fsspeckit` provides an enhanced caching mechanism that improves performance for repeated file operations, especially useful for remote filesystems.

This example demonstrates how caching improves read performance. The first read populates the cache, while subsequent reads retrieve data directly from the cache, significantly reducing access time. It also shows that data can still be retrieved from the cache even if the original source becomes unavailable.

**Caching in fsspeckit** is an enhanced mechanism that improves performance for repeated file operations, especially useful for remote filesystems where network latency can significantly impact performance.

The `filesystem()` function provides several parameters for configuring caching:

- `cached`: When set to `True`, enables caching for all read operations
- `cache_storage`: Specifies the directory where cached files will be stored
- `verbose`: When set to `True`, provides detailed logging about cache operations

**Step-by-step walkthrough:**

1.  **First read (populating cache)**: When reading a file for the first time, the data is retrieved from the source (disk, network, etc.) and stored in the cache directory. This takes longer than subsequent reads because it involves both reading from the source and writing to the cache.

2.  **Second read (using cache)**: When the same file is read again, the data is retrieved directly from the cache instead of the source. This is significantly faster because it avoids network latency or disk I/O.

3.  **Demonstrating cache effectiveness**: Even after the original file is removed, the cached version can still be accessed. This demonstrates that the cache acts as a persistent copy of the data, independent of the source file.

4.  **Performance comparison**: The timing results clearly show the performance benefits of caching, with subsequent reads being orders of magnitude faster than the initial read.

This caching mechanism is particularly valuable when working with:

- Remote filesystems (S3, GCS, Azure) where network latency is a bottleneck
- Frequently accessed files that don't change often
- Applications that read the same data multiple times
- Environments with unreliable network connections

#### Setup and First Read (Populating Cache)

In this step, we create a sample JSON file and initialize the `fsspeckit` filesystem with caching enabled. The first read operation retrieves data from the source and populates the cache.

**Setup steps:**

1. Create a temporary directory for our example
2. Create sample data file
3. Configure filesystem with caching

```python
import tempfile
import time
import os
import shutil
from fsspeckit import filesystem
from examples.caching.setup_data import create_sample_data_file

tmpdir = tempfile.mkdtemp()
print(f"Created temporary directory: {tmpdir}")

sample_file = create_sample_data_file(tmpdir)

cache_dir = os.path.join(tmpdir, "cache")
fs = filesystem(
    protocol_or_path="file",
    cached=True,
    cache_storage=cache_dir,
    verbose=True
)

print("\n=== First read (populating cache) ===")
start_time = time.time()
data1 = fs.read_json(sample_file)
first_read_time = time.time() - start_time
print(f"First read completed in {first_read_time:.4f} seconds")
```

#### Second Read (Using Cache)

Now, let's read the same file again to see the performance improvement from using the cache.

```python
print("\n=== Second read (using cache) ===")
start_time = time.time()
data2 = fs.read_json(sample_file)
second_read_time = time.time() - start_time
print(f"Second read completed in {second_read_time:.4f} seconds")
```

The second read retrieves data directly from the cache, which is significantly faster than reading from the source again.

#### Reading from Cache after Deletion

To demonstrate that the cache is persistent, we'll remove the original file and try to read it again.

```python
print("\n=== Demonstrating cache effectiveness ===")
print("Removing original file...")
os.remove(sample_file)
print(f"Original file exists: {os.path.exists(sample_file)}")

print("\n=== Third read (from cache only) ===")
start_time = time.time()
data3 = fs.read_json(sample_file)
third_read_time = time.time() - start_time
print(f"Third read completed in {third_read_time:.4f} seconds")
print("✓ Successfully read from cache even after original file was removed")

print("\n=== Performance Comparison ===")
print(f"First read (from disk): {first_read_time:.4f} seconds")
print(f"Second read (from cache): {second_read_time:.4f} seconds")
print(f"Third read (from cache): {third_read_time:.4f} seconds")

shutil.rmtree(tmpdir)
print(f"Cleaned up temporary directory: {tmpdir}")
```

This step proves that the cache acts as a persistent copy of the data, allowing access even if the original source is unavailable.


## Custom Filesystem Implementations

`fsspeckit` provides specialized filesystem implementations for unique use cases:

### GitLab Filesystem (`GitLabFileSystem`)

Access files directly from GitLab repositories. This is particularly useful for configuration files, datasets, or code stored in private or public GitLab instances.

**Example: Reading from a GitLab Repository**

```python
from fsspeckit.core import filesystem

# Instantiate a GitLab filesystem
gitlab_fs = filesystem(
    "gitlab",
    storage_options={
        "project_name": "your-group/your-project", # Or "project_id": 12345
        "ref": "main", # Branch, tag, or commit SHA
        "token": "glpat_YOUR_PRIVATE_TOKEN" # Required for private repos
    }
)

# List files in the repository root
print(gitlab_fs.ls("/"))

# Read a specific file
content = gitlab_fs.cat("README.md").decode("utf-8")
print(content[:200]) # Print first 200 characters
```

## Advanced Data Reading and Writing (`read_files`, `write_files`)

The `fsspeckit.core.ext` module (exposed via `AbstractFileSystem` extensions) provides powerful functions for reading and writing various data formats (JSON, CSV, Parquet) with advanced features like:

-   **Batch Processing**: Efficiently handle large datasets by processing files in configurable batches.
-   **Parallel Processing**: Leverage multi-threading to speed up file I/O operations.
-   **Schema Unification & Optimization**: Automatically unifies schemas when concatenating multiple files and optimizes data types for memory efficiency (e.g., using Polars' `opt_dtypes` or PyArrow's schema casting).
-   **File Path Tracking**: Optionally include the source file path as a column in the resulting DataFrame/Table.

### Universal `read_files`

The `read_files` function acts as a universal reader, delegating to format-specific readers (JSON, CSV, Parquet) while maintaining consistent options.

**Example: Reading CSVs in Batches with Parallelism**

```python
from fsspeckit.core import filesystem

# Assuming you have multiple CSV files like 'data/part_0.csv', 'data/part_1.csv', etc.
# on your local filesystem
fs = filesystem("file")

# Read CSV files in batches of 10, using multiple threads, and including file path
for batch_df in fs.read_files(
    "data/*.csv",
    format="csv",
    batch_size=10,
    include_file_path=True,
    use_threads=True,
    verbose=True
):
    print(f"Processed batch with {len(batch_df)} rows. Columns: {batch_df.columns}")
    print(batch_df.head(2))
```

### Reading and Processing Multiple Files (PyArrow Tables, Batch Processing)

`fsspeckit` simplifies reading multiple files of various formats (Parquet, CSV, JSON) from a folder into a single PyArrow Table or Polars DataFrame.

**Reading multiple files into a single table** is a powerful feature that allows you to efficiently process data distributed across multiple files. This is particularly useful when dealing with large datasets that are split into smaller files for better organization or parallel processing.

**Key concepts demonstrated:**

1.  **Glob patterns**: The `**/*.parquet`, `**/*.csv`, and `**/*.json` patterns are used to select files recursively from the directory and its subdirectories. The `**` pattern matches any directories, allowing the function to find files in nested directories.

2.  **Concat parameter**: The `concat=True` parameter tells the function to combine data from multiple files into a single table or DataFrame. When set to `False`, the function would return a list of individual tables/DataFrames.

3.  **Format flexibility**: The same interface can be used to read different file formats (Parquet, CSV, JSON), making it easy to work with heterogeneous data sources.

**Step-by-step explanation:**

1.  **Creating sample data**: We create two subdirectories and populate them with sample data in three different formats (Parquet, CSV, JSON). Each format contains the same structured data but in different serialization formats.

2.  **Reading Parquet files**: Using `fs.read_parquet("**/*.parquet", concat=True)`, we read all Parquet files recursively and combine them into a single PyArrow Table. Parquet is a columnar storage format that is highly efficient for analytical workloads.

3.  **Reading CSV files**: Using `fs.read_csv("**/*.csv", concat=True)`, we read all CSV files and combine them into a Polars DataFrame, which we then convert to a PyArrow Table for consistency.

4.  **Reading JSON files**: Using `fs.read_json("**/*.json", as_dataframe=True, concat=True)`, we read all JSON files and combine them into a Polars DataFrame, then convert it to a PyArrow Table.

5.  **Verification**: Finally, we verify that all three tables have the same number of rows, confirming that the data was correctly read and combined across all files and formats.

The flexibility of `fsspeckit` allows you to use the same approach with different data sources, including remote filesystems like S3, GCS, or Azure Blob Storage, simply by changing the filesystem path.

#### Setup

First, we'll create a temporary directory with sample data in different formats.

**Setup steps:**

1. Create a temporary directory for our example
2. Create sample data in subdirectories

```python
import tempfile
import shutil
import os
from examples.read_folder.create_dataset import create_sample_dataset

temp_dir = tempfile.mkdtemp()
print(f"Created temporary directory: {temp_dir}")

create_sample_dataset(temp_dir)
```

This step sets up the environment by creating a temporary directory and populating it with sample data files.

#### Reading Parquet Files

Now, let's read all the Parquet files from the directory and its subdirectories into a single PyArrow Table.

**Reading Parquet files:**

1. Read Parquet files using glob pattern
2. Display table information and sample data

```python
print("\n=== Reading Parquet Files ===")
from fsspeckit import filesystem
fs = filesystem(temp_dir)
parquet_table = fs.read_parquet("**/*.parquet", concat=True)
print(f"Successfully read Parquet files into PyArrow Table")
print(f"Table shape: {parquet_table.num_rows} rows x {parquet_table.num_columns} columns")
print("First 3 rows:")
print(parquet_table.slice(0, 3).to_pandas())
```

We use the `read_parquet` method with a glob pattern `**/*.parquet` to find all Parquet files recursively. The `concat=True` parameter combines them into a single table.

#### Reading CSV Files

Next, we'll read all the CSV files into a Polars DataFrame and then convert it to a PyArrow Table.

**Reading CSV files:**

1. Read CSV files using glob pattern
2. Display DataFrame information and sample data
3. Convert to PyArrow Table for consistency

```python
print("\n=== Reading CSV Files ===")
csv_df = fs.read_csv("**/*.csv", concat=True)
print(f"Successfully read CSV files into Polars DataFrame")
print(f"DataFrame shape: {csv_df.shape}")
print("First 3 rows:")
print(csv_df.head(3))
csv_table = csv_df.to_arrow()
```

Similarly, we use `read_csv` with the same glob pattern to read all CSV files.

#### Reading JSON Files

Finally, let's read all the JSON files.

**Reading JSON files:**

1. Read JSON files using glob pattern
2. Display DataFrame information and sample data
3. Convert to PyArrow Table for consistency

```python
print("\n=== Reading JSON Files ===")
json_df = fs.read_json("**/*.json", as_dataframe=True, concat=True)
print(f"Successfully read JSON files into Polars DataFrame")
print(f"DataFrame shape: {json_df.shape}")
print("First 3 rows:")
print(json_df.head(3))
json_table = json_df.to_arrow()
```

The `read_json` method is used to read all JSON files. We set `as_dataframe=True` to get a Polars DataFrame.

#### Verification

Let's verify that all the tables have the same number of rows.

```python
print("\n=== Verification ===")
print(f"All tables have the same number of rows: {parquet_table.num_rows == csv_table.num_rows == json_table.num_rows}")

shutil.rmtree(temp_dir)
print(f"\nCleaned up temporary directory: {temp_dir}")
```

This final step confirms that our data reading and concatenation were successful.

This example shows how to read various file formats from a directory, including subdirectories, into a unified PyArrow Table or Polars DataFrame. It highlights the flexibility of `fsspeckit` in handling different data sources and formats.

`fsspeckit` enables efficient batch processing of large datasets by reading files in smaller, manageable chunks. This is particularly useful for memory-constrained environments or when processing streaming data.

**Batch processing** is a technique for handling large datasets by dividing them into smaller, manageable chunks. This is particularly important for:

1.  **Memory-constrained environments**: When working with datasets that are too large to fit in memory, batch processing allows you to process the data incrementally.
2.  **Streaming data**: When data is continuously generated (e.g., from IoT devices or real-time applications), batch processing enables you to process data as it arrives.
3.  **Distributed processing**: In distributed computing environments, batch processing allows different nodes to work on different chunks of data simultaneously.

**The `batch_size` parameter** controls how many files or records are processed together in each batch. A smaller batch size reduces memory usage but may increase processing overhead, while a larger batch size improves throughput but requires more memory.

**Step-by-step walkthrough:**

1.  **Creating sample batched data**: We generate sample data and distribute it across multiple files in each format (Parquet, CSV, JSON). Each file contains a subset of the total data, simulating a real-world scenario where data is split across multiple files.

2.  **Reading Parquet files in batches**: Using `fs.read_parquet(parquet_path, batch_size=2)`, we read all Parquet files in batches of 2 files at a time. Each iteration of the loop processes a batch of files, and the `batch` variable contains the combined data from those files.

3.  **Reading CSV files in batches**: Similarly, we use `fs.read_csv(csv_path, batch_size=2)` to read CSV files in batches. The result is a Polars DataFrame for each batch, which we can process individually.

4.  **Reading JSON files in batches**: Finally, we use `fs.read_json(json_path, batch_size=2)` to read JSON files in batches. The JSON data is automatically converted to Polars DataFrames for easy processing.
```
```python
print("\n=== JSON Batch Reading ===")
json_path = os.path.join(temp_dir, "*.json")
print("\nReading JSON files in batches (batch_size=2):")
for i, batch in enumerate(fs.read_json(json_path, batch_size=2)):
    print(f"   Batch {i+1}: shape={batch.shape}")
    print(f"   - Data preview: {batch.head(1).to_dicts()}")

shutil.rmtree(temp_dir)
print(f"\nCleaned up temporary directory: {temp_dir}")
```

The `read_json` method is also used with `batch_size=2` to process JSON files in batches.

This example illustrates how to read Parquet, CSV, and JSON files in batches using the `batch_size` parameter. This approach allows for processing of large datasets without loading the entire dataset into memory at once.

### Advanced Parquet Handling and Delta Lake Integration

`fsspeckit` enhances Parquet operations with deep integration with PyArrow, enabling efficient dataset management, partitioning, and delta lake capabilities.

-   **`pyarrow_dataset`**: Create PyArrow datasets for optimized querying, partitioning, and predicate pushdown.
-   **`pyarrow_parquet_dataset`**: Specialized for Parquet, handling `_metadata` files for overall dataset schemas.

**Example: Writing to a PyArrow Dataset with Partitioning**

```python
import polars as pl
from fsspeckit.core import filesystem

fs = filesystem("file")
base_path = "output/my_partitioned_data"

# Sample data
data = pl.DataFrame({
    "id": [1, 2, 3, 4],
    "value": ["A", "B", "C", "D"],
    "year": [2023, 2023, 2024, 2024],
    "month": [10, 11, 1, 2]
})

# Write data as a partitioned PyArrow dataset
fs.write_pyarrow_dataset(
    data=data,
    path=base_path,
    partition_by=["year", "month"], # Partition by year and month
    format="parquet",
    compression="zstd",
    mode="overwrite" # Overwrite if path exists
)

print(f"Data written to {base_path} partitioned by year/month.")
# Expected structure: output/my_partitioned_data/year=2023/month=10/data-*.parquet
```

<!--**Example: Delta Lake Operations with Pydala Dataset**

```python
import polars as pl
from fsspeckit.core import filesystem

fs = filesystem("file")
delta_path = "output/my_delta_table"

# Initial data
initial_data = pl.DataFrame({
    "id": [1, 2],
    "name": ["Alice", "Bob"],
    "version": [1, 1]
})

# Write initial data to a Pydala dataset
fs.write_pydala_dataset(
    data=initial_data,
    path=delta_path,
    mode="overwrite"
)
print("Initial Delta table created.")

# New data for an upsert: update Alice, add Charlie
new_data = pl.DataFrame({
    "id": [1, 3],
    "name": ["Alicia", "Charlie"],
    "version": [2, 1]
})

# Perform a delta merge (upsert)
fs.write_pydala_dataset(
    data=new_data,
    path=delta_path,
    mode="delta",
    delta_subset=["id"] # Column(s) to use for merging
)
print("Delta merge completed.")

# Read the updated table
updated_df = fs.pydala_dataset(delta_path).to_polars()
print("Updated Delta table:")
print(updated_df)
# Expected: id=1 Alicia version=2, id=2 Bob version=1, id=3 Charlie version=1
```-->

`fsspeckit` facilitates integration with Delta Lake by providing `StorageOptions` that can be used to configure `deltalake`'s `DeltaTable` for various storage backends.

This example demonstrates how to use `LocalStorageOptions` with `deltalake`'s `DeltaTable`. It shows how to initialize a `DeltaTable` instance by passing the `fsspeckit` storage options, enabling seamless interaction with Delta Lake tables across different storage types.

**Step-by-step walkthrough:**

1. Create a temporary directory for our example
2. Create a simple Polars DataFrame
3. Write initial data to create the Delta table
4. Create a LocalStorageOptions object for the temporary directory
5. Create a DeltaTable instance, passing storage options
   - Note: deltalake expects storage_options as a dict, which to_object_store_kwargs provides
6. Read data from the DeltaTable
7. Clean up the temporary directory

**Delta Lake** is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It provides a reliable, scalable, and performant way to work with data lakes, combining the benefits of data lakes (low cost, flexibility) with data warehouses (reliability, performance).

```python
from deltalake import DeltaTable
from fsspeckit.storage_options import LocalStorageOptions
import tempfile
import shutil
import os
import polars as pl

temp_dir = tempfile.mkdtemp()
print(f"Created temporary directory: {temp_dir}")

delta_table_path = os.path.join(temp_dir, "my_delta_table")
print(f"Creating a dummy Delta table at: {delta_table_path}")

data = pl.DataFrame({
    "id": [1, 2, 3],
    "value": ["A", "B", "C"]
})

data.write_delta(delta_table_path, mode="overwrite")
print("Initial data written to Delta table.")

local_options = LocalStorageOptions(path=temp_dir)

dt = DeltaTable(delta_table_path, storage_options=local_options.to_object_store_kwargs())
print(f"\nSuccessfully created DeltaTable instance from: {delta_table_path}")
print(f"DeltaTable version: {dt.version()}")
print(f"DeltaTable files: {dt.files()}")

table_data = dt.to_pyarrow_table()
print("\nData read from DeltaTable:")
print(table_data.to_pandas())

shutil.rmtree(temp_dir)
print(f"Cleaned up temporary directory: {temp_dir}")
```

**Key features of Delta Lake:**

- ACID transactions: Ensures data integrity even with concurrent operations
- Time travel: Allows querying data as it existed at any point in time
- Schema enforcement: Maintains data consistency with schema validation
- Scalable metadata: Handles billions of files efficiently
- Unified analytics: Supports both batch and streaming workloads

**Integrating fsspeckit with Delta Lake:**

The `fsspeckit` `StorageOptions` classes can be used to configure `deltalake`'s `DeltaTable` for various storage backends. This integration allows you to:

1.  Use consistent configuration patterns across different storage systems
2.  Leverage the benefits of fsspec's unified filesystem interface
3.  Seamlessly switch between local and cloud storage without changing your Delta Lake code

**The `to_object_store_kwargs()` method** converts `fsspeckit` storage options into a dictionary format that `deltalake` expects for its `storage_options` parameter. This is necessary because `deltalake` requires storage options as a dictionary, while `fsspeckit` provides them as structured objects.

**Step-by-step walkthrough:**

1.  **Creating a temporary directory**: We create a temporary directory to store our Delta table, ensuring the example is self-contained and doesn't leave artifacts on your system.

2.  **Creating sample data**: We create a simple Polars DataFrame with sample data that will be written to our Delta table.

3.  **Writing to Delta table**: Using the `write_delta` method, we convert our DataFrame into a Delta table. This creates the necessary Delta Lake metadata alongside the data files.

4.  **Configuring storage options**: We create a `LocalStorageOptions` object that points to our temporary directory. This object contains all the information needed to access the Delta table.

5.  **Initializing DeltaTable**: We create a `DeltaTable` instance by passing the table path and the storage options converted to a dictionary via `to_object_store_kwargs()`. This allows `deltalake` to locate and access the Delta table files.

6.  **Verifying the DeltaTable**: We check the version and files of our Delta table to confirm it was created correctly. Delta tables maintain version history, allowing you to track changes over time.

7.  **Reading data**: Finally, we read the data from our Delta table back into a PyArrow Table, demonstrating that we can successfully interact with the Delta Lake table using the fsspeckit configuration.

This integration is particularly valuable when working with Delta Lake in cloud environments, as it allows you to use the same configuration approach for local development and production deployments across different cloud providers.

## Storage Options Management

`fsspeckit` provides a robust system for managing storage configurations, simplifying credential handling and environment setup.

### Loading from Environment Variables

Instead of hardcoding credentials, you can load storage options directly from environment variables.

**Example: Loading AWS S3 Configuration from Environment**

Set these environment variables before running your script:
```bash
export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"
export AWS_DEFAULT_REGION="us-west-2"
```

Then in Python:
```python
from fsspeckit.storage_options import AwsStorageOptions

# Load AWS options directly from environment variables
aws_opts = AwsStorageOptions.from_env()
print(f"Loaded AWS region: {aws_opts.region}")

# Use it to create a filesystem
# fs = aws_opts.to_filesystem()
```

### Merging Storage Options

Combine multiple storage option configurations, useful for layering default settings with user-specific overrides.

**Example: Merging S3 Options**

```python
from fsspeckit.storage_options import AwsStorageOptions, merge_storage_options

# Base configuration
base_opts = AwsStorageOptions(
    protocol="s3",
    region="us-east-1",
    access_key_id="DEFAULT_KEY"
)

# User-provided overrides
user_overrides = {
    "access_key_id": "USER_KEY",
    "allow_http": True # New setting
}

# Merge, with user_overrides taking precedence
merged_opts = merge_storage_options(base_opts, user_overrides, overwrite=True)

print(f"Merged Access Key ID: {merged_opts.access_key_id}") # USER_KEY
print(f"Merged Region: {merged_opts.region}") # us-east-1
print(f"Allow HTTP: {merged_opts.allow_http}") # True
```

### Note on GitHub Examples

For a comprehensive collection of executable examples demonstrating various functionalities and advanced patterns of `fsspeckit`, including those discussed in this document, please refer to the [examples directory on GitHub](https://github.com/legout/fsspeckit/tree/main/examples). Each example is designed to be runnable and provides detailed insights into practical usage.

## Performance Tips

-   **Caching**: Always consider using `cached=True` with the `filesystem` function, especially for remote filesystems, to minimize repeated downloads.
-   **Parallel Reading**: For multiple files, set `use_threads=True` in `read_json`, `read_csv`, and `read_parquet` to leverage concurrent I/O.
-   **Batch Processing**: When dealing with a very large number of files or extremely large individual files, use the `batch_size` parameter in reading functions to process data in chunks, reducing memory footprint.
-   **`opt_dtypes`**: Utilize `opt_dtypes=True` in reading functions when converting to Polars or PyArrow to automatically optimize column data types, leading to more efficient memory usage and faster subsequent operations.
-   **Parquet Datasets**: For large, partitioned Parquet datasets, use `pyarrow_dataset` or `pyarrow_parquet_dataset`. These leverage PyArrow's dataset API for efficient metadata handling, partition pruning, and predicate pushdown, reading only the necessary data.
-   **Compression**: When writing Parquet files, choose an appropriate compression codec (e.g., `zstd`, `snappy`) to reduce file size and improve I/O performance. `zstd` often provides a good balance of compression ratio and speed.

## Flexible Storage Configuration

`fsspeckit` simplifies configuring connections to various storage systems, including local filesystems, AWS S3, Azure Storage, and Google Cloud Storage, using `StorageOptions` classes. These options can then be converted into `fsspec` filesystems.

### Local Storage Example

This example demonstrates how to initialize `LocalStorageOptions` and use it to interact with the local filesystem.

**Step-by-step walkthrough:**

1. Create a temporary directory for our test
2. Create a test file and write content to it
3. List files in the directory to verify our file was created
4. Read the content back to verify it was written correctly
5. Clean up the temporary directory

**StorageOptions classes** simplify configuration for different storage systems and provide a consistent interface for creating fsspec filesystem objects.

```python
import os
import tempfile
import shutil
from fsspeckit.storage_options import LocalStorageOptions

print("=== LocalStorageOptions Example ===\n")

local_options = LocalStorageOptions(auto_mkdir=True)
local_fs = local_options.to_filesystem()

temp_dir = tempfile.mkdtemp()
print(f"Working in temporary directory: {temp_dir}")

temp_file = os.path.join(temp_dir, "test_file.txt")
with local_fs.open(temp_file, "w") as f:
    f.write("Hello, LocalStorageOptions!")
print(f"Created test file: {temp_file}")

files = local_fs.ls(temp_dir)
print(f"Files in {temp_dir}: {[os.path.basename(f) for f in files]}")

with local_fs.open(temp_file, "r") as f:
    content = f.read()
print(f"File content: '{content}'")

shutil.rmtree(temp_dir)
print(f"Cleaned up temporary directory: {temp_dir}")
print("Local storage example completed.\n")
```



### Conceptual AWS S3 Configuration

This example demonstrates the configuration pattern for `AwsStorageOptions`. It is expected to fail when attempting to connect to actual cloud services because it uses dummy credentials.

**Note:** The `to_filesystem()` method converts StorageOptions into fsspec-compatible objects, allowing seamless integration with any fsspec-compatible library.
```python
from fsspeckit.storage_options import AwsStorageOptions

print("=== Conceptual AwsStorageOptions Example (using a dummy endpoint) ===\n")

aws_options = AwsStorageOptions(
    endpoint_url="http://s3.dummy-endpoint.com",
    access_key_id="DUMMY_KEY",
    secret_access_key="DUMMY_SECRET",
    allow_http=True,
    region="us-east-1"
)

aws_fs = aws_options.to_filesystem()
print(f"Created fsspec filesystem for S3: {type(aws_fs).__name__}")
print("AWS storage example completed.\n")
```



### Conceptual Azure Configuration

This example shows how to configure `AzureStorageOptions`.  It is expected to fail when attempting to connect to actual cloud services because it uses dummy credentials.


```python
from fsspeckit.storage_options import AzureStorageOptions

print("=== Conceptual AzureStorageOptions Example (using a dummy connection string) ===\n")
azure_options = AzureStorageOptions(
    protocol="az",
    account_name="demoaccount",
    connection_string="DefaultEndpointsProtocol=https;AccountName=demoaccount;AccountKey=demokey==;EndpointSuffix=core.windows.net"
)

azure_fs = azure_options.to_filesystem()
print(f"Created fsspec filesystem for Azure: {type(azure_fs).__name__}")
print("Azure storage example completed.\n")
```



### Conceptual GCS Configuration

This example shows how to configure `GcsStorageOptions`.  It is expected to fail when attempting to connect to actual cloud services because it uses dummy credentials.

**StorageOptions classes** provide a simplified, consistent interface for configuring connections to various storage systems. They abstract away the complexity of different storage backends and provide a unified way to create fsspec filesystem objects.

The `to_filesystem()` method converts these options into `fsspec` compatible objects, enabling seamless integration with any fsspec-compatible library or tool.

**Important Note**: The AWS, Azure, and GCS examples use dummy credentials and are for illustrative purposes only. These examples are expected to fail when attempting to connect to actual cloud services because:

1.  The endpoint URLs are not real service endpoints
2.  The credentials are placeholder values that don't correspond to actual accounts
3.  The connection strings and tokens are examples, not valid credentials

This approach allows you to understand the configuration pattern without needing actual cloud credentials. When using these examples in production, you would replace the dummy values with your real credentials and service endpoints.

```python
from fsspeckit.storage_options import GcsStorageOptions

print("=== Conceptual GcsStorageOptions Example (using a dummy token path) ===\n")
gcs_options = GcsStorageOptions(
    protocol="gs",
    project="demo-project",
    token="path/to/dummy-service-account.json"
)

gcs_fs = gcs_options.to_filesystem()
print(f"Created fsspec filesystem for GCS: {type(gcs_fs).__name__}")
print("GCS storage example completed.\n")
