# Read and Write Datasets

This guide covers how to read and write datasets in various formats using fsspeckit's extended I/O helpers and dataset operations.

## Reading JSON Data

### Single JSON Files

```python
from fsspeckit import filesystem

fs = filesystem(".")

# Read JSON file as dictionary
data = fs.read_json_file("data.json")
print(f"Keys: {list(data.keys())}")

# Read JSON file as DataFrame
df = fs.read_json_file("data.json", as_dataframe=True)
print(df.head())
```

### Multiple JSON Files

```python
# Read multiple JSON files with batching
for batch in fs.read_json("data/*.json", batch_size=5):
    print(f"Processing batch with {len(batch)} files")
    # Process batch
    process_batch(batch)

# Read all JSON files and concatenate
df = fs.read_json("data/*.json", concat=True)
print(f"Total rows: {len(df)}")

# Read with threading for performance
df = fs.read_json("data/*.json", use_threads=True, num_threads=4)

# Include source file path
df = fs.read_json("data/*.json", include_file_path=True)
print(df.columns)  # Includes '_source_file' column
```

### JSON Lines Format

```python
# Read JSON Lines (newline-delimited JSON)
df = fs.read_json("data/lines.jsonl", as_dataframe=True)
print(f"Records: {len(df)}")

# Read JSON Lines in batches
for batch in fs.read_json("data/lines.jsonl", batch_size=1000):
    process_batch(batch)
```

## Reading CSV Data

### Single CSV Files

```python
# Read CSV file
df = fs.read_csv_file("data.csv")
print(df.head())

# Read with specific columns
df = fs.read_csv_file("data.csv", columns=["id", "name", "value"])

# Read with data type optimization
df = fs.read_csv_file("data.csv", opt_dtypes=True)
```

### Multiple CSV Files

```python
# Read multiple CSV files
df = fs.read_csv("data/*.csv", concat=True)
print(f"Combined rows: {len(df)}")

# Batch processing
for batch in fs.read_csv("data/*.csv", batch_size=10):
    print(f"Batch size: {len(batch)}")
    process_batch(batch)

# With parallel processing
df = fs.read_csv("data/*.csv", use_threads=True, num_threads=4)

# Optimize data types automatically
df = fs.read_csv("data/*.csv", opt_dtypes=True)
```

### Advanced CSV Options

```python
# Read with custom delimiter
df = fs.read_csv_file("data.tsv", delimiter="\t")

# Read with specific encoding
df = fs.read_csv_file("data.csv", encoding="utf-8")

# Read with header handling
df = fs.read_csv_file("data_no_header.csv", header=None, names=["col1", "col2"])
```

## Reading Parquet Data

### Single Parquet Files

```python
# Read single Parquet file
table = fs.read_parquet_file("data.parquet")
print(f"Schema: {table.schema}")
print(f"Rows: {len(table)}")

# Read as DataFrame
df = fs.read_parquet_file("data.parquet", as_dataframe=True)
print(df.head())
```

### Multiple Parquet Files

```python
# Read multiple Parquet files with schema unification
table = fs.read_parquet("data/*.parquet", concat=True)
print(f"Combined rows: {len(table)}")

# Batch reading for large datasets
for batch in fs.read_parquet("data/*.parquet", batch_size=20):
    print(f"Batch rows: {len(batch)}")
    process_batch(batch)

# Read partitioned data
table = fs.read_parquet("partitioned_data/**/*.parquet", concat=True)
print(f"Partitioned rows: {len(table)}")

# Include source file path
table = fs.read_parquet("data/*.parquet", include_file_path=True)
print(f"Columns: {table.column_names}")
```

### Column Selection and Filtering

```python
# Read specific columns
table = fs.read_parquet_file("data.parquet", columns=["id", "name", "value"])

# Read with row filtering (PyArrow dataset)
dataset = fs.pyarrow_dataset("data/")
filtered_table = dataset.to_table(
    filter=pyarrow.compute.greater(dataset.column("value"), 100)
)
```

## Universal File Reading

### Auto-Detect Format

```python
# Auto-detect format from file extension
df = fs.read_files("data/mixed/*", format="auto")

# Process mixed file types
for file_path in ["data.json", "data.csv", "data.parquet"]:
    df = fs.read_files(file_path, format="auto")
    print(f"File: {file_path}, Rows: {len(df)}")
```

### Explicit Format Specification

```python
# Force specific format
df = fs.read_files("data.txt", format="csv")

# Control result type
df_polars = fs.read_files("data/*.parquet", as_dataframe=True)
table_arrow = fs.read_files("data/*.parquet", as_dataframe=False)
```

### Threading and Performance

The `use_threads` parameter controls parallel processing:

```python
# Enable parallel reading (default: True)
df = fs.read_files("data/*.csv", format="csv", use_threads=True)

# Sequential processing for debugging
df = fs.read_files("data/*.csv", format="csv", use_threads=False)

# Batch processing with threading
for batch in fs.read_files(
    "large_dataset/*.parquet",
    format="parquet",
    batch_size=10,
    use_threads=True  # Parallel processing within batches
):
    process_batch(batch)
```

**Performance Guidelines:**
- Single files: `use_threads` has no effect
- Multiple files: `use_threads=True` can provide 2-10x speedup
- Use `use_threads=False` for debugging or when parallel processing causes issues

## Writing Data

### Writing DataFrames

```python
import polars as pl

# Create sample DataFrame
df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "value": [10.5, 20.3, 15.7, 25.1, 12.8],
    "category": ["A", "B", "A", "B", "A"]
})

# Write to Parquet
fs.write_parquet(df, "output.parquet")

# Write to CSV
fs.write_csv(df, "output.csv")

# Write to JSON
fs.write_json(df, "output.json")

# Write with compression
fs.write_parquet(df, "output.parquet", compression="zstd")
```

### Writing Multiple Files with Threading

```python
# Create sample data
data1 = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})
data2 = pl.DataFrame({"id": [3, 4], "value": ["c", "d"]})

# Write multiple files with parallel processing (default)
fs.write_files(
    data=[data1, data2],
    path=["output1.json", "output2.json"],
    format="json",
    use_threads=True  # Enable parallel writing
)

# Sequential writing for debugging
fs.write_files(
    data=[data1, data2],
    path=["output1.csv", "output2.csv"],
    format="csv",
    use_threads=False  # Sequential processing
)

# Single path, multiple data (auto-replicates path)
fs.write_files(
    data=[data1, data2],
    path="output.json",
    format="json"
)
# Creates: output.json, output-1.json
```

### Writing PyArrow Tables

```python
import pyarrow as pa

# Create PyArrow table
table = pa.table({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "value": [10.5, 20.3, 15.7, 25.1, 12.8]
})

# Write table
fs.write_parquet(table, "output.parquet")
```

### Append Mode

```python
# Append to existing files
fs.write_csv(new_df, "output.csv", mode="append")
fs.write_parquet(new_df, "output.parquet", mode="append")
```

## Writing Datasets

### Partitioned Datasets

```python
import pyarrow as pa

# Create table with partition columns
table = pa.table({
    "year": [2023, 2023, 2024, 2024],
    "month": [1, 2, 1, 2],
    "value": [10, 20, 30, 40],
    "category": ["A", "B", "A", "B"]
})

# Write partitioned dataset
fs.write_pyarrow_dataset(
    data=table,
    path="partitioned_data",
    partition_by=["year", "month"],
    format="parquet",
    compression="zstd"
)

# Result structure:
# partitioned_data/year=2023/month=1/...parquet
# partitioned_data/year=2023/month=2/...parquet
# partitioned_data/year=2024/month=1/...parquet
# partitioned_data/year=2024/month=2/...parquet
```

### Dataset with Custom Options

```python
# Write with specific options
fs.write_pyarrow_dataset(
    data=table,
    path="dataset",
    format="parquet",
    compression="snappy",
    max_rows_per_file=1000000,
    existing_data_behavior="overwrite_or_ignore"
)
```

## Dataset Write Operations

fsspeckit provides explicit write and merge operations for managing datasets. Use `write_dataset()` for simple append/overwrite operations and `merge()` for sophisticated merge logic.

### Writing Datasets

Use `write_dataset()` to append or overwrite datasets with detailed metadata tracking:

```python
from fsspeckit.datasets import PyarrowDatasetIO
import pyarrow as pa

# Initialize dataset handler
io = PyarrowDatasetIO()

# Create sample data
data = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "value": [10.5, 20.3, 15.7]
})

# Append to dataset (default mode)
result = io.write_dataset(data, "dataset/", mode="append")
print(f"Wrote {result.total_rows} rows across {len(result.files)} files")
print(f"Backend: {result.backend}, Mode: {result.mode}")

# Access file-level metadata
for file_info in result.files:
    print(f"File: {file_info.path}, Rows: {file_info.row_count}, Size: {file_info.size_bytes}")

# Overwrite existing dataset
result = io.write_dataset(data, "dataset/", mode="overwrite")
```

**WriteDatasetResult fields:**
- `files`: List of `FileWriteMetadata` (path, row_count, size_bytes, metadata)
- `total_rows`: Total rows written across all files
- `mode`: Write mode used ('append' or 'overwrite')
- `backend`: Backend that performed the write ('pyarrow' or 'duckdb')

### Merge Operations

Use `merge()` for sophisticated incremental updates that only rewrite affected files:

```python
# Create initial dataset
initial = pa.table({"id": [1, 2], "value": ["a", "b"]})
io.write_dataset(initial, "dataset/", mode="overwrite")

# Merge strategies
new_data = pa.table({"id": [2, 3], "value": ["updated", "c"]})

# UPSERT: Insert new records, update existing ones
result = io.merge(
    data=new_data,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"]
)

# INSERT: Only insert new records (ignore existing keys)
result = io.merge(
    data=new_data,
    path="dataset/",
    strategy="insert",
    key_columns=["id"]
)

# UPDATE: Only update existing records (ignore new keys)
result = io.merge(
    data=new_data,
    path="dataset/",
    strategy="update",
    key_columns=["id"]
)
```

**MergeResult fields:**
- `strategy`: Merge strategy used ('insert', 'update', 'upsert')
- `source_count`: Rows in source data
- `target_count_before`, `target_count_after`: Dataset row counts
- `inserted`, `updated`, `deleted`: Operation counts
- `files`: List of `MergeFileMetadata`
- `rewritten_files`, `inserted_files`, `preserved_files`: Affected file paths

### Using Merge Results

```python
# Track merge operation details
result = io.merge(data, "dataset/", strategy="upsert", key_columns=["id"])

# Log operation summary
print(f"Strategy: {result.strategy}")
print(f"Inserted: {result.inserted}, Updated: {result.updated}")
print(f"Dataset rows: {result.target_count_before} -> {result.target_count_after}")

# Monitor affected files
print(f"Rewritten: {len(result.rewritten_files)} files")
print(f"Inserted: {len(result.inserted_files)} files")
print(f"Preserved: {len(result.preserved_files)} files")

# Access detailed file metadata
for file_meta in result.files:
    print(f"{file_meta.operation}: {file_meta.path} ({file_meta.row_count} rows)")
```

### Advanced Merge Features

#### Composite Keys
```python
# Use multiple columns as keys
result = io.merge(
    data=data,
    path="dataset/",
    strategy="upsert",
    key_columns=["customer_id", "order_date"]
)
```

#### Partition-Aware Merges
```python
# Merge only affects partitions present in source data
result = io.merge(
    data=data,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"],
    partition_columns=["year", "month"]
)
```

### Write vs Merge: When to Use Each

**Use `write_dataset()` when:**
- Appending new data without deduplication
- Overwriting entire datasets
- Simple ETL pipelines without merge logic
- No key-based matching required

**Use `merge()` when:**
- Updating existing records based on keys
- Implementing CDC (Change Data Capture) patterns
- Deduplicating data during ingestion
- Only rewriting affected partitions/files for efficiency

### DuckDB Dataset Operations

```python
from fsspeckit.datasets import DuckDBDatasetIO
import polars as pl

# Initialize DuckDB handler
io = DuckDBDatasetIO()

# Write dataset
data = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
result = io.write_dataset(data, "dataset/", mode="append")
print(f"Wrote {result.total_rows} rows using {result.backend} backend")

# Merge with DuckDB
new_data = pl.DataFrame({"id": [2, 3, 4], "value": ["updated", "c", "d"]})
result = io.merge(
    data=new_data,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"]
)
print(f"Inserted: {result.inserted}, Updated: {result.updated}")
```

### Backend Selection Guidance
- **PyArrow**: Best for in-memory operations, schema flexibility, cloud storage
- **DuckDB**: Best for large datasets, complex analytics, SQL integration

## DuckDB Dataset Operations with SQL

### Basic Dataset Operations

```python
from fsspeckit.datasets import DuckDBDatasetIO
import polars as pl

# Initialize handler
io = DuckDBDatasetIO()

# Create sample data
data = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "B", "A"],
    "value": [10.5, 20.3, 15.7, 25.1, 12.8],
    "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
})

# Write dataset
result = io.write_dataset(data, "s3://bucket/my-dataset/", mode="append")
print(f"Wrote {result.total_rows} rows")

# Read dataset back
result_df = io.read_parquet("s3://bucket/my-dataset/")
print(result_df)
```

### SQL Analytics

```python
from fsspeckit.datasets import DuckDBDatasetHandler

# Initialize handler with SQL capabilities
handler = DuckDBDatasetHandler()

# Execute SQL queries on datasets
result = handler.execute_sql("""
    SELECT 
        category,
        COUNT(*) as count,
        AVG(value) as avg_value,
        SUM(value) as total_value,
        MIN(timestamp) as first_date,
        MAX(timestamp) as last_date
    FROM parquet_scan('s3://bucket/my-dataset/')
    GROUP BY category
    ORDER BY category
""")

print(result)

# Complex analytics
analytics = handler.execute_sql("""
    WITH daily_stats AS (
        SELECT 
            DATE(timestamp) as date,
            category,
            COUNT(*) as daily_count,
            AVG(value) as daily_avg
        FROM parquet_scan('s3://bucket/my-dataset/')
        GROUP BY DATE(timestamp), category
    )
    SELECT 
        date,
        category,
        daily_count,
        daily_avg,
        LAG(daily_avg) OVER (PARTITION BY category ORDER BY date) as prev_day_avg
    FROM daily_stats
    ORDER BY date, category
""")

print(analytics)
```

## Performance Optimization

### Batch Processing

```python
# Process large datasets in batches
def process_batch(batch_table):
    """Process individual batch"""
    # Your processing logic here
    return len(batch_table)

# Process in batches
total_rows = 0
for batch in fs.read_parquet("large_dataset/*.parquet", batch_size="100MB"):
    batch_rows = process_batch(batch)
    total_rows += batch_rows
    print(f"Processed {total_rows} total rows")
```

### Parallel Processing

```python
# Use threading for multiple files
df = fs.read_csv("data/*.csv", use_threads=True, num_threads=4)

# Use parallel processing for custom operations
from fsspeckit.common.misc import run_parallel

def process_file(file_path):
    df = fs.read_csv_file(file_path)
    return len(df)

file_list = ["file1.csv", "file2.csv", "file3.csv"]
results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=4,
    progress=True
)

print(f"File row counts: {results}")
```

### Memory Optimization

```python
# Optimize data types
df = fs.read_csv("data.csv", opt_dtypes=True)

# Read specific columns only
df = fs.read_parquet_file("large_data.parquet", columns=["id", "name"])

# Use column projection for datasets
dataset = fs.pyarrow_dataset("data/")
filtered_data = dataset.to_table(
    columns=["id", "name", "value"],
    filter=pyarrow.compute.greater(dataset.column("value"), 100)
)
```

## Error Handling

### Robust File Operations

```python
def safe_read_dataset(path, max_retries=3):
    """Read dataset with retry logic"""
    for attempt in range(max_retries):
        try:
            return fs.read_parquet_file(path)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff

# Usage
try:
    data = safe_read_dataset("s3://bucket/dataset.parquet")
except Exception as e:
    print(f"Failed to read dataset: {e}")
    # Fallback to local copy
    data = fs.read_parquet_file("local_backup.parquet")
```

### Validation

```python
# Validate data before writing
def validate_dataframe(df):
    """Basic DataFrame validation"""
    if len(df) == 0:
        raise ValueError("DataFrame is empty")
    
    if df.is_null().any().any():
        print("Warning: DataFrame contains null values")
    
    return True

# Write with validation
data = pl.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
validate_dataframe(data)
fs.write_parquet(data, "validated_data.parquet")
```

## Best Practices

1. **Use Appropriate Format**: Choose Parquet for analytics, CSV for simplicity, JSON for flexibility
2. **Batch Processing**: Process large datasets in batches to manage memory
3. **Compression**: Use compression (snappy, zstd) for storage efficiency
4. **Partitioning**: Partition datasets by query patterns for better performance
5. **Column Projection**: Read only needed columns to reduce I/O
6. **Error Handling**: Implement retry logic and fallback strategies
7. **Validation**: Validate data before writing to ensure quality

For more information on SQL filtering, see [Use SQL Filters](use-sql-filters.md).