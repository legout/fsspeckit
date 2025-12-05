# Read and Write Datasets

This guide covers how to read and write datasets in various formats using fsspeckit's extended I/O helpers and dataset operations.

## Reading JSON Data

### Single JSON Files

```python
from fsspeckit.core.filesystem import filesystem

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

## DuckDB Dataset Operations

### Basic Dataset Operations

```python
from fsspeckit.datasets import DuckDBParquetHandler
import polars as pl

# Initialize handler
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

# Read dataset back
result = handler.read_parquet_dataset("s3://bucket/my-dataset/")
print(result)
```

### SQL Analytics

```python
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