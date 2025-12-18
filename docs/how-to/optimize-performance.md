# Optimize Performance

This guide covers performance optimization techniques for fsspeckit, including caching, parallel processing, and dataset optimization strategies.

## Caching Strategies

### Filesystem Caching

```python
from fsspeckit import filesystem

# Enable caching for remote filesystems
fs = filesystem("s3://bucket/", cached=True)

# Configure cache directory
fs = filesystem("s3://bucket/", cached=True, cache_storage="/fast/ssd/cache")

# Enable verbose cache logging
fs = filesystem("s3://bucket/", cached=True, verbose=True)

# Cache with specific size limits
fs = filesystem(
    "s3://bucket/", 
    cached=True, 
    cache_storage="/tmp/cache",
    use_listings_cache=True,
    skip_instance_cache=False
)
```

### Cache Management

```python
# Monitor cache usage
if hasattr(fs, 'cache_size'):
    print(f"Current cache size: {fs.cache_size}")

# Clear cache when needed
fs.clear_cache()

# Sync cache to ensure data is written
fs.sync_cache()

# Force cache refresh
fs.invalidate_cache()
```

### Cache Best Practices

```python
# Good: Use caching for remote filesystems
remote_fs = filesystem("s3://data/", cached=True)

# Good: Use fast storage for cache
remote_fs = filesystem(
    "s3://data/", 
    cached=True, 
    cache_storage="/nvme/cache"  # Fast NVMe storage
)

# Not necessary: Local filesystems don't benefit from caching
local_fs = filesystem("file")  # cached=False by default

# Configure for different workloads
# For read-heavy workloads
read_heavy_fs = filesystem("s3://data/", cached=True, cache_storage="/ssd/cache")

# For write-heavy workloads
write_heavy_fs = filesystem("s3://output/", cached=False)  # Avoid cache for writes
```

## Parallel Processing

### Parallel File Operations

```python
from fsspeckit.common.misc import run_parallel

def process_file(file_path):
    """Process individual file"""
    # Your processing logic here
    return len(file_path)

# List of files to process
file_list = [f"file_{i}.parquet" for i in range(100)]

# Process files in parallel
results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=8,  # Use 8 parallel workers
    progress=True   # Show progress bar
)

print(f"Processed {len(results)} files")
```

### Parallel I/O Operations

```python
# Parallel CSV reading
df = fs.read_csv("data/*.csv", use_threads=True, num_threads=4)

# Parallel JSON reading
df = fs.read_json("data/*.json", use_threads=True, num_threads=4)

# Parallel Parquet reading
table = fs.read_parquet("data/*.parquet", use_threads=True)
```

### Parallel Dataset Processing

```python
def process_batch(batch_table):
    """Process individual batch"""
    # Example: calculate statistics
    total_rows = len(batch_table)
    if "amount" in batch_table.column_names:
        total_amount = batch_table.column("amount").to_pandas().sum()
        return {"rows": total_rows, "total_amount": total_amount}
    return {"rows": total_rows}

# Process dataset in parallel batches
from fsspeckit.datasets.pyarrow import process_dataset_in_batches

batch_results = []
for i, result in enumerate(process_dataset_in_batches(
    dataset_path="s3://bucket/large-dataset/",
    batch_size="100MB",
    process_func=process_batch,
    max_workers=4
)):
    batch_results.append(result)
    print(f"Batch {i+1}: {result}")

# Aggregate results
total_rows = sum(r["rows"] for r in batch_results)
total_amount = sum(r.get("total_amount", 0) for r in batch_results)
print(f"Total rows: {total_rows}, Total amount: {total_amount}")
```

## Parallel Execution Configuration

### Joblib and Threading Requirements

Parallel execution in fsspeckit is powered by **joblib**, but it's treated as an optional dependency. This design ensures that basic operations work even in minimal environments, while parallel processing is available when needed.

#### Installing Joblib

To enable parallel execution, install fsspeckit with the `datasets` extra which includes joblib:

```bash
# Install with parallel processing support
pip install "fsspeckit[datasets]"

# Or install joblib separately
pip install joblib>=1.5.0
```

#### Serial Execution (Default)

By default, all CSV/Parquet/JSON read helpers use **serial execution** (`use_threads=False`), which does not require joblib:

```python
# Serial execution - no joblib required
df = fs.read_csv("data/*.csv")  # use_threads=False by default
table = fs.read_parquet("data/*.parquet")  # use_threads=False by default
df_json = fs.read_json("data/*.json")  # use_threads=False by default

# This works even without joblib installed
```

#### Parallel Execution (Opt-In)

To enable parallel execution, explicitly set `use_threads=True`:

```python
# Parallel execution - requires joblib
df_parallel = fs.read_csv("data/*.csv", use_threads=True)
table_parallel = fs.read_parquet("data/*.parquet", use_threads=True)
df_json_parallel = fs.read_json("data/*.json", use_threads=True)

# This requires joblib to be installed
```

#### Error Handling

If you request parallel execution without joblib installed, you'll get a clear error message:

```python
# Without joblib installed
df = fs.read_csv("data/*.csv", use_threads=True)
# Raises: ImportError: joblib is required for this function.
# Install with: pip install fsspeckit[datasets]
```

#### Using run_parallel

The `run_parallel` helper function also requires joblib:

```python
from fsspeckit.common.misc import run_parallel

# This requires joblib
results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=8,
    progress=True
)

# Without joblib, raises clear ImportError with installation instructions
```

#### Best Practices

```python
# Good: Default to serial execution for compatibility
df = fs.read_csv("data.csv")  # Fast, no extra dependencies

# Good: Explicitly enable parallel when needed
df = fs.read_csv("large_data/*.csv", use_threads=True)

# Good: Check joblib availability before using parallel features
try:
    import joblib
    df = fs.read_csv("data/*.csv", use_threads=True)
except ImportError:
    print("Parallel execution not available. Install with: pip install fsspeckit[datasets]")
    df = fs.read_csv("data/*.csv")

# Not recommended: Assuming joblib is always available
df = fs.read_csv("data/*.csv", use_threads=True)  # May fail in minimal environments
```

## Dataset Optimization

### Parquet Dataset Optimization

```python
from fsspeckit.datasets.pyarrow import (
    optimize_parquet_dataset_pyarrow,
    compact_parquet_dataset_pyarrow
)

# Optimize dataset with Z-ordering
optimize_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/large-dataset/",
    z_order_columns=["category", "timestamp"],
    target_file_size="256MB",
    compression="zstd"
)

# Compact small files
compact_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/fragmented-dataset/",
    target_file_size="128MB",
    compression="snappy"
)
```

### Advanced Optimization

```python
# Optimize with multiple strategies
optimize_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/dataset/",
    z_order_columns=["user_id", "event_date", "category"],
    target_file_size="512MB",
    compression="zstd",
    max_rows_per_group=1000000,
    max_rows_per_file=5000000
)

# Compact with filtering
compact_parquet_dataset_pyarrow(
    dataset_path="s3://bucket/dataset/",
    target_file_size="256MB",
    filters=[("status", "=", "active")],  # Only compact active files
    delete_intermediate=True
)
```

### Schema Optimization

```python
from fsspeckit.common.types import convert_large_types_to_normal
from fsspeckit.datasets import opt_dtype_pa
import pyarrow as pa

# Convert large string types to normal strings
large_string_table = pa.Table.from_pydict({
    "text": pa.array(["value1", "value2"], type=pa.large_string())
})

optimized_table = convert_large_types_to_normal(large_string_table)
print(f"Original schema: {large_string_table.schema}")
print(f"Optimized schema: {optimized_table.schema}")

# Optimize data types
optimized_table = opt_dtype_pa(large_string_table)
print(f"Further optimized schema: {optimized_table.schema}")
```

## Memory Optimization

### Batch Processing

```python
def process_large_dataset_efficiently(dataset_path):
    """Process large dataset with memory efficiency"""
    
    from fsspeckit.datasets.pyarrow import process_dataset_in_batches
    import pyarrow as pa
    
    total_rows = 0
    batch_count = 0
    
    # Process in small batches to manage memory
    for batch in process_dataset_in_batches(
        dataset_path=dataset_path,
        batch_size="50MB",  # Small batch size
        process_func=lambda batch: len(batch),
        max_workers=2  # Fewer workers to reduce memory pressure
    ):
        total_rows += batch
        batch_count += 1
        
        if batch_count % 10 == 0:
            print(f"Processed {batch_count} batches, {total_rows} total rows")
    
    return total_rows

# Usage
row_count = process_large_dataset_efficiently("s3://bucket/huge-dataset/")
```

### Column Projection

```python
# Read only needed columns to reduce memory usage
essential_columns = ["id", "timestamp", "user_id", "event_type"]

# For PyArrow
import pyarrow.parquet as pq
dataset = pq.ParquetDataset("large_dataset.parquet")
filtered_table = dataset.read(columns=essential_columns)

# For Polars
import polars as pl
df = pl.read_parquet("large_dataset.parquet", columns=essential_columns)

# For fsspeckit extended I/O
table = fs.read_parquet_file("large_dataset.parquet", columns=essential_columns)
```

### Memory-Efficient Data Types

```python
import polars as pl
from fsspeckit.common.polars import opt_dtype_pl

# Create DataFrame with suboptimal types
df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],  # Could be int32
    "category": ["A", "B", "A", "B", "A"],  # Could be categorical
    "value": [10.5, 20.3, 15.7, 25.1, 12.8],  # Could be float32
    "flag": [True, False, True, False, True]  # Could be boolean
})

# Optimize data types
optimized_df = opt_dtype_pl(df, shrink_numerics=True)
print(f"Original memory usage: {df.estimated_size('mb'):.2f} MB")
print(f"Optimized memory usage: {optimized_df.estimated_size('mb'):.2f} MB")
```

## I/O Optimization

### Efficient File Reading

```python
# Use appropriate batch sizes for different file types
csv_batch_size = "50MB"    # Smaller for text files
parquet_batch_size = "200MB"  # Larger for columnar files
json_batch_size = "25MB"     # Smallest for JSON

# Read with optimal settings
df_csv = fs.read_csv("data/*.csv", batch_size=csv_batch_size, use_threads=True)
table_parquet = fs.read_parquet("data/*.parquet", batch_size=parquet_batch_size)
df_json = fs.read_json("data/*.json", batch_size=json_batch_size, use_threads=True)
```

### Write Optimization

```python
import pyarrow as pa

# Write with optimal file size and compression
table = pa.table({...})

fs.write_pyarrow_dataset(
    data=table,
    path="optimized_dataset",
    format="parquet",
    compression="zstd",  # Good compression ratio
    max_rows_per_file=1000000,  # Target ~100MB files
    existing_data_behavior="overwrite_or_ignore"
)

# Write with partitioning for query performance
fs.write_pyarrow_dataset(
    data=table,
    path="partitioned_dataset",
    partition_by=["year", "month", "day"],  # Partition by date
    format="parquet",
    compression="snappy",  # Faster compression for hot data
    max_rows_per_file=500000
)
```

### Connection Pooling

```python
# Reuse filesystem instances for connection pooling
class FilesystemPool:
    def __init__(self):
        self._pool = {}
    
    def get_filesystem(self, protocol, storage_options=None):
        key = (protocol, str(storage_options))
        
        if key not in self._pool:
            self._pool[key] = filesystem(protocol, storage_options=storage_options)
        
        return self._pool[key]

# Usage
pool = FilesystemPool()

# Reuse filesystem instances
s3_fs = pool.get_filesystem("s3", s3_options)
gcs_fs = pool.get_filesystem("gs", gcs_options)

# This reuses existing connections
s3_fs_again = pool.get_filesystem("s3", s3_options)
```

## Performance Monitoring

### Benchmarking Operations

```python
import time
from contextlib import contextmanager

@contextmanager
def benchmark(operation_name):
    """Simple benchmarking context manager"""
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        duration = end_time - start_time
        print(f"{operation_name}: {duration:.2f} seconds")

# Benchmark different operations
with benchmark("CSV Read"):
    df_csv = fs.read_csv("data.csv")

with benchmark("Parquet Read"):
    table_parquet = fs.read_parquet_file("data.parquet")

with benchmark("JSON Read"):
    df_json = fs.read_json_file("data.json")

with benchmark("Parallel CSV Read"):
    df_parallel = fs.read_csv("data/*.csv", use_threads=True, num_threads=4)
```

### Cache Performance Analysis

```python
def analyze_cache_performance(fs, test_files):
    """Analyze cache hit/miss performance"""
    
    cache_stats = {"hits": 0, "misses": 0, "total_time": 0}
    
    for file_path in test_files:
        start_time = time.time()
        
        # First access (cache miss)
        content1 = fs.cat(file_path)
        first_time = time.time() - start_time
        
        # Second access (cache hit)
        start_time = time.time()
        content2 = fs.cat(file_path)
        second_time = time.time() - start_time
        
        # Verify content is same
        assert content1 == content2, "Cache returned different content"
        
        cache_stats["misses"] += 1
        cache_stats["hits"] += 1
        cache_stats["total_time"] += first_time + second_time
        
        print(f"File: {file_path}")
        print(f"  First access: {first_time:.3f}s (miss)")
        print(f"  Second access: {second_time:.3f}s (hit)")
        print(f"  Speedup: {first_time/second_time:.1f}x")
    
    return cache_stats

# Usage
test_files = fs.ls("/test/data/")[:5]  # Test first 5 files
stats = analyze_cache_performance(fs, test_files)
print(f"Cache analysis: {stats}")
```

## Configuration Tuning

### Worker Count Optimization

```python
import os
import psutil

def get_optimal_workers():
    """Calculate optimal worker count based on system resources"""
    
    # Get CPU count
    cpu_count = os.cpu_count()
    
    # Get available memory
    available_memory_gb = psutil.virtual_memory().available / (1024**3)
    
    # Estimate memory per worker (conservative estimate)
    memory_per_worker_gb = 1  # 1GB per worker
    
    # Calculate workers based on memory
    memory_limited_workers = int(available_memory_gb / memory_per_worker_gb)
    
    # Use the minimum of CPU and memory limits
    optimal_workers = min(cpu_count, memory_limited_workers)
    
    # Ensure at least 1 worker
    optimal_workers = max(1, optimal_workers)
    
    return optimal_workers

# Usage
optimal_workers = get_optimal_workers()
print(f"Using {optimal_workers} workers for parallel operations")

results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=optimal_workers,
    progress=True
)
```

### Batch Size Optimization

```python
def find_optimal_batch_size(dataset_path, test_sizes):
    """Find optimal batch size through experimentation"""
    
    import pyarrow as pa
    
    results = []
    
    for batch_size in test_sizes:
        start_time = time.time()
        batch_count = 0
        total_rows = 0
        
        try:
            for batch in process_dataset_in_batches(
                dataset_path=dataset_path,
                batch_size=batch_size,
                process_func=lambda batch: len(batch),
                max_workers=2
            ):
                batch_count += 1
                total_rows += batch
            
            duration = time.time() - start_time
            throughput = total_rows / duration
            
            results.append({
                "batch_size": batch_size,
                "duration": duration,
                "throughput": throughput,
                "batch_count": batch_count
            })
            
            print(f"Batch size {batch_size}: {throughput:.0f} rows/sec")
            
        except Exception as e:
            print(f"Batch size {batch_size} failed: {e}")
    
    # Find best throughput
    best_result = max(results, key=lambda x: x["throughput"])
    print(f"\nOptimal batch size: {best_result['batch_size']}")
    print(f"Throughput: {best_result['throughput']:.0f} rows/sec")
    
    return best_result

# Usage
test_sizes = ["25MB", "50MB", "100MB", "200MB", "500MB"]
optimal = find_optimal_batch_size("s3://bucket/dataset/", test_sizes)
```

## Best Practices

### General Performance Tips

```python
# 1. Enable caching for remote filesystems
remote_fs = filesystem("s3://data/", cached=True)

# 2. Use appropriate batch sizes
large_batch_fs = fs.read_parquet("large_files/*.parquet", batch_size="200MB")
small_batch_fs = fs.read_csv("text_files/*.csv", batch_size="25MB")

# 3. Leverage parallel processing
parallel_results = run_parallel(process_func, data_list, max_workers=8)

# 4. Optimize data types
optimized_df = opt_dtype_pl(df, shrink_numerics=True)

# 5. Use column projection
essential_data = fs.read_parquet_file("data.parquet", columns=["id", "value"])
```

### Environment-Specific Optimization

```python
# Development environment - prioritize speed
dev_fs = filesystem(
    "s3://dev-data/",
    cached=True,
    cache_storage="/tmp/dev_cache",
    use_listings_cache=False  # Skip caching for frequently changing data
)

# Production environment - prioritize stability
prod_fs = filesystem(
    "s3://prod-data/",
    cached=True,
    cache_storage="/ssd/prod_cache",
    use_listings_cache=True,
    skip_instance_cache=False
)

# Analytics environment - prioritize throughput
analytics_fs = filesystem(
    "s3://analytics-data/",
    cached=True,
    cache_storage="/nvme/analytics_cache"
)
```

### Monitoring and Alerting

```python
def setup_performance_monitoring():
    """Setup performance monitoring for production"""
    
    import logging
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("fsspeckit_performance")
    
    def monitor_operation(operation_func, operation_name):
        """Monitor operation performance"""
        start_time = time.time()
        
        try:
            result = operation_func()
            duration = time.time() - start_time
            
            logger.info(f"{operation_name} completed in {duration:.2f}s")
            
            # Alert on slow operations
            if duration > 300:  # 5 minutes
                logger.warning(f"Slow operation: {operation_name} took {duration:.2f}s")
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"{operation_name} failed after {duration:.2f}s: {e}")
            raise
    
    return monitor_operation

# Usage
monitor = setup_performance_monitoring()

def monitored_sync():
    return sync_dir(src_fs, dst_fs, "/src/", "/dst/")

# Monitor the sync operation
monitor(monitored_sync, "directory_sync")
```

## Troubleshooting Performance Issues

### Common Performance Problems

```python
# 1. Slow first access, fast subsequent access
# Solution: Enable caching
fs = filesystem("s3://data/", cached=True)

# 2. High memory usage
# Solution: Use smaller batch sizes and column projection
df = fs.read_parquet("data.parquet", columns=["id", "name"], batch_size="50MB")

# 3. Slow processing of many small files
# Solution: Use parallel processing and compact files
results = run_parallel(process_file, file_list, max_workers=8)
compact_parquet_dataset_pyarrow("path/", target_file_size="256MB")

# 4. Intermittent slowdowns
# Solution: Use connection pooling and retry logic
pool = FilesystemPool()
fs = pool.get_filesystem("s3", options)
```

### Performance Debugging

```python
def debug_performance(fs, operation):
    """Debug performance issues"""
    
    import psutil
    import tracemalloc
    
    # Start memory tracing
    tracemalloc.start()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    
    start_time = time.time()
    
    try:
        result = operation()
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        current, peak = tracemalloc.get_traced_memory()
        
        print(f"Operation completed in {end_time - start_time:.2f}s")
        print(f"Memory usage: {end_memory - start_memory:.1f}MB increase")
        print(f"Peak memory: {peak / 1024 / 1024:.1f}MB")
        
        return result
        
    finally:
        tracemalloc.stop()

# Usage
def test_operation():
    return fs.read_parquet("large_dataset.parquet")

debug_performance(fs, test_operation)
```

For more information on dataset operations, see [Read and Write Datasets](read-and-write-datasets.md).