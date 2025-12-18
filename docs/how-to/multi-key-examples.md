# Multi-Key Usage Examples

This guide provides practical examples for using PyArrow's multi-key vectorization capabilities, including deduplication with composite keys, merge operations, and handling fallback scenarios.

## Prerequisites

Ensure you have the latest fsspeckit with PyArrow support:

```python
from fsspeckit.datasets.pyarrow import (
    deduplicate_pyarrow,
    merge_parquet_dataset_pyarrow,
    _create_composite_key_array,
    _filter_by_key_membership,
    _create_string_key_array
)
import pyarrow as pa
```

## Multi-Tenant Data Deduplication

### Basic Multi-Tenant Example

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow import deduplicate_pyarrow

# Create sample multi-tenant data
data = {
    "tenant_id": [1, 1, 1, 2, 2, 2, 1],
    "user_id": [100, 100, 101, 200, 201, 200, 102],
    "record_id": [1, 1, 2, 1, 1, 1, 3],  # Duplicates exist
    "value": [10, 20, 30, 40, 50, 60, 70],  # Conflicting values for duplicates
    "timestamp": ["2024-01-01", "2024-01-02", "2024-01-01", 
                  "2024-01-01", "2024-01-01", "2024-01-03", "2024-01-01"]
}

table = pa.Table.from_pydict(data)

# Deduplicate using composite key [tenant_id, user_id, record_id]
unique_table = deduplicate_pyarrow(
    table=table,
    key_columns=["tenant_id", "user_id", "record_id"],
    dedup_order_by=["timestamp"],  # Keep first occurrence by timestamp
    keep="first"
)

print(f"Original rows: {table.num_rows}")
print(f"Unique rows: {unique_table.num_rows}")
print(f"Removed {table.num_rows - unique_table.num_rows} duplicate rows")
```

### Multi-Tenant with Custom Ordering

```python
# Advanced deduplication with business logic
from fsspeckit.datasets.pyarrow import deduplicate_pyarrow

# Sample data with business scenarios
business_data = {
    "tenant_id": [1, 1, 1, 1, 2, 2],
    "customer_id": [100, 100, 100, 100, 200, 200],
    "order_id": [1001, 1001, 1002, 1001, 2001, 2001],  # Multiple duplicates
    "status": ["pending", "confirmed", "pending", "cancelled", "pending", "confirmed"],
    "amount": [100.0, 100.0, 200.0, 100.0, 150.0, 150.0],
    "updated_at": ["2024-01-01 10:00", "2024-01-01 11:00", 
                   "2024-01-01 12:00", "2024-01-01 13:00",
                   "2024-01-01 10:00", "2024-01-01 11:00"]
}

table = pa.Table.from_pydict(business_data)

# Keep the most recent record for each [tenant_id, customer_id, order_id]
latest_records = deduplicate_pyarrow(
    table=table,
    key_columns=["tenant_id", customer_id", "order_id"],
    dedup_order_by=["updated_at"],
    keep="last"
)

print("Latest records for each order:")
print(latest_records.to_pandas())
```

## Merge Operations with Composite Keys

### Upsert with Composite Keys

```python
from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow

# Existing dataset with customer orders
existing_data = {
    "tenant_id": [1, 1, 2, 2],
    "customer_id": [100, 101, 200, 201],
    "order_id": [1001, 1002, 2001, 2002],
    "status": ["confirmed", "confirmed", "pending", "confirmed"],
    "amount": [150.0, 200.0, 100.0, 250.0]
}

# New incoming data with updates and new records
new_data = {
    "tenant_id": [1, 1, 2, 2, 3],
    "customer_id": [100, 103, 200, 203, 300],
    "order_id": [1001, 1003, 2001, 2004, 3001],
    "status": ["shipped", "confirmed", "confirmed", "pending", "confirmed"],
    "amount": [150.0, 175.0, 100.0, 300.0, 400.0]
}

existing_table = pa.Table.from_pydict(existing_data)
new_table = pa.Table.from_pydict(new_data)

# Write existing data first
import tempfile
import os

with tempfile.TemporaryDirectory() as temp_dir:
    dataset_path = os.path.join(temp_dir, "orders")
    
    # Write initial dataset
    merge_parquet_dataset_pyarrow(
        data=existing_table,
        path=dataset_path,
        strategy="append"  # Initial load
    )
    
    # Upsert new data using composite key
    stats = merge_parquet_dataset_pyarrow(
        data=new_table,
        path=dataset_path,
        strategy="upsert",
        key_columns=["tenant_id", "customer_id", "order_id"]
    )
    
    print(f"Merge Statistics:")
    print(f"  Source rows: {stats.source_rows}")
    print(f"  Existing rows: {stats.existing_rows}")
    print(f"  Upserted rows: {stats.upserted_rows}")
    print(f"  Inserted rows: {stats.inserted_rows}")
    print(f"  Updated rows: {stats.updated_rows}")
```

### Incremental Updates with Composite Keys

```python
# Example: Daily data updates for analytics
from datetime import datetime, timedelta

def process_daily_analytics(base_dataset, new_daily_data, date_column):
    """Process daily updates using composite business keys."""
    
    # Business logic composite key
    business_key = ["tenant_id", "event_type", "entity_id", date_column]
    
    # First deduplicate within the daily data
    clean_daily = deduplicate_pyarrow(
        table=new_daily_data,
        key_columns=business_key,
        dedup_order_by=["timestamp"],
        keep="last"
    )
    
    # Then upsert into the main dataset
    stats = merge_parquet_dataset_pyarrow(
        data=clean_daily,
        path=base_dataset,
        strategy="upsert",
        key_columns=business_key
    )
    
    return stats

# Usage example with synthetic daily data
daily_data = {
    "tenant_id": [1, 1, 2, 1, 2],
    "event_type": ["click", "view", "click", "purchase", "view"],
    "entity_id": [1001, 1002, 2001, 1001, 2002],
    "event_date": ["2024-01-15", "2024-01-15", "2024-01-15", "2024-01-15", "2024-01-15"],
    "timestamp": ["2024-01-15 10:00", "2024-01-15 10:01", "2024-01-15 10:02",
                  "2024-01-15 10:03", "2024-01-15 10:04"],
    "properties": ["{...}", "{...}", "{...}", "{...}", "{...}"]
}

daily_table = pa.Table.from_pydict(daily_data)

with tempfile.TemporaryDirectory() as temp_dir:
    analytics_path = os.path.join(temp_dir, "analytics")
    
    # Process the daily data
    stats = process_daily_analytics(
        base_dataset=analytics_path,
        new_daily_data=daily_table,
        date_column="event_date"
    )
    
    print(f"Daily analytics processing complete:")
    print(f"  Processed {stats.source_rows} events")
    print(f"  Added {stats.inserted_rows} new events")
    print(f"  Updated {stats.updated_rows} existing events")
```

## Fallback Scenarios

### Heterogeneous Type Handling

```python
# Example with mixed data types in composite keys
mixed_type_data = {
    "tenant_id": [1, 1, 2, 2, 3],  # int64
    "record_id": ["A001", "A002", "B001", "B002", "C001"],  # string
    "event_timestamp": [1704067200, 1704067260, 1704067320, 1704067380, 1704067440],  # timestamp (int64)
    "status_code": [200, 404, 200, 200, 500],  # int32
    "data": ["value1", "value2", "value3", "value4", "value5"]
}

table = pa.Table.from_pydict(mixed_type_data)

# This will use vectorized approach for compatible types, fallback for complex combos
unique_records = deduplicate_pyarrow(
    table=table,
    key_columns=["tenant_id", "record_id", "event_timestamp"],
    dedup_order_by=["event_timestamp"],
    keep="first"
)

print(f"Processed mixed-type keys successfully: {unique_records.num_rows} unique records")
```

### Manual Fallback to String Keys

```python
# Sometimes you might want to force string-based keys
from fsspeckit.datasets.pyarrow import _create_string_key_array

# Create string-based composite keys manually
string_keys = _create_string_key_array(
    table=table,
    key_columns=["tenant_id", "record_id", "event_timestamp"]
)

print(f"String composite keys: {string_keys.to_pylist()[:3]}...")

# Use for manual filtering when needed
mask = pa.compute.is_in(string_keys, value_set=string_keys[:2])  # Example filter
filtered_table = table.filter(mask)
```

## Advanced Composite Key Patterns

### Hierarchical Data Keys

```python
# Multi-level hierarchy: region -> site -> device -> sensor
hierarchical_data = {
    "region": ["US-West", "US-West", "US-East", "US-East"],
    "site": ["SF-01", "SF-02", "NY-01", "NY-02"],
    "device_id": ["dev001", "dev002", "dev003", "dev004"],
    "sensor_type": ["temp", "humidity", "temp", "pressure"],
    "timestamp": [1704067200, 1704067260, 1704067320, 1704067380],
    "reading": [22.5, 45.2, 21.8, 1013.25]
}

table = pa.Table.from_pydict(hierarchical_data)

# Deduplicate using full hierarchical key
unique_readings = deduplicate_pyarrow(
    table=table,
    key_columns=["region", "site", "device_id", "sensor_type", "timestamp"],
    keep="first"  # Keep first reading for each sensor at each timestamp
)

print(f"Unique hierarchical readings: {unique_readings.num_rows}")
```

### Time-Series Deduplication

```python
# Deduplicate time-series data with composite keys
timeseries_data = {
    "metric_name": ["cpu_usage", "memory_usage", "cpu_usage", "disk_usage", "cpu_usage"],
    "host_id": ["host001", "host001", "host001", "host001", "host001"],
    "timestamp": [1704067200, 1704067200, 1704067200, 1704067200, 1704067260],
    "value": [75.5, 62.3, 78.1, 45.2, 76.9],
    "collection_time": [1704067205, 1704067205, 1704067210, 1704067205, 1704067265]
}

table = pa.Table.from_pydict(timeseries_data)

# Keep the most recent value for each [metric_name, host_id, timestamp] combination
latest_metrics = deduplicate_pyarrow(
    table=table,
    key_columns=["metric_name", "host_id", "timestamp"],
    dedup_order_by=["collection_time"],
    keep="last"
)

print("Latest metrics per timestamp:")
print(latest_metrics.to_pandas().sort_values(["metric_name", "timestamp"]))
```

## Performance Monitoring Examples

### Benchmarking Composite Key Operations

```python
import time
from fsspeckit.datasets.pyarrow.dataset import PerformanceMonitor

def benchmark_composite_keys(table, key_scenarios):
    """Benchmark different composite key scenarios."""
    
    monitor = PerformanceMonitor()
    
    results = {}
    
    for scenario_name, key_columns in key_scenarios.items():
        print(f"\nTesting scenario: {scenario_name}")
        print(f"Key columns: {key_columns}")
        
        # Reset monitor for each scenario
        monitor.__init__(max_pyarrow_mb=1024)
        monitor.start_op("deduplication")
        
        start_time = time.time()
        
        try:
            result = deduplicate_pyarrow(
                table=table,
                key_columns=key_columns,
                dedup_order_by=["timestamp"] if "timestamp" in table.column_names else None,
                keep="first"
            )
            
            end_time = time.time()
            monitor.end_op()
            
            # Get metrics
            metrics = monitor.get_metrics(
                total_rows_before=table.num_rows,
                total_rows_after=result.num_rows,
                total_bytes=table.nbytes
            )
            
            results[scenario_name] = {
                "duration": end_time - start_time,
                "rows_before": table.num_rows,
                "rows_after": result.num_rows,
                "duplicates_removed": table.num_rows - result.num_rows,
                "memory_peak_mb": metrics["memory_peak_mb"],
                "throughput_rows_sec": metrics["rows_per_sec"]
            }
            
            print(f"  Duration: {end_time - start_time:.2f}s")
            print(f"  Rows removed: {table.num_rows - result.num_rows}")
            print(f"  Peak memory: {metrics['memory_peak_mb']:.1f} MB")
            print(f"  Throughput: {metrics['rows_per_sec']:.0f} rows/sec")
            
        except Exception as e:
            print(f"  Failed: {e}")
            results[scenario_name] = {"error": str(e)}
    
    return results

# Create test data
test_data = {
    "id1": [i % 100 for i in range(10000)],
    "id2": [i % 50 for i in range(10000)],
    "id3": [i % 25 for i in range(10000)],
    "value": list(range(10000)),
    "timestamp": [1704067200 + i for i in range(10000)]
}

test_table = pa.Table.from_pydict(test_data)

# Test different key scenarios
key_scenarios = {
    "single_key": ["id1"],
    "dual_key": ["id1", "id2"],
    "triple_key": ["id1", "id2", "id3"],
    "four_key": ["id1", "id2", "id3", "timestamp"]
}

results = benchmark_composite_keys(test_table, key_scenarios)

# Compare performance
print("\n=== Performance Comparison ===")
for scenario, metrics in results.items():
    if "error" not in metrics:
        print(f"{scenario:12}: {metrics['duration']:6.2f}s, "
              f"{metrics['throughput_rows_sec']:8.0f} rows/sec, "
              f"{metrics['memory_peak_mb']:6.1f}MB memory")
    else:
        print(f"{scenario:12}: FAILED - {metrics['error']}")
```

### Memory-Optimized Processing

```python
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor

def process_large_dataset_with_monitoring(dataset_path, key_columns):
    """Process large dataset with active memory monitoring."""
    
    memory_monitor = MemoryMonitor(
        max_pyarrow_mb=1024,  # Limit PyArrow to 1GB
        min_system_available_mb=512  # Keep 512MB free for system
    )
    
    stats = {
        "chunks_processed": 0,
        "total_rows": 0,
        "memory_pressure_events": 0,
        "max_memory_mb": 0
    }
    
    try:
        # Process with memory monitoring
        result = deduplicate_pyarrow(
            path=dataset_path,
            key_columns=key_columns,
            chunk_size="50MB",  # Small chunks for memory efficiency
            memory_monitor=memory_monitor,
            progress_callback=lambda chunk: update_stats(stats, chunk)
        )
        
        print(f"Successfully processed {stats['total_rows']:,} rows")
        print(f"Peak memory usage: {stats['max_memory_mb']:.1f} MB")
        print(f"Memory pressure events: {stats['memory_pressure_events']}")
        
        return result
        
    except MemoryError as e:
        print(f"Memory limit reached: {e}")
        
        # Fallback: process with even smaller chunks
        print("Retrying with reduced chunk size...")
        return deduplicate_pyarrow(
            path=dataset_path,
            key_columns=key_columns,
            chunk_size="25MB",  # Even smaller chunks
            memory_monitor=memory_monitor
        )

def update_stats(stats, chunk_info):
    """Update processing statistics."""
    stats["chunks_processed"] += 1
    stats["total_rows"] += chunk_info.get("rows", 0)
    
    # Monitor memory pressure
    memory_status = memory_monitor.get_memory_status()
    current_memory = memory_status.get("pyarrow_allocated_mb", 0)
    if current_memory > stats["max_memory_mb"]:
        stats["max_memory_mb"] = current_memory
    
    # Check for memory pressure
    pressure_level = memory_monitor.check_memory_pressure()
    if pressure_level.value != "green":
        stats["memory_pressure_events"] += 1
```

## Best Practices

### 1. Key Column Ordering

```python
# Good: High cardinality columns first
optimal_keys = ["user_id", "tenant_id", "timestamp"]  # user_id most selective

# Less optimal: Low cardinality first  
suboptimal_keys = ["status", "tenant_id", "user_id"]  # status least selective
```

### 2. Chunk Size Selection

```python
# For memory-constrained environments
small_chunks = "25MB"  # ~1-2M rows per chunk

# For performance-optimized environments  
large_chunks = "200MB"  # ~8-10M rows per chunk

# For balanced approach
balanced_chunks = "100MB"  # ~4-5M rows per chunk
```

### 3. Error Handling

```python
def robust_deduplication(table, key_columns):
    """Deduplicate with comprehensive error handling."""
    
    try:
        # Try vectorized approach first
        return deduplicate_pyarrow(
            table=table,
            key_columns=key_columns,
            chunk_size="100MB"
        )
    except Exception as e:
        print(f"Vectorized deduplication failed: {e}")
        
        # Fallback to smaller chunks
        try:
            return deduplicate_pyarrow(
                table=table,
                key_columns=key_columns,
                chunk_size="25MB"
            )
        except Exception as e2:
            print(f"Small chunk fallback failed: {e2}")
            raise RuntimeError(f"All deduplication attempts failed: {e2}")
```

For performance characteristics and optimization details, see [Multi-Key Performance Guide](./multi-key-performance.md).

For complete API reference, see [PyArrow Dataset API](../api/fsspeckit.datasets.md).