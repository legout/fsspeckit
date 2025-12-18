# Adaptive Key Tracking for High-Cardinality Data

Learn how to use the AdaptiveKeyTracker to handle efficient deduplication across datasets of varying cardinalities while maintaining memory bounds.

## Overview

The AdaptiveKeyTracker automatically adapts to your data's cardinality, providing:
- **Exact tracking** for low-cardinality data (100% accuracy)
- **LRU caching** for medium-cardinality data (high accuracy, bounded memory)
- **Bloom filtering** for high-cardinality data (probabilistic accuracy, fixed memory)

## Quick Start

### Basic Usage
```python
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

# Create tracker with default settings
tracker = AdaptiveKeyTracker()

# Process streaming data
for record in data_stream:
    key = (record['user_id'], record['timestamp'])  # Composite key
    if key not in tracker:
        tracker.add(key)
        process_new_record(record)
    else:
        handle_duplicate(record)

# Check performance metrics
metrics = tracker.get_metrics()
print(f"Processed {metrics['unique_keys_estimate']} unique keys")
print(f"Current tier: {metrics['tier']}")
```

## Configuration for Different Data Scenarios

### Low Cardinality (< 100K unique keys)
Perfect for:
- Daily transaction logs
- User session data
- Small to medium datasets

```python
tracker = AdaptiveKeyTracker(
    max_exact_keys=200_000,     # Generous exact tier
    max_lru_keys=1_000_000,     # Large LRU backup
    false_positive_rate=0.001  # Standard accuracy
)

# Expected behavior: Stays in EXACT tier
# Memory usage: ~72 bytes per key
# Accuracy: 100%
```

### Medium Cardinality (100K - 10M unique keys)
Perfect for:
- Monthly user activity logs
- E-commerce order histories
- Sensor data from multiple devices

```python
tracker = AdaptiveKeyTracker(
    max_exact_keys=500_000,     # Moderate exact tier
    max_lru_keys=5_000_000,     # Substantial LRU cache
    false_positive_rate=0.001
)

# Expected behavior: May transition to LRU tier
# Memory usage: Bounded at ~360MB for 5M keys
# Accuracy: ~95-99% (depends on access patterns)
```

### High Cardinality (> 10M unique keys)
Perfect for:
- Global clickstream data
- Large-scale IoT sensor networks
- Web crawling deduplication

```python
tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,     # Small exact tier for hot keys
    max_lru_keys=1_000_000,     # Limited LRU for recent keys
    false_positive_rate=0.0001  # High accuracy for Bloom tier
)

# Expected behavior: Transitions to Bloom tier
# Memory usage: Fixed at ~20-50MB regardless of key count
# Accuracy: 99.99% (configurable)
```

## Real-World Examples

### Example 1: User Activity Deduplication

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

def deduplicate_user_activity(activity_stream):
    """Remove duplicate user activities while preserving first occurrence."""
    
    # Configure for expected ~1M daily active users
    tracker = AdaptiveKeyTracker(
        max_exact_keys=2_000_000,
        max_lru_keys=5_000_000,
        false_positive_rate=0.001
    )
    
    deduplicated_activities = []
    
    for batch in activity_stream:
        # Create composite keys for deduplication
        user_ids = batch['user_id'].to_pylist()
        activity_types = batch['activity_type'].to_pylist()
        timestamps = batch['timestamp'].to_pylist()
        
        for i, (user_id, activity_type, timestamp) in enumerate(
            zip(user_ids, activity_types, timestamps)
        ):
            key = (user_id, activity_type, timestamp // 3600)  # Hourly granularity
            
            if key not in tracker:
                tracker.add(key)
                deduplicated_activities.append(batch[i])
            else:
                print(f"Duplicate activity: {key}")
    
    # Report metrics
    metrics = tracker.get_metrics()
    print(f"Deduplication complete:")
    print(f"  - Total processed: {metrics['total_add_calls']}")
    print(f"  - Unique activities: {metrics['unique_keys_estimate']}")
    print(f"  - Final tier: {metrics['tier']}")
    print(f"  - Tier transitions: {metrics['transitions']}")
    
    return pa.Table.from_arrays(deduplicated_activities, schema=batch.schema)

# Usage
activity_stream = read_activity_batches()
deduplicated = deduplicate_user_activity(activity_stream)
```

### Example 2: Memory-Constrained Environment

```python
import psutil
import os
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

def memory_constrained_deduplication(data_stream, memory_limit_mb=100):
    """Deduplicate with strict memory limits."""
    
    # Very conservative configuration
    tracker = AdaptiveKeyTracker(
        max_exact_keys=10_000,      # Small exact tier
        max_lru_keys=100_000,       # Small LRU cache
        false_positive_rate=0.01   # Higher FP rate for less memory
    )
    
    process = psutil.Process(os.getpid())
    
    for batch in data_stream:
        # Check memory before processing
        current_memory_mb = process.memory_info().rss / 1024 / 1024
        if current_memory_mb > memory_limit_mb:
            print(f"Memory limit exceeded: {current_memory_mb:.1f}MB")
            break
        
        # Process batch
        for record in batch:
            key = record['id']
            if key not in tracker:
                tracker.add(key)
                yield record
        
        # Report status
        metrics = tracker.get_metrics()
        print(f"Batch processed - Tier: {metrics['tier']}, "
              f"Keys: {metrics['unique_keys_estimate']}, "
              f"Memory: {current_memory_mb:.1f}MB")

# Usage
for unique_record in memory_constrained_deduplication(large_stream):
    process_record(unique_record)
```

### Example 3: Multi-Column Key Vectorization

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

def vectorized_multi_column_deduplication(table, key_columns):
    """High-performance multi-column deduplication using vectorization."""
    
    tracker = AdaptiveKeyTracker(
        max_exact_keys=1_000_000,
        max_lru_keys=10_000_000,
        false_positive_rate=0.001
    )
    
    # Extract key columns as lists for vectorized processing
    key_arrays = [table[col] for col in key_columns]
    
    # Create composite keys vectorized
    # Note: This is a simplified example - actual implementation may vary
    mask = pa.array([True] * len(table))
    deduplicated_rows = []
    
    for i in range(len(table)):
        # Extract key values for this row
        key = tuple(arr[i].as_py() for arr in key_arrays)
        
        if key not in tracker:
            tracker.add(key)
            deduplicated_rows.append(i)
        # else: duplicate, skip
    
    # Create deduplicated table
    deduplicated_table = table.take(deduplicated_rows)
    
    metrics = tracker.get_metrics()
    print(f"Vectorized deduplication:")
    print(f"  - Original rows: {len(table)}")
    print(f"  - Deduplicated rows: {len(deduplicated_table)}")
    print(f"  - Final tier: {metrics['tier']}")
    
    return deduplicated_table

# Usage
data = pa.Table.from_pydict({
    'user_id': [1, 2, 1, 3, 2, 4],
    'event_id': [100, 101, 100, 102, 103, 104],
    'timestamp': [1000, 1001, 1002, 1003, 1004, 1005],
    'data': ['a', 'b', 'c', 'd', 'e', 'f']
})

deduplicated = vectorized_multi_column_deduplication(data, ['user_id', 'event_id'])
```

## Monitoring and Metrics

### Real-Time Monitoring
```python
import time
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

class MonitoredTracker:
    def __init__(self, **config):
        self.tracker = AdaptiveKeyTracker(**config)
        self.start_time = time.time()
        self.last_report = time.time()
        self.report_interval = 60  # Report every minute
    
    def add_and_monitor(self, key):
        """Add key and report metrics periodically."""
        self.tracker.add(key)
        
        current_time = time.time()
        if current_time - self.last_report > self.report_interval:
            self.report_metrics()
            self.last_report = current_time
    
    def report_metrics(self):
        """Report current tracker metrics."""
        metrics = self.tracker.get_metrics()
        elapsed = time.time() - self.start_time
        
        print(f"\n=== Tracker Report (t={elapsed:.1f}s) ===")
        print(f"Tier: {metrics['tier']}")
        print(f"Keys processed: {metrics['total_add_calls']}")
        print(f"Unique keys: {metrics['unique_keys_estimate']}")
        print(f"Transitions: {metrics['transitions']}")
        print(f"Keys/sec: {metrics['total_add_calls'] / elapsed:.1f}")
        
        if 'current_count' in metrics:
            print(f"Current tracked: {metrics['current_count']}")

# Usage
monitored = MonitoredTracker(
    max_exact_keys=100_000,
    max_lru_keys=1_000_000
)

for key in large_key_stream:
    monitored.add_and_monitor(key)
```

### Memory Usage Analysis
```python
import tracemalloc
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

def analyze_memory_usage():
    """Analyze memory usage patterns across different configurations."""
    
    configurations = [
        {'max_exact_keys': 1000, 'max_lru_keys': 10000, 'name': 'Small'},
        {'max_exact_keys': 100000, 'max_lru_keys': 1000000, 'name': 'Medium'},
        {'max_exact_keys': 1000000, 'max_lru_keys': 10000000, 'name': 'Large'},
    ]
    
    test_keys = [f"key_{i}" for i in range(500000)]  # 500K test keys
    
    for config in configurations:
        tracemalloc.start()
        
        tracker = AdaptiveKeyTracker(
            max_exact_keys=config['max_exact_keys'],
            max_lru_keys=config['max_lru_keys']
        )
        
        # Add keys
        for key in test_keys:
            tracker.add(key)
        
        # Get memory snapshot
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics = tracker.get_metrics()
        
        print(f"\n{config['name']} Configuration:")
        print(f"  Final tier: {metrics['tier']}")
        print(f"  Unique keys: {metrics['unique_keys_estimate']}")
        print(f"  Current memory: {current / 1024 / 1024:.1f} MB")
        print(f"  Peak memory: {peak / 1024 / 1024:.1f} MB")
        print(f"  Memory per key: {current / metrics['unique_keys_estimate']:.1f} bytes")

analyze_memory_usage()
```

## Performance Optimization

### Batch Processing
```python
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

def batch_process_deduplication(data_stream, batch_size=10000):
    """Process data in batches for better performance."""
    
    tracker = AdaptiveKeyTracker(
        max_exact_keys=500_000,
        max_lru_keys=5_000_000
    )
    
    batch = []
    deduplicated_count = 0
    
    for record in data_stream:
        key = record['id']
        
        if key not in tracker:
            tracker.add(key)
            batch.append(record)
            deduplicated_count += 1
        
        # Process batch when full
        if len(batch) >= batch_size:
            yield from process_batch(batch)
            batch = []
    
    # Process remaining records
    if batch:
        yield from process_batch(batch)
    
    metrics = tracker.get_metrics()
    print(f"Batch processing complete:")
    print(f"  - Deduplicated: {deduplicated_count}")
    print(f"  - Final tier: {metrics['tier']}")

def process_batch(batch):
    """Process a batch of deduplicated records."""
    # Your batch processing logic here
    return batch
```

### Parallel Processing
```python
import threading
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

class ThreadSafeDeduplicator:
    def __init__(self, **config):
        self.tracker = AdaptiveKeyTracker(**config)
        self.lock = threading.Lock()
    
    def process_record(self, record):
        """Thread-safe record processing."""
        key = record['id']
        
        with self.lock:
            if key not in self.tracker:
                self.tracker.add(key)
                return True
            return False
    
    def process_parallel(self, data_stream, num_workers=4):
        """Process data with multiple worker threads."""
        import queue
        
        work_queue = queue.Queue()
        result_queue = queue.Queue()
        
        # Fill work queue
        for record in data_stream:
            work_queue.put(record)
        
        def worker():
            while True:
                try:
                    record = work_queue.get(timeout=1)
                    if self.process_record(record):
                        result_queue.put(record)
                    work_queue.task_done()
                except queue.Empty:
                    break
        
        # Start workers
        workers = []
        for _ in range(num_workers):
            t = threading.Thread(target=worker)
            t.start()
            workers.append(t)
        
        # Wait for completion
        work_queue.join()
        
        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())
        
        return results

# Usage
deduplicator = ThreadSafeDeduplicator(
    max_exact_keys=1_000_000,
    max_lru_keys=10_000_000
)

results = deduplicator.process_parallel(data_stream, num_workers=8)
```

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: High Memory Usage
**Problem**: Memory usage grows unexpectedly
**Solution**:
```python
# Check if Bloom filter is available
metrics = tracker.get_metrics()
if not metrics['has_bloom_dependency']:
    print("Warning: Bloom filter unavailable")
    print("Install with: pip install pybloom-live")
    
# Use more aggressive limits
tracker = AdaptiveKeyTracker(
    max_exact_keys=10_000,    # Smaller exact tier
    max_lru_keys=100_000,     # Smaller LRU
    false_positive_rate=0.01  # Higher FP rate
)
```

#### Issue 2: Too Many False Positives
**Problem**: Bloom filter returns too many false positives
**Solution**:
```python
# Reduce false positive rate
tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,
    max_lru_keys=1_000_000,
    false_positive_rate=0.0001  # Lower FP rate
)

# Or stay in LRU tier longer
tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,
    max_lru_keys=10_000_000,  # Larger LRU
    false_positive_rate=0.001
)
```

#### Issue 3: Poor Performance
**Problem**: Slow add/lookup operations
**Solution**:
```python
# Batch operations where possible
batch_keys = [record['id'] for record in batch]
for key in batch_keys:
    tracker.add(key)

# Use simpler keys if possible
# Instead of complex objects as keys
complex_key = (record['user_id'], record['timestamp'], record['event_type'])

# Use hashable primitives
simple_key = f"{record['user_id']}_{record['timestamp']}"
tracker.add(simple_key)
```

## Best Practices

1. **Choose appropriate tier sizes** based on your expected data volume
2. **Monitor metrics** during processing to detect unexpected behavior
3. **Install pybloom-live** for optimal high-cardinality performance
4. **Test with sample data** to tune configuration for your specific use case
5. **Consider key design** - simpler keys perform better
6. **Use batch processing** for better throughput
7. **Monitor memory usage** in production environments
8. **Plan for edge cases** - empty data, very high cardinality, etc.

## Integration with fsspeckit

The AdaptiveKeyTracker is integrated into PyArrow streaming operations for automatic deduplication:

```python
from fsspeckit.datasets import PyarrowDatasetHandler

handler = PyarrowDatasetHandler()

# Streaming deduplication happens automatically
# using AdaptiveKeyTracker internally
result = handler.merge(
    data=new_data,
    path="s3://bucket/existing-dataset/",
    strategy="upsert",
    key_columns=["user_id", "timestamp"]
)

# The merge operation uses AdaptiveKeyTracker internally
# for efficient memory-bounded deduplication
```