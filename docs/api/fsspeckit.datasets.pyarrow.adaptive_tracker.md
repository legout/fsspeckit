# `fsspeckit.datasets.pyarrow.adaptive_tracker` API Reference

## Overview

The `AdaptiveKeyTracker` provides tiered memory management for tracking unique keys during streaming operations. It automatically adapts to data cardinality, transitioning between three tiers to balance accuracy and memory usage.

## Tiers

### Tier 1: Exact (Set)
- **Memory**: Uses Python `set()` for exact key tracking
- **Accuracy**: 100% accurate, no false positives or negatives
- **Capacity**: Up to `max_exact_keys` unique keys
- **Use Case**: Low to medium cardinality datasets where exact deduplication is required

### Tier 2: LRU (OrderedDict)
- **Memory**: Uses Python `OrderedDict` with LRU eviction
- **Accuracy**: Exact for keys in cache, false negatives for evicted keys
- **Capacity**: Up to `max_lru_keys` unique keys
- **Use Case**: Medium to high cardinality where occasional false negatives are acceptable

### Tier 3: Bloom Filter (Probabilistic)
- **Memory**: Uses `ScalableBloomFilter` for fixed memory footprint
- **Accuracy**: Configurable false positive rate, no false negatives
- **Capacity**: Unlimited growth with scalable filters
- **Use Case**: Very high cardinality where probabilistic accuracy is acceptable

## AdaptiveKeyTracker

### Constructor

```python
AdaptiveKeyTracker(
    max_exact_keys: int = 1_000_000,
    max_lru_keys: int = 10_000_000,
    false_positive_rate: float = 0.001,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_exact_keys` | `int` | `1_000_000` | Maximum number of keys to track exactly in the Exact tier |
| `max_lru_keys` | `int` | `10_000_000` | Maximum number of keys to track in the LRU tier before transitioning to Bloom |
| `false_positive_rate` | `float` | `0.001` | Target false positive rate for Bloom filter tier (0.0-1.0) |

### Methods

#### `add(key: Any) -> None`

Add a key to the tracker. Handles automatic tier transitions and key normalization.

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `Any` | The key to track. Can be single value, tuple, or list (converted to tuple) |

**Notes:**
- Lists are automatically converted to tuples for hashability
- Thread-safe with internal locking
- May trigger tier transitions if limits are exceeded

#### `__contains__(key: Any) -> bool`

Check if a key has been seen before using the current tier's logic.

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `Any` | The key to check for membership |

**Returns:**
- `True`: Key is present according to current tier logic
- `False`: Key is not present (Bloom tier may have false positives)

**Notes:**
- In LRU tier, successful lookups refresh the key's position
- Thread-safe operation

#### `get_metrics() -> Dict[str, Any]`

Get comprehensive metrics about the tracker's current state and performance.

**Returns:** Dictionary containing:
- `tier`: Current active tier (`"EXACT"`, `"LRU"`, or `"BLOOM"`)
- `total_add_calls`: Total number of `add()` operations
- `unique_keys_estimate`: Estimated number of unique keys seen
- `transitions`: Number of tier transitions performed
- `has_bloom_dependency`: Boolean indicating Bloom filter availability

**Tier-specific fields:**
- **Exact tier**: `current_count`, `accuracy_type="exact"`
- **LRU tier**: `current_count`, `accuracy_type="bounded_lru"`
- **Bloom tier**: `accuracy_type="probabilistic"`, `false_positive_rate_target`

## Configuration Guidelines

### Low Cardinality (< 100K keys)
```python
tracker = AdaptiveKeyTracker(
    max_exact_keys=200_000,  # Generous exact tier
    max_lru_keys=1_000_000,  # Large LRU backup
    false_positive_rate=0.001
)
```

### Medium Cardinality (100K - 10M keys)
```python
tracker = AdaptiveKeyTracker(
    max_exact_keys=500_000,   # Moderate exact tier
    max_lru_keys=5_000_000,    # Substantial LRU cache
    false_positive_rate=0.001
)
```

### High Cardinality (> 10M keys)
```python
tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,   # Small exact tier for hot keys
    max_lru_keys=1_000_000,    # Limited LRU for recent keys
    false_positive_rate=0.0001  # Lower false positive rate
)
```

### Memory-Constrained Environments
```python
tracker = AdaptiveKeyTracker(
    max_exact_keys=10_000,     # Very small exact tier
    max_lru_keys=100_000,       # Small LRU cache
    false_positive_rate=0.01    # Higher false positive rate for smaller memory
)
```

## Dependency Requirements

### Optional Dependency
- **pybloom-live**: Required for Bloom filter tier
- **Installation**: `pip install pybloom-live`

### Fallback Behavior
If `pybloom-live` is not available:
- Tracker stays in LRU tier after `max_lru_keys` is reached
- Memory usage may exceed expected bounds for very high cardinality
- Accuracy remains exact for keys in LRU cache

## Thread Safety

- All operations are thread-safe using internal `threading.Lock`
- Concurrent reads and writes are supported
- Lock is held for minimal duration to maximize throughput

## Performance Characteristics

| Tier | Add Performance | Lookup Performance | Memory per Key | Accuracy |
|------|-----------------|-------------------|---------------|----------|
| Exact | O(1) | O(1) | ~72 bytes | 100% |
| LRU | O(1) | O(1) | ~72 bytes | ~95-99%* |
| Bloom | O(k) | O(k) | ~10-20 bits | 99.9%† |

*\* Depends on access pattern and cache hit ratio*  
*† Configurable via `false_positive_rate` parameter*

## Integration Examples

### With PyArrow Streaming
```python
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

tracker = AdaptiveKeyTracker(max_exact_keys=100_000, max_lru_keys=1_000_000)

# Process streaming data
for batch in table_stream:
    # Extract key columns for deduplication
    keys = batch.select(['user_id', 'timestamp']).to_pylist()
    
    # Check for duplicates
    for i, key in enumerate(keys):
        if key in tracker:
            # Handle duplicate
            continue
        tracker.add(key)
        
        # Process new record
        process_record(batch[i])
```

### Memory Monitoring
```python
import psutil
import os

def monitor_deduplication(data_stream, tracker):
    process = psutil.Process(os.getpid())
    
    for batch in data_stream:
        tracker.add_batch(batch['dedup_key'])
        
        # Monitor memory usage
        metrics = tracker.get_metrics()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        print(f"Tier: {metrics['tier']}, "
              f"Keys: {metrics['unique_keys_estimate']}, "
              f"Memory: {memory_mb:.1f}MB")
```

## Error Handling

### Common Issues

1. **Unhashable Keys**
   ```python
   # Bad: Lists cause errors
   tracker.add([1, 2, 3])  # Converted to tuple automatically
   
   # Good: Use tuples or hashable types
   tracker.add((1, 2, 3))
   ```

2. **Memory Exhaustion**
   ```python
   # Monitor metrics to detect issues
   metrics = tracker.get_metrics()
   if metrics['tier'] == 'LRU' and not metrics['has_bloom_dependency']:
       logger.warning("Bloom filter unavailable - memory may grow unbounded")
   ```

3. **High False Positive Rate**
   ```python
   # Tune for your accuracy requirements
   tracker = AdaptiveKeyTracker(
       false_positive_rate=0.0001  # Higher accuracy, more memory
   )
   ```

## Best Practices

1. **Choose appropriate tier sizes** based on expected data volume
2. **Monitor metrics** during processing to detect unexpected patterns
3. **Install pybloom-live** for optimal high-cardinality performance
4. **Test with sample data** to tune configuration for your use case
5. **Consider memory constraints** when setting tier limits

## Migration from Simple Set

### Before
```python
seen_keys = set()
for key in key_stream:
    if key not in seen_keys:
        seen_keys.add(key)
        process_key(key)
```

### After
```python
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

tracker = AdaptiveKeyTracker(max_exact_keys=1_000_000)
for key in key_stream:
    if key not in tracker:
        tracker.add(key)
        process_key(key)
        
# Get insights
metrics = tracker.get_metrics()
print(f"Processed {metrics['unique_keys_estimate']} unique keys in {metrics['tier']} tier")
```