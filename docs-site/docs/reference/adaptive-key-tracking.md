# AdaptiveKeyTracker Reference Guide

Comprehensive technical reference for understanding accuracy trade-offs, memory characteristics, and performance optimization strategies for the AdaptiveKeyTracker.

## Architecture Overview

The AdaptiveKeyTracker implements a three-tier memory management system that automatically transitions between strategies based on data cardinality:

```
Low Cardinality     →     Medium Cardinality     →     High Cardinality
     |                          |                           |
     V                          V                           V
[ EXACT TIER ]         [ LRU TIER ]            [ BLOOM TIER ]
     |                          |                           |
   100% Accurate         ~95-99% Accurate        99.9%+ Accurate
     |                          |                           |
  Bounded Memory          Bounded Memory         Fixed Memory
```

## Tier Characteristics

### 1. Exact Tier (Set-Based)

**Implementation**: Python `set()` data structure

**Memory Characteristics**:
- **Per-key overhead**: ~72 bytes (Python object overhead)
- **Memory growth**: Linear with unique key count
- **Maximum capacity**: `max_exact_keys` configuration parameter

**Accuracy Characteristics**:
- **True Positives**: 100% - every seen key is correctly identified
- **False Positives**: 0% - no unseen key is incorrectly marked as seen
- **False Negatives**: 0% - no seen key is incorrectly marked as unseen

**Performance Characteristics**:
- **Add Operation**: O(1) average case
- **Lookup Operation**: O(1) average case
- **Memory Access**: Cache-friendly for small datasets

**Use Case Examples**:
```python
# Perfect for daily logs with known user base
daily_tracker = AdaptiveKeyTracker(
    max_exact_keys=200_000,  # Expect <200K daily active users
    max_lru_keys=1_000_000
)
```

**Memory Calculation**:
```
Memory = unique_keys × 72 bytes
100K keys ≈ 7.2 MB
1M keys ≈ 72 MB
10M keys ≈ 720 MB
```

### 2. LRU Tier (OrderedDict-Based)

**Implementation**: Python `OrderedDict` with LRU eviction policy

**Memory Characteristics**:
- **Per-key overhead**: ~72 bytes (similar to exact tier)
- **Memory growth**: Bounded at `max_lru_keys` configuration
- **Eviction Strategy**: Least Recently Used (LRU)

**Accuracy Characteristics**:
- **True Positives**: 100% for keys in cache
- **False Positives**: 0% - LRU never produces false positives
- **False Negatives**: Proportional to cache miss rate

**Accuracy Formula**:
```
Accuracy = 1 - (eviction_rate)
eviction_rate = (unique_keys - max_lru_keys) / unique_keys
```

**Real-World Accuracy Examples**:
```
Scenario: 1M unique keys, 100K LRU capacity
- Hot keys (last 100K): 100% accuracy
- Cold keys (older): 0% accuracy (evicted)
- Overall accuracy: 100K / 1M = 10%
```

**Access Pattern Impact**:
```python
# Temporal locality improves accuracy
for timestamp, data in sorted(data, key='timestamp'):  # Chronological order
    key = data['user_id']
    if key not in tracker:  # High hit rate for recent users
        tracker.add(key)
        process_data(data)

# Random access reduces accuracy  
for data in random_order(data):  # Random order
    key = data['user_id']
    if key not in tracker:  # Lower hit rate
        tracker.add(key)
        process_data(data)
```

**Performance Characteristics**:
- **Add Operation**: O(1) amortized
- **Lookup Operation**: O(1) amortized  
- **Cache Update**: O(1) for successful lookups

### 3. Bloom Filter Tier (Probabilistic)

**Implementation**: `ScalableBloomFilter` from pybloom-live

**Memory Characteristics**:
- **Per-key overhead**: ~10-20 bits (1.25-2.5 bytes)
- **Memory growth**: Fixed regardless of key count
- **Scalability**: Automatically adds new filter layers when capacity reached

**Accuracy Characteristics**:
- **True Positives**: 100% - all seen keys are correctly identified
- **False Positives**: Configurable via `false_positive_rate` parameter
- **False Negatives**: 0% - Bloom filters never produce false negatives

**False Positive Rate Impact**:
```
False Positive Rate | Memory per Key | Practical Use Cases
------------------- | -------------- | -------------------
1.0% (0.01)         | ~2.5 bytes     | High-throughput, tolerant of duplicates
0.1% (0.001)        | ~3.75 bytes    | Production systems, balanced approach
0.01% (0.0001)      | ~5.0 bytes     | High-accuracy requirements
0.001% (0.00001)    | ~6.25 bytes    | Critical deduplication scenarios
```

**Memory Calculation**:
```
Memory = unique_keys × bits_per_key / 8
10M keys × 10 bits/key ÷ 8 = 12.5 MB
10M keys × 20 bits/key ÷ 8 = 25 MB
```

**Collision Mathematics**:
```
False Positive Rate ≈ (1 - e^(-k×n/m))^k

Where:
- k = number of hash functions
- n = number of items in filter  
- m = number of bits in filter
```

**Scalable Bloom Filter Behavior**:
```python
# Demonstrates how scalable filters work
from fsspeckit.datasets.pyarrow.adaptive_tracker import ScalableBloomFilter

# Initial capacity with error rate 0.001
filter = ScalableBloomFilter(initial_capacity=1000, error_rate=0.001)

# Adding more than initial capacity automatically scales
for i in range(10000):  # 10x initial capacity
    filter.add(f"key_{i}")
    
# Accuracy remains ~99.9% despite growth
```

## Configuration Trade-offs

### Memory vs Accuracy Matrix

| Configuration | Memory (10M keys) | Accuracy | Best For |
|---------------|------------------|----------|----------|
| Exact Only | ~720 MB | 100% | Small datasets, exact deduplication |
| LRU 1M | ~72 MB | 10-90%* | Temporal locality, medium datasets |
| Bloom 0.1% | ~37.5 MB | 99.9% | Large datasets, high accuracy |
| Bloom 1% | ~12.5 MB | 99% | Very large datasets, tolerant |

*\*LRU accuracy varies with access patterns*

### Decision Framework

#### Choose Exact Tier When:
```python
# Scenario: Small, critical dataset
scenario_tracker = AdaptiveKeyTracker(
    max_exact_keys=5_000_000,  # Large enough for expected cardinality
    max_lru_keys=10_000_000,   # Fallback capacity
    false_positive_rate=0.001  # Not used but keep sensible default
)

# Characteristics:
# - Expected unique keys < 5M
# - 100% accuracy required
# - Memory budget > 360 MB
# - Processing time not critical
```

#### Choose LRU Configuration When:
```python
# Scenario: Medium dataset with temporal patterns
temporal_tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,    # Small hot set
    max_lru_keys=2_000_000,    # Large recent set
    false_positive_rate=0.001  # Fallback option
)

# Characteristics:
# - Expected unique keys 2-20M
# - High temporal locality (recent keys accessed more)
# - Memory budget ~150 MB
# - Accept some false negatives for old keys
```

#### Choose Bloom Configuration When:
```python
# Scenario: Large dataset, accuracy-critical
large_tracker = AdaptiveKeyTracker(
    max_exact_keys=10_000,     # Very small hot set
    max_lru_keys=100_000,      # Small recent set
    false_positive_rate=0.0001 # Very high accuracy
)

# Characteristics:
# - Expected unique keys > 20M
# - High accuracy required (>99.99%)
# - Memory budget < 100 MB
# - Can tolerate few false positives
```

## Performance Analysis

### Benchmark Results (Synthetic Data)

```python
import time
import psutil
import os
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

def benchmark_tracker(num_keys, config):
    """Benchmark tracker performance with given configuration."""
    process = psutil.Process(os.getpid())
    
    tracker = AdaptiveKeyTracker(**config)
    
    # Benchmark additions
    start_time = time.time()
    start_memory = process.memory_info().rss
    
    for i in range(num_keys):
        tracker.add(f"benchmark_key_{i}")
    
    add_time = time.time() - start_time
    add_memory = process.memory_info().rss - start_memory
    
    # Benchmark lookups (50% hit rate)
    start_time = time.time()
    
    for i in range(num_keys):
        # Mix of existing and non-existing keys
        if i % 2 == 0:
            _ = f"benchmark_key_{i}" in tracker
        else:
            _ = f"non_existing_key_{i}" in tracker
    
    lookup_time = time.time() - start_time
    
    metrics = tracker.get_metrics()
    
    return {
        'config': config,
        'num_keys': num_keys,
        'final_tier': metrics['tier'],
        'add_time': add_time,
        'lookup_time': lookup_time,
        'memory_mb': add_memory / 1024 / 1024,
        'memory_per_key_bytes': add_memory / num_keys if num_keys > 0 else 0,
        'adds_per_sec': num_keys / add_time,
        'lookups_per_sec': num_keys / lookup_time,
        'metrics': metrics
    }

# Run benchmarks
results = []
test_configurations = [
    {'max_exact_keys': 1000000, 'max_lru_keys': 5000000, 'false_positive_rate': 0.001},
    {'max_exact_keys': 100000, 'max_lru_keys': 1000000, 'false_positive_rate': 0.001},
    {'max_exact_keys': 10000, 'max_lru_keys': 100000, 'false_positive_rate': 0.001},
]

for config in test_configurations:
    result = benchmark_tracker(1000000, config)  # 1M keys
    results.append(result)
    
    print(f"Configuration: {config}")
    print(f"  Final tier: {result['final_tier']}")
    print(f"  Add time: {result['add_time']:.2f}s ({result['adds_per_sec']:.0f} ops/sec)")
    print(f"  Lookup time: {result['lookup_time']:.2f}s ({result['lookups_per_sec']:.0f} ops/sec)")
    print(f"  Memory: {result['memory_mb']:.1f}MB ({result['memory_per_key_bytes']:.1f} bytes/key)")
    print()
```

### Expected Performance Characteristics

| Operation | Exact Tier | LRU Tier | Bloom Tier |
|-----------|------------|----------|------------|
| **Add** | 50-100K ops/sec | 40-80K ops/sec | 30-60K ops/sec |
| **Lookup** | 200-500K ops/sec | 150-400K ops/sec | 100-300K ops/sec |
| **Memory/Key** | 72 bytes | 72 bytes | 1.25-2.5 bytes |
| **Thread Safety** | O(1) lock contention | O(1) lock contention | O(1) lock contention |

## Advanced Usage Patterns

### 1. Multi-Tracker Configuration

```python
class TieredDeduplicationSystem:
    """Advanced system using multiple trackers for different purposes."""
    
    def __init__(self):
        # Hot tracker for very recent, critical keys
        self.hot_tracker = AdaptiveKeyTracker(
            max_exact_keys=10_000,
            max_lru_keys=50_000,
            false_positive_rate=0.0001
        )
        
        # Warm tracker for recent keys with higher capacity
        self.warm_tracker = AdaptiveKeyTracker(
            max_exact_keys=100_000,
            max_lru_keys=1_000_000,
            false_positive_rate=0.001
        )
        
        # Cold tracker for long-term storage
        self.cold_tracker = AdaptiveKeyTracker(
            max_exact_keys=1_000,
            max_lru_keys=10_000,
            false_positive_rate=0.01
        )
    
    def process_record(self, record, timestamp):
        """Process record through tiered system."""
        key = record['id']
        current_time = timestamp
        
        # Check hot tracker (last hour)
        if key in self.hot_tracker:
            return "hot_duplicate"
        
        # Check warm tracker (last day)  
        if key in self.warm_tracker:
            # Add to hot tracker if recently accessed
            if current_time - record['last_access'] < 3600:
                self.hot_tracker.add(key)
            return "warm_duplicate"
        
        # Check cold tracker (all time)
        if key in self.cold_tracker:
            # Promote to warm tracker
            self.warm_tracker.add(key)
            self.hot_tracker.add(key)
            return "cold_duplicate"
        
        # New record - add to all trackers
        self.hot_tracker.add(key)
        self.warm_tracker.add(key) 
        self.cold_tracker.add(key)
        return "new_record"
```

### 2. Adaptive Configuration

```python
class AdaptiveConfiguration:
    """Dynamically adjusts tracker configuration based on observed patterns."""
    
    def __init__(self, initial_config):
        self.config = initial_config
        self.tracker = AdaptiveKeyTracker(**initial_config)
        self.samples = []
        self.adjustment_interval = 10000
        self.sample_count = 0
    
    def add_with_learning(self, key):
        """Add key and learn from patterns."""
        self.tracker.add(key)
        self.sample_count += 1
        
        # Periodically evaluate and adjust
        if self.sample_count % self.adjustment_interval == 0:
            self.evaluate_and_adjust()
    
    def evaluate_and_adjust(self):
        """Evaluate performance and adjust configuration."""
        metrics = self.tracker.get_metrics()
        
        # If we're transitioning too frequently, adjust limits
        if metrics['transitions'] > 3:
            new_config = self.config.copy()
            
            if metrics['tier'] == 'LRU':
                # Increase LRU capacity to reduce Bloom transitions
                new_config['max_lru_keys'] *= 2
                print(f"Increasing LRU capacity to {new_config['max_lru_keys']}")
                
            elif metrics['tier'] == 'EXACT':
                # Increase exact capacity to reduce LRU transitions
                new_config['max_exact_keys'] *= 2
                print(f"Increasing exact capacity to {new_config['max_exact_keys']}")
            
            # Recreate tracker with new configuration
            # Note: This loses existing state - for production,
            # you'd want to preserve existing keys
            self.tracker = AdaptiveKeyTracker(**new_config)
            self.config = new_config
```

### 3. Memory-Aware Processing

```python
import gc
import psutil
import os

class MemoryAwareTracker:
    """Tracker that monitors memory usage and adjusts accordingly."""
    
    def __init__(self, memory_limit_mb=100, **base_config):
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024
        self.process = psutil.Process(os.getpid())
        self.tracker = AdaptiveKeyTracker(**base_config)
        self.check_interval = 1000
        self.add_count = 0
    
    def add_with_memory_check(self, key):
        """Add key with memory monitoring."""
        self.tracker.add(key)
        self.add_count += 1
        
        # Periodic memory check
        if self.add_count % self.check_interval == 0:
            current_memory = self.process.memory_info().rss
            
            if current_memory > self.memory_limit_bytes:
                self.handle_memory_pressure()
    
    def handle_memory_pressure(self):
        """Handle memory pressure by aggressive configuration."""
        metrics = self.tracker.get_metrics()
        
        print(f"Memory pressure detected! Current tier: {metrics['tier']}")
        
        if metrics['tier'] == 'EXACT':
            # Force transition to LRU to free memory
            self.tracker._transition_to_lru()
            print("Forced transition to LRU tier")
            
        elif metrics['tier'] == 'LRU':
            # Force transition to Bloom if available
            if metrics['has_bloom_dependency']:
                self.tracker._transition_to_bloom()
                print("Forced transition to Bloom tier")
            else:
                # Force garbage collection
                gc.collect()
                print("Forced garbage collection")
        
        # Trigger immediate garbage collection
        gc.collect()
```

## Error Handling and Edge Cases

### 1. Key Hashability Issues

```python
def safe_key_normalization(key):
    """Safely normalize potentially problematic keys."""
    try:
        # Convert lists to tuples
        if isinstance(key, list):
            return tuple(key)
        
        # Handle unhashable types
        if isinstance(key, dict):
            # Convert dict to tuple of sorted items
            return tuple(sorted(key.items()))
        
        # Check hashability
        hash(key)  # Will raise TypeError if unhashable
        return key
        
    except (TypeError, AttributeError):
        # Fallback to string representation
        return str(key)

# Usage with AdaptiveKeyTracker
def safe_add(tracker, key):
    """Safely add key to tracker with normalization."""
    safe_key = safe_key_normalization(key)
    tracker.add(safe_key)
```

### 2. Bloom Filter Unavailability

```python
def create_robust_tracker(**config):
    """Create tracker with graceful Bloom filter fallback."""
    try:
        from pybloom_live import ScalableBloomFilter
        return AdaptiveKeyTracker(**config)
    except ImportError:
        print("Warning: pybloom-live not available")
        print("Install with: pip install pybloom-live")
        print("Using LRU-only configuration")
        
        # Adjust configuration for LRU-only operation
        lru_config = {
            'max_exact_keys': config.get('max_exact_keys', 100_000),
            'max_lru_keys': max(config.get('max_lru_keys', 1_000_000), 
                              config.get('max_exact_keys', 100_000) * 10)
        }
        
        return AdaptiveKeyTracker(**lru_config)
```

### 3. Large Key Handling

```python
def optimize_large_keys(data_stream):
    """Optimize handling of large composite keys."""
    
    tracker = AdaptiveKeyTracker(
        max_exact_keys=100_000,
        max_lru_keys=1_000_000,
        false_positive_rate=0.001
    )
    
    for record in data_stream:
        # Create compact key representation
        key_parts = []
        
        # Use hash instead of large strings when possible
        if len(str(record.get('large_field', ''))) > 100:
            key_parts.append(hash(record['large_field']))
        else:
            key_parts.append(record.get('large_field'))
        
        # Truncate very long fields
        text_field = record.get('text_field', '')
        if len(text_field) > 50:
            key_parts.append(text_field[:50])
        else:
            key_parts.append(text_field)
        
        # Keep small fields as-is
        key_parts.extend([
            record.get('user_id'),
            record.get('timestamp')
        ])
        
        # Create optimized key
        optimized_key = tuple(key_parts)
        
        if optimized_key not in tracker:
            tracker.add(optimized_key)
            process_record(record)
```

## Integration Best Practices

### 1. Monitoring Integration

```python
import logging
from contextlib import contextmanager

@contextmanager
def tracked_operation(tracker, operation_name):
    """Context manager for monitoring tracker operations."""
    start_metrics = tracker.get_metrics()
    start_time = time.time()
    
    try:
        yield tracker
    finally:
        end_time = time.time()
        end_metrics = tracker.get_metrics()
        
        duration = end_time - start_time
        adds = end_metrics['total_add_calls'] - start_metrics['total_add_calls']
        transitions = end_metrics['transitions'] - start_metrics['transitions']
        
        logging.info(f"Operation '{operation_name}' completed:")
        logging.info(f"  Duration: {duration:.2f}s")
        logging.info(f"  Keys added: {adds}")
        logging.info(f"  Keys/sec: {adds/duration:.1f}")
        logging.info(f"  Tier transitions: {transitions}")
        logging.info(f"  Final tier: {end_metrics['tier']}")
```

### 2. Configuration Management

```python
import yaml
from dataclasses import dataclass
from typing import Optional

@dataclass
class TrackerConfig:
    """Configuration for AdaptiveKeyTracker."""
    max_exact_keys: int = 1_000_000
    max_lru_keys: int = 10_000_000
    false_positive_rate: float = 0.001
    
    @classmethod
    def from_yaml(cls, config_path: str) -> 'TrackerConfig':
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        return cls(**config_dict)
    
    @classmethod
    def for_workload(cls, workload_type: str) -> 'TrackerConfig':
        """Get configuration for specific workload type."""
        configs = {
            'small_exact': cls(
                max_exact_keys=5_000_000,
                max_lru_keys=10_000_000,
                false_positive_rate=0.001
            ),
            'medium_temporal': cls(
                max_exact_keys=100_000,
                max_lru_keys=2_000_000,
                false_positive_rate=0.001
            ),
            'large_accurate': cls(
                max_exact_keys=10_000,
                max_lru_keys=100_000,
                false_positive_rate=0.0001
            ),
            'memory_constrained': cls(
                max_exact_keys=10_000,
                max_lru_keys=100_000,
                false_positive_rate=0.01
            )
        }
        
        return configs.get(workload_type, cls())

# Usage
config = TrackerConfig.for_workload('medium_temporal')
tracker = AdaptiveKeyTracker(
    max_exact_keys=config.max_exact_keys,
    max_lru_keys=config.max_lru_keys,
    false_positive_rate=config.false_positive_rate
)
```

## Troubleshooting Guide

### Performance Issues

**Symptom**: Slow add operations
```python
# Diagnose
metrics = tracker.get_metrics()
if metrics['tier'] == 'EXACT' and metrics['current_count'] > 500_000:
    print("Large exact set causing performance issues")
    print("Consider reducing max_exact_keys or using LRU tier")
```

**Symptom**: High memory usage
```python
# Diagnose
if not metrics['has_bloom_dependency']:
    print("Bloom filter unavailable - install pybloom-live")
    print("Current LRU size may grow unbounded")
```

### Accuracy Issues

**Symptom**: Too many false positives (Bloom tier)
```python
# Solution: Reduce false positive rate
tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,
    max_lru_keys=1_000_000,
    false_positive_rate=0.0001  # Was 0.001
)
```

**Symptom**: Too many false negatives (LRU tier)
```python
# Solution: Increase LRU capacity
tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,
    max_lru_keys=5_000_000,  # Was 1_000_000
    false_positive_rate=0.001
)
```

This reference guide provides comprehensive technical details for understanding and optimizing the AdaptiveKeyTracker across various use cases and performance requirements.