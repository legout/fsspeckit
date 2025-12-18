# Working with Memory-Constrained Environments

When working with large datasets in memory-constrained environments, proper memory management becomes crucial. This guide provides practical strategies and examples for using fsspeckit's enhanced PyArrow memory monitoring and AdaptiveKeyTracker to prevent out-of-memory errors while maintaining efficient deduplication.

## Understanding Memory Constraints

### Common Scenarios

Memory-constrained environments include:
- **Development machines** with limited RAM (4-8GB)
- **Container environments** with memory limits
- **Shared servers** with multiple concurrent processes
- **Laptop development** where other applications consume memory

### Memory Types Monitored

The enhanced monitoring system tracks:
- **PyArrow allocation**: Memory used by PyArrow's internal allocator
- **Process RSS**: Total memory used by your Python process
- **System available**: Free memory available system-wide

## Basic Memory Monitoring Setup

### Install Dependencies

```bash
# Install with monitoring support
pip install fsspeckit[monitoring]

# Or install psutil separately
pip install psutil>=5.9.0
```

### Simple Memory Monitoring

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor

# Create a monitor with conservative limits
monitor = MemoryMonitor(
    max_pyarrow_mb=1024,      # 1GB for PyArrow
    max_process_memory_mb=2048, # 2GB total process limit
    min_system_available_mb=512 # Keep 512MB free
)

# Check memory status
status = monitor.get_memory_status()
print(f"PyArrow: {status['pyarrow_allocated_mb']:.1f}MB")
print(f"Process: {status.get('process_rss_mb', 'N/A')}MB")
print(f"System: {status.get('system_available_mb', 'N/A')}MB")
```

## Memory-Constrained Chunked Processing

### Example: Processing Large Datasets

```python
import gc
from fsspeckit.datasets.pyarrow.dataset import process_in_chunks
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor, MemoryPressureLevel

def process_large_dataset_safely(dataset_path, output_path):
    """Process large dataset with memory monitoring and graceful degradation."""
    
    # Configure conservative memory limits for constrained environments
    memory_monitor = MemoryMonitor(
        max_pyarrow_mb=512,      # Reduced from default 2048MB
        max_process_memory_mb=1024, # 1GB total process limit
        min_system_available_mb=256, # Keep 256MB free
    )
    
    # Import dataset
    import pyarrow.dataset as ds
    dataset = ds.dataset(dataset_path)
    
    chunks_processed = 0
    total_rows = dataset.count_rows()
    
    print(f"Processing {total_rows:,} rows with memory monitoring...")
    
    try:
        for chunk in process_in_chunks(
            dataset=dataset,
            chunk_size_rows=50_000,  # Smaller chunks for memory-constrained environments
            max_memory_mb=512,
            memory_monitor=memory_monitor,
            enable_progress=True
        ):
            chunks_processed += 1
            
            # Memory-aware processing
            if chunks_processed % 10 == 0:  # Check every 10 chunks
                pressure = memory_monitor.check_memory_pressure()
                
                if pressure == MemoryPressureLevel.WARNING:
                    print(f"⚠️  Memory warning at chunk {chunks_processed}")
                    gc.collect()  # Trigger garbage collection
                    
                elif pressure == MemoryPressureLevel.CRITICAL:
                    print(f"🔥 Memory critical at chunk {chunks_processed}, reducing chunk size")
                    # Could implement dynamic chunk size reduction here
                    gc.collect()
                    
                elif pressure == MemoryPressureLevel.EMERGENCY:
                    raise MemoryError("Memory limit exceeded, cannot continue safely")
            
            # Process chunk (example: filter and write)
            processed_chunk = chunk.filter(pa.compute.field("status") == "active")
            
            if chunks_processed == 1:
                # Write first chunk with schema
                processed_chunk.write_parquet(
                    output_path,
                    compression='snappy'
                )
            else:
                # Append subsequent chunks
                processed_chunk.write_parquet(
                    output_path,
                    compression='snappy',
                    write_append=True
                )
            
            # Progress reporting
            rows_processed = chunks_processed * 50_000
            progress = min(rows_processed / total_rows * 100, 100)
            print(f"Progress: {progress:.1f}% ({rows_processed:,}/{total_rows:,} rows)")
    
    except MemoryError as e:
        print(f"❌ Memory error: {e}")
        print(f"Processed {chunks_processed} chunks before failure")
        raise
    
    print(f"✅ Completed processing {chunks_processed} chunks successfully")

# Usage
process_large_dataset_safely(
    dataset_path="large_dataset.parquet",
    output_path="filtered_output.parquet"
)
```

## Memory-Constrained Merge Operations

### Example: Safe Merging in Limited Memory

```python
from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor, MemoryPressureLevel

def merge_safely_in_constrained_environment():
    """Demonstrate safe merge operations in memory-constrained environments."""
    
    # Create enhanced IO handler with memory monitoring
    io_handler = PyarrowDatasetIO()
    
    # Prepare source data
    import pyarrow as pa
    source_data = pa.table({
        "id": list(range(100_000)),
        "value": [f"source_value_{i}" for i in range(100_000)],
        "timestamp": [f"2024-01-{i%30+1:02d}" for i in range(100_000)]
    })
    
    try:
        # Perform merge with enhanced memory monitoring
        result = io_handler.merge(
            data=source_data,
            path="target_dataset/",
            strategy="upsert",
            key_columns=["id"],
            
            # Enhanced memory parameters for constrained environments
            merge_max_memory_mb=256,              # Conservative PyArrow limit
            merge_max_process_memory_mb=512,      # Conservative process limit
            merge_min_system_available_mb=128,    # Keep system responsive
            
            # Conservative processing parameters
            merge_chunk_size_rows=10_000,         # Smaller chunks
            enable_streaming_merge=True,          # Always enable streaming
            merge_progress_callback=lambda current, total: print(f"Merge progress: {current}/{total}")
        )
        
        print(f"✅ Merge completed:")
        print(f"   - Inserted: {result.inserted:,} rows")
        print(f"   - Updated: {result.updated:,} rows")
        print(f"   - Total processed: {result.source_count:,} rows")
        
    except MemoryError as e:
        print(f"❌ Merge failed due to memory constraints: {e}")
        print("Consider reducing merge_chunk_size_rows or memory limits")

# Usage
merge_safely_in_constrained_environment()
```

## Environment-Specific Configuration

### Container Environment Example

```python
import os
import psutil

def configure_for_container():
    """Configure memory monitoring for containerized environments."""
    
    # Get container memory limits (if running in container)
    container_limit = os.environ.get('CONTAINER_MEMORY_LIMIT')
    if container_limit:
        # Parse memory limit (e.g., "2g", "512m")
        if container_limit.lower().endswith('g'):
            total_memory_mb = int(container_limit[:-1]) * 1024
        elif container_limit.lower().endswith('m'):
            total_memory_mb = int(container_limit[:-1])
        else:
            total_memory_mb = int(container_limit)
    else:
        # Fallback to actual system memory
        total_memory_mb = psutil.virtual_memory().total // (1024 * 1024)
    
    # Conservative allocation: use 60% of available memory
    process_limit = int(total_memory_mb * 0.6)
    pyarrow_limit = int(total_memory_mb * 0.4)
    system_reserve = int(total_memory_mb * 0.2)
    
    return MemoryMonitor(
        max_pyarrow_mb=pyarrow_limit,
        max_process_memory_mb=process_limit,
        min_system_available_mb=system_reserve
    )

# Usage in container
monitor = configure_for_container()
print(f"Container memory configuration: {monitor.max_pyarrow_mb}MB PyArrow, "
      f"{monitor.max_process_memory_mb}MB process, "
      f"{monitor.min_system_available_mb}MB system reserve")
```

### Development Environment Example

```python
def configure_for_development():
    """Configure memory monitoring for development machines."""
    
    # Check available system memory
    import psutil
    available_mb = psutil.virtual_memory().available // (1024 * 1024)
    
    if available_mb < 4096:  # Less than 4GB
        # Very constrained environment
        return MemoryMonitor(
            max_pyarrow_mb=256,
            max_process_memory_mb=512,
            min_system_available_mb=128
        )
    elif available_mb < 8192:  # Less than 8GB
        # Moderately constrained environment
        return MemoryMonitor(
            max_pyarrow_mb=512,
            max_process_memory_mb=1024,
            min_system_available_mb=256
        )
    else:
        # More comfortable environment
        return MemoryMonitor(
            max_pyarrow_mb=1024,
            max_process_memory_mb=2048,
            min_system_available_mb=512
        )

# Auto-configure for development
dev_monitor = configure_for_development()
```

## Graceful Degradation Strategies

### Dynamic Chunk Size Adjustment

```python
def adaptive_chunked_processing(dataset, initial_chunk_size=100_000):
    """Process dataset with adaptive chunk sizing based on memory pressure."""
    
    monitor = MemoryMonitor(max_pyarrow_mb=512)
    chunk_size = initial_chunk_size
    chunks_processed = 0
    
    for chunk in process_in_chunks(
        dataset=dataset,
        chunk_size_rows=chunk_size,
        max_memory_mb=512,
        memory_monitor=monitor
    ):
        chunks_processed += 1
        
        # Monitor memory pressure
        if chunks_processed > 1:  # Don't adjust on first chunk
            pressure = monitor.check_memory_pressure()
            
            if pressure == MemoryPressureLevel.WARNING:
                # Reduce chunk size by 20%
                chunk_size = int(chunk_size * 0.8)
                print(f"⚠️  Reducing chunk size to {chunk_size:,} due to memory pressure")
                
            elif pressure == MemoryPressureLevel.CRITICAL:
                # Reduce chunk size by 50%
                chunk_size = int(chunk_size * 0.5)
                print(f"🔥 Aggressively reducing chunk size to {chunk_size:,}")
                gc.collect()
                
            elif pressure == MemoryPressureLevel.EMERGENCY:
                raise MemoryError("Cannot continue safely with current memory constraints")
        
        # Process the chunk
        yield chunk

# Usage
for chunk in adaptive_chunked_processing(large_dataset):
    process_chunk(chunk)
```

## Memory-Bounded Deduplication with AdaptiveKeyTracker

The AdaptiveKeyTracker provides sophisticated memory management for deduplication operations, automatically transitioning between three tiers based on cardinality and memory constraints.

### Configuration for Memory-Constrained Environments

```python
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

# Very memory-constrained configuration (< 100MB)
constrained_tracker = AdaptiveKeyTracker(
    max_exact_keys=5_000,       # Small exact tier
    max_lru_keys=50_000,         # Small LRU cache  
    false_positive_rate=0.01     # Higher FP rate for smaller memory
)

# Conservative configuration (< 200MB)
conservative_tracker = AdaptiveKeyTracker(
    max_exact_keys=25_000,      # Medium exact tier
    max_lru_keys=250_000,        # Medium LRU cache
    false_positive_rate=0.005    # Moderate FP rate
)

# Balanced configuration (< 500MB)
balanced_tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,      # Larger exact tier
    max_lru_keys=1_000_000,      # Larger LRU cache
    false_positive_rate=0.001     # Standard FP rate
)
```

### Memory Usage Estimation

```python
def estimate_tracker_memory(num_keys, config):
    """Estimate memory usage for different configurations."""
    
    # Exact tier: ~72 bytes per key
    exact_memory = min(num_keys, config['max_exact_keys']) * 72
    
    # LRU tier: ~72 bytes per key (bounded)
    lru_keys = min(max(0, num_keys - config['max_exact_keys']), config['max_lru_keys'])
    lru_memory = lru_keys * 72
    
    # Bloom tier: ~1.25-2.5 bytes per key (fixed)
    if num_keys > config['max_exact_keys'] + config['max_lru_keys']:
        bloom_memory = num_keys * 2.0  # Average 2 bytes per key
    else:
        bloom_memory = 0
    
    total_memory_mb = (exact_memory + lru_memory + bloom_memory) / 1024 / 1024
    return total_memory_mb

# Example: Estimate memory for 10M keys
configs = {
    'constrained': {'max_exact_keys': 5000, 'max_lru_keys': 50000, 'false_positive_rate': 0.01},
    'conservative': {'max_exact_keys': 25000, 'max_lru_keys': 250000, 'false_positive_rate': 0.005},
    'balanced': {'max_exact_keys': 100000, 'max_lru_keys': 1000000, 'false_positive_rate': 0.001}
}

for name, config in configs.items():
    memory_mb = estimate_tracker_memory(10_000_000, config)
    print(f"{name.title()} config for 10M keys: {memory_mb:.1f}MB")
```

### Memory-Aware Deduplication Pipeline

```python
import psutil
import os
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

class MemoryAwareDeduplicator:
    """Deduplicator that actively monitors and manages memory usage."""
    
    def __init__(self, memory_limit_mb=100):
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024
        self.process = psutil.Process(os.getpid())
        
        # Start with very conservative configuration
        self.tracker = AdaptiveKeyTracker(
            max_exact_keys=1_000,
            max_lru_keys=10_000,
            false_positive_rate=0.01
        )
        
        self.add_count = 0
        self.check_interval = 1000
    
    def add_with_monitoring(self, key):
        """Add key with continuous memory monitoring."""
        
        # Add key first
        self.tracker.add(key)
        self.add_count += 1
        
        # Periodic memory check
        if self.add_count % self.check_interval == 0:
            self.check_memory_pressure()
    
    def check_memory_pressure(self):
        """Check memory and adjust configuration if needed."""
        current_memory = self.process.memory_info().rss
        
        if current_memory > self.memory_limit_bytes:
            print(f"Memory pressure detected: {current_memory / 1024 / 1024:.1f}MB")
            self.handle_memory_pressure()
    
    def handle_memory_pressure(self):
        """Handle memory pressure by aggressive configuration changes."""
        metrics = self.tracker.get_metrics()
        
        if metrics['tier'] == 'EXACT':
            print("Forcing transition to LRU tier to free memory")
            self.tracker._transition_to_lru()
            
        elif metrics['tier'] == 'LRU':
            if metrics['has_bloom_dependency']:
                print("Forcing transition to Bloom tier")
                self.tracker._transition_to_bloom()
            else:
                print("Reducing LRU capacity and forcing GC")
                # Would need to recreate tracker with smaller limits
                # This is a simplified example
                
        import gc
        gc.collect()
        
        # Check again after cleanup
        new_memory = self.process.memory_info().rss
        print(f"Memory after cleanup: {new_memory / 1024 / 1024:.1f}MB")
    
    def process_stream(self, data_stream):
        """Process data stream with memory monitoring."""
        processed_count = 0
        
        for record in data_stream:
            key = record['id']  # Or composite key
            
            if key not in self.tracker:
                self.add_with_monitoring(key)
                processed_count += 1
                yield record
        
        metrics = self.tracker.get_metrics()
        print(f"\nDeduplication Summary:")
        print(f"  - Records processed: {processed_count}")
        print(f"  - Total keys added: {metrics['total_add_calls']}")
        print(f"  - Unique keys: {metrics['unique_keys_estimate']}")
        print(f"  - Final tier: {metrics['tier']}")
        print(f"  - Memory used: {self.process.memory_info().rss / 1024 / 1024:.1f}MB")

# Usage
deduplicator = MemoryAwareDeduplicator(memory_limit_mb=100)

for unique_record in deduplicator.process_stream(large_data_stream):
    process_record(unique_record)
```

### Integration with Memory Monitor

```python
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

class IntegratedMemoryManager:
    """Combines memory monitoring with adaptive deduplication."""
    
    def __init__(self, memory_limit_mb=200):
        self.memory_monitor = MemoryMonitor(
            max_pyarrow_mb=memory_limit_mb // 2,
            max_process_memory_mb=memory_limit_mb,
            min_system_available_mb=100
        )
        
        # Configure tracker based on available memory
        available_mb = self.memory_monitor.get_memory_status().get('system_available_mb', 1000)
        self.tracker = self.configure_tracker_for_memory(available_mb)
    
    def configure_tracker_for_memory(self, available_mb):
        """Configure tracker based on available system memory."""
        
        if available_mb < 1000:  # < 1GB available
            return AdaptiveKeyTracker(
                max_exact_keys=5_000,
                max_lru_keys=50_000,
                false_positive_rate=0.01
            )
        elif available_mb < 2000:  # < 2GB available
            return AdaptiveKeyTracker(
                max_exact_keys=25_000,
                max_lru_keys=250_000,
                false_positive_rate=0.005
            )
        else:  # 2GB+ available
            return AdaptiveKeyTracker(
                max_exact_keys=100_000,
                max_lru_keys=1_000_000,
                false_positive_rate=0.001
            )
    
    def process_with_monitoring(self, data_stream):
        """Process data with comprehensive memory monitoring."""
        
        for batch in data_stream:
            # Check memory pressure before processing batch
            pressure = self.memory_monitor.check_memory_pressure()
            
            if pressure.value in ['critical', 'emergency']:
                print(f"Memory pressure: {pressure.value} - adjusting strategy")
                
                if pressure == MemoryPressureLevel.EMERGENCY:
                    # Emergency: force Bloom transition
                    if self.tracker.get_metrics()['tier'] != 'BLOOM':
                        self.tracker._transition_to_bloom()
                        print("Emergency: Forced Bloom filter transition")
                
                # Trigger garbage collection
                import gc
                gc.collect()
            
            # Process batch with deduplication
            for record in batch:
                key = record.get('composite_key', record['id'])
                
                if key not in self.tracker:
                    self.tracker.add(key)
                    yield record
            
            # Check memory after batch
            final_pressure = self.memory_monitor.check_memory_pressure()
            if final_pressure == MemoryPressureLevel.EMERGENCY:
                print("Emergency memory condition - stopping processing")
                break

# Usage
memory_manager = IntegratedMemoryManager(memory_limit_mb=200)

for unique_record in memory_manager.process_with_monitoring(data_stream):
    process_record(unique_record)
```

### Benchmarking Memory Performance

```python
def benchmark_memory_usage(data_stream, configurations):
    """Benchmark different configurations for memory efficiency."""
    
    results = {}
    
    for config_name, config in configurations.items():
        print(f"\nTesting {config_name} configuration...")
        
        tracker = AdaptiveKeyTracker(**config)
        process = psutil.Process(os.getpid())
        
        start_memory = process.memory_info().rss
        start_time = time.time()
        
        processed_count = 0
        for record in data_stream:
            key = record['id']
            
            if key not in tracker:
                tracker.add(key)
                processed_count += 1
        
        end_time = time.time()
        end_memory = process.memory_info().rss
        
        metrics = tracker.get_metrics()
        
        results[config_name] = {
            'processed_count': processed_count,
            'final_tier': metrics['tier'],
            'unique_keys': metrics['unique_keys_estimate'],
            'transitions': metrics['transitions'],
            'duration': end_time - start_time,
            'memory_used_mb': (end_memory - start_memory) / 1024 / 1024,
            'memory_per_key_bytes': (end_memory - start_memory) / processed_count if processed_count > 0 else 0
        }
        
        print(f"  - Final tier: {metrics['tier']}")
        print(f"  - Memory used: {results[config_name]['memory_used_mb']:.1f}MB")
        print(f"  - Memory/key: {results[config_name]['memory_per_key_bytes']:.1f} bytes")
        print(f"  - Processing time: {results[config_name]['duration']:.2f}s")
    
    return results

# Test configurations
test_configs = {
    'minimal': {'max_exact_keys': 1000, 'max_lru_keys': 10000, 'false_positive_rate': 0.01},
    'conservative': {'max_exact_keys': 10000, 'max_lru_keys': 100000, 'false_positive_rate': 0.005},
    'balanced': {'max_exact_keys': 50000, 'max_lru_keys': 500000, 'false_positive_rate': 0.001}
}

# Run benchmark
results = benchmark_memory_usage(large_test_stream, test_configs)
```

### Best Practices for Memory-Constrained Deduplication

1. **Start Conservative**: Begin with small exact and LRU tiers
2. **Monitor Actively**: Use process memory monitoring to detect pressure
3. **Install Dependencies**: Ensure `pybloom-live` is available for Bloom tier
4. **Choose Right Accuracy**: Higher false positive rates use less memory
5. **Test with Sample Data**: Benchmark with your actual data patterns
6. **Plan for Growth**: Consider how cardinality will change over time

### Memory-Aware Data Pipeline

```python
class MemoryAwarePipeline:
    """Example pipeline that adapts to memory constraints."""
    
    def __init__(self, memory_limit_mb=1024):
        self.monitor = MemoryMonitor(max_pyarrow_mb=memory_limit_mb)
        self.gc_threshold = 0.8  # Trigger GC at 80% of limit
    
    def process_pipeline(self, stages):
        """Process data through pipeline stages with memory monitoring."""
        
        data = self.load_initial_data()
        
        for i, stage in enumerate(stages):
            print(f"Processing stage {i+1}/{len(stages)}: {stage.__name__}")
            
            # Check memory before stage
            pressure = self.monitor.check_memory_pressure()
            
            if pressure.value in ['warning', 'critical']:
                print(f"Memory pressure before stage: {pressure.value}")
                
                if pressure == MemoryPressureLevel.CRITICAL:
                    print("Triggering garbage collection")
                    gc.collect()
                    
                    # Recheck pressure
                    pressure = self.monitor.check_memory_pressure()
                    if pressure == MemoryPressureLevel.EMERGENCY:
                        raise MemoryError("Cannot continue pipeline")
            
            # Process stage
            data = stage(data)
            
            # Force memory check after stage
            final_pressure = self.monitor.check_memory_pressure()
            if final_pressure == MemoryPressureLevel.EMERGENCY:
                raise MemoryError(f"Pipeline stage {stage.__name__} exceeded memory limits")
        
        return data

# Usage example
pipeline = MemoryAwarePipeline(memory_limit_mb=512)

def stage1_filter(data):
    return data.filter(pa.compute.field("status") == "active")

def stage2_transform(data):
    return data.select(["id", "value", "processed_date"])

def stage3_aggregate(data):
    return data.group_by("category").aggregate([("value", "sum")])

result = pipeline.process_pipeline([stage1_filter, stage2_transform, stage3_aggregate])
```

## Monitoring and Alerting

### Memory Monitoring Dashboard

```python
import time
from datetime import datetime

def start_memory_monitoring(interval_seconds=30):
    """Start background memory monitoring with logging."""
    
    monitor = MemoryMonitor()
    
    def monitor_loop():
        while True:
            try:
                status = monitor.get_memory_status()
                pressure = monitor.check_memory_pressure()
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                
                # Format status message
                msg = f"[{timestamp}] "
                msg += f"PyArrow: {status['pyarrow_allocated_mb']:.1f}MB "
                
                if 'process_rss_mb' in status:
                    msg += f"Process: {status['process_rss_mb']:.1f}MB "
                
                if 'system_available_mb' in status:
                    msg += f"System: {status['system_available_mb']:.1f}MB "
                
                msg += f"Pressure: {pressure.value.upper()}"
                
                # Color code based on pressure
                if pressure == MemoryPressureLevel.NORMAL:
                    print(f"✅ {msg}")
                elif pressure == MemoryPressureLevel.WARNING:
                    print(f"⚠️  {msg}")
                elif pressure == MemoryPressureLevel.CRITICAL:
                    print(f"🔥 {msg}")
                else:  # EMERGENCY
                    print(f"🚨 {msg}")
                
                time.sleep(interval_seconds)
                
            except Exception as e:
                print(f"❌ Monitoring error: {e}")
                time.sleep(interval_seconds)
    
    return monitor_loop

# Start monitoring (run in separate thread in production)
monitoring_thread = start_memory_monitoring(interval_seconds=60)
```

## Troubleshooting Memory Issues

### Common Issues and Solutions

#### Issue: "Memory limit exceeded" errors

**Diagnosis:**
```python
# Get detailed memory status
monitor = MemoryMonitor()
status = monitor.get_detailed_status()
print(status)

# Check pressure levels
pressure = monitor.check_memory_pressure()
print(f"Current pressure: {pressure.value}")
```

**Solutions:**
1. Reduce `max_pyarrow_mb` and `max_process_memory_mb`
2. Increase `min_system_available_mb`
3. Use smaller chunk sizes
4. Enable garbage collection more frequently

#### Issue: Slow performance with monitoring

**Diagnosis:**
```python
# Check if psutil is available (slower without it)
from fsspeckit.datasets.pyarrow.memory import psutil
print(f"psutil available: {psutil is not None}")
```

**Solutions:**
1. Increase memory check intervals using `should_check_memory()`
2. Consider running without psutil for better performance
3. Use simpler monitoring configuration

#### Issue: Inconsistent memory readings

**Diagnosis:**
This can occur due to:
- Platform differences in memory reporting
- Other processes consuming memory
- Python garbage collection timing

**Solutions:**
1. Use RSS (Resident Set Size) as the most consistent metric
2. Implement longer monitoring intervals
3. Add buffer zones in memory limits (e.g., use 80% of available memory)

## Best Practices Summary

1. **Start Conservative**: Use lower memory limits and increase gradually
2. **Monitor Regularly**: Check memory pressure frequently during processing
3. **Plan for Failure**: Implement graceful degradation strategies
4. **Test Thoroughly**: Validate memory limits with representative datasets
5. **Document Limits**: Clearly document memory requirements for your use cases
6. **Use Streaming**: Always prefer streaming operations in constrained environments
7. **Regular Cleanup**: Trigger garbage collection at appropriate intervals
8. **Environment Awareness**: Adjust limits based on deployment environment

By following these practices and using the enhanced memory monitoring capabilities, you can safely process large datasets even in memory-constrained environments.