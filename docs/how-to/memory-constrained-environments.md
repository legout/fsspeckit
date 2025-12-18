# Working with Memory-Constrained Environments

When working with large datasets in memory-constrained environments, proper memory management becomes crucial. This guide provides practical strategies and examples for using fsspeckit's enhanced PyArrow memory monitoring to prevent out-of-memory errors.

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