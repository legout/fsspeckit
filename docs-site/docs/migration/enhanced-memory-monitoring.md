# Migration Guide: Enhanced PyArrow Memory Monitoring

This guide helps existing fsspeckit users migrate to the enhanced PyArrow memory monitoring system introduced in version 0.8.3.

## What's New

The enhanced memory monitoring system provides:

- **Dual memory tracking**: Monitors both PyArrow allocation and system memory
- **Tiered pressure levels**: NORMAL, WARNING, CRITICAL, EMERGENCY with automatic thresholds
- **Graceful degradation**: Automatic GC triggering and chunk size reduction
- **Enhanced metrics**: New performance metrics including process memory and pressure statistics
- **Backward compatibility**: All existing code continues to work unchanged

## No Action Required (Backward Compatibility)

**Good news**: Existing code continues to work without any changes. The enhanced memory monitoring is **opt-in** and only activates when you explicitly use the new features.

### Existing Code Still Works

```python
# This code continues to work exactly as before
from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO

handler = PyarrowDatasetIO()
result = handler.merge(
    data=source_table,
    path=target_path,
    strategy="upsert",
    key_columns=["id"],
    merge_max_memory_mb=1024  # This still works
)
```

## Optional: Enable Enhanced Monitoring

If you want to take advantage of the new features, here's how to gradually adopt them.

### Step 1: Install Optional Dependencies

```bash
# Optional: Install psutil for enhanced system monitoring
pip install psutil>=5.9.0

# Or install with fsspeckit monitoring extras
pip install fsspeckit[monitoring]
```

### Step 2: Basic Memory Monitoring

Replace existing memory monitoring with enhanced version:

```python
# Before (still works)
from fsspeckit.datasets.pyarrow.dataset import process_in_chunks

for chunk in process_in_chunks(
    dataset=large_dataset,
    chunk_size_rows=100_000,
    max_memory_mb=1024
):
    process_chunk(chunk)

# After (enhanced monitoring)
from fsspeckit.datasets.pyarrow.dataset import process_in_chunks
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor, MemoryPressureLevel

# Create enhanced monitor
monitor = MemoryMonitor(
    max_pyarrow_mb=1024,
    max_process_memory_mb=2048,      # NEW: Monitor total process memory
    min_system_available_mb=512      # NEW: Keep system responsive
)

for chunk in process_in_chunks(
    dataset=large_dataset,
    chunk_size_rows=100_000,
    max_memory_mb=1024,
    memory_monitor=monitor           # NEW: Pass enhanced monitor
):
    # Enhanced processing with memory awareness
    pressure = monitor.check_memory_pressure()
    
    if pressure == MemoryPressureLevel.WARNING:
        print("Memory usage is high")
        import gc
        gc.collect()
    
    process_chunk(chunk)
```

### Step 3: Enhanced Merge Operations

Upgrade merge operations with new memory parameters:

```python
# Before (still works)
handler = PyarrowDatasetIO()
result = handler.merge(
    data=source_table,
    path=target_path,
    strategy="upsert",
    key_columns=["id"],
    merge_max_memory_mb=1024
)

# After (enhanced with new parameters)
handler = PyarrowDatasetIO()
result = handler.merge(
    data=source_table,
    path=target_path,
    strategy="upsert",
    key_columns=["id"],
    
    # Existing parameter (unchanged)
    merge_max_memory_mb=1024,
    
    # NEW enhanced parameters
    merge_max_process_memory_mb=2048,      # Monitor total process memory
    merge_min_system_available_mb=512,     # Keep system responsive
)
```

### Step 4: Enhanced Performance Monitoring

Upgrade performance monitoring for detailed metrics:

```python
# Before
from fsspeckit.datasets.pyarrow.dataset import PerformanceMonitor

monitor = PerformanceMonitor()  # Basic monitoring
monitor.start_op("processing")
# ... operations ...
monitor.end_op()

metrics = monitor.get_metrics(rows_before, rows_after, total_bytes)
print(f"Peak memory: {metrics['memory_peak_mb']} MB")

# After
from fsspeckit.datasets.pyarrow.dataset import PerformanceMonitor

monitor = PerformanceMonitor(
    max_pyarrow_mb=1024,
    max_process_memory_mb=2048,
    min_system_available_mb=512,
)  # Enhanced monitoring

monitor.start_op("processing")
# ... operations ...
monitor.end_op()

metrics = monitor.get_metrics(rows_before, rows_after, total_bytes)

# Enhanced metrics available
print(f"PyArrow peak: {metrics['memory_peak_mb']} MB")
print(f"Process peak: {metrics['process_memory_peak_mb']} MB")           # NEW
print(f"System available: {metrics['system_available_mb']} MB")           # NEW
print(f"Pressure stats: {metrics['memory_pressure_stats']}")              # NEW
```

## Configuration Migration Patterns

### Pattern 1: Conservative Upgrade

Start with conservative limits to ensure stability:

```python
def migrate_conservative():
    """Migrate with conservative memory limits."""
    
    # Start with existing limits
    existing_pyarrow_limit = 1024  # Your current max_memory_mb
    
    # Calculate conservative enhanced limits
    enhanced_monitor = MemoryMonitor(
        max_pyarrow_mb=existing_pyarrow_limit,        # Keep same PyArrow limit
        max_process_memory_mb=existing_pyarrow_limit * 2,  # 2x for total process
        min_system_available_mb=512,                  # Keep 512MB free
    )
    
    return enhanced_monitor
```

### Pattern 2: Environment-Aware Migration

Adjust limits based on deployment environment:

```python
import psutil

def migrate_environment_aware():
    """Migrate with environment-specific limits."""
    
    available_memory_mb = psutil.virtual_memory().available // (1024 * 1024)
    
    # Calculate limits as percentage of available memory
    pyarrow_limit = int(available_memory_mb * 0.4)    # 40% for PyArrow
    process_limit = int(available_memory_mb * 0.7)    # 70% total process
    system_reserve = int(available_memory_mb * 0.2)   # 20% for system
    
    return MemoryMonitor(
        max_pyarrow_mb=pyarrow_limit,
        max_process_memory_mb=process_limit,
        min_system_available_mb=system_reserve,
    )
```

### Pattern 3: Gradual Migration

Test enhanced monitoring alongside existing code:

```python
def migrate_gradual():
    """Gradually migrate to enhanced monitoring."""
    
    # Keep existing code as baseline
    existing_monitor = None  # Use default monitoring
    
    # Add enhanced monitoring for comparison
    enhanced_monitor = MemoryMonitor(
        max_pyarrow_mb=1024,
        max_process_memory_mb=2048,
        min_system_available_mb=512,
    )
    
    # Run both monitoring approaches
    for chunk in process_in_chunks(dataset, memory_monitor=enhanced_monitor):
        # Process with enhanced monitoring
        process_chunk(chunk)
        
        # Log enhanced metrics for analysis
        status = enhanced_monitor.get_memory_status()
        print(f"Enhanced monitoring: PyArrow={status['pyarrow_allocated_mb']:.1f}MB, "
              f"Process={status.get('process_rss_mb', 'N/A')}MB")
```

## API Changes Reference

### New Classes and Functions

#### New Classes

```python
from fsspeckit.datasets.pyarrow.memory import (
    MemoryMonitor,           # NEW: Enhanced memory monitoring
    MemoryPressureLevel,     # NEW: Pressure level enum
)

# New pressure levels
MemoryPressureLevel.NORMAL      # < 70% usage
MemoryPressureLevel.WARNING     # 70-90% usage  
MemoryPressureLevel.CRITICAL    # 90-100% usage
MemoryPressureLevel.EMERGENCY   # > 100% usage
```

#### New Methods

**MemoryMonitor methods:**
- `get_memory_status()` - Get current memory metrics
- `check_memory_pressure()` - Evaluate memory pressure level
- `should_check_memory()` - Determine if memory check is needed
- `get_detailed_status()` - Get formatted status string

**Enhanced PerformanceMonitor methods:**
- New constructor parameters: `max_process_memory_mb`, `min_system_available_mb`
- Enhanced `get_metrics()` with new fields

### Enhanced Method Signatures

#### process_in_chunks()

```python
# Before
def process_in_chunks(
    dataset, 
    chunk_size_rows=1_000_000,
    max_memory_mb=2048,
    enable_progress=True,
    progress_callback=None
):

# After (enhanced)
def process_in_chunks(
    dataset, 
    chunk_size_rows=1_000_000,
    max_memory_mb=2048,
    enable_progress=True,
    progress_callback=None,
    memory_monitor=None  # NEW: Optional MemoryMonitor
):
```

#### PyarrowDatasetIO.merge()

```python
# Before
def merge(
    self,
    data, path, strategy, key_columns,
    merge_max_memory_mb=1024,
    # ... other parameters
):

# After (enhanced)
def merge(
    self,
    data, path, strategy, key_columns,
    merge_max_memory_mb=1024,
    merge_max_process_memory_mb=None,     # NEW
    merge_min_system_available_mb=512,    # NEW
    # ... other parameters
):
```

#### PerformanceMonitor

```python
# Before
def __init__(self):
    # Basic initialization

# After (enhanced)
def __init__(
    self,
    max_pyarrow_mb=2048,              # NEW
    max_process_memory_mb=None,       # NEW  
    min_system_available_mb=512,      # NEW
):
```

### New Metrics in Performance Reports

Enhanced `get_metrics()` now includes:

```python
{
    # Existing metrics (unchanged)
    "total_process_time_sec": 45.2,
    "memory_peak_mb": 1024.0,
    "throughput_mb_sec": 1024.0,
    
    # NEW enhanced metrics
    "process_memory_peak_mb": 1536.0,      # Peak process RSS
    "system_available_mb": 2048.0,         # Current system memory
    "memory_pressure_stats": {             # Pressure level occurrences
        "normal": 45,
        "warning": 8,
        "critical": 2,
        "emergency": 0
    }
}
```

## Troubleshooting Migration Issues

### Issue 1: Import Errors

If you get import errors for the new memory monitoring classes:

```python
# Make sure you're importing from the correct location
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor  # CORRECT

# NOT from these locations (they don't exist)
# from fsspeckit.memory import MemoryMonitor  # WRONG
# from fsspeckit.datasets.memory import MemoryMonitor  # WRONG
```

### Issue 2: psutil Not Available

If psutil is not installed, the system falls back gracefully:

```python
# This works even without psutil
monitor = MemoryMonitor(
    max_pyarrow_mb=1024,
    max_process_memory_mb=2048,  # This will be ignored with a warning
    min_system_available_mb=512  # This will be ignored with a warning
)

# You'll see: "psutil not available; system memory thresholds will be ignored."
# But PyArrow monitoring still works
```

### Issue 3: Performance Impact

If enhanced monitoring impacts performance:

```python
# Use less frequent memory checks
monitor = MemoryMonitor()

# Check memory pressure every 20 chunks instead of every chunk
for chunk in process_in_chunks(dataset, memory_monitor=monitor):
    chunks_processed += 1
    
    # Use should_check_memory to avoid excessive monitoring
    if monitor.should_check_memory(chunks_processed, check_interval=20):
        pressure = monitor.check_memory_pressure()
        # ... handle pressure ...
```

### Issue 4: Memory Limits Too Restrictive

If new memory limits are too restrictive:

```python
# Increase limits based on your system's actual capacity
import psutil

available_mb = psutil.virtual_memory().available // (1024 * 1024)

monitor = MemoryMonitor(
    max_pyarrow_mb=int(available_mb * 0.5),      # Use 50% for PyArrow
    max_process_memory_mb=int(available_mb * 0.8), # Use 80% for process
    min_system_available_mb=int(available_mb * 0.1), # Keep 10% free
)
```

## Performance Considerations

### Monitoring Overhead

The enhanced monitoring system is designed to minimize overhead:

- **Caching**: Memory status cached for 100ms to avoid excessive psutil calls
- **Lazy evaluation**: System memory only checked when needed  
- **Configurable intervals**: Can throttle memory checks

### Expected Overhead

- **With psutil**: ~2-5% performance overhead
- **Without psutil**: ~1% performance overhead
- **Optimized monitoring**: <1% overhead with infrequent checks

### Optimization Tips

```python
# Optimize for your use case

# For high-throughput processing:
monitor = MemoryMonitor()
# Check memory every 50 chunks
if monitor.should_check_memory(chunks_processed, check_interval=50):
    pressure = monitor.check_memory_pressure()

# For safety-critical operations:
monitor = MemoryMonitor()
# Check memory every chunk
if monitor.should_check_memory(chunks_processed, check_interval=1):
    pressure = monitor.check_memory_pressure()
```

## Testing Your Migration

### Test Script Template

```python
def test_migration():
    """Test script to validate enhanced memory monitoring."""
    
    # Test 1: Basic monitoring
    monitor = MemoryMonitor(max_pyarrow_mb=512)
    status = monitor.get_memory_status()
    assert 'pyarrow_allocated_mb' in status
    print("✅ Basic monitoring works")
    
    # Test 2: Enhanced monitoring (if psutil available)
    try:
        import psutil
        enhanced_monitor = MemoryMonitor(
            max_pyarrow_mb=512,
            max_process_memory_mb=1024,
            min_system_available_mb=256,
        )
        status = enhanced_monitor.get_memory_status()
        assert 'process_rss_mb' in status
        print("✅ Enhanced monitoring works")
    except ImportError:
        print("⚠️  psutil not available, enhanced monitoring limited")
    
    # Test 3: Pressure detection
    pressure = monitor.check_memory_pressure()
    assert pressure in [MemoryPressureLevel.NORMAL, MemoryPressureLevel.WARNING, 
                       MemoryPressureLevel.CRITICAL, MemoryPressureLevel.EMERGENCY]
    print("✅ Pressure detection works")
    
    # Test 4: Enhanced PerformanceMonitor
    perf_monitor = PerformanceMonitor(
        max_pyarrow_mb=512,
        max_process_memory_mb=1024,
    )
    metrics = perf_monitor.get_metrics(1000, 900, 1024*1024)
    
    # Check new fields are present
    assert 'process_memory_peak_mb' in metrics
    assert 'memory_pressure_stats' in metrics
    print("✅ Enhanced PerformanceMonitor works")
    
    print("🎉 Migration test completed successfully!")

if __name__ == "__main__":
    test_migration()
```

## Conclusion

The enhanced PyArrow memory monitoring system is designed to be:

- **Backward compatible**: Existing code works unchanged
- **Opt-in**: New features only activate when used
- **Performant**: Minimal overhead with optimization options
- **Safe**: Graceful fallback when dependencies unavailable
- **Comprehensive**: Detailed monitoring and metrics

Start with conservative configurations and gradually adopt enhanced features based on your specific needs and system capacity.