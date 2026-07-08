# Memory Monitoring API

The enhanced PyArrow memory monitoring system provides dual tracking of both PyArrow allocation and system memory usage to prevent out-of-memory errors during large dataset operations.

## MemoryMonitor Class

The `MemoryMonitor` class is the core component for monitoring memory usage across both PyArrow and system levels.

```python
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor, MemoryPressureLevel
```

### Constructor

```python
MemoryMonitor(
    max_pyarrow_mb: int = 2048,
    max_process_memory_mb: Optional[int] = None,
    min_system_available_mb: int = 512,
)
```

**Parameters:**
- `max_pyarrow_mb`: Maximum allowed PyArrow-allocated memory in MB (default: 2048)
- `max_process_memory_mb`: Optional maximum total process memory (RSS) in MB
- `min_system_available_mb`: Minimum required system available memory in MB (default: 512)

### Methods

#### `get_memory_status() -> Dict[str, float]`

Returns current memory metrics in MB.

**Returns:**
- `pyarrow_allocated_mb`: Bytes allocated by PyArrow
- `process_rss_mb`: Resident Set Size of the process (if psutil available)
- `system_available_mb`: Total system available memory (if psutil available)

```python
monitor = MemoryMonitor(max_pyarrow_mb=1024)
status = monitor.get_memory_status()
print(f"PyArrow: {status['pyarrow_allocated_mb']:.1f} MB")
# Output: PyArrow: 256.0 MB
```

#### `check_memory_pressure() -> MemoryPressureLevel`

Evaluates current memory usage against thresholds and returns the appropriate pressure level.

**Pressure Levels:**
- `NORMAL`: Usage < 70% of limits
- `WARNING`: Usage 70-90% of limits  
- `CRITICAL`: Usage 90-100% of limits
- `EMERGENCY`: Usage exceeds absolute limits

```python
pressure = monitor.check_memory_pressure()
if pressure == MemoryPressureLevel.WARNING:
    print("Memory usage is high, consider reducing batch size")
elif pressure == MemoryPressureLevel.EMERGENCY:
    raise MemoryError("Critical memory pressure detected")
```

#### `should_check_memory(chunks_processed: int, check_interval: int = 10) -> bool`

Determines if memory should be checked based on processing progress to avoid excessive monitoring overhead.

```python
if monitor.should_check_memory(chunks_processed, check_interval=10):
    pressure = monitor.check_memory_pressure()
```

#### `get_detailed_status() -> str`

Returns a formatted string with current memory metrics and limits for logging.

```python
status_str = monitor.get_detailed_status()
print(status_str)
# Output: PyArrow: 512.0/1024 MB | Process RSS: 2048.0/4096 MB | System Available: 1024.0 MB (min: 512 MB)
```

## MemoryPressureLevel Enum

Defines the memory pressure levels for graceful degradation:

```python
from fsspeckit.datasets.pyarrow.memory import MemoryPressureLevel

# Available levels
MemoryPressureLevel.NORMAL      # < 70% usage
MemoryPressureLevel.WARNING     # 70-90% usage  
MemoryPressureLevel.CRITICAL    # 90-100% usage
MemoryPressureLevel.EMERGENCY   # > 100% usage
```

## Enhanced PerformanceMonitor

The `PerformanceMonitor` class now includes enhanced memory monitoring capabilities:

```python
from fsspeckit.datasets.pyarrow.dataset import PerformanceMonitor
```

### Constructor

```python
PerformanceMonitor(
    max_pyarrow_mb: int = 2048,
    max_process_memory_mb: Optional[int] = None,
    min_system_available_mb: int = 512,
)
```

### Enhanced Metrics

The `get_metrics()` method now includes additional memory-related metrics:

```python
metrics = monitor.get_metrics(
    total_rows_before=1000,
    total_rows_after=900, 
    total_bytes=1024*1024,
)

# Enhanced memory metrics
print(f"PyArrow Peak: {metrics['memory_peak_mb']:.1f} MB")
print(f"Process Peak: {metrics['process_memory_peak_mb']:.1f} MB")
print(f"System Available: {metrics['system_available_mb']:.1f} MB")
print(f"Pressure Stats: {metrics['memory_pressure_stats']}")
```

**New metrics added:**
- `process_memory_peak_mb`: Peak process-level memory usage
- `system_available_mb`: Current system memory availability
- `memory_pressure_stats`: Statistics on pressure level occurrences

## Utility Functions

### `max_pressure(p1: MemoryPressureLevel, p2: MemoryPressureLevel) -> MemoryPressureLevel`

Returns the more severe of two memory pressure levels.

```python
from fsspeckit.datasets.pyarrow.memory import max_pressure

combined_pressure = max_pressure(
    MemoryPressureLevel.WARNING, 
    MemoryPressureLevel.CRITICAL
)
# Returns: MemoryPressureLevel.CRITICAL
```

### `bytes_to_mb(b: int) -> float`

Converts bytes to megabytes.

### `mb_to_bytes(mb: float) -> int`

Converts megabytes to bytes.

## Error Handling

The memory monitoring system handles various error conditions gracefully:

### psutil Unavailable

When `psutil` is not installed, the system falls back to PyArrow-only monitoring:

```python
# Warning is logged when system monitoring is disabled
monitor = MemoryMonitor(max_process_memory_mb=4096)
# INFO: psutil not available; system memory thresholds will be ignored.
```

### Permission Errors

If system memory access is denied, the system continues with available metrics:

```python
# Graceful handling of permission errors
status = monitor.get_memory_status()
# Only PyArrow metrics available, system metrics may be missing
```

## Best Practices

### 1. Appropriate Memory Limits

Set realistic memory limits based on your system capacity:

```python
# For systems with 8GB RAM
monitor = MemoryMonitor(
    max_pyarrow_mb=4096,      # Reserve 4GB for PyArrow
    max_process_memory_mb=6144, # Allow up to 6GB total process memory
    min_system_available_mb=1024 # Keep 1GB free for system
)
```

### 2. Regular Memory Checks

Monitor memory pressure regularly during processing:

```python
chunks_processed = 0
for chunk in process_in_chunks(dataset, memory_monitor=monitor):
    chunks_processed += 1
    
    # Check memory every 10 chunks
    if monitor.should_check_memory(chunks_processed):
        pressure = monitor.check_memory_pressure()
        
        if pressure == MemoryPressureLevel.WARNING:
            # Reduce batch size or trigger cleanup
            gc.collect()
        elif pressure == MemoryPressureLevel.EMERGENCY:
            raise MemoryError("Memory limit exceeded")
```

### 3. Enhanced Merge Operations

Use enhanced memory parameters for merge operations:

```python
result = handler.merge(
    data=source_table,
    path=target_path,
    strategy="upsert",
    key_columns=["id"],
    merge_max_memory_mb=2048,              # PyArrow limit
    merge_max_process_memory_mb=4096,      # Process limit  
    merge_min_system_available_mb=1024,    # System minimum
)
```

### 4. Monitoring Integration

Integrate memory monitoring with existing workflows:

```python
monitor = PerformanceMonitor(
    max_pyarrow_mb=1024,
    max_process_memory_mb=2048,
    min_system_available_mb=512,
)

monitor.start_op("large_processing")
# ... perform operations ...
monitor.end_op()

# Get comprehensive metrics
metrics = monitor.get_metrics(rows_before, rows_after, total_bytes)
```

## Cross-Platform Compatibility

The memory monitoring system works consistently across platforms:

- **Linux**: Full RSS and system memory monitoring
- **macOS**: Full RSS and system memory monitoring  
- **Windows**: Full RSS and system memory monitoring

RSS (Resident Set Size) is used for process memory tracking as it's the most consistent metric across platforms.

## Performance Considerations

The memory monitoring system is designed to minimize overhead:

- **Caching**: Memory status is cached for 100ms to avoid excessive psutil calls
- **Lazy Evaluation**: System memory info is only checked when needed
- **Configurable Intervals**: Memory checks can be throttled using `should_check_memory()`

For most workloads, the monitoring overhead is < 5% of total processing time.