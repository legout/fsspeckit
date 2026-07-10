# Design: Fix PyArrow Memory Monitoring

## Context
The PyArrow dataset performance optimizations introduced memory monitoring to enforce limits during chunked processing. However, the current implementation only tracks PyArrow's internal allocator (`pa.total_allocated_bytes()`), missing system-level memory consumption from Python objects and other processes.

**Current Implementation** (dataset.py:82-86, 1021-1032):
```python
def track_memory(self):
    current_mem_mb = pa.total_allocated_bytes() / (1024 * 1024)
    if current_mem_mb > self.memory_peak_mb:
        self.memory_peak_mb = current_mem_mb
```

**Problem**: This only tracks Arrow's C++ allocator, not:
- Python objects created during processing
- Memory used by temporary structures (sets, lists)
- System memory pressure from other processes

## Goals / Non-Goals

### Goals
- Track total system memory usage in addition to PyArrow allocation
- Provide early warning when system memory pressure is high
- Enable graceful degradation when memory is constrained
- Maintain backward compatibility with existing API

### Non-Goals
- Implementing memory pooling or custom allocators
- Cross-process memory coordination
- Real-time memory pressure monitoring (polling is acceptable)

## Decisions

### Decision 1: Use psutil for System Memory Monitoring
**Problem**: Need to track system-level memory usage
**Solution**: Use `psutil.Process().memory_info()` for process memory and `psutil.virtual_memory()` for system-wide availability
**Rationale**: 
- `psutil` is a mature, cross-platform library
- Already commonly used in Python data processing stacks
- Provides accurate RSS (Resident Set Size) measurements

**Implementation**:
```python
import psutil

def get_memory_usage() -> dict[str, float]:
    """Get current memory usage metrics."""
    process = psutil.Process()
    mem_info = process.memory_info()
    vm = psutil.virtual_memory()
    
    return {
        "process_rss_mb": mem_info.rss / (1024 * 1024),
        "pyarrow_allocated_mb": pa.total_allocated_bytes() / (1024 * 1024),
        "system_available_mb": vm.available / (1024 * 1024),
        "system_percent_used": vm.percent,
    }
```

### Decision 2: Dual-Threshold Memory Limiting
**Problem**: Different memory types require different handling
**Solution**: Support both PyArrow-specific and system memory limits
**Rationale**: Allows fine-grained control for different deployment scenarios

**Configuration**:
```python
# Existing parameter (PyArrow allocation limit)
max_memory_mb: int = 2048

# New parameters
max_process_memory_mb: int | None = None  # Process RSS limit
min_system_available_mb: int = 512  # Minimum system memory to maintain
```

### Decision 3: Graceful Degradation Strategy
**Problem**: What to do when memory limits are approached
**Solution**: Implement tiered response based on memory pressure level
**Rationale**: Avoid hard failures when possible

**Memory Pressure Levels**:
1. **Normal** (< 70% of limit): Continue processing
2. **Warning** (70-90% of limit): Log warning, reduce chunk size
3. **Critical** (> 90% of limit): Pause, trigger GC, retry with smaller chunks
4. **Emergency** (> limit): Raise MemoryError with detailed diagnostics

### Decision 4: Optional Dependency Pattern
**Problem**: `psutil` adds an external dependency
**Solution**: Make system memory monitoring optional, fallback to PyArrow-only
**Rationale**: Maintains zero-dependency core functionality

```python
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    logger.info("psutil not available, system memory monitoring disabled")
```

## Risks / Trade-offs

### Risk 1: Performance Overhead from Memory Polling
**Risk**: Frequent memory checks could slow down processing
**Mitigation**: 
- Poll memory every N chunks (configurable, default: every 10 chunks)
- Cache memory readings for short periods
- Use lazy evaluation for detailed metrics

### Risk 2: Platform Differences
**Risk**: Memory metrics may differ across operating systems
**Mitigation**:
- Use RSS which is consistent across platforms
- Document platform-specific behaviors
- Test on Linux, macOS, and Windows

### Risk 3: Inaccurate Available Memory Estimation
**Risk**: Other processes may consume memory between checks
**Mitigation**:
- Use conservative thresholds
- Check memory before AND after chunk processing
- Provide configurable safety margins

## Migration Plan

### Phase 1: Add Optional psutil Support
1. Add `psutil` as optional dependency
2. Implement `MemoryMonitor` class with dual tracking
3. Update `PerformanceMonitor` to use new memory tracking
4. Add system memory metrics to performance reports

### Phase 2: Integrate with Chunked Processing
1. Update `process_in_chunks` to use new memory monitoring
2. Implement graceful degradation logic
3. Add configuration parameters for memory thresholds
4. Update documentation with new options

### Rollback Strategy
- Feature flag to disable system memory monitoring
- Fallback to PyArrow-only monitoring if psutil unavailable
- No breaking changes to existing API

## Open Questions

### Question 1: Memory Polling Frequency
**Question**: How often should we check system memory?
**Options**:
- Every chunk (most accurate, highest overhead)
- Every N chunks (configurable, default N=10)
- Time-based (every X seconds)

**Current Decision**: Every N chunks with configurable N, default 10
