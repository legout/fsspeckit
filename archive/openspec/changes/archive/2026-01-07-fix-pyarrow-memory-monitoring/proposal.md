# Change: Fix PyArrow Memory Monitoring to Track System Memory

## Why
The current memory monitoring implementation only tracks PyArrow's internal memory allocation via `pa.total_allocated_bytes()`. This is insufficient because:

1. **System memory not monitored**: Other processes consuming memory can cause OOM errors even when PyArrow allocation is within limits
2. **Python object overhead ignored**: Temporary Python objects created during operations consume system memory but aren't tracked
3. **False safety**: Users may set memory limits expecting system-level protection, but the current implementation only protects PyArrow allocations

This can lead to OOM errors on systems with limited memory or when running alongside other memory-intensive processes.

## What Changes
- Add system memory monitoring using `psutil` or similar
- Track both PyArrow allocation AND total system memory usage
- Implement configurable memory thresholds for both types
- Add memory pressure detection to enable graceful degradation
- Provide accurate memory metrics in performance reports

## Impact
- **Affected specs**: utils-pyarrow, datasets-pyarrow
- **Affected code**: 
  - `src/fsspeckit/datasets/pyarrow/dataset.py` (PerformanceMonitor class, process_in_chunks function)
  - `src/fsspeckit/datasets/pyarrow/io.py` (merge operations)
- **Breaking changes**: None (additive changes only)
- **Dependencies**: Optional `psutil` dependency for system memory monitoring
