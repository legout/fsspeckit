# Change: Add Memory Bounds to PyArrow Streaming Deduplication

## Why
The current streaming deduplication implementation uses a Python `set` to track seen keys (`seen_keys`), which can grow unbounded for high-cardinality datasets. This causes:

1. **Unbounded memory growth**: For datasets with millions of unique keys, the `seen_keys` set can consume all available memory
2. **OOM errors**: Processing large datasets with high cardinality can crash the process
3. **Memory fragmentation**: The Python set implementation causes memory fragmentation for very large key sets

This is particularly problematic for:
- Time-series data with unique timestamps
- Event logs with unique event IDs  
- Multi-tenant data with many unique composite keys

## What Changes
- Implement bounded memory for key tracking using LRU caching or approximate data structures
- Add configurable maximum keys to track before switching to approximate methods
- Implement Bloom filter fallback for very large key sets
- Add key cardinality estimation to guide memory strategy selection

## Impact
- **Affected specs**: datasets-pyarrow
- **Affected code**: 
  - `src/fsspeckit/datasets/pyarrow/dataset.py` (lines 650-656, streaming deduplication)
  - `src/fsspeckit/datasets/pyarrow/io.py` (merge key tracking)
- **Performance impact**: Slight overhead for bounded structures, but prevents OOM
- **Breaking changes**: None (behavioral improvement)
