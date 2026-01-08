# Change: Optimize PyArrow Dataset Operations Performance

## Why
PyArrow dataset operations have severe performance bottlenecks that make them unusable for large datasets:

1. **Inefficient deduplication**: Converting entire PyArrow tables to Python lists and using Python loops (O(n²) complexity)
2. **Memory inefficiency**: Loading entire datasets into memory instead of streaming
3. **Missing vectorized operations**: Using Python loops instead of optimized PyArrow operations

## What Changes
- Replace Python list-based deduplication with vectorized PyArrow operations
- Implement chunked processing for large datasets to avoid memory issues
- Optimize `deduplicate_parquet_dataset_pyarrow()` to use PyArrow's built-in deduplication
- Add streaming processing for merge operations
- Implement proper batch processing for large table operations

## Impact
- **Affected specs**: datasets-pyarrow
- **Affected code**: 
  - `src/fsspeckit/datasets/pyarrow/dataset.py` (lines 430-484, 302-512)
  - `src/fsspeckit/datasets/pyarrow/io.py` (lines 696-759)
- **Performance impact**: 10-100x performance improvement for large datasets
- **Breaking changes**: None
