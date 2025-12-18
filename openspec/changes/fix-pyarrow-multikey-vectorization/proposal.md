# Change: Fix PyArrow Multi-Column Key Vectorization

## Why
The current implementation of multi-column key handling in merge and deduplication operations falls back to Python loops and `to_pylist()` conversions, which eliminates the performance benefits of vectorization. This regression is present in:

1. **dataset.py lines 639-656**: Streaming deduplication extracts keys using `to_pylist()` for multi-column keys
2. **io.py lines 590-611**: Merge operations use Python loops for multi-column key matching
3. **io.py lines 1397-1408**: UPDATE merge falls back to `to_pylist()` for multi-column filtering

This causes significant performance regression for datasets using composite keys (e.g., `[tenant_id, record_id]`), which are common in multi-tenant applications.

## What Changes
- Replace `to_pylist()` conversions with vectorized PyArrow operations for multi-column keys
- Use `pc.struct_field()` or `pc.binary_join_element_wise()` for efficient key comparison
- Implement vectorized set membership using PyArrow's join operations
- Maintain performance parity between single-key and multi-key operations

## Impact
- **Affected specs**: datasets-pyarrow
- **Affected code**: 
  - `src/fsspeckit/datasets/pyarrow/dataset.py` (lines 639-656, streaming deduplication)
  - `src/fsspeckit/datasets/pyarrow/io.py` (lines 590-611, 1397-1408, merge operations)
- **Performance impact**: 10-100x improvement for multi-column key operations
- **Breaking changes**: None (internal implementation change)
