# Design: Fix PyArrow Multi-Column Key Vectorization

## Context
The PyArrow optimization implementation correctly uses vectorized operations for single-column keys but falls back to Python loops for multi-column keys. This creates a performance cliff where datasets with composite keys perform 10-100x slower than those with single-column keys.

**Current Implementation** (dataset.py:639-656):
```python
if len(k_cols) == 1:
    keys = chunk_deduped[k_cols[0]].to_pylist()  # Vectorized
else:
    # Falls back to Python loops
    keys = [
        tuple(d.values())
        for d in _make_struct_safe(chunk_deduped, k_cols).to_pylist()
    ]
```

**Problem**: The `to_pylist()` call converts entire Arrow columns to Python objects, negating all vectorization benefits.

## Goals / Non-Goals

### Goals
- Achieve performance parity between single-key and multi-key operations
- Use native PyArrow operations for all key matching
- Maintain correctness for all data types in composite keys
- Support arbitrary number of key columns

### Non-Goals
- Optimizing for extremely high numbers of key columns (>10)
- Supporting nested/complex types as key components
- Changing the public API

## Decisions

### Decision 1: Use StructArray for Multi-Column Key Representation
**Problem**: Need efficient representation of composite keys
**Solution**: Use `pa.StructArray.from_arrays()` to create a single column representing the composite key
**Rationale**: 
- StructArrays support efficient comparison operations
- Native Arrow type, no Python conversion needed
- Works with `pc.is_in()` for set membership

**Implementation**:
```python
def _create_composite_key_array(table: pa.Table, key_columns: list[str]) -> pa.Array:
    """Create a single array representing composite keys."""
    arrays = [table[c].combine_chunks() for c in key_columns]
    return pa.StructArray.from_arrays(arrays, names=key_columns)
```

### Decision 2: Use PyArrow Join for Set Membership
**Problem**: Need efficient "is key in set" operation for multi-column keys
**Solution**: Use `pa.Table.join()` with `join_type="semi"` for set membership
**Rationale**:
- Native PyArrow operation, fully vectorized
- Handles all data types correctly
- No Python conversion needed

**Implementation**:
```python
def _filter_by_key_membership(
    table: pa.Table,
    key_columns: list[str],
    reference_keys: pa.Table,
    keep_matches: bool = True,
) -> pa.Table:
    """Filter table rows based on key membership in reference set."""
    join_type = "semi" if keep_matches else "anti"
    return table.join(reference_keys, keys=key_columns, join_type=join_type)
```

### Decision 3: Streaming Key Tracking with Arrow Tables
**Problem**: Current implementation uses Python set for `seen_keys`, which doesn't work with Arrow arrays
**Solution**: Maintain seen keys as an Arrow Table, use join for deduplication
**Rationale**:
- Keeps all operations in Arrow space
- Enables vectorized deduplication checking
- Memory usage comparable to Python set

**Implementation**:
```python
# Instead of:
seen_keys = set()
for k in keys:
    if k not in seen_keys:
        seen_keys.add(k)

# Use:
seen_keys_table = None
for chunk in chunks:
    if seen_keys_table is None:
        seen_keys_table = chunk.select(key_columns).drop_duplicates()
    else:
        # Anti-join to find new keys
        new_rows = chunk.join(seen_keys_table, keys=key_columns, join_type="anti")
        seen_keys_table = pa.concat_tables([
            seen_keys_table, 
            new_rows.select(key_columns)
        ]).drop_duplicates()
```

### Decision 4: Binary Join Fallback for Heterogeneous Types
**Problem**: StructArray comparison may fail for certain type combinations
**Solution**: Use `pc.binary_join_element_wise()` to create string keys as fallback
**Rationale**:
- Works with any column types
- Slightly slower but guaranteed to work
- Only used when struct comparison fails

## Risks / Trade-offs

### Risk 1: Memory Usage for Large Key Sets
**Risk**: Maintaining seen_keys as Arrow Table may use more memory than Python set for high-cardinality data
**Mitigation**:
- Periodically compact the seen_keys table
- Use chunked processing to bound memory
- Consider using approximate data structures (e.g., Bloom filter) for very large key sets

### Risk 2: Type Compatibility
**Risk**: Some type combinations may not compare correctly in StructArrays
**Mitigation**:
- Comprehensive test coverage for all type combinations
- Fallback to string-based comparison when needed
- Clear error messages for unsupported types

### Risk 3: Join Performance for Very Large Tables
**Risk**: PyArrow join may be slower than hash set for very large reference sets
**Mitigation**:
- Benchmark against Python set implementation
- Consider hybrid approach for extreme cases
- Document performance characteristics

## Migration Plan

### Phase 1: Implement Vectorized Key Operations
1. Create `_create_composite_key_array()` helper function
2. Create `_filter_by_key_membership()` helper function
3. Update deduplication to use Arrow Table for seen keys
4. Add comprehensive tests for multi-column keys

### Phase 2: Update Merge Operations
1. Replace Python loops in merge key matching
2. Use join operations for set membership
3. Benchmark against current implementation
4. Update documentation

### Rollback Strategy
- Feature flag to use old Python-based implementation
- Automatic fallback if vectorized path fails
- No API changes required

## Open Questions

### Question 1: Optimal Seen Keys Data Structure
**Question**: Is Arrow Table the best structure for tracking seen keys?
**Options**:
- Arrow Table with periodic compaction
- PyArrow's dictionary/hash index (if available)
- Hybrid: Arrow for small sets, Bloom filter for large

**Current Decision**: Arrow Table with periodic compaction, benchmark to validate
