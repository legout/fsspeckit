# Feature: Add DuckDB MERGE Statement Support

## Why

The current DuckDB merge implementation in `fsspeckit` uses UNION ALL combined with NOT EXISTS subqueries to perform INSERT, UPDATE, and UPSERT operations. While this works and was a significant improvement over the previous PyArrow in-memory approach, DuckDB 1.4.0 introduced native MERGE support that offers several advantages:

### Limitations of Current UNION ALL Approach
- **Atomicity not guaranteed**: Multiple SQL statements without explicit transaction wrapping
- **Multiple execution passes**: Separate queries for matching and non-matching rows
- **Limited auditability**: No built-in mechanism to track which rows were inserted vs updated
- **Complex query structure**: UNION ALL + NOT EXISTS subqueries are harder to maintain

### Advantages of DuckDB MERGE (1.4.0+)
- **Full ACID compliance**: MERGE operations are atomic and transactional
- **Single-pass execution**: Optimized query plan processes source and target in one operation
- **Declarative logic**: Clear WHEN MATCHED/NOT MATCHED clauses vs nested subqueries
- **Built-in audit trail**: RETURNING clause provides visibility into merge actions
- **Industry standard**: MERGE follows SQL:2008 standard, well-understood pattern
- **Performance potential**: Expected 20-40% improvement for large datasets based on DuckDB optimizations

## What Changes

Add native DuckDB MERGE statement support as the **default merge implementation** while maintaining the current UNION ALL approach as a **fallback** for older DuckDB versions.

### Core Changes

1. **Version Detection System**
   - Add runtime DuckDB version detection via `pragma_version()`
   - Implement version gating logic (MERGE for >=1.4.0, UNION ALL otherwise)

2. **MERGE Implementation**
   - New `_merge_using_duckdb_merge()` method supporting all three strategies
   - Proper MERGE syntax with `USING parquet_scan()` for file-level operations
   - RETURNING clause for audit trails

3. **Fallback Mechanism**
   - Rename current `_merge_file_with_sql()` to `_merge_using_union_all()`
   - Add version-gated routing in main `merge()` method
   - Graceful degradation when MERGE unavailable

4. **User Controls**
   - Optional `use_merge` parameter for explicit strategy selection
   - Automatic detection with logging when MERGE is used vs fallback

### Strategy Support

All three merge strategies will be supported via MERGE:

```sql
-- INSERT strategy: Only insert new keys
MERGE INTO target
    USING source
    ON (target.key = source.key)
    WHEN NOT MATCHED THEN INSERT BY NAME;

-- UPDATE strategy: Only update existing keys
MERGE INTO target
    USING source
    ON (target.key = source.key)
    WHEN MATCHED THEN UPDATE SET *;

-- UPSERT strategy: Update existing keys, insert new keys
MERGE INTO target
    USING source
    ON (target.key = source.key)
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT BY NAME;
```

## Impact

### Affected Code
- **Primary**: `src/fsspeckit/datasets/duckdb/dataset.py`
  - `DuckDBDatasetIO.merge()` method
  - New `_merge_using_duckdb_merge()` method
  - Refactored `_merge_using_union_all()` method
  - New version detection utilities

### Breaking Changes
- **None**: This change is fully backward compatible
  - Existing behavior preserved via UNION ALL fallback for DuckDB < 1.4.0
  - New `use_merge` parameter is optional (default: auto-detect)

### Performance
- **Expected improvement**: 20-40% faster for large merge operations
- **Memory usage**: Similar to current implementation (both use DuckDB streaming)
- **I/O patterns**: Reduced due to single-pass MERGE execution

### API Changes
- **Add to `DuckDBDatasetIO.merge()`**:
  ```python
  def merge(
      self,
      data: pa.Table | list[pa.Table],
      path: str,
      strategy: Literal["insert", "update", "upsert"],
      key_columns: list[str] | str,
      *,
      partition_columns: list[str] | str | None = None,
      schema: pa.Schema | None = None,
      compression: str | None = "snappy",
      max_rows_per_file: int | None = 5_000_000,
      row_group_size: int | None = 500_000,
      use_merge: bool | None = None,  # NEW: Explicit control over MERGE usage
  ) -> "MergeResult":
  ```

### Dependencies
- **Minimum DuckDB version**: 1.0.0 (maintains current compatibility)
- **Recommended DuckDB version**: 1.4.0+ for MERGE features
- **New Python dependencies**: None

## Risks / Trade-offs

### Risks
1. **New feature stability**: MERGE was just introduced in DuckDB 1.4.0
   - **Mitigation**: Fallback to UNION ALL for older versions, comprehensive testing
2. **Edge case differences**: MERGE behavior may differ from UNION ALL for null handling
   - **Mitigation**: Full test coverage, specific tests for null/edge cases
3. **Version detection complexity**: Runtime version checks add minor overhead
   - **Mitigation**: Cache version check result per connection

### Trade-offs
1. **Code complexity**: Additional routing logic for version gating
   - **Benefit**: Better performance, future-proof design
2. **Testing surface**: Need to test both MERGE and UNION ALL paths
   - **Benefit**: Ensures backward compatibility

## Migration Guide

### For Users
**No migration required**: Users will automatically benefit from MERGE if they have DuckDB 1.4.0+ installed.

**Optional configuration**: Users can explicitly control merge strategy:
```python
# Force MERGE (will raise error if not available)
result = io.merge(data, path, strategy="upsert", key_columns=["id"], use_merge=True)

# Force UNION ALL fallback
result = io.merge(data, path, strategy="upsert", key_columns=["id"], use_merge=False)

# Auto-detect (default)
result = io.merge(data, path, strategy="upsert", key_columns=["id"])
```

### For Developers
**Testing changes**: Ensure tests run with both:
- DuckDB >= 1.4.0 (MERGE path)
- DuckDB < 1.4.0 (UNION ALL fallback)

**Logging**: Watch for merge strategy selection:
```python
logger.info("Using MERGE for merge operation")  # For DuckDB >= 1.4.0
logger.info("Using UNION ALL fallback for merge operation")  # For DuckDB < 1.4.0
```

## Acceptance Criteria

1. ✅ MERGE implementation works for all three strategies (INSERT, UPDATE, UPSERT)
2. ✅ Version detection correctly identifies DuckDB >= 1.4.0
3. ✅ Fallback to UNION ALL works for DuckDB < 1.4.0
4. ✅ All existing merge tests pass with both MERGE and UNION ALL
5. ✅ New tests added specifically for MERGE functionality
6. ✅ RETURNING clause properly tracks insert/update counts
7. ✅ `use_merge` parameter correctly overrides auto-detection
8. ✅ Performance benchmarks show improvement over UNION ALL approach
9. ✅ Documentation updated with MERGE examples and version requirements
10. ✅ No breaking changes to existing API

## Related Work

- **Completed**: `refactor-duckdb-module-simplification` (established SQL-based merge foundation)
- **Future**: Could extend MERGE to support DELETE strategies (soft deletes, SCD Type 2)
