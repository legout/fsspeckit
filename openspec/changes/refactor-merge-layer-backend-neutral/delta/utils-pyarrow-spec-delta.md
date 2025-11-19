# Spec Delta: utils-pyarrow - Backend-Neutral Merge Layer

## Overview

This delta modifies the `utils-pyarrow` specification to integrate the new backend-neutral merge layer while preserving existing streaming semantics. The changes ensure consistent merge behavior across PyArrow and DuckDB backends through shared validation, statistics, and strategy semantics.

## Changes

### 1. New Shared Dependencies

Add requirement for backend-neutral merge core integration:

```yaml
- Requirement: merge_parquet_dataset_pyarrow SHALL use shared validation and statistics from fsspeckit.core.merge
- Acceptance Criteria:
  - Key normalization via normalize_keys() helper
  - Schema compatibility via shared validate_keys_and_schema()
  - Statistics via shared compute_merge_stats() helper
```

### 2. Enhanced Input Validation

Update validation requirements to reference shared semantics:

```yaml
- Requirement: merge_parquet_dataset_pyarrow SHALL perform canonical key validation using backend-neutral helpers
- Acceptance Criteria:
  - Normalizes key_columns input using shared normalize_keys()
  - Validates key existence in both source and target schemas
  - Checks for NULL keys using shared validation patterns
  - Raises consistent ValueError messages across backends
```

### 3. Canonical Strategy Semantics

Define merge strategy behavior using shared canonical semantics:

```yaml
- Requirement: All merge strategies SHALL follow canonical semantics defined in backend-neutral core
- Acceptance Criteria:
  - UPSERT: Insert new rows, update existing rows based on key match
  - INSERT: Insert only rows with keys not present in target
  - UPDATE: Update only rows with keys present in target
  - FULL_MERGE: Insert new rows, update existing rows, delete target rows not in source
  - DEDUPLICATE: Remove duplicates from source, then apply UPSERT semantics
```

### 4. Streaming Scanner Requirements

Enhance filtered scanner usage as primary mechanism:

```yaml
- Requirement: merge_parquet_dataset_pyarrow SHALL use filtered scanners as primary mechanism
- Acceptance Criteria:
  - Use pyarrow.dataset.Dataset scanners with key-based filters
  - Process target data in batches via scanner.to_batches()
  - Avoid dataset.to_table() on full target dataset
  - Filter expressions built around key column values
```

### 5. Per-Group Processing

Require per-group streaming execution:

```yaml
- Requirement: merge operations SHALL process data in streaming batches
- Acceptance Criteria:
  - Use batch_rows parameter to control memory usage
  - Process each target batch independently
  - Build output tables incrementally per batch
  - Track statistics incrementally during batch processing
```

### 6. Canonical Statistics Structure

Require consistent statistics across backends:

```yaml
- Requirement: merge_parquet_dataset_pyarrow SHALL return canonical statistics structure
- Acceptance Criteria:
  - Returns dict with keys: inserted, updated, deleted, total
  - All values are non-negative integers
  - Statistics computed from actual row processing, not heuristics
  - Consistent with DuckDB backend for identical inputs
```

### 7. Enhanced NULL Key Detection

Improve NULL key checking with filtered scanners:

```yaml
- Requirement: NULL key detection SHALL use efficient filtering
- Acceptance Criteria:
  - Use filtered scanners to detect NULL values in key columns
  - Raise ValueError before processing if NULL keys found
  - Apply to both source table and target dataset
  - Avoid materializing full dataset for NULL checking
```

### 8. Schema Compatibility

Leverage shared schema compatibility logic:

```yaml
- Requirement: merge operations SHALL use shared schema compatibility helpers
- Acceptance Criteria:
  - Use unify_schemas() from shared core for compatible type promotion
  - Apply consistent type casting across both backends
  - Raise clear errors for incompatible schema combinations
  - Preserve existing casting behavior for compatible schemas
```

### 9. Edge Case Alignment

Define behavior for edge cases using shared semantics:

```yaml
- Requirement: Edge case behavior SHALL match shared canonical definitions
- Acceptance Criteria:
  - Empty target + UPSERT/INSERT: All source rows inserted, deleted=0
  - Empty target + UPDATE: Zero updated rows, no error (consistent no-op)
  - Empty source + FULL_MERGE: Target becomes empty, deleted=original_target_count
  - Schema incompatibility: Use unify_schemas() for compatible types, raise for incompatible
```

## Impact

These changes ensure:

1. **Consistency**: PyArrow and DuckDB merge behavior becomes identical
2. **Performance**: Enhanced filtered scanner usage improves efficiency
3. **Memory Efficiency**: Strict streaming execution maintains bounded memory usage
4. **Maintainability**: Shared validation reduces duplicate code and edge case divergence

## Migration

Existing code using `merge_parquet_dataset_pyarrow` will continue to work unchanged, with improved consistency and streaming efficiency as a transparent benefit of the refactored implementation.

## Backward Compatibility

- All existing function signatures remain unchanged
- Existing merge strategies work identically
- Return format and behavior preserved
- Enhanced error messages improve debugging without breaking changes