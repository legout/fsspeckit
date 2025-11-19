# Spec Delta: utils-duckdb - Backend-Neutral Merge Layer

## Overview

This delta modifies the `utils-duckdb` specification to integrate the new backend-neutral merge layer while preserving existing API semantics. The changes ensure consistent merge behavior across DuckDB and PyArrow backends through shared validation, statistics, and strategy semantics.

## Changes

### 1. New Shared Dependencies

Add requirement for backend-neutral merge core integration:

```yaml
- Requirement: DuckDBParquetHandler.merge_parquet_dataset SHALL use shared validation and statistics from fsspeckit.core.merge
- Acceptance Criteria:
  - Key normalization via normalize_keys() helper
  - Schema compatibility via shared validate_keys_and_schema()
  - Statistics via shared compute_merge_stats() helper
```

### 2. Enhanced Input Validation

Update validation requirements to reference shared semantics:

```yaml
- Requirement: merge_parquet_dataset SHALL perform canonical key validation using backend-neutral helpers
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

### 4. Streaming Execution Requirements

Add requirements to avoid full dataset materialization:

```yaml
- Requirement: merge_parquet_dataset SHALL process target datasets in streaming fashion
- Acceptance Criteria:
  - Use parquet_scan with key filters instead of full dataset.read_parquet()
  - Process data in batches or via SQL queries that avoid Python materialization
  - Memory usage remains bounded relative to batch size, not total dataset size
```

### 5. Canonical Statistics Structure

Require consistent statistics across backends:

```yaml
- Requirement: merge_parquet_dataset SHALL return canonical statistics structure
- Acceptance Criteria:
  - Returns dict with keys: inserted, updated, deleted, total
  - All values are non-negative integers
  - Statistics computed from actual operation results, not heuristics
  - Consistent with PyArrow backend for identical inputs
```

### 6. Atomicity and Error Handling

Enhance atomicity requirements:

```yaml
- Requirement: merge operations SHALL maintain dataset atomicity
- Acceptance Criteria:
  - Write merged output to temporary directory before swapping
  - Preserve original dataset if merge fails mid-operation
  - Handle empty target and empty source cases consistently
  - Provide clear error messages for schema incompatibility
```

### 7. Edge Case Alignment

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

1. **Consistency**: DuckDB and PyArrow merge behavior becomes identical
2. **Scalability**: Streaming execution handles large datasets efficiently
3. **Maintainability**: Shared validation reduces duplicate code and edge case divergence
4. **Reliability**: Atomic operations and consistent error handling improve robustness

## Migration

Existing code using `DuckDBParquetHandler.merge_parquet_dataset` will continue to work unchanged, with improved consistency and performance as a transparent benefit of the refactored implementation.