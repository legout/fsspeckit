# Schema and Partition Logic Consolidation

This document describes the consolidation of schema and partition logic in fsspeckit to ensure consistent behavior across backends.

## Overview

The consolidation effort moves schema and partition-related functionality from backend-specific modules into shared utilities in `fsspeckit.common`. This ensures that DuckDB and PyArrow backends make identical decisions when working with the same data.

## Changes Made

### 1. New Shared Modules

#### `fsspeckit.common.schema`
Consolidates schema-related functionality:
- **Schema unification**: `unify_schemas()` with intelligent conflict resolution
- **Timezone handling**: `standardize_schema_timezones()`, `dominant_timezone_per_column()`
- **Type conversion**: `convert_large_types_to_normal()`, `cast_schema()`
- **Schema optimization**: Data type optimization and empty column removal
- **Fallback strategies**: Multiple fallback strategies for difficult unification scenarios

#### `fsspeckit.common.partitions`
Consolidates partition-related functionality:
- **Partition extraction**: `get_partitions_from_path()` with support for Hive and directory schemes
- **Partition validation**: `validate_partition_columns()`, `normalize_partition_value()`
- **Path building**: `build_partition_path()` for constructing partitioned paths
- **Filtering**: `filter_paths_by_partitions()`, `apply_partition_pruning()`
- **Scheme inference**: `infer_partitioning_scheme()` for automatic detection
- **Expression creation**: `create_partition_expression()` for backend-specific filters

### 2. Backend Refactoring

#### PyArrow Backend (`fsspeckit.datasets.pyarrow`)
- Schema functions now delegate to shared utilities
- Partition parsing uses shared implementation
- Maintained backward compatibility during transition

#### DuckDB Backend (`fsspeckit.datasets.duckdb`)
- Already used shared core maintenance functions
- No changes needed for partition logic (uses core functions)

#### Core Maintenance (`fsspeckit.core.maintenance`)
- Remains the authoritative implementation for maintenance operations
- Both backends delegate to this module for consistency

### 3. Testing

#### Schema Tests (`tests/test_common_schema.py`)
- Comprehensive tests for all schema utilities
- Tests for timezone handling, type conversion, and unification
- Edge case and error handling tests

#### Partition Tests (`tests/test_common_partitions.py`)
- Complete test coverage for partition utilities
- Tests for different partitioning schemes and edge cases
- Integration tests for partition filtering and path building

#### Integration Tests (`tests/test_backend_integration.py`)
- Tests to verify consistent behavior across backends
- Schema and partition utility integration tests
- Error handling consistency tests

## Benefits

### 1. Consistency
- Both backends now use identical schema unification logic
- Partition parsing is consistent across all components
- Timezone handling is standardized

### 2. Maintainability
- Single source of truth for schema and partition logic
- Easier to add new features and fix bugs
- Clear separation of concerns

### 3. Testability
- Shared utilities can be tested independently
- Reduced test duplication across backends
- Better coverage for edge cases

### 4. Performance
- Partition pruning reduces unnecessary I/O
- Optimized schema unification with early exits
- Efficient conflict detection and resolution

## Usage

### Schema Operations
```python
from fsspeckit.common.schema import unify_schemas, standardize_schema_timezones

# Unify multiple schemas with timezone standardization
schemas = [schema1, schema2, schema3]
unified = unify_schemas(schemas, standardize_timezones=True, timezone="auto")

# Convert large types to standard types
normal_schema = convert_large_types_to_normal(schema_with_large_types)
```

### Partition Operations
```python
from fsspeckit.common.partitions import (
    get_partitions_from_path, 
    filter_paths_by_partitions,
    infer_partitioning_scheme
)

# Extract partitions from path
partitions = get_partitions_from_path("data/year=2023/month=01/file.parquet", "hive")

# Filter files by partition values
filtered = filter_paths_by_partitions(file_list, {"year": "2023", "month": "01"})

# Infer partitioning scheme from file list
scheme = infer_partitioning_scheme(file_list)
```

## Migration Guide

### For Backend Developers
- Use `fsspeckit.common.schema.unify_schemas()` instead of custom unification
- Use `fsspeckit.common.partitions.get_partitions_from_path()` for partition parsing
- Delegate to `fsspeckit.core.maintenance` for maintenance operations

### For Users
- No breaking changes expected
- Existing code continues to work during transition
- New shared utilities available for direct use

## Future Work

1. Complete backend refactoring to use shared utilities
2. Add more sophisticated partitioning scheme support
3. Enhance schema unification with user-defined rules
4. Add performance benchmarks for unified operations

## Compatibility

- Backward compatible with existing fsspeckit APIs
- No changes to public interfaces required
- Shared utilities follow existing patterns and conventions