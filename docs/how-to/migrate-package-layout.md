# Migrate to the New Package Layout

This guide helps you migrate from the old import structure to the new domain-driven package layout introduced in fsspeckit.

## Overview

The package has been refactored to use a domain-driven architecture with clear layering:

- **`fsspeckit.common`** - Shared utilities used across all backends
- **`fsspeckit.core`** - Core filesystem and I/O operations
- **`fsspeckit.datasets`** - Backend-specific dataset operations (PyArrow, DuckDB)
- **`fsspeckit.sql`** - SQL-related functionality

This refactoring eliminates code duplication, resolves circular imports, and establishes clear architectural boundaries.

## Backwards Compatibility

**Important**: All old import paths continue to work through the `fsspeckit.utils` façade. No breaking changes - you can migrate at your own pace.

## Import Migration Guide

### Schema Utilities

#### Old → New Mappings

| Old Import | New Import | Notes |
|------------|-----------|-------|
| `from fsspeckit.datasets.pyarrow.schema import cast_schema` | `from fsspeckit.common.schema import cast_schema` | Canonical location |
| `from fsspeckit.datasets.pyarrow.schema import opt_dtype` | `from fsspeckit.common.schema import opt_dtype` | Canonical location |
| `from fsspeckit.datasets.pyarrow.schema import unify_schemas` | `from fsspeckit.common.schema import unify_schemas` | Canonical location |
| `from fsspeckit.datasets.pyarrow.schema import convert_large_types_to_normal` | `from fsspeckit.common.schema import convert_large_types_to_normal` | Canonical location |

#### Example Migration

**Before:**
```python
from fsspeckit.datasets.pyarrow.schema import cast_schema, opt_dtype

table = pa.table({"a": [1, 2, 3]})
optimized = opt_dtype(table)
```

**After:**
```python
from fsspeckit.common.schema import cast_schema, opt_dtype

table = pa.table({"a": [1, 2, 3]})
optimized = opt_dtype(table)
```

### Dataset Handlers

#### Old → New Mappings

| Old Import | New Import | Notes |
|------------|-----------|-------|
| `from fsspeckit.utils import DuckDBParquetHandler` | `from fsspeckit.datasets.duckdb import DuckDBParquetHandler` | Direct import |
| `from fsspeckit.datasets.pyarrow import ...` | `from fsspeckit.datasets.pyarrow import ...` | No change needed - re-exports from common.schema |

#### Example Migration

**Before:**
```python
from fsspeckit.utils import DuckDBParquetHandler

handler = DuckDBParquetHandler()
```

**After:**
```python
from fsspeckit.datasets.duckdb import DuckDBParquetHandler

handler = DuckDBParquetHandler()
```

### Logging Utilities

#### Old → New Mappings

| Old Import | New Import | Notes |
|------------|-----------|-------|
| `from fsspeckit.utils import setup_logging` | `from fsspeckit.common.logging import setup_logging` | Direct import |

#### Example Migration

**Before:**
```python
from fsspeckit.utils import setup_logging

setup_logging(level="INFO")
```

**After:**
```python
from fsspeckit.common.logging import setup_logging

setup_logging(level="INFO")
```

## Complete Migration Example

### Before (Old Structure)

```python
import pyarrow as pa
from fsspeckit.utils import DuckDBParquetHandler, setup_logging
from fsspeckit.datasets.pyarrow.schema import cast_schema, opt_dtype

# Setup
setup_logging(level="INFO")

# Work with data
table = pa.table({"id": [1, 2, 3], "value": [1.0, 2.0, 3.0]})
optimized_table = opt_dtype(table)

handler = DuckDBParquetHandler()
```

### After (New Structure)

```python
import pyarrow as pa
from fsspeckit.datasets.duckdb import DuckDBParquetHandler
from fsspeckit.common.logging import setup_logging
from fsspeckit.common.schema import cast_schema, opt_dtype

# Setup
setup_logging(level="INFO")

# Work with data
table = pa.table({"id": [1, 2, 3], "value": [1.0, 2.0, 3.0]})
optimized_table = opt_dtype(table)

handler = DuckDBParquetHandler()
```

## When to Migrate

### Recommended Migration Timeline

- **Immediate (Optional)**: Update imports in new code for better clarity
- **Within 1-2 weeks**: Update imports in frequently modified modules
- **Before next major version**: Migrate all imports (old imports will continue to work indefinitely)

### Benefits of Migration

1. **Clearer Dependencies**: Direct imports show which domain package you're using
2. **Better IDE Support**: Autocomplete shows relevant functions for your use case
3. **Future-Proof**: Aligns with long-term architecture direction
4. **Consistency**: All backends use same canonical implementations

## Troubleshooting

### Import Errors

If you encounter import errors after migration:

1. **Verify the new import path** exists in the table above
2. **Check for typos** in the import path
3. **Ensure you're using the correct package** (`common` vs `datasets` vs `core`)

### Circular Imports

The refactor fixes a critical circular import issue. If you still experience circular import errors:

1. Ensure you're not importing from `fsspeckit.core.filesystem` in a module that's already imported by `fsspeckit.core`
2. Use dependency injection or lazy imports where needed

### Performance

No performance impact expected - the refactor actually reduces import overhead by eliminating code duplication.

## Still Using Old Imports?

**That's okay!** The `fsspeckit.utils` façade continues to work and will be maintained indefinitely. All re-exports are verified to point to the same canonical implementations.

## Getting Help

- **Documentation**: See [API Reference](../api/fsspeckit.common.md) for common schema utilities
- **Issues**: Report problems on the project issue tracker
- **Discussions**: Join the community discussion for migration questions

## Summary

| Change Type | Impact | Action Required |
|-------------|--------|-----------------|
| Schema utilities moved to `common.schema` | Low - new canonical location | Optional update |
| Dataset handlers use domain packages | Low - clearer imports | Optional update |
| Logging uses `common.logging` | Low - direct import | Optional update |
| Backwards compatibility | **None** - all old imports work | **No action needed** |

The refactor improves code organization without breaking any existing code. Migrate at your own pace!
