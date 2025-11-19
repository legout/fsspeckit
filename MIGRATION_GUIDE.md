# Migration Guide: Package Layout Refactoring

## Overview

This guide helps you migrate from the old `fsspeckit.utils` structure to the new domain package structure introduced in version 0.x.

**Breaking Change**: The `fsspeckit.utils` submodules have been reorganized into domain-specific packages for better discoverability and clearer architectural boundaries.

## New Package Structure

The refactoring introduces three new domain packages:

- **`fsspeckit.datasets`** - Dataset-level operations (DuckDB & PyArrow)
- **`fsspeckit.sql`** - SQL-to-filter translation helpers
- **`fsspeckit.common`** - Cross-cutting utilities

## Import Migration

### Old → New Import Mappings

| Old Import | New Import |
|-----------|------------|
| `from fsspeckit.utils.duckdb import DuckDBParquetHandler` | `from fsspeckit.datasets import DuckDBParquetHandler` |
| `from fsspeckit.utils.pyarrow import cast_schema` | `from fsspeckit.datasets import cast_schema` |
| `from fsspeckit.utils.pyarrow import convert_large_types_to_normal` | `from fsspeckit.datasets import convert_large_types_to_normal` |
| `from fsspeckit.utils.pyarrow import opt_dtype as opt_dtype_pa` | `from fsspeckit.datasets import opt_dtype_pa` |
| `from fsspeckit.utils.pyarrow import unify_schemas` | `from fsspeckit.datasets import unify_schemas` |
| `from fsspeckit.utils.sql import sql2pyarrow_filter` | `from fsspeckit.sql.filters import sql2pyarrow_filter` |
| `from fsspeckit.utils.sql import sql2polars_filter` | `from fsspeckit.sql.filters import sql2polars_filter` |
| `from fsspeckit.utils.datetime import timestamp_from_string` | `from fsspeckit.common.datetime import timestamp_from_string` |
| `from fsspeckit.utils.datetime import get_timestamp_column` | `from fsspeckit.common import get_timestamp_column` |
| `from fsspeckit.utils.logging import setup_logging` | `from fsspeckit.common.logging import setup_logging` |
| `from fsspeckit.utils.logging import get_logger` | `from fsspeckit.common.logging import get_logger` |
| `from fsspeckit.utils.misc import run_parallel` | `from fsspeckit.common.misc import run_parallel` |
| `from fsspeckit.utils.polars import opt_dtype as opt_dtype_pl` | `from fsspeckit.common.polars import opt_dtype as opt_dtype_pl` |
| `from fsspeckit.utils.types import dict_to_dataframe` | `from fsspeckit.common.types import dict_to_dataframe` |
| `from fsspeckit.utils.types import to_pyarrow_table` | `from fsspeckit.common.types import to_pyarrow_table` |

### Backwards Compatibility

**All existing `fsspeckit.utils` imports continue to work without changes!**

The `fsspeckit.utils` module now acts as a façade that re-exports from the new domain packages, ensuring that existing code continues to function.

However, we recommend migrating to the new domain packages for:
- Better discoverability
- Clearer semantic boundaries
- Future compatibility

## Migration Examples

### Example 1: DuckDB Dataset Handler

**Before:**
```python
from fsspeckit.utils.duckdb import DuckDBParquetHandler

handler = DuckDBParquetHandler(storage_options=opts)
```

**After:**
```python
from fsspeckit.datasets import DuckDBParquetHandler

handler = DuckDBParquetHandler(storage_options=opts)
```

### Example 2: SQL Filter Conversion

**Before:**
```python
from fsspeckit.utils.sql import sql2pyarrow_filter

filter_expr = sql2pyarrow_filter("age > 25", schema)
```

**After:**
```python
from fsspeckit.sql.filters import sql2pyarrow_filter

filter_expr = sql2pyarrow_filter("age > 25", schema)
```

### Example 3: Common Utilities

**Before:**
```python
from fsspeckit.utils.datetime import timestamp_from_string
from fsspeckit.utils.logging import setup_logging
from fsspeckit.utils.polars import opt_dtype as opt_dtype_pl

timestamp = timestamp_from_string("2023-01-01T10:00:00")
setup_logging()
df_optimized = opt_dtype_pl(df)
```

**After:**
```python
from fsspeckit.common.datetime import timestamp_from_string
from fsspeckit.common.logging import setup_logging
from fsspeckit.common.polars import opt_dtype as opt_dtype_pl

timestamp = timestamp_from_string("2023-01-01T10:00:00")
setup_logging()
df_optimized = opt_dtype_pl(df)
```

### Example 4: PyArrow Dataset Operations

**Before:**
```python
from fsspeckit.utils.pyarrow import cast_schema, convert_large_types_to_normal

# Large type conversion
normal_schema = convert_large_types_to_normal(original_schema)
# Schema casting
table_casted = cast_schema(table, target_schema)
```

**After:**
```python
from fsspeckit.datasets import cast_schema, convert_large_types_to_normal

# Large type conversion
normal_schema = convert_large_types_to_normal(original_schema)
# Schema casting
table_casted = cast_schema(table, target_schema)
```

## Package Summary

### fsspeckit.datasets
Contains dataset-specific functionality:
- `DuckDBParquetHandler` - High-performance DuckDB parquet operations
- `MergeStrategy` - Dataset merge strategies
- PyArrow utilities for schema management
- Dataset optimization and merging tools

### fsspeckit.sql
Contains SQL parsing and filter conversion:
- `sql2pyarrow_filter` - Convert SQL to PyArrow expressions
- `sql2polars_filter` - Convert SQL to Polars expressions
- `get_table_names` - Extract table names from SQL queries

### fsspeckit.common
Contains cross-cutting utilities:
- **datetime**: Timestamp parsing, timezone handling
- **logging**: Log configuration and logger utilities
- **misc**: File operations, parallel processing, sync utilities
- **polars**: DataFrame optimization and manipulation
- **types**: Data conversion and transformation utilities

## Testing Your Migration

1. **Test with existing imports first** - your current code should continue to work
2. **Gradually migrate to new imports** - change imports one module at a time
3. **Run your test suite** - ensure functionality remains unchanged
4. **Update documentation** - use new import paths in examples

## Troubleshooting

### Import Errors

If you encounter import errors after migration:

1. **Check spelling** - ensure new package names are spelled correctly
2. **Verify module existence** - make sure the function/class exists in the expected location
3. **Use backwards compatibility** - revert to old imports if needed

### Test Failures

Test files that import from `fsspeckit.utils.*` may need updates to use the new import paths.

## Support

If you encounter issues during migration:
1. Check this guide for correct import mappings
2. Verify you're using the latest version of fsspeckit
3. Review the package documentation for updated examples
4. Open an issue on the project repository with migration questions

## Benefits of Migration

- **Better Discoverability**: Find functionality faster with domain-specific packages
- **Clearer Architecture**: Understand the relationship between different components
- **Improved Maintainability**: Clear boundaries make future contributions easier
- **Better IDE Support**: More accurate auto-completion and navigation
- **Semantic Clarity**: Package names clearly indicate their purpose