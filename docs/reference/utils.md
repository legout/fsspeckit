# Utils Reference

This page explains the `fsspeckit.utils` module as a backwards-compatible façade and provides guidance for migrating to domain packages.

> **Package Structure Note:** fsspeckit has been refactored to use a package-based structure. This page provides migration guidance from the old flat module structure to the new package-based organization.

## Overview

`fsspeckit.utils` serves as a backwards-compatible façade that re-exports selected helpers from domain packages. While existing code continues to work, new development should import directly from domain packages for better discoverability and type hints.

## Migration Mapping

### Common Utilities

| Legacy Import | Domain Package | Recommended Import |
|---------------|----------------|-------------------|
| `from fsspeckit.utils import setup_logging` | Common | `from fsspeckit.common.logging import setup_logging` |
| `from fsspeckit.utils import run_parallel` | Common | `from fsspeckit.common.misc import run_parallel` |
| `from fsspeckit.utils import get_partitions_from_path` | Common | `from fsspeckit.common.misc import get_partitions_from_path` |
| `from fsspeckit.utils import sync_files` | Common | `from fsspeckit.common.misc import sync_files` |
| `from fsspeckit.utils import sync_dir` | Common | `from fsspeckit.common.misc import sync_dir` |
| `from fsspeckit.utils import dict_to_dataframe` | Common | `from fsspeckit.common.types import dict_to_dataframe` |
| `from fsspeckit.utils import to_pyarrow_table` | Common | `from fsspeckit.common.types import to_pyarrow_table` |
| `from fsspeckit.utils import convert_large_types_to_normal` | Common | `from fsspeckit.common.types import convert_large_types_to_normal` |
| `from fsspeckit.utils import opt_dtype_pl` | Common | `from fsspeckit.common.polars import opt_dtype_pl` |
| `from fsspeckit.utils import opt_dtype_pa` | Common | `from fsspeckit.common.types import opt_dtype_pa` |
| `from fsspeckit.utils import cast_schema` | Common | `from fsspeckit.common.types import cast_schema` |

### Dataset Operations

| Legacy Import | Domain Package | Recommended Import |
|---------------|----------------|-------------------|
| `from fsspeckit.utils import DuckDBParquetHandler` | Datasets | `from fsspeckit.datasets import DuckDBParquetHandler` |

### SQL Filtering

| Legacy Import | Domain Package | Recommended Import |
|---------------|----------------|-------------------|
| `from fsspeckit.utils import sql2pyarrow_filter` | SQL | `from fsspeckit.sql.filters import sql2pyarrow_filter` |
| `from fsspeckit.utils import sql2polars_filter` | SQL | `from fsspeckit.sql.filters import sql2polars_filter` |

### Storage Options

| Legacy Import | Domain Package | Recommended Import |
|---------------|----------------|-------------------|
| `from fsspeckit.utils import AwsStorageOptions` | Storage Options | `from fsspeckit.storage_options import AwsStorageOptions` |
| `from fsspeckit.utils import GcsStorageOptions` | Storage Options | `from fsspeckit.storage_options import GcsStorageOptions` |
| `from fsspeckit.utils import AzureStorageOptions` | Storage Options | `from fsspeckit.storage_options import AzureStorageOptions` |
| `from fsspeckit.utils import storage_options_from_env` | Storage Options | `from fsspeckit.storage_options import storage_options_from_env` |

## Available Utils Functions

### Logging

```python
# Legacy (still works)
from fsspeckit.utils import setup_logging

# Recommended (new code)
from fsspeckit.common.logging import setup_logging

# Usage
setup_logging(level="INFO", format_string="{time} | {level} | {message}")
```

### Parallel Processing

```python
# Legacy (still works)
from fsspeckit.utils import run_parallel

# Recommended (new code)
from fsspeckit.common.misc import run_parallel

# Usage
results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=4,
    progress=True
)
```

### Type Conversion

```python
# Legacy (still works)
from fsspeckit.utils import (
    dict_to_dataframe,
    to_pyarrow_table,
    convert_large_types_to_normal
)

# Recommended (new code)
from fsspeckit.common.types import (
    dict_to_dataframe,
    to_pyarrow_table,
    convert_large_types_to_normal
)

# Usage
df = dict_to_dataframe({"col1": [1, 2, 3]}, library="polars")
table = to_pyarrow_table(df)
normal_table = convert_large_types_to_normal(large_string_table)
```

### Data Type Optimization

```python
# Legacy (still works)
from fsspeckit.utils import opt_dtype_pl, opt_dtype_pa

# Recommended (new code)
from fsspeckit.common.polars import opt_dtype_pl
from fsspeckit.common.types import opt_dtype_pa

# Usage
optimized_df = opt_dtype_pl(df, shrink_numerics=True)
optimized_table = opt_dtype_pa(table)
```

### File Operations

```python
# Legacy (still works)
from fsspeckit.utils import (
    sync_files,
    sync_dir,
    get_partitions_from_path
)

# Recommended (new code)
from fsspeckit.common.misc import (
    sync_files,
    sync_dir,
    get_partitions_from_path
)

# Usage
sync_files(
    add_files=["file1.txt", "file2.txt"],
    delete_files=[],
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="/source/",
    dst_path="/target/"
)

sync_dir(
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="/source/",
    dst_path="/target/",
    parallel=True
)

partitions = get_partitions_from_path("/data/year=2023/month=01/file.parquet")
# Returns: {'year': '2023', 'month': '01'}
```

### Dataset Operations

```python
# Legacy (still works)
from fsspeckit.utils import DuckDBParquetHandler

# Recommended (new code)
from fsspeckit.datasets import DuckDBParquetHandler

# Usage
handler = DuckDBParquetHandler(storage_options=storage_options)
handler.write_parquet_dataset(data, "s3://bucket/dataset/")
result = handler.execute_sql("SELECT * FROM parquet_scan('s3://bucket/dataset/')")
```

### SQL Filtering

```python
# Legacy (still works)
from fsspeckit.utils import sql2pyarrow_filter, sql2polars_filter

# Recommended (new code)
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

# Usage
pyarrow_filter = sql2pyarrow_filter("id > 100", schema)
polars_filter = sql2polars_filter("id > 100", schema)
```

### Storage Options

```python
# Legacy (still works)
from fsspeckit.utils import (
    AwsStorageOptions,
    GcsStorageOptions,
    AzureStorageOptions,
    storage_options_from_env
)

# Recommended (new code)
from fsspeckit.storage_options import (
    AwsStorageOptions,
    GcsStorageOptions,
    AzureStorageOptions,
    storage_options_from_env
)

# Usage
aws_options = storage_options_from_env("s3")
aws_fs = AwsStorageOptions(
    region="us-east-1",
    access_key_id="key",
    secret_access_key="secret"
).to_filesystem()
```

## Migration Strategy

### Phase 1: Immediate Compatibility

Existing code continues to work without changes:

```python
# This continues to work unchanged
from fsspeckit.utils import run_parallel, DuckDBParquetHandler

results = run_parallel(process_func, data_list)
handler = DuckDBParquetHandler()
```

### Phase 2: Gradual Migration

Gradually update imports to domain packages:

```python
# Mix of legacy and new imports during transition
from fsspeckit.utils import run_parallel  # Legacy
from fsspeckit.datasets import DuckDBParquetHandler  # New

# Both work together
results = run_parallel(process_func, data_list)
handler = DuckDBParquetHandler()
```

### Phase 3: Complete Migration

Fully migrate to domain packages:

```python
# All imports from domain packages
from fsspeckit.common.misc import run_parallel
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.storage_options import AwsStorageOptions
from fsspeckit.sql.filters import sql2pyarrow_filter

# Clean, discoverable imports
results = run_parallel(process_func, data_list)
handler = DuckDBParquetHandler()
aws_options = AwsStorageOptions(...)
filter_expr = sql2pyarrow_filter("id > 100", schema)
```

## Benefits of Domain Packages

### Better Discoverability

```python
# Clear what you're importing from
from fsspeckit.datasets import DuckDBParquetHandler  # Obvious: dataset operations
from fsspeckit.sql.filters import sql2pyarrow_filter  # Obvious: SQL filtering
from fsspeckit.common.misc import run_parallel  # Obvious: general utilities
```

### Improved Type Hints

```python
# Domain packages provide better type hints
from fsspeckit.datasets import DuckDBParquetHandler

# IDE shows proper type information
handler: DuckDBParquetHandler = DuckDBParquetHandler()
```

### Reduced Import Conflicts

```python
# Domain-specific imports reduce conflicts
from fsspeckit.common.types import dict_to_dataframe
from fsspeckit.datasets import DuckDBParquetHandler

# vs. unclear utils imports
from fsspeckit.utils import dict_to_dataframe, DuckDBParquetHandler
```

## Backwards Compatibility Guarantees

### What's Guaranteed

- All existing `fsspeckit.utils` imports continue to work
- Function signatures remain unchanged
- Behavior is identical to domain package equivalents
- No breaking changes in minor/patch versions

### What's Not Guaranteed

- New features will be added to domain packages first
- Some advanced features may only be available in domain packages
- Deep import paths (e.g., `fsspeckit.utils.misc.function`) are deprecated

### Deprecation Timeline

- **Current**: All utils imports work with deprecation warnings
- **Next Major Version**: Utils imports may show stronger deprecation warnings
- **Future**: Utils module may be removed (with advance notice)

## Best Practices

### For New Code

```python
# Recommended: Import from domain packages
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.common.misc import run_parallel
from fsspeckit.storage_options import AwsStorageOptions
from fsspeckit.sql.filters import sql2pyarrow_filter
```

### For Existing Code

```python
# Option 1: Keep working (no immediate action required)
from fsspeckit.utils import DuckDBParquetHandler, run_parallel

# Option 2: Gradual migration (recommended)
from fsspeckit.datasets import DuckDBParquetHandler  # Migrate this
from fsspeckit.utils import run_parallel  # Keep this for now

# Option 3: Full migration (best for active development)
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.common.misc import run_parallel
```

### For Libraries and Frameworks

```python
# Recommended: Use domain packages in libraries
class MyDataProcessor:
    def __init__(self):
        # Clear dependencies
        from fsspeckit.datasets import DuckDBParquetHandler
        from fsspeckit.common.misc import run_parallel
        
        self.handler = DuckDBParquetHandler()
        self.run_parallel = run_parallel
```

## Troubleshooting

### Import Errors

```python
# If you see ImportError for domain packages
# Make sure you're using a recent version of fsspeckit
pip install --upgrade fsspeckit

# Domain packages were introduced in fsspeckit 0.5.x
```

### Mixed Import Conflicts

```python
# Avoid mixing legacy and new imports for same functionality
# Bad: This can cause confusion
from fsspeckit.utils import run_parallel
from fsspeckit.common.misc import run_parallel  # Conflict!

# Good: Choose one approach
from fsspeckit.common.misc import run_parallel  # Recommended
```

### Finding Right Domain Package

```python
# Not sure where to find a function?
# Check the mapping table above or look at domain packages:

# fsspeckit.common.* - General utilities, parallel processing, type conversion
# fsspeckit.datasets.* - Dataset operations, DuckDB handler
# fsspeckit.sql.* - SQL filter translation
# fsspeckit.storage_options.* - Cloud storage configuration
# fsspeckit.core.* - Filesystem creation, extended I/O
```

## API Reference

For detailed documentation of specific functions and classes:

- [Common Utilities](../api/fsspeckit.common.md)
- [Dataset Operations](../api/fsspeckit.datasets.md)
- [SQL Filtering](../api/fsspeckit.sql.filters.md)
- [Storage Options](../api/fsspeckit.storage_options.base.md)
- [Core Filesystem](../api/fsspeckit.core.filesystem.md)

## Related Documentation

- [API Guide](api-guide.md) - Capability-oriented API overview
- [How-to Guides](../how-to/index.md) - Task-oriented recipes
- [Architecture](../explanation/architecture.md) - Design principles
