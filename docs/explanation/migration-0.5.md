# Migration Guide: fsspeckit 0.5.x

This guide helps you migrate to fsspeckit 0.5.x, which introduced significant architectural changes including domain package organization, enhanced error handling, and security features.

## Table of Contents

- [Overview of Changes](#overview-of-changes)
- [From fsspec-utils to fsspeckit](#from-fsspec-utils-to-fsspeckit)
- [Domain Package Migration](#domain-package-migration)
- [Logging Changes](#logging-changes)
- [Error Handling Improvements](#error-handling-improvements)
- [Dataset Operations](#dataset-operations)
- [Optional Dependencies](#optional-dependencies)
- [Security Features](#security-features)

## Overview of Changes

fsspeckit 0.5.x introduced major architectural improvements:

1. **Domain Package Organization**: Functionality reorganized into `fsspeckit.core`, `fsspeckit.storage_options`, `fsspeckit.datasets`, `fsspeckit.sql`, and `fsspeckit.common`
2. **Enhanced Error Handling**: Consistent error types and improved error messages
3. **Security Features**: Built-in helpers for path validation, credential scrubbing, and compression safety
4. **Logging Infrastructure**: New logging setup with `FSSPECKIT_LOG_LEVEL` environment variable
5. **Path Safety**: `filesystem()` wraps filesystems in `DirFileSystem` by default for enhanced security

## From fsspec-utils to fsspeckit

### Import Changes

**Before (fsspec-utils):**
```python
from fsspec_utils import run_parallel, storage_options_from_env
from fsspec_utils.datasets import DuckDBParquetHandler
```

**After (fsspeckit 0.5.x):**
```python
from fsspeckit.common.misc import run_parallel
from fsspeckit.storage_options import storage_options_from_env
from fsspeckit.datasets import DuckDBParquetHandler
```

### Filesystem Creation

**Before:**
```python
import fsspec
fs = fsspec.filesystem("s3", **storage_options)
```

**After:**
```python
from fsspeckit.core.filesystem import filesystem
fs = filesystem("s3", storage_options=storage_options)
```

**Key Changes:**
- Use `filesystem()` instead of `fsspec.filesystem()`
- Protocol detection works from URIs: `filesystem("s3://bucket/path")`
- Automatic wrapping in `DirFileSystem` for path safety

### Dataset Operations

**Before:**
```python
from fsspec_utils.datasets import write_parquet_dataset
write_parquet_dataset(data, path, storage_options=storage_options)
```

**After:**
```python
from fsspeckit.datasets import DuckDBParquetHandler
handler = DuckDBParquetHandler(storage_options=storage_options.to_dict())
handler.write_parquet_dataset(data, path)
```

**Key Changes:**
- Use `DuckDBParquetHandler` class instead of standalone functions
- Atomic write guarantees by default
- Enhanced SQL integration with `execute_sql()`

## Domain Package Migration

For existing `fsspeckit.utils` users, migrate to domain packages:

### Common Utilities

**Old (still works):**
```python
from fsspeckit.utils import run_parallel, setup_logging, dict_to_dataframe
```

**New (recommended):**
```python
from fsspeckit.common.misc import run_parallel
from fsspeckit.common.logging import setup_logging
from fsspeckit.common.types import dict_to_dataframe
```

### Dataset Operations

**Old (still works):**
```python
from fsspeckit.utils import DuckDBParquetHandler
```

**New (recommended):**
```python
from fsspeckit.datasets import DuckDBParquetHandler
```

### SQL Filtering

**Old (still works):**
```python
from fsspeckit.utils import sql2pyarrow_filter
```

**New (recommended):**
```python
from fsspeckit.sql.filters import sql2pyarrow_filter
```

### Storage Options

**Old (still works):**
```python
from fsspeckit.utils import AwsStorageOptions
```

**New (recommended):**
```python
from fsspeckit.storage_options import AwsStorageOptions
```

## Logging Changes

### Environment Variable

**Before:**
```bash
export fsspeckit_LOG_LEVEL=DEBUG
```

**After:**
```bash
export FSSPECKIT_LOG_LEVEL=DEBUG
```

### Logging Setup

**Before (implicit setup):**
```python
# Logging was basic or manual
import logging
logging.basicConfig(level=logging.DEBUG)
```

**After (explicit setup):**
```python
from fsspeckit.common.logging import setup_logging

# Configure with format and level
setup_logging(level="DEBUG", format_string="{time} | {level} | {message}")
```

**Key Changes:**
- New top-level `setup_logging()` entrypoint
- `FSSPECKIT_LOG_LEVEL` environment variable (instead of `fsspeckit_LOG_LEVEL`)
- Loguru-based logging with structured output
- Application-level loggers via `fsspeckit.common.logging.get_logger()`

### Safe Error Logging

**New in 0.5.x:**
```python
from fsspeckit.common.security import scrub_credentials, safe_format_error

# Prevent credential leakage in logs
error_msg = scrub_credentials(f"Failed: access_key={access_key}")
logger.error(error_msg)

# Safe error formatting
safe_error = safe_format_error(
    operation="read file",
    path=path,
    error=e
)
logger.error(safe_error)
```

## Error Handling Improvements

### Consistent Exception Types

**Before:** Inconsistent exception handling
```python
try:
    risky_operation()
except Exception as e:
    # Basic error handling
    pass
```

**After:** Structured error handling
```python
try:
    risky_operation()
except ValueError as e:
    # Configuration/validation errors
    logger.error(f"Invalid configuration: {e}")
except FileNotFoundError as e:
    # Missing resources
    logger.error(f"Resource not found: {e}")
except PermissionError as e:
    # Access control issues
    logger.error(f"Access denied: {e}")
```

### Input Validation

**New in 0.5.x:**
```python
from fsspeckit.common.security import (
    validate_path,
    validate_compression_codec,
    validate_columns
)

# Validate paths before use
safe_path = validate_path(user_path, base_dir="/data/allowed")

# Validate compression codecs
safe_codec = validate_compression_codec(user_codec)

# Validate column selections
validated_cols = validate_columns(user_cols, valid_columns=schema_columns)
```

## Dataset Operations

### DuckDB Handler

**Before:**
```python
# Simple function-based API
result = query_dataset(path, query)
```

**After:**
```python
# Class-based API with context management
from fsspeckit.datasets import DuckDBParquetHandler

with DuckDBParquetHandler() as handler:
    # Atomic operations
    handler.compact_parquet_dataset(
        path="/data/events/",
        target_rows_per_file=500_000,
        compression="zstd"
    )

    # SQL execution with fsspec integration
    result = handler.execute_sql("""
        SELECT category, COUNT(*)
        FROM parquet_scan('/data/events/')
        GROUP BY category
    """)
```

**Key Features:**
- Context manager support for resource cleanup
- Atomic write operations
- Enhanced SQL integration
- Storage options integration

### PyArrow Operations

**Before:**
```python
# Limited functionality
```

**After:**
```python
from fsspeckit.datasets.pyarrow import (
    merge_parquet_dataset_pyarrow,
    optimize_parquet_dataset_pyarrow,
    compact_parquet_dataset_pyarrow
)

# Merge with schema evolution
merge_parquet_dataset_pyarrow(
    dataset_paths=["/data/part1/", "/data/part2/"],
    target_path="/data/merged/",
    merge_strategy="schema_evolution"
)

# Optimize with Z-ordering
optimize_parquet_dataset_pyarrow(
    dataset_path="/data/events/",
    z_order_columns=["user_id", "event_date"],
    target_file_size="256MB"
)
```

## Optional Dependencies

### Lazy Imports

**Before:** All dependencies required upfront
```python
# All imports happened at module load time
import fsspec_utils  # Failed if any optional dep missing
```

**After:** Lazy loading of optional dependencies
```python
# Core works without optional dependencies
from fsspeckit.datasets import DuckDBParquetHandler

# Dependencies loaded only when used
try:
    handler = DuckDBParquetHandler()
except ImportError as e:
    print(f"Install with: pip install duckdb")
```

### Dependency Installation

**For Dataset Operations:**
```bash
# Install with DuckDB, PyArrow, Polars support
pip install "fsspeckit[datasets]"

# Or install individually
pip install duckdb pyarrow polars
```

**For SQL Filtering:**
```bash
pip install sqlglot pyarrow
```

**For Cloud Storage:**
```bash
pip install "fsspeckit[aws,gcp,azure]"
```

**Complete Installation:**
```bash
pip install "fsspeckit[aws,gcp,azure,datasets,sql]"
```

## Security Features

### Path Validation

**New in 0.5.x:**
```python
from fsspeckit.common.security import validate_path

# Prevent path traversal
safe_path = validate_path(
    user_provided_path,
    base_dir="/data/allowed"
)

# Use in filesystem operations
fs.open(safe_path, "r")
```

### Credential Scrubbing

**New in 0.5.x:**
```python
from fsspeckit.common.security import scrub_credentials

# Prevent secrets in logs
error_msg = f"Failed: secret_access_key=SECRET123"
safe_msg = scrub_credentials(error_msg)
# Output: "Failed: secret_access_key=[REDACTED]"
```

### Compression Safety

**New in 0.5.x:**
```python
from fsspeckit.common.security import validate_compression_codec

# Ensure only safe codecs
safe_codec = validate_compression_codec(user_codec)
# Valid: snappy, gzip, lz4, zstd, brotli, uncompressed, none
# Invalid: malicious values are rejected
```

### Path-Safe Filesystem

**New in 0.5.x:**
```python
from fsspeckit.core.filesystem import filesystem, DirFileSystem

# Automatic path safety (default behavior)
fs = filesystem("s3://bucket/")  # Wrapped in DirFileSystem

# Manual path restriction
base_fs = filesystem("file")
safe_fs = DirFileSystem(fs=base_fs, path="/data/allowed")
```

## Configuration Changes

### Environment-Based Configuration

**Before:**
```python
# Manual environment reading
import os
storage_options = {
    "key": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret": os.getenv("AWS_SECRET_ACCESS_KEY")
}
```

**After:**
```python
from fsspeckit.storage_options import storage_options_from_env

# Automatic environment loading
aws_options = storage_options_from_env("s3")
fs = filesystem("s3", storage_options=aws_options.to_dict())
```

### Storage Options Classes

**Before:**
```python
# Raw dictionaries
storage_options = {
    "region": "us-east-1",
    "key": "value"
}
```

**After:**
```python
# Structured classes with validation
from fsspeckit.storage_options import AwsStorageOptions

aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="KEY",
    secret_access_key="SECRET"
)

# Convert to filesystem
fs = aws_options.to_filesystem()

# Serialize to YAML
yaml_str = aws_options.to_yaml()
```

## Summary of Breaking Changes

1. **Import paths changed** - Move from `fsspeckit.utils` to domain packages
2. **Environment variable renamed** - `fsspeckit_LOG_LEVEL` → `FSSPECKIT_LOG_LEVEL`
3. **Filesystem creation** - Use `filesystem()` instead of `fsspec.filesystem()`
4. **Dataset API** - Class-based `DuckDBParquetHandler` instead of functions
5. **Path safety** - Automatic `DirFileSystem` wrapping (may affect existing paths)

## Summary of New Features

1. **Domain package organization** - Better discoverability and type hints
2. **Security helpers** - Path validation, credential scrubbing, compression safety
3. **Enhanced logging** - Structured logging with loguru
4. **Lazy imports** - Optional dependencies loaded on demand
5. **Storage options classes** - Type-safe configuration with validation
6. **Path safety by default** - Enhanced security through `DirFileSystem`

## Backwards Compatibility

All `fsspeckit.utils` imports continue to work unchanged:

```python
# This still works but is discouraged
from fsspeckit.utils import DuckDBParquetHandler, setup_logging, run_parallel

# Recommended: import from domain packages
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.common.logging import setup_logging
from fsspeckit.common.misc import run_parallel
```

## Need Help?

- **Architecture Details**: See [Architecture](architecture.md)
- **API Reference**: See [API Reference](../api/index.md)
- **Examples and Task Guides**: See [How-to Guides](../how-to/)
- **Advanced Topics**: See [Optimize Performance](../how-to/optimize-performance.md) and [Architecture & Concepts](.)

For issues or questions, please open an issue on the [fsspeckit repository](https://github.com/legout/fsspeckit).
