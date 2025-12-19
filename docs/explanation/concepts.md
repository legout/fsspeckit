# Concepts

This page explains key concepts that help you understand how and why fsspeckit's APIs are designed the way they are.

## Import Patterns

fsspeckit provides a hierarchical import system designed for clarity and convenience. Understanding these import patterns helps you write clean, maintainable code.

### Three-Level Import Hierarchy

**Top-Level Imports**: For core symbols and common operations
```python
from fsspeckit import filesystem, AwsStorageOptions, GcsStorageOptions
```

Use top-level imports when working with frequently-used core functionality. These symbols are re-exported at the package level for convenience.

**Package-Level Imports**: For domain-specific features
```python
from fsspeckit.datasets import PyarrowDatasetIO, DuckDBParquetHandler
from fsspeckit.storage_options import storage_options_from_env
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter
```

Use package-level imports when working with specialized features within a specific domain (datasets, storage, SQL, etc.).

**Module-Level Imports**: For accessing specific implementations
```python
from fsspeckit.core.filesystem import filesystem, DirFileSystem
from fsspeckit.core.merge import MergeStrategy
from fsspeckit.common.security import scrub_credentials
```

Use module-level imports when you need explicit control over which implementation you're using, or when accessing internal utilities.

### Import Guidelines

**Prefer simpler imports**: Start with top-level imports for core features, move to package-level for domain features, and use module-level only when necessary.

**Be consistent**: Within a single module or project, stick to one import style.

**Consider IDE support**: Top-level and package-level imports often provide better autocomplete support.

### Backwards Compatibility

All import paths continue to work for backward compatibility. Legacy code using older import patterns will function without modification.

## Path Safety and DirFileSystem

### Why Path Safety Matters

Traditional filesystems allow access to any path the user has permissions for, which can create security vulnerabilities:

```python
# Dangerous: Unrestricted filesystem access
import os
os.open("../../../etc/passwd", os.O_RDONLY)  # Can access system files

# Safe: Path-constrained access
from fsspeckit import filesystem
from fsspeckit.core.filesystem import DirFileSystem

# Automatic path safety (default)
safe_fs = filesystem("/data/allowed", dirfs=True)
safe_fs.open("../../../etc/passwd", "r")  # Raises ValueError/PermissionError

# Manual path confinement
base_fs = filesystem("file")
confined_fs = DirFileSystem(fs=base_fs, path="/data/allowed")
```

### DirFileSystem Behavior

`DirFileSystem` wraps any filesystem and confines all operations to a specified base directory:

- **Path Resolution**: All paths are resolved relative to the base directory
- **Escape Prevention**: Attempts to access parent directories are blocked
- **Security**: Prevents path traversal attacks in multi-tenant environments
- **Isolation**: Each user/process gets its own safe filesystem space

### Use Cases

```python
# Multi-tenant data processing
tenant_fs = filesystem(f"/data/tenants/{tenant_id}", dirfs=True)

# Test isolation
test_fs = filesystem("/tmp/test_data", dirfs=True)

# Production data isolation
prod_fs = filesystem("/data/production", dirfs=True)
```

## Dataset vs File-Level Operations

### Why Datasets Matter

Individual file operations are simple but don't scale to modern data workloads:

```python
# File-level: Manual and error-prone
import glob
import pandas as pd

dataframes = []
for file_path in glob.glob("data/*.parquet"):
    df = pd.read_parquet(file_path)
    dataframes.append(df)

combined_df = pd.concat(dataframes, ignore_index=True)

# Dataset-level: Automatic and optimized
from fsspeckit.datasets import process_dataset_in_batches

for batch in process_dataset_in_batches(
    dataset_path="data/",
    batch_size="100MB",
    process_func=lambda batch: process_batch(batch)
):
    # Process optimized batches
    pass
```

### Dataset Advantages

**Schema Evolution**: Datasets handle schema changes across files
```python
# Datasets automatically handle schema differences
table = fs.read_parquet("data/*.parquet", concat=True)  # Schema unification
```

**Partitioning**: Datasets understand partitioned directory structures
```python
# Automatic partition discovery
dataset = fs.pyarrow_dataset("partitioned_data/")
# Reads: data/year=2023/month=01/*.parquet, data/year=2023/month=02/*.parquet
```

**Pushdown Operations**: Datasets can push filters and projections to storage
```python
# Efficient filtering at storage level
dataset = fs.pyarrow_dataset("large_dataset/")
filtered = dataset.to_table(
    filter=pyarrow.compute.greater(dataset.column("value"), 100),
    columns=["id", "name", "value"]  # Column projection
)
```

### When to Use Each

**File-level operations**:
- Small numbers of files
- Simple read/write operations
- When you need explicit file paths
- Legacy file processing workflows

**Dataset operations**:
- Large numbers of files
- Complex data processing pipelines
- When schema evolution is needed
- Partitioned data structures
- Analytics workloads

## SQL Filter Abstraction

### Why SQL Filters

Different data frameworks have different filter syntaxes, creating maintenance overhead:

```python
# Framework-specific filtering (complex to maintain)
# PyArrow
import pyarrow.compute as pc
pyarrow_filter = pc.and_(
    pc.greater(pc.field("value"), 100),
    pc.equal(pc.field("category"), "important")
)

# Polars
import polars as pl
polars_filter = (pl.col("value") > 100) & (pl.col("category") == "important")

# Pandas
pandas_filter = (df["value"] > 100) & (df["category"] == "important")
```

### SQL Filter Benefits

**Unified Syntax**: Write filters once using familiar SQL
```python
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

sql_filter = "value > 100 AND category = 'important'"

# Convert to any framework
pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)
polars_filter = sql2polars_filter(sql_filter, schema)
```

**Cross-Framework Compatibility**: Same filter works everywhere
```python
# Use same filter across different frameworks
filters = [
    "amount > 1000",
    "category IN ('A', 'B', 'C')",
    "timestamp >= '2023-01-01'",
    "status = 'active'"
]

for filter_sql in filters:
    # Works with PyArrow
    arrow_expr = sql2pyarrow_filter(filter_sql, arrow_schema)
    
    # Works with Polars
    polars_expr = sql2polars_filter(filter_sql, polars_schema)
    
    # Same logic, different frameworks
```

**Schema Awareness**: SQL filters are converted based on actual data types
```python
import pyarrow as pa

schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("value", pa.float64()),
    ("timestamp", pa.timestamp("us"))
])

# Type-aware conversion
filter_expr = sql2pyarrow_filter("value > 100", schema)  # Uses float comparison
filter_expr = sql2pyarrow_filter("name = 'test'", schema)  # Uses string comparison
```

### Supported SQL Features

**Comparison Operators**: `=`, `!=`, `>`, `>=`, `<`, `<=`, `BETWEEN`
**Logical Operators**: `AND`, `OR`, `NOT`
**String Operations**: `LIKE`, `IN`, `NOT IN`
**Null Handling**: `IS NULL`, `IS NOT NULL`
**Date Functions**: `YEAR()`, `MONTH()`, `DAY()`, `DATE()`

## Lazy Optional Dependencies

### Why Lazy Dependencies

fsspeckit supports many optional features, but not all users need all dependencies:

```python
# Problem: Heavy dependencies for all users
# Traditional approach forces all dependencies:
pip install fsspeckit duckdb pyarrow polars sqlglot orjson pandas

# Even if you only need basic filesystem operations
```

### Lazy Loading Benefits

**Lightweight Installation**: Core functionality works without heavy dependencies
```python
# This works with minimal installation
from fsspeckit import filesystem, AwsStorageOptions

fs = filesystem("s3", storage_options=options.to_dict())
```

**On-Demand Loading**: Dependencies loaded only when features are used
```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO

# Import succeeds even without duckdb
io = DuckDBDatasetIO()

# duckdb is loaded only when actually used
try:
    io.write_dataset(data, "path/", mode="append")
except ImportError as e:
    print(f"Install with: pip install duckdb")
```

**Clear Error Messages**: Users get helpful messages about missing dependencies
```python
# Clear guidance on what to install
ImportError: The 'duckdb' package is required for DuckDBParquetHandler.
Install it with: pip install duckdb
```

### Dependency Management

**Core Dependencies** (always required):
- `fsspec` - Filesystem abstraction
- `pydantic` - Configuration validation

**Optional Dependencies** (loaded on demand):
- `duckdb` - Dataset operations and SQL analytics
- `pyarrow` - Dataset operations and type conversion
- `polars` - DataFrame operations and optimization
- `sqlglot` - SQL filter translation
- `orjson` - Fast JSON processing

## Storage Options Factory Pattern

### Why Factory Pattern

Different cloud providers have different configuration patterns, creating complexity:

```python
# Provider-specific configuration (complex to maintain)
# AWS
aws_config = {
    "key": "AKIA...",
    "secret": "secret...",
    "region": "us-east-1",
    "client_kwargs": {"region_name": "us-east-1"}
}

# GCS
gcs_config = {
    "token": "path/to/service-account.json",
    "project": "my-project"
}

# Azure
azure_config = {
    "account_name": "storageaccount",
    "account_key": "secret..."
}
```

### Factory Pattern Benefits

**Unified Interface**: Same pattern across all providers
```python
from fsspeckit import AwsStorageOptions, GcsStorageOptions, AzureStorageOptions
from fsspeckit.storage_options import storage_options_from_env

# Same pattern for all providers
aws_opts = AwsStorageOptions(region="us-east-1", ...)
gcs_opts = GcsStorageOptions(project="my-project", ...)
azure_opts = AzureStorageOptions(account_name="account", ...)

# All have same methods
aws_fs = aws_opts.to_filesystem()
gcs_fs = gcs_opts.to_filesystem()
azure_fs = azure_opts.to_filesystem()
```

**Environment Integration**: Automatic configuration from environment variables
```python
# Load from environment (production pattern)
aws_options = storage_options_from_env("s3")  # Reads AWS_* env vars
gcs_options = storage_options_from_env("gs")   # Reads GOOGLE_* env vars
azure_options = storage_options_from_env("az")  # Reads AZURE_* env vars
```

**Type Safety**: Structured configuration with validation
```python
from fsspeckit import AwsStorageOptions

# Type-safe configuration
aws_options = AwsStorageOptions(
    region="us-east-1",  # Validated
    access_key_id="key",   # Validated
    secret_access_key="secret"  # Validated
)

# vs. raw dictionaries (error-prone)
aws_config = {
    "region": "us-east-1",
    "key": "AKIA...",  # Wrong key name
    "secret": "secret"
}
```

### Configuration Methods

**Manual Configuration**: For development and specific requirements
**Environment Configuration**: For production deployments
**URI-Based Configuration**: For dynamic configuration from strings
**YAML Serialization**: For persistent configuration files

## Domain Package Architecture

### Why Domain Packages

Monolithic utility modules create discoverability and maintenance issues:

```python
# Problem: Large, flat module structure
from fsspeckit.utils import (
    run_parallel,           # Parallel processing
    DuckDBParquetHandler,   # Dataset operations
    sql2pyarrow_filter,     # SQL filtering
    AwsStorageOptions,       # Storage configuration
    dict_to_dataframe,      # Type conversion
    # ... 50+ more functions
)
```

### Domain Benefits

**Discoverability**: Clear organization by functionality
```python
# Clear what you're importing
from fsspeckit.datasets import DuckDBParquetHandler  # Obvious: datasets
from fsspeckit.sql.filters import sql2pyarrow_filter  # Obvious: SQL
from fsspeckit.common.misc import run_parallel  # Obvious: utilities
```

**Maintainability**: Changes to one domain don't affect others
**Type Hints**: Better IDE support and autocomplete
**Testing**: Isolated testing for each domain
**Documentation**: Focused docs for specific use cases

### Domain Boundaries

**Core** (`fsspeckit.core`): Filesystem creation and extended I/O
**Storage Options** (`fsspeckit.storage_options`): Cloud provider configuration
**Datasets** (`fsspeckit.datasets`): Large-scale data processing
**SQL** (`fsspeckit.sql`): Query translation and filtering
**Common** (`fsspeckit.common`): Shared utilities and helpers
**Utils** (`fsspeckit.utils`): Backwards compatibility façade

## Integration Patterns

### Cross-Domain Communication

Domains communicate through well-defined interfaces:

```python
# Storage Options → Core Filesystem
from fsspeckit import filesystem, AwsStorageOptions

options = AwsStorageOptions(region="us-east-1")
fs = filesystem("s3", storage_options=options.to_dict())

# Datasets → Core Incremental Merge Logic
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
import pyarrow as pa

io = PyarrowDatasetIO()
data = pa.table({"id": [1, 2], "value": ["a", "b"]})

# Write and merge operations share core merge planning
result = io.write_dataset(data, "dataset/", mode="append")
merge_result = io.merge(data, "dataset/", strategy="upsert", key_columns=["id"])
```

### Configuration Flow

```python
# Environment → Storage Options → Filesystem → Operations
from fsspeckit import filesystem
from fsspeckit.storage_options import storage_options_from_env
from fsspeckit.datasets.duckdb import DuckDBDatasetIO

# 1. Load configuration
options = storage_options_from_env("s3")

# 2. Create filesystem
fs = filesystem("s3", storage_options=options.to_dict())

# 3. Use in operations
io = DuckDBDatasetIO(storage_options=options.to_dict())
```

## Performance Architecture

### Caching Strategy

**Filesystem-Level Caching**: Reduce remote storage access
```python
# Transparent caching for remote filesystems
fs = filesystem("s3://bucket/", cached=True)

# First access: downloads and caches
data1 = fs.cat("large_file.parquet")

# Second access: reads from cache
data2 = fs.cat("large_file.parquet")  # Much faster
```

**Memory Management**: Efficient data processing
```python
# Batch processing for memory efficiency
for batch in fs.read_parquet("large_dataset/*.parquet", batch_size="100MB"):
    process_batch(batch)  # Only one batch in memory at a time
```

### Parallel Processing

**I/O Parallelism**: Multiple files processed concurrently
```python
# Parallel file operations
df = fs.read_csv("data/*.csv", use_threads=True, num_threads=4)

# Parallel custom processing
from fsspeckit.common.misc import run_parallel

results = run_parallel(
    func=process_file,
    data=file_list,
    max_workers=8
)
```

## Security Architecture

### Multi-Layer Security

**Path Safety**: Prevent directory traversal
```python
# Confined filesystem operations
safe_fs = filesystem("/data/allowed", dirfs=True)
```

**Credential Protection**: Prevent secret leakage
```python
from fsspeckit.common.security import scrub_credentials

error_msg = f"Failed: access_key=AKIAIOSFODNN7EXAMPLE"
safe_msg = scrub_credentials(error_msg)
# Output: "Failed: access_key=[REDACTED]"
```

**Input Validation**: Prevent injection attacks
```python
from fsspeckit.common.security import (
    validate_path,
    validate_columns,
    validate_compression_codec
)

safe_path = validate_path(user_input, base_dir="/data/allowed")
safe_columns = validate_columns(user_columns, valid_columns=schema_columns)
safe_codec = validate_compression_codec(user_codec)
```

### Production Security

**Environment-Based Configuration**: No hardcoded credentials
**Audit Logging**: Safe logging with credential scrubbing
**Multi-Tenant Isolation**: Path confinement per tenant
**Compliance**: Built-in controls for regulatory requirements

## Related Documentation

- [Architecture](architecture.md) - Detailed system design
- [API Guide](../reference/api-guide.md) - Capability overview
- [How-to Guides](../how-to/index.md) - Practical implementation
