# fsspeckit

Enhanced utilities and extensions for fsspec filesystems with multi-format I/O support.

## Overview

`fsspeckit` is a comprehensive toolkit that extends [fsspec](https://filesystem-spec.readthedocs.io/) with:

- **Multi-cloud storage configuration** - Easy setup for AWS S3, Google Cloud Storage, Azure Storage, GitHub, and GitLab
- **Enhanced caching** - Improved caching filesystem with monitoring and path preservation
- **Extended I/O operations** - Read/write operations for JSON, CSV, Parquet with Polars/PyArrow integration
- **Domain-specific packages** - Organized into logical packages for better discoverability

## Package Structure

`fsspeckit` is organized into domain-specific packages:

- **`fsspeckit.core`** - Core filesystem APIs and backend-neutral planning logic
- **`fsspeckit.storage_options`** - Multi-cloud storage configuration classes
- **`fsspeckit.datasets`** - Dataset-level operations (DuckDB & PyArrow helpers)
- **`fsspeckit.sql`** - SQL-to-filter translation helpers
- **`fsspeckit.common`** - Cross-cutting utilities (logging, parallelism, type conversion)
- **`fsspeckit.utils`** - Backwards-compatible façade that re-exports from domain packages

> **Note:** The `fsspeckit.utils` module is maintained for backwards compatibility. New code should import directly from the domain packages for better discoverability.

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/legout/fsspeckit)

## Installation

```bash
# Basic installation
pip install fsspeckit

# Specific cloud providers
pip install "fsspeckit[aws]"     # AWS S3 support
pip install "fsspeckit[gcp]"     # Google Cloud Storage
pip install "fsspeckit[azure]"   # Azure Storage

# Multiple cloud providers
pip install "fsspeckit[aws,gcp,azure]"

# Feature-specific extras
pip install "fsspeckit[datasets]"  # Dataset operations (polars, pandas, pyarrow, duckdb, sqlglot, orjson)
pip install "fsspeckit[sql]"      # SQL functionality (duckdb, sqlglot, orjson)
pip install "fsspeckit[polars]"   # Polars data frame support

# Complete installation
pip install "fsspeckit[aws,gcp,azure,datasets,sql]"
```

## Quick Start

### Basic Filesystem Operations

```python
from fsspeckit import filesystem

# Local filesystem
fs = filesystem("file")
files = fs.ls("/path/to/data")

# S3 with caching
fs = filesystem("s3://my-bucket/", cached=True)
data = fs.cat("data/file.txt")
```

### Storage Configuration

```python
from fsspeckit.storage_options import AwsStorageOptions

# Configure S3 access
options = AwsStorageOptions(
    region="us-west-2",
    access_key_id="YOUR_KEY",
    secret_access_key="YOUR_SECRET"
)

fs = filesystem("s3", storage_options=options, cached=True)
```

### Environment-based Configuration

```python
from fsspeckit.storage_options import AwsStorageOptions

# Load from environment variables
options = AwsStorageOptions.from_env()
fs = filesystem("s3", storage_options=options)

# Load with anonymous access from environment
# Set AWS_S3_ANONYMOUS=true in environment
options = AwsStorageOptions.from_env()
fs = filesystem("s3", storage_options=options)
```

### DuckDB Parquet Maintenance

```python
from fsspeckit.datasets import DuckDBParquetHandler

with DuckDBParquetHandler() as handler:
    # Inspect fragmentation without writing
    dry_stats = handler.compact_parquet_dataset(
        path="/data/events/",
        target_mb_per_file=256,
        dry_run=True,
    )

    # Compact tiny files and recompress with zstd
    handler.compact_parquet_dataset(
        path="/data/events/",
        target_rows_per_file=500_000,
        compression="zstd",
    )

    # Recluster partitions with z-order style ordering
    handler.optimize_parquet_dataset(
        path="/data/events/",
        zorder_columns=["user_id", "event_date"],
        partition_filter=["date=2025-11-10"],
    )
```

### Multiple Cloud Providers

```python
from fsspeckit.storage_options import (
    AwsStorageOptions, 
    GcsStorageOptions,
    GitHubStorageOptions
)

# AWS S3
s3_fs = filesystem("s3", storage_options=AwsStorageOptions.from_env())

# Google Cloud Storage  
gcs_fs = filesystem("gs", storage_options=GcsStorageOptions.from_env())

# GitHub repository
github_fs = filesystem("github", storage_options=GitHubStorageOptions(
    org="microsoft",
    repo="vscode", 
    token="ghp_xxxx"
))
```

## Storage Options

### AWS S3

```python
from fsspeckit.storage_options import AwsStorageOptions

# Basic credentials
options = AwsStorageOptions(
    access_key_id="AKIAXXXXXXXX",
    secret_access_key="SECRET",
    region="us-east-1"
)

# From AWS profile
options = AwsStorageOptions.create(profile="dev")

# S3-compatible service (MinIO)
options = AwsStorageOptions(
    endpoint_url="http://localhost:9000",
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
    allow_http=True
)

# Anonymous access for public buckets
options = AwsStorageOptions(anonymous=True)
```

### Google Cloud Storage

```python
from fsspeckit.storage_options import GcsStorageOptions

# Service account
options = GcsStorageOptions(
    token="path/to/service-account.json",
    project="my-project-123"
)

# From environment
options = GcsStorageOptions.from_env()
```

### Azure Storage

```python
from fsspeckit.storage_options import AzureStorageOptions

# Account key
options = AzureStorageOptions(
    protocol="az",
    account_name="mystorageacct",
    account_key="key123..."
)

# Connection string
options = AzureStorageOptions(
    protocol="az",
    connection_string="DefaultEndpoints..."
)
```

### GitHub

```python
from fsspeckit.storage_options import GitHubStorageOptions

# Public repository
options = GitHubStorageOptions(
    org="microsoft",
    repo="vscode",
    ref="main"
)

# Private repository
options = GitHubStorageOptions(
    org="myorg",
    repo="private-repo",
    token="ghp_xxxx",
    ref="develop"
)
```

### GitLab

```python
from fsspeckit.storage_options import GitLabStorageOptions

# Public project
options = GitLabStorageOptions(
    project_name="group/project",
    ref="main"
)

# Private project with token
options = GitLabStorageOptions(
    project_id=12345,
    token="glpat_xxxx",
    ref="develop"
)
```

## Enhanced Caching

```python
from fsspeckit import filesystem

# Enable caching with monitoring
fs = filesystem(
    "s3://my-bucket/",
    cached=True,
    cache_storage="/tmp/my_cache",
    verbose=True
)

# Cache preserves directory structure
data = fs.cat("deep/nested/path/file.txt")
# Cached at: /tmp/my_cache/deep/nested/path/file.txt
```

## Utilities

### Parallel Processing

```python
from fsspeckit.common import run_parallel

# Run function in parallel
def process_file(path, multiplier=1):
    return len(path) * multiplier

results = run_parallel(
    process_file,
    ["/path1", "/path2", "/path3"],
    multiplier=2,
    n_jobs=4,
    verbose=True
)
```

### Type Conversion

```python
from fsspeckit.common.types import dict_to_dataframe, to_pyarrow_table

# Convert dict to DataFrame
data = {"col1": [1, 2, 3], "col2": [4, 5, 6]}
df = dict_to_dataframe(data)

# Convert to PyArrow table
table = to_pyarrow_table(df)
```

### Logging

```python
from fsspeckit.common.logging import setup_logging

# Configure logging
setup_logging(level="DEBUG", format_string="{time} | {level} | {message}")
```

## Migration Guide

The package structure was refactored in version X.X.0 to improve discoverability and organization.

**For new code**, use the canonical imports from domain packages:
- Dataset operations: `from fsspeckit.datasets import ...`
- SQL helpers: `from fsspeckit.sql import ...`
- Common utilities: `from fsspeckit.common import ...`

**For existing code**, all `fsspeckit.utils` imports continue to work unchanged.

For detailed migration instructions, see the [Migration Guide](docs/migration-0.5.md).

## Dependencies

### Core Dependencies
- `fsspec>=2023.1.0` - Filesystem interface
- `msgspec>=0.18.0` - Serialization
- `pyyaml>=6.0` - YAML support
- `requests>=2.25.0` - HTTP requests
- `loguru>=0.7.0` - Logging

### Optional Dependencies

`fsspeckit` uses **lazy imports** for optional dependencies, meaning you only need to install what you actually use:

#### Data Processing (installed on-demand)
- `orjson>=3.8.0` - Fast JSON processing
- `polars>=0.19.0` - Fast DataFrames (required for `fsspeckit.common.polars`)
- `pyarrow>=10.0.0` - Columnar data (required for `fsspeckit.datasets.pyarrow`)
- `duckdb>=1.4.0` - SQL analytics (required for `fsspeckit.datasets.DuckDBParquetHandler`)
- `sqlglot>=20.0.0` - SQL parsing (required for `fsspeckit.sql.filters`)
- `pandas>=1.5.0` - Data analysis (optional, for compatibility)
- `joblib>=1.3.0` - Parallel processing (optional)
- `rich>=13.0.0` - Progress bars (optional)

#### Cloud Provider Dependencies (install as needed)
- `boto3>=1.26.0`, `s3fs>=2023.1.0` - AWS S3 (`pip install "fsspeckit[aws]"`)
- `gcsfs>=2023.1.0` - Google Cloud Storage (`pip install "fsspeckit[gcp]"`)
- `adlfs>=2023.1.0` - Azure Storage (`pip install "fsspeckit[azure]"`)

**How it works:**
- Core modules import without any optional dependencies
- Optional features are imported lazily when first used
- Clear error messages indicate which package to install if a dependency is missing
- This keeps installations lightweight and allows you to install only what you need

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

