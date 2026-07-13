# fsspeckit

Enhanced utilities and extensions for fsspec filesystems with multi-format I/O
support.

## Overview

`fsspeckit` is a toolkit that extends [fsspec](https://filesystem-spec.readthedocs.io/)
with:

- **Multi-cloud storage configuration** - easy setup for AWS S3, Google Cloud Storage, Azure Storage, GitHub, and GitLab
- **Enhanced caching** - improved caching filesystem with monitoring and path preservation
- **Extended I/O operations** - read/write operations for JSON, CSV, Parquet with Polars/PyArrow integration
- **Dataset operations** - Parquet processing with DuckDB and PyArrow backends, including merge and maintenance
- **SQL filter translation** - write filters once and run them across PyArrow and Polars
- **Domain-specific packages** - organized into logical packages for discoverability

## Start here

New to fsspeckit? Work through the
[Local Dataset Lifecycle tutorial](docs/tutorials/local-dataset-lifecycle.md).
It is a single, copyable, offline script that covers the canonical workflow:
configure a local filesystem, write a Parquet dataset, read it back, verify the
result, and clean up a named sandbox. No cloud credentials required.

For the full documentation, see the [docs site](https://legout.github.io/fsspeckit).

## Package structure

`fsspeckit` is organized into domain-specific packages:

- **`fsspeckit.core`** - core filesystem APIs and backend-neutral planning logic
- **`fsspeckit.storage_options`** - multi-cloud storage configuration classes
- **`fsspeckit.datasets`** - dataset-level operations (DuckDB and PyArrow helpers)
- **`fsspeckit.sql`** - SQL-to-filter translation helpers
- **`fsspeckit.common`** - cross-cutting utilities (logging, parallelism, synchronization, and path safety)
- **`fsspeckit.utils`** - backwards-compatible facade that re-exports from domain packages

> **Note:** The `fsspeckit.utils` module is maintained for backwards compatibility. New code should import directly from the domain packages for better discoverability.

## Installation

```bash
# Basic installation
pip install fsspeckit

# Dataset operations (PyArrow, DuckDB, schema utilities)
pip install "fsspeckit[datasets]"

# Cloud providers
pip install "fsspeckit[aws]"     # AWS S3 support
pip install "fsspeckit[gcp]"     # Google Cloud Storage
pip install "fsspeckit[azure]"   # Azure Storage
```

For the complete extras matrix, see
[Installation and optional extras](docs/installation.md).

## Quick start

This is the canonical local dataset lifecycle. For the full narrative version,
see the [tutorial](docs/tutorials/local-dataset-lifecycle.md).

```python
from pathlib import Path
import shutil

import pyarrow as pa

from fsspeckit import filesystem
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

# Named sandbox directory
sandbox = Path("fsspeckit_tutorial_sandbox")
dataset_path = sandbox / "sensors"
sandbox.mkdir(parents=True, exist_ok=True)

# Local filesystem and explicit schema
fs = filesystem("file", dirfs=False)
schema = pa.schema([
    pa.field("sensor_id", pa.int64()),
    pa.field("reading", pa.float64()),
    pa.field("recorded_at", pa.string()),
])

# Write, then read back and verify
io = PyarrowDatasetIO(filesystem=fs)
records = pa.table(
    {"sensor_id": [1, 2, 3], "reading": [21.4, 22.1, 19.8],
     "recorded_at": ["2026-07-10T08:00", "2026-07-10T08:05", "2026-07-10T08:10"]},
    schema=schema,
)
result = io.write_dataset(records, str(dataset_path), schema=schema, mode="overwrite")
table = io.read_parquet(str(dataset_path))
assert table.num_rows == result.total_rows

# Clean up only the sandbox
shutil.rmtree(sandbox)
```

## Canonical imports

Import from the domain packages that own each feature:

```python
# Filesystem creation
from fsspeckit import filesystem

# Dataset operations
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
from fsspeckit.datasets.duckdb import DuckDBDatasetIO

# Storage configuration
from fsspeckit.storage_options import AwsStorageOptions, GcsStorageOptions

# SQL filter translation
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

# Common utilities
from fsspeckit.common import run_parallel
```

For the full import hierarchy and deprecation mappings, see the
[Public API Inventory](docs/reference/public-api-inventory.md) and
[Legacy Imports](docs/reference/legacy-imports.md).

## Examples

The [examples directory](examples/README.md) contains runnable demonstrations of
datasets, SQL filters, common utilities, caching, and more. Most examples use
local or generated data. Cloud operations require the applicable extra,
credentials, and a real provider resource. They support the tutorial rather
than replace it.

## Migration

If you are moving from older module layouts, see the
[Migration Guide](docs/migration/dataset-module-refactor.md). All `fsspeckit.utils`
imports continue to work unchanged.

## Contributing

Contributions are welcome. Please submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file
for details.

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/legout/fsspeckit)
