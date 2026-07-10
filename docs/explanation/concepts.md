# Key Concepts

This page explains the concepts behind fsspeckit's design: how imports are
organized, why path safety matters, the difference between file and dataset
operations, and how optional dependencies work.

For the authoritative import list and extras, see the
[API Guide](../reference/api-guide.md) and the
[extras matrix](../installation.md#optional-extras).

## Import organization

fsspeckit splits its surface into domain packages so that an import tells you
what a dependency does. Prefer root or domain-package imports; fall back to a
direct module import only for backend-specific symbols.

```python
# Root: the most common symbols
from fsspeckit import filesystem, AwsStorageOptions, GcsStorageOptions

# Domain package: a specific feature area
from fsspeckit.datasets.duckdb import DuckDBDatasetIO
from fsspeckit.sql import sql2pyarrow_filter

# Direct module: backend-specific symbols
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO, AdaptiveKeyTracker
```

The full canonical-import list is in the
[Public API Inventory](../reference/public-api-inventory.md). Older import
paths that still work are mapped on the
[Legacy Imports](../reference/legacy-imports.md) page.

### Importing `fsspeckit` registers extended I/O methods

Importing the package registers fsspeckit's extended I/O methods
(`read_json`, `read_csv`, `read_parquet`, `write_parquet`, `read_files`, and
others) on every fsspec `AbstractFileSystem`. Once you have a filesystem from
`filesystem()` or `get_filesystem()`, those methods are available directly:

```python
from fsspeckit import filesystem

fs = filesystem(".")
table = fs.read_parquet_file("data.parquet")
```

## Path safety and DirFileSystem

A plain fsspec filesystem grants access to any path the user can reach. By
default, `filesystem()` wraps the underlying filesystem in a fsspec
`DirFileSystem` (`dirfs=True`), confining every operation to a base directory.
Path traversal outside that root is rejected.

```python
from fsspeckit import filesystem

# Operations are confined to /data/allowed
safe_fs = filesystem("/data/allowed", dirfs=True)

# This raises rather than escaping the root
try:
    safe_fs.open("../../../etc/passwd", "r")
except (ValueError, PermissionError) as e:
    print(f"Blocked: {e}")
```

You can also build a `DirFileSystem` directly when you need explicit control,
or pass an existing filesystem as `base_fs` to nest confinement. This is the
basis for safe multi-tenant isolation, where each tenant gets a filesystem
rooted at its own directory.

## Datasets versus files

fsspeckit offers two layers of data access:

- **File-level I/O** reads or writes a single file (or a glob of files) in one
  call. Use the extended I/O methods on a filesystem for JSON, CSV, and Parquet
  files. See [Work with Filesystems](../how-to/work-with-filesystems.md).
- **Dataset handlers** treat a directory of parquet files as one logical
  dataset with a shared schema, partitioning, and merge semantics. Use
  `DuckDBDatasetIO` or `PyarrowDatasetIO` for partitioned writes, incremental
  merges, and maintenance. See
  [Dataset Handlers](../dataset-handlers.md).

The key distinction for writes is between two operations that look similar:

- `write_dataset(data, path, mode="append"|"overwrite")` writes new parquet
  files and returns per-file metadata. It never touches existing rows.
- `merge(data, path, strategy, key_columns, ...)` incrementally reconciles the
  source against the existing dataset, rewriting only the files that contain
  matching keys. It returns a `MergeResult` with inserted, updated, and deleted
  counts.

See [Merge Datasets](../how-to/merge-datasets.md) for the practical recipe.

## SQL filter abstraction

Each data framework has its own filter syntax. fsspeckit's SQL filter
translation lets you write a filter once as a SQL `WHERE` clause and convert it
to a PyArrow or Polars expression against a known schema. The translation is
type-aware: comparisons are built from the schema's column types.

```python
import pyarrow as pa
from fsspeckit.sql import sql2pyarrow_filter, sql2polars_filter

schema = pa.schema([
    ("id", pa.int64()),
    ("value", pa.float64()),
    ("timestamp", pa.timestamp("us")),
])

pa_filter = sql2pyarrow_filter("value > 100 AND timestamp >= '2023-01-01'", schema)
```

SQL filter translation requires the `sql` extra. See
[Use SQL Filters](../how-to/use-sql-filters.md).

## Optional dependencies

fsspeckit installs with the dependencies needed for core filesystem operations.
Cloud providers and data-processing backends are opt-in extras. Imports always
succeed; an extra is required only when you actually call the feature that
needs it, at which point fsspeckit raises an `ImportError` naming the package.

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

# Import succeeds even without the datasets extra installed
conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)

# duckdb is loaded only when write_dataset is called
io.write_dataset(data, "dataset/", mode="append")
```

Match a missing-dependency error to a row in the
[extras matrix](../installation.md#optional-extras) to see which extra to
install. Cloud provider options classes (`AwsStorageOptions`,
`GcsStorageOptions`, `AzureStorageOptions`) likewise require their respective
`aws`, `gcp`, or `azure` extras.

## Domain package boundaries

Each package owns a coherent responsibility:

- **Core** (`fsspeckit.core`): filesystem factories (`filesystem`,
  `get_filesystem`), `DirFileSystem`, caching, and extended I/O.
- **Storage options** (`fsspeckit.storage_options`): structured configuration
  for local, cloud, and Git providers, plus factory helpers.
- **Datasets** (`fsspeckit.datasets`): parquet dataset handlers, schema
  utilities, and maintenance operations across DuckDB and PyArrow backends.
- **SQL** (`fsspeckit.sql`): SQL-to-filter translation.
- **Common** (`fsspeckit.common`): dependency-free utilities (parallelism, sync,
  partitions, security, logging).

`fsspeckit.utils` is a deprecated backwards-compatibility facade, not a primary
import route. New code imports from the domain packages above.

## Configuration flow

A typical pipeline threads storage options through a filesystem into a dataset
handler:

```python
from fsspeckit import filesystem, AwsStorageOptions
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

# 1. Build structured configuration
options = AwsStorageOptions(region="us-east-1")

# 2. Create a filesystem bound to a bucket
fs = filesystem("s3://my-bucket/data/", storage_options=options.to_dict())

# 3. Register it with DuckDB and run operations
conn = create_duckdb_connection(filesystem=fs)
io = DuckDBDatasetIO(conn)
io.write_dataset(data, "dataset/", mode="append")
```

## Security architecture

fsspeckit layers security controls across its surface:

- **Path safety**: `DirFileSystem` confines operations to a base directory,
  preventing traversal escapes.
- **Credential protection**: `scrub_credentials()` redacts secrets from log
  messages and error text before they are written or displayed.
- **Input validation**: `validate_path()`, `validate_columns()`, and
  `validate_compression_codec()` reject unsafe input early.

```python
from fsspeckit.common.security import scrub_credentials

safe_msg = scrub_credentials("Failed: access_key=AKIAIOSFODNN7EXAMPLE")
# "Failed: access_key=[REDACTED]"
```

## Related documentation

- [Architecture](architecture.md) - system design and domain boundaries.
- [API Guide](../reference/api-guide.md) - import selection across all packages.
- [How-to Guides](../how-to/index.md) - task-oriented recipes.
