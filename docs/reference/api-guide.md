# API Guide

This guide explains how to choose the right import, backend, and configuration
for a task, and how to interpret the results. For exact signatures and
docstrings, follow the links to the [generated API](../api/index.md).

The explicit list of supported public symbols, their canonical imports, and
compatibility status is in the [Public API Inventory](public-api-inventory.md).

## Choosing an import

fsspeckit re-exports the most common symbols from the root package and from
domain packages. Prefer root or domain-package imports; use direct module
imports only for backend-specific symbols.

| What you need | Canonical import | Extra |
|---------------|------------------|-------|
| Create a filesystem | `from fsspeckit import filesystem, get_filesystem` | none |
| Filesystem type hints | `from fsspeckit import AbstractFileSystem, DirFileSystem` | none |
| AWS / GCP / Azure options | `from fsspeckit import AwsStorageOptions, GcsStorageOptions, AzureStorageOptions` | `aws`, `gcp`, `azure` |
| Git provider options | `from fsspeckit import GitHubStorageOptions, GitLabStorageOptions` | none |
| Storage-options factory | `from fsspeckit.storage_options import from_env, from_dict, merge_storage_options, storage_options_from_uri` | none |
| DuckDB dataset handler | `from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection` | `datasets` |
| PyArrow dataset handler | `from fsspeckit.datasets.pyarrow import PyarrowDatasetIO` | `datasets` |
| Schema utilities | `from fsspeckit.datasets import cast_schema, convert_large_types_to_normal, opt_dtype_pa, unify_schemas_pa` | `datasets` |
| PyArrow maintenance functions | `from fsspeckit.datasets.pyarrow import compact_parquet_dataset_pyarrow, optimize_parquet_dataset_pyarrow, collect_dataset_stats_pyarrow` | `datasets` |
| Adaptive key tracking | `from fsspeckit.datasets.pyarrow import AdaptiveKeyTracker` | `datasets` |
| SQL filter translation | `from fsspeckit.sql import sql2pyarrow_filter, sql2polars_filter` | `sql` |
| Parallel processing | `from fsspeckit.common import run_parallel` | none |
| File synchronization | `from fsspeckit.common import sync_dir, sync_files` | none |
| Partition helpers | `from fsspeckit.common import get_partitions_from_path, validate_partition_columns` | none |
| Path and credential security | `from fsspeckit.common import validate_path, scrub_credentials` | none |
| Logging | `from fsspeckit.common import setup_logging, get_logger` | none |

See the [Public API Inventory](public-api-inventory.md) for the full per-symbol
breakdown, and [Installation](../installation.md) for the extras matrix.

## Filesystem and core

Use `filesystem()` when you need protocol inference, storage-options objects,
caching, or `DirFileSystem` path confinement. Use `get_filesystem()` for a
thinner factory when you already know the protocol and options.

```python
from fsspeckit import filesystem, AwsStorageOptions

# Protocol is inferred from the URI; options object provides credentials.
fs = filesystem("s3://my-bucket/data/", storage_options=AwsStorageOptions(region="us-east-1"))
```

Extended I/O methods (`read_json`, `read_csv`, `read_parquet`, `write_json`,
`write_csv`, `write_parquet`, `read_files`, `write_file`, `write_files`) are
registered on the filesystem instance by importing `fsspeckit.core.ext`.

Signatures: [fsspeckit.core](../api/fsspeckit.core.md).

## Dataset backends

fsspeckit offers two dataset backends that share the same handler interface.
See [Dataset Handlers](../dataset-handlers.md) for the shared contract.

### Choosing a backend

| Choose DuckDB when | Choose PyArrow when |
|--------------------|-------------------------|
| You want SQL-based merge logic and ad-hoc `parquet_scan` queries | You need streaming merge with explicit memory controls |
| You work in SQL-heavy workflows | You rely on the PyArrow ecosystem and predicate pushdown |
| Your dataset is very large and benefits from DuckDB's optimizer | You are in a memory-constrained environment |

### DuckDB

The DuckDB backend requires the `datasets` extra. Create a connection, then the
handler.

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)

result = io.write_dataset(data, "dataset/", mode="append")
merge_result = io.merge(data, "dataset/", strategy="upsert", key_columns=["id"])
```

Signatures: [fsspeckit.datasets.duckdb](../api/fsspeckit.datasets.duckdb.md).

### PyArrow

The PyArrow backend requires the `datasets` extra. The handler takes an optional
filesystem.

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

result = io.write_dataset(data, "dataset/", mode="append")
merge_result = io.merge(
    data, "dataset/", strategy="upsert", key_columns=["id"],
    enable_streaming_merge=True,
)
```

Signatures: [fsspeckit.datasets.pyarrow](../api/fsspeckit.datasets.pyarrow.md).

### Result interpretation

`write_dataset()` returns a `WriteDatasetResult` with per-file metadata
(`files`, `total_rows`, `mode`, `backend`). `merge()` returns a `MergeResult`
with row-level counts (`inserted`, `updated`, `deleted`) and file-level lists
(`rewritten_files`, `inserted_files`, `preserved_files`). Use these fields for
auditing and downstream planning rather than re-reading the dataset.

## Storage configuration

Storage options are structured objects. Configure them directly, from
environment variables, or from a URI. See [Storage Options](storage-options.md)
for the full guide.

## SQL filters

Translate SQL `WHERE` clauses into PyArrow or Polars filter expressions with a
single function. Requires the `sql` extra.

```python
from fsspeckit.sql import sql2pyarrow_filter, sql2polars_filter

pa_filter = sql2pyarrow_filter("value > 100", schema)
pl_filter = sql2polars_filter("value > 100", schema)
```

Signatures: [fsspeckit.sql.filters](../api/fsspeckit.sql.filters.md).

## Common utilities

Cross-cutting helpers live in `fsspeckit.common` and require no extra
dependency.

```python
from fsspeckit.common import run_parallel, sync_dir, get_partitions_from_path
from fsspeckit.common import validate_path, scrub_credentials
```

Signatures: [fsspeckit.common](../api/fsspeckit.common.md).

## Optional dependencies

fsspeckit uses lazy imports. Calling a feature whose extra is not installed
raises an `ImportError` naming the package. Match the error to a row in the
[extras matrix](../installation.md#optional-extras).

## Related documentation

- [Dataset Handlers](../dataset-handlers.md) - shared handler interface and backend differences.
- [Storage Options](storage-options.md) - provider configuration.
- [Legacy Imports](legacy-imports.md) - deprecation mappings.
- [How-to Guides](../how-to/index.md) - task-oriented recipes.
- [Explanation](../explanation/index.md) - concepts and architecture.
