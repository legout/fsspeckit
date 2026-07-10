# Upgrade from the Pre-refactor Dataset Module

This migration covers the dataset API changes introduced in fsspeckit 0.22.x.
It targets code written against the immediately preceding public layout
(pre-0.22) and shows how to reach the current canonical, class-based handler
surface.

For the broader package-import migration (schema, logging, and utility
relocations), see [Move Your Package Imports](migrate-package-layout.md).
For every renamed symbol and legacy import path, see
[Deprecation and Legacy Imports](../reference/legacy-imports.md).

## Scope

Pre-0.22 to 0.22.x. If you are starting a new project, skip this page and read
the [Local Dataset Lifecycle](../tutorials/local-dataset-lifecycle.md) tutorial
instead.

## What changed

The standalone dataset helper functions under `fsspeckit.core.ext.dataset` were
removed. The per-method merge entry points on `DuckDBDatasetIO`
(`insert_dataset`, `upsert_dataset`, `update_dataset`,
`deduplicate_dataset`, `merge_parquet_dataset`) were collapsed into the unified
`merge` method.

### Removed APIs

These no longer exist and must be replaced:

| Removed API | Replacement |
|-------------|-------------|
| `fsspeckit.core.ext.dataset.write_pyarrow_dataset` | `PyarrowDatasetIO.write_dataset` |
| `fsspeckit.core.ext.dataset.insert_dataset` | `DuckDBDatasetIO.merge(..., strategy="insert")` |
| `fsspeckit.core.ext.dataset.upsert_dataset` | `DuckDBDatasetIO.merge(..., strategy="upsert")` |
| `fsspeckit.core.ext.dataset.update_dataset` | `DuckDBDatasetIO.merge(..., strategy="update")` |
| `fsspeckit.core.ext.dataset.deduplicate_dataset` | `optimize_parquet_dataset` on either backend |
| `DuckDBDatasetIO.merge_parquet_dataset` | `DuckDBDatasetIO.merge` |
| `DuckDBDatasetIO.insert_dataset` | `DuckDBDatasetIO.merge(..., strategy="insert")` |
| `DuckDBDatasetIO.upsert_dataset` | `DuckDBDatasetIO.merge(..., strategy="upsert")` |
| `DuckDBDatasetIO.update_dataset` | `DuckDBDatasetIO.merge(..., strategy="update")` |
| `DuckDBDatasetIO.deduplicate_dataset` | `DuckDBDatasetIO.optimize_parquet_dataset` |

## Canonical handler surface

Both backends expose the same two entry points:

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection
```

### PyArrow

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()            # optional: filesystem=...
result = io.write_dataset(table, "dataset/")
merged = io.merge(
    table, "dataset/",
    strategy="upsert",
    key_columns=["id"],
)
```

### DuckDB

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

conn = create_duckdb_connection()  # optional: filesystem=...
io = DuckDBDatasetIO(conn)
result = io.write_dataset(table, "dataset/")
merged = io.merge(
    table, "dataset/",
    strategy="upsert",
    key_columns=["id"],
)
```

For handler selection guidance and how to interpret `WriteDatasetResult` and
`MergeResult`, see [Dataset Handlers](../dataset-handlers.md).

## Write modes

`write_dataset` defaults to `mode="append"` on both backends. Pass
`mode="overwrite"` explicitly when you intend to replace an existing dataset.

```python
io.write_dataset(table, "dataset/", mode="overwrite")
```

## Merge strategies

`merge` accepts `strategy="insert"`, `"update"`, or `"upsert"` on both backends.
The `key_columns` argument identifies the primary key for the merge.

### Backend differences

The signatures share the same keyword surface, but execution differs:

| Aspect | PyArrow | DuckDB |
|--------|---------|--------|
| Rewrite granularity | Partition pruning at the file level | SQL-based file rewrite |
| Streaming memory | Configurable via `merge_max_memory_mb` and related kwargs | Managed by DuckDB's query engine |
| `partition_by` on `write_dataset` | Supported | Supported |

Several keyword arguments on `merge` (`merge_chunk_size_rows`,
`merge_max_memory_mb`, `enable_streaming_merge`) are meaningful for the PyArrow
backend and ignored by DuckDB. For the full parameter list and per-backend
behavior, see the generated signatures in
[PyArrow Backend](../api/fsspeckit.datasets.pyarrow.md) and
[DuckDB Backend](../api/fsspeckit.datasets.duckdb.md).

## End-to-end migration example

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
io = PyarrowDatasetIO()

# initial write (append by default)
io.write_dataset(table, "dataset/")

# incremental upsert
new_rows = pa.table({"id": [2, 4], "value": ["bb", "d"]})
result = io.merge(
    new_rows, "dataset/",
    strategy="upsert",
    key_columns=["id"],
)
print(f"rows before={result.target_count_before}  rows after={result.target_count_after}")
```

## Optimization and deduplication

`optimize_parquet_dataset` compacts small files and optionally deduplicates.
Both backends delegate planning to the shared `fsspeckit.core.maintenance`
layer, so the dry-run and group-planning output is identical; only the
read/concat/write step differs per backend.

```python
io.optimize_parquet_dataset("dataset/", target_mb_per_file=128, dry_run=True)
```

For the full maintenance API, see
[Maintenance](../api/fsspeckit.core.maintenance.md).

## Legacy import paths

Old import paths and renamed symbols (for example `DuckDBParquetHandler`)
still work through the `fsspeckit.utils` facade but emit no long-term support
promise. Use canonical imports in all new and migrated code. For the complete
mapping, see
[Deprecation and Legacy Imports](../reference/legacy-imports.md).
