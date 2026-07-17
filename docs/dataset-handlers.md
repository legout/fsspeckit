# Dataset Handler Interface

Dataset handlers provide a consistent API for reading, writing, and maintaining
parquet datasets across backends. This page explains the shared interface, how to
choose a backend, and how to interpret results. For exact signatures, see the
generated API for each backend.

Both backends require the `datasets` extra. See the
[extras matrix](installation.md#optional-extras).

## Shared interface

All dataset handlers implement the `DatasetHandler` protocol
(`fsspeckit.datasets.interfaces`). The core operations are:

- `write_dataset(data, path, *, mode="append"|"overwrite")` - write a parquet
  dataset, returning a `WriteDatasetResult` with per-file metadata.
- `merge(data, path, strategy, key_columns, ...)` - incrementally merge data
  into an existing dataset, returning a `MergeResult` with row and file counts.
- `read_parquet(path, ...)` - read parquet files into a PyArrow table.

Dataset maintenance (compaction, deduplication, repartitioning, optimization)
is no longer part of the handler interface. Since 0.25.0 it lives on the
filesystem via coordinator-backed facades such as
`fs.compact_parquet_dataset(...)` and `fs.plan_parquet_compaction(...)` -
see [Maintain Parquet Datasets](how-to/maintain-parquet-datasets.md) for the
task guide and [Coordinator-backed Maintenance](migration/maintenance-api.md)
for the migration mapping.

For the authoritative signatures and parameter lists, see
[fsspeckit.datasets.duckdb](api/fsspeckit.datasets.duckdb.md) and
[fsspeckit.datasets.pyarrow](api/fsspeckit.datasets.pyarrow.md).

## Choosing a backend

| Choose DuckDB when | Choose PyArrow when |
|--------------------|-------------------------|
| SQL-based merge logic and ad-hoc `parquet_scan` queries | Streaming merge with explicit memory controls |
| Very large datasets that benefit from DuckDB's optimizer | Predicate pushdown and the PyArrow ecosystem |
| SQL-heavy workflows | Memory-constrained environments |

Both backends share the same core parameters for `write_dataset` and `merge`
(`mode`, `key_columns`, `partition_by`, `compression`, `max_rows_per_file`,
`row_group_size`). Backend-specific knobs are noted below.

## DuckDB backend

`DuckDBDatasetIO` requires a `DuckDBConnection`. Create one with
`create_duckdb_connection()`, optionally passing a filesystem.

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)

io.write_dataset(data, "dataset/", mode="append")
result = io.merge(data, "dataset/", strategy="upsert", key_columns=["id"])
```

The connection object also exposes `execute_sql(query)` for ad-hoc SQL against
registered parquet files.

Backend-specific notes:

- `read_parquet` accepts SQL `WHERE` clause strings as filters.
- `use_merge` is accepted for backwards compatibility and ignored.

## PyArrow backend

`PyarrowDatasetIO` takes an optional filesystem (defaults to local).

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

io.write_dataset(data, "dataset/", mode="append")
result = io.merge(
    data, "dataset/", strategy="upsert", key_columns=["id"],
    enable_streaming_merge=True,
)
```

Backend-specific notes:

- `read_parquet` accepts PyArrow expressions, DNF tuples, or SQL-like strings
  (converted to expressions).
- `merge` exposes streaming controls: `merge_chunk_size_rows`,
  `enable_streaming_merge`, `merge_max_memory_mb`, `merge_max_process_memory_mb`,
  `merge_min_system_available_mb`, and `merge_progress_callback`.
- `use_threads` is accepted for `write_dataset` but ignored by the PyArrow engine.

## Result types

### WriteDatasetResult

Fields: `files` (list of `FileWriteMetadata` with `path`, `row_count`,
`size_bytes`), `total_rows`, `mode`, `backend`.

Use `result.total_rows` and `result.files` to audit what was written without
re-reading the dataset.

### MergeResult

Fields: `strategy`, `source_count`, `target_count_before`,
`target_count_after`, `inserted`, `updated`, `deleted`, `files`,
`rewritten_files`, `inserted_files`, `preserved_files`.

Use the row counts (`inserted`, `updated`, `deleted`) and file lists
(`rewritten_files`, `inserted_files`, `preserved_files`) for auditing and
downstream planning.

## Backend comparison

| Feature | DuckDB | PyArrow |
|---------|--------|---------|
| Filters | SQL `WHERE` strings | PyArrow expressions, DNF tuples, SQL-like strings |
| Merge controls | `use_merge` ignored | streaming knobs (`merge_chunk_size_rows`, etc.) |
| Write threading | `use_threads` honored | `use_threads` accepted, ignored |
| Optimization | SQL-based query optimization | Adaptive key tracking + streaming controls |
| Best for | Complex SQL merge logic, very large datasets | Partitioned datasets, predicate pushdown, low memory |

Both backends share core merge invariants from `fsspeckit.core.incremental` and
validation from `fsspeckit.datasets.base`.

## Type safety

Both handlers satisfy the `DatasetHandler` protocol, so you can type against the
protocol and swap backends:

```python
from fsspeckit.datasets.interfaces import DatasetHandler

def process(handler: DatasetHandler, data) -> None:
    handler.write_dataset(data, "output/")
    handler.merge(data, "output/", strategy="upsert", key_columns=["id"])
```

## Related documentation

- [API Guide](reference/api-guide.md) - import selection across all packages.
- [Adaptive Key Tracking](reference/adaptive-key-tracking.md) - tiered memory management for deduplication.
- [Multi-Key API](reference/multi-key-api.md) - vectorized multi-column key helpers.
- [Read and Write Datasets](how-to/read-and-write-datasets.md) - task-oriented recipe.
- [Merge Datasets](how-to/merge-datasets.md) - task-oriented recipe.
