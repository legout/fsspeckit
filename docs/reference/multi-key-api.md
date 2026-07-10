# Multi-Key API

fsspeckit supports multi-column (composite) keys for merge and deduplication
operations. This page explains how to use composite keys and how to interpret the
results. For exact signatures, see
[fsspeckit.datasets.pyarrow](../api/fsspeckit.datasets.pyarrow.md).

The PyArrow backend requires the `datasets` extra. See the
[extras matrix](../installation.md#optional-extras).

## Multi-column keys in merge

Both `DuckDBDatasetIO` and `PyarrowDatasetIO` accept a list of column names for
`key_columns` in `merge()`. A single string is equivalent to a one-column list.

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

# Single-column key
io.merge(data, "dataset/", strategy="upsert", key_columns="id")

# Composite key (multiple columns)
io.merge(
    data, "dataset/", strategy="upsert",
    key_columns=["tenant_id", "user_id", "record_id"],
)
```

All strategies (`insert`, `update`, `upsert`) support composite keys. When
duplicates are found, use `dedup_order_by` to control which record survives.

## Multi-column key deduplication

`deduplicate_parquet_dataset_pyarrow()` removes duplicate rows from an existing
dataset, supporting both single-column and multi-column keys.

```python
from fsspeckit.datasets.pyarrow.dataset import deduplicate_parquet_dataset_pyarrow

# Single-column deduplication
deduplicate_parquet_dataset_pyarrow("dataset/", key_columns=["id"])

# Multi-column deduplication with ordering
deduplicate_parquet_dataset_pyarrow(
    "dataset/",
    key_columns=["tenant_id", "user_id", "record_id"],
    dedup_order_by=["updated_at"],
)
```

Pass `key_columns=None` to remove exact duplicate rows across all columns.

### Interpreting results

`deduplicate_parquet_dataset_pyarrow()` returns a dictionary with deduplication
statistics and performance metrics. Inspect the `deduplicated_rows` count and
file-level metrics to audit the operation.

### Memory-aware deduplication

For datasets larger than memory, control chunking and peak memory with
`chunk_size_rows` and `max_memory_mb`:

```python
deduplicate_parquet_dataset_pyarrow(
    "dataset/",
    key_columns=["tenant_id", "user_id"],
    chunk_size_rows=500_000,
    max_memory_mb=1024,
)
```

## How composite keys work internally

Multi-column key matching uses vectorized PyArrow operations:

1. Composite keys are built as StructArrays for efficient comparison.
2. Key membership is resolved with semi-join and anti-join operations on
   PyArrow tables.
3. When native joins fail due to heterogeneous type combinations, the engine
   falls back to string-based key serialization.

These mechanisms are internal to the merge and deduplication engines. You do not
need to call them directly; they activate automatically when you pass a list to
`key_columns`.

## Optimization with composite keys

`optimize_parquet_dataset_pyarrow()` combines deduplication with compaction. Pass
`deduplicate_key_columns` to deduplicate before compacting:

```python
from fsspeckit.datasets.pyarrow import optimize_parquet_dataset_pyarrow

optimize_parquet_dataset_pyarrow(
    "dataset/",
    target_mb_per_file=64,
    deduplicate_key_columns=["tenant_id", "order_id"],
    dedup_order_by=["-updated_at"],
)
```

## Related documentation

- [Dataset Handlers](../dataset-handlers.md) - shared merge and write interface.
- [Adaptive Key Tracking](adaptive-key-tracking.md) - memory-bounded key tracking for streaming merges.
- [Multi-Key Performance](../how-to/multi-key-performance.md) - benchmarking composite-key operations.
- [Generated API](../api/fsspeckit.datasets.pyarrow.md) - signatures.
