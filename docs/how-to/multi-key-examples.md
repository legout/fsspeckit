# Multi-Key Examples

This guide provides practical recipes for composite-key (multi-column)
operations: incremental merges and deduplication of an existing dataset. For
the composite-key semantics reference, see
[Multi-Key API](../reference/multi-key-api.md).

The PyArrow backend requires the `datasets` extra. See the
[extras matrix](../installation.md#optional-extras).

## Composite-key merge

Both `DuckDBDatasetIO` and `PyarrowDatasetIO` accept a list of column names for
`key_columns`. Pass a composite key as a list; a single string is equivalent to
a one-column list.

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

existing = pa.table({
    "tenant_id": [1, 1, 2, 2],
    "customer_id": [100, 101, 200, 201],
    "order_id": [1001, 1002, 2001, 2002],
    "amount": [150.0, 200.0, 100.0, 250.0],
})
io.write_dataset(existing, "orders/", mode="overwrite")

updates = pa.table({
    "tenant_id": [1, 1, 2, 3],
    "customer_id": [100, 103, 200, 300],
    "order_id": [1001, 1003, 2001, 3001],
    "amount": [175.0, 175.0, 100.0, 400.0],
})

result = io.merge(
    updates,
    "orders/",
    strategy="upsert",
    key_columns=["tenant_id", "customer_id", "order_id"],
)

print(f"Inserted: {result.inserted}, Updated: {result.updated}")
```

All strategies (`insert`, `update`, `upsert`) support composite keys. The merge
rewrites only the files that contain matching keys.

## Deduplicate an existing dataset

`deduplicate_parquet_dataset_pyarrow()` removes duplicate rows from an existing
dataset on disk. Pass `key_columns` to deduplicate by a composite key, and
`dedup_order_by` to control which record survives.

```python
from fsspeckit.datasets.pyarrow.dataset import deduplicate_parquet_dataset_pyarrow

stats = deduplicate_parquet_dataset_pyarrow(
    "orders/",
    key_columns=["tenant_id", "customer_id", "order_id"],
    dedup_order_by=["updated_at"],
)
```

- Pass `key_columns=None` to remove exact duplicate rows across all columns.
- Use a leading `-` in `dedup_order_by` (for example `"-updated_at"`) to keep
  the most recent record for each key.

The function returns a dictionary with deduplication statistics and file-level
metrics. Inspect `deduplicated_rows` to audit the operation. See
[Multi-Key API](../reference/multi-key-api.md) for the result details.

## Memory-aware deduplication

For datasets larger than memory, control chunking and peak memory with
`chunk_size_rows` and `max_memory_mb`:

```python
stats = deduplicate_parquet_dataset_pyarrow(
    "large_dataset/",
    key_columns=["tenant_id", "record_id"],
    chunk_size_rows=500_000,
    max_memory_mb=1024,
)
```

## Deduplicate-then-compact

`optimize_parquet_dataset_pyarrow()` combines deduplication with compaction.
Pass `deduplicate_key_columns` to deduplicate before compacting:

```python
from fsspeckit.datasets.pyarrow import optimize_parquet_dataset_pyarrow

summary = optimize_parquet_dataset_pyarrow(
    "orders/",
    target_mb_per_file=64,
    deduplicate_key_columns=["tenant_id", "order_id"],
    dedup_order_by=["-updated_at"],
)
```

## How composite keys are matched

Composite-key matching is vectorized in PyArrow:

1. Composite keys are built as StructArrays for efficient comparison.
2. Key membership is resolved with semi-join and anti-join operations.
3. When native joins fail on heterogeneous type combinations, the engine falls
   back to string-based key serialization.

These mechanisms are internal; they activate automatically when you pass a list
to `key_columns`. See [Multi-Key API](../reference/multi-key-api.md).

## Key requirements

- Key columns must be present in the data.
- Keys may contain null values. Null key components are compared with
  null-equal semantics (SQL `IS NOT DISTINCT FROM`): `NULL` matches `NULL`,
  `NULL` never matches a non-null value, and a composite key matches only when
  every component matches under those rules.
- For a composite key, the full column combination must identify a row uniquely.
- Use `dedup_order_by` to make survivor selection deterministic when the source
  contains duplicate keys.

## Related documentation

- [Multi-Key API](../reference/multi-key-api.md) - composite-key semantics and
  deduplication reference.
- [Merge Datasets](merge-datasets.md) - strategy reference.
- [Adaptive Key Tracking](adaptive-key-tracking.md) - memory-bounded key
  tracking for streaming merges.
