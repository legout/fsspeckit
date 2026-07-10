# Multi-Key Performance

This guide explains how composite (multi-column) keys behave in fsspeckit's
merge and deduplication operations, and when to prefer them over single-column
keys. It focuses on the mechanisms that affect performance and memory, without
inventing benchmark numbers.

For the composite-key semantics reference, see
[Multi-Key API](../reference/multi-key-api.md). The PyArrow backend requires
the `datasets` extra. See the [extras matrix](../installation.md#optional-extras).

## How composite keys are processed

Composite-key matching stays in PyArrow's native format rather than converting
to Python objects:

1. Composite keys are built as StructArrays for efficient comparison.
2. Key membership is resolved with semi-join and anti-join operations on
   PyArrow tables.
3. When native joins fail on heterogeneous type combinations, the engine falls
   back to string-based key serialization.

This matters because the alternative - converting keys to Python tuples and
building a `set` - scales poorly: each row requires a Python object, memory is
duplicated across Arrow and Python space, and per-row type conversion adds
overhead. The vectorized path keeps comparisons in Arrow space, so the cost of
adding a second or third key column is far smaller than a naive conversion
would imply.

## Single-column versus composite keys

| | Single-column key | Composite key |
|---|---|---|
| Natural fit | A unique id already exists | No single column is unique |
| Per-key memory | One value per row | A small struct or serialized tuple per row |
| Match mechanism | Vectorized comparisons | Vectorized semi/anti-join, with a string fallback |
| When to prefer | Maximum throughput, minimal overhead | Correctness requires the combination |

Choose a single-column key when one exists naturally. Choose a composite key
when no single column uniquely identifies a row (common in multi-tenant and
event data), accepting a small per-key overhead for correct matching.

## Controlling memory during multi-key operations

Composite-key operations on large datasets benefit from the same memory
controls as single-key ones:

- `PyarrowDatasetIO.merge()` accepts `enable_streaming_merge`,
  `merge_chunk_size_rows`, and the `merge_max_*` thresholds. See
  [Dataset Handlers](../dataset-handlers.md).
- `deduplicate_parquet_dataset_pyarrow()` accepts `chunk_size_rows` and
  `max_memory_mb` to bound peak memory. See
  [Multi-Key API](../reference/multi-key-api.md).
- During incremental rewrites, an `AdaptiveKeyTracker` escalates through
  exact, LRU, and Bloom tiers as cardinality grows, bounding memory
  automatically. See [Adaptive Key Tracking](adaptive-key-tracking.md).

## When the fallback path engages

The string-based fallback engages only when native join fails on a particular
combination of column types (for example, mixing types that cannot be compared
directly). It is transparent: you still pass a list of column names, and the
engine selects the path internally. The fallback costs more per key than the
vectorized path, so if a workload is unexpectedly slow, check whether the key
columns have heterogeneous types that could be normalized.

## Practical guidance

- Prefer the vectorized path by keeping key columns of consistent, comparable
  types where possible.
- Use composite keys only when correctness requires the combination; a natural
  single-column key is always cheaper.
- Bound memory with the streaming/chunking parameters rather than hoping the
  dataset fits.
- Measure on your own data. fsspeckit does not publish synthetic benchmark
  tables; the right comparison is your workload before and after enabling the
  memory controls.

## Related documentation

- [Multi-Key API](../reference/multi-key-api.md) - composite-key semantics.
- [Multi-Key Examples](multi-key-examples.md) - merge and deduplication recipes.
- [Memory-Constrained Environments](memory-constrained-environments.md) -
  memory tuning.
- [Adaptive Key Tracking](adaptive-key-tracking.md) - tiered key tracking.
