# Merge Datasets

This guide covers incremental dataset merges with fsspeckit's `merge()` method.
Both the DuckDB and PyArrow backends share the same merge interface and result
type.

`merge()` is distinct from `write_dataset()`: `write_dataset()` writes files
and does not reconcile existing rows, while `merge()` reconciles a source
against the existing dataset by rewriting only the files that contain matching
keys. Use `write_dataset(mode="overwrite")` to clear existing dataset files
first. For the shared contract and backend differences, see
[Dataset Handlers](../dataset-handlers.md).

Both backends require the `datasets` extra. See the
[extras matrix](../installation.md#optional-extras).

## Set up a handler

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

initial = pa.table({"id": [1, 2], "value": ["a", "b"]})
io.write_dataset(initial, "events/", mode="overwrite")
```

For DuckDB, create a connection first and pass it to the handler:

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)
```

## Merge strategies

`merge()` takes a `strategy` and `key_columns`. A single string is equivalent
to a one-column list.

### insert

Appends only rows whose keys do not already exist in the target. Existing
files are never rewritten. Use it for append-only loads where duplicates should
be ignored.

```python
new_events = pa.table({"id": [2, 3, 4], "value": ["b_dup", "c", "d"]})
result = io.merge(
    new_events,
    "events/",
    strategy="insert",
    key_columns=["id"],
)

print(f"Inserted: {result.inserted}")  # 2 (ids 3 and 4; id 2 already exists)
```

### update

Rewrites only the files that contain keys being updated. Rows with keys absent
from the target are ignored. Use it for dimension-table maintenance where new
records should be rejected.

```python
price_updates = pa.table({"id": [1, 2, 9], "value": ["A2", "B2", "X"]})
result = io.merge(
    price_updates,
    "events/",
    strategy="update",
    key_columns=["id"],
)

print(f"Updated: {result.updated}")  # 2 (ids 1 and 2; id 9 ignored)
```

### upsert

Rewrites affected files for existing keys and appends inserted keys as new
files. Use it for change-data-capture and synchronization where you want both
inserts and updates.

```python
updates = pa.table({"id": [2, 3, 5], "value": ["B3", "C3", "E"]})
result = io.merge(
    updates,
    "events/",
    strategy="upsert",
    key_columns=["id"],
)

print(f"Inserted: {result.inserted}, Updated: {result.updated}")
```

## Interpreting the result

`merge()` returns a `MergeResult` with row-level and file-level counts. Use
these fields to audit the operation without re-reading the dataset:

```python
result = io.merge(updates, "events/", strategy="upsert", key_columns=["id"])

# Row counts
result.source_count          # rows in the source data
result.target_count_before   # target rows before the merge
result.target_count_after    # target rows after the merge
result.inserted
result.updated
result.deleted

# File lists
result.rewritten_files       # existing files rewritten
result.inserted_files        # new files created
result.preserved_files       # files left untouched
```

## Composite keys

Pass a list of column names for a composite key. All strategies support
composite keys.

```python
result = io.merge(
    new_orders,
    "orders/",
    strategy="upsert",
    key_columns=["order_id", "line_number"],
)
```

See [Multi-Key API](../reference/multi-key-api.md) for how composite keys are
resolved internally.

## Nullable key columns

Merge keys may contain null values. fsspeckit matches keys using null-equal
identity (equivalent to SQL `IS NOT DISTINCT FROM` applied component-wise):

- `NULL` matches `NULL`.
- `NULL` never matches a non-null value.
- A composite key matches only when every component matches under those rules.
- Equality of non-null values is unchanged.
- Duplicate source keys are last-row-wins, including keys with null components.

```python
# Target: (121221, "abc")
# Source: (121221, NULL)  -> different key, inserted
# Source: (121221, NULL)  -> matches the inserted row, not duplicated
result = io.merge(
    source,
    "orders/",
    strategy="upsert",
    key_columns=["id", "value"],
)
```

All three strategies honor this contract: `insert` appends rows whose
null-safe key is absent, `update` rewrites only rows whose null-safe key is
present, and `upsert` does both. Earlier releases rejected null keys; that
restriction no longer applies and sources do not need to be cleaned of nulls
before merging.

## Partition-aware merges

When the source is partitioned, pass `partition_columns` so the merge prunes
partitions and rewrites only the relevant ones. Files in untouched partitions
appear in `preserved_files`.

```python
result = io.merge(
    updates,
    "partitioned_data/",
    strategy="upsert",
    key_columns=["id"],
    partition_columns=["year", "month"],
)
```

## Schema on new files

Pass `schema` to enforce a schema for newly written files during the merge.
This lets you evolve a dataset by adding columns to appended or rewritten
files.

```python
schema = pa.schema([("id", pa.int64()), ("value", pa.string()), ("note", pa.string())])

result = io.merge(
    updates,
    "events/",
    strategy="upsert",
    key_columns=["id"],
    schema=schema,
)
```

## Backend differences

The two backends share the same core parameters and result type. They differ
in the knobs that are honored:

- **DuckDB**: uses SQL-based merge logic. The streaming parameters
  (`enable_streaming_merge`, `merge_max_memory_mb`, and friends) are accepted
  for compatibility but ignored.
- **PyArrow**: honors the streaming controls
  (`merge_chunk_size_rows`, `enable_streaming_merge`, `merge_max_memory_mb`,
  `merge_max_process_memory_mb`, `merge_min_system_available_mb`,
  `merge_progress_callback`) for memory-bounded merges.

For the full parameter list and the result-type reference, see
[Dataset Handlers](../dataset-handlers.md). For memory tuning, see
[Memory-Constrained Environments](memory-constrained-environments.md).

## Related documentation

- [Dataset Handlers](../dataset-handlers.md) - shared interface and result
  types.
- [Merge Operations Examples](merge-operations-examples.md) - end-to-end
  scenarios.
- [Multi-Key API](../reference/multi-key-api.md) - composite-key semantics.
- [Read and Write Datasets](read-and-write-datasets.md) - `write_dataset()`.
