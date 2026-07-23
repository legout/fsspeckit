# Merge Operations Examples

This page walks through end-to-end merge scenarios using the public `merge()`
method on both backends. It focuses on strategy selection, composite keys, and
interpreting the `MergeResult`.

Both backends require the `datasets` extra. See the
[extras matrix](../installation.md#optional-extras). For the shared contract,
see [Dataset Handlers](../dataset-handlers.md).

## PyArrow upsert

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

customers = pa.table({
    "customer_id": [1, 2, 3],
    "email": ["alice@example.com", "bob@example.com", "carlie@example.com"],
    "status": ["active", "active", "inactive"],
})
io.write_dataset(customers, "customers/", mode="overwrite")

updates = pa.table({
    "customer_id": [2, 3, 4],
    "email": ["bob.new@example.com", "charlie@example.com", "diana@example.com"],
    "status": ["active", "active", "active"],
})

result = io.merge(
    updates,
    "customers/",
    strategy="upsert",
    key_columns=["customer_id"],
)

print(f"Inserted: {result.inserted}, Updated: {result.updated}")
# Inserted: 1 (id 4), Updated: 2 (ids 2 and 3)
```

## DuckDB update

The DuckDB backend takes a connection. The same strategies apply:

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)

io.write_dataset(products, "products/", mode="overwrite")

price_updates = pa.table({
    "product_id": [101, 102, 999],
    "name": ["Widget A", "Widget B", "Unknown"],
    "price": [21.99, 32.99, 1.00],
})

result = io.merge(
    price_updates,
    "products/",
    strategy="update",
    key_columns=["product_id"],
)

print(f"Updated: {result.updated}")  # 2 (ids 101 and 102; 999 ignored)
```

## Choosing a strategy

| Strategy | Inserts new keys? | Rewrites existing files? | Typical use |
|----------|-------------------|--------------------------|-------------|
| `insert` | Yes (new files) | No | Append-only loads, event logs |
| `update` | No | Yes (only affected files) | Dimension updates, price changes |
| `upsert` | Yes (new files) | Yes (only affected files) | CDC, synchronization |

All three strategies require `key_columns`. Key columns may contain nulls:
fsspeckit matches them with null-equal (SQL `IS NOT DISTINCT FROM`) semantics,
so `NULL` matches `NULL` but never a non-null value. `insert` and `upsert`
work on an empty target; `update` requires an existing target dataset.

## Composite-key merge

A composite key identifies a row by several columns. Pass them as a list:

```python
result = io.merge(
    order_updates,
    "orders/",
    strategy="upsert",
    key_columns=["order_id", "line_number"],
)
```

Key matching is vectorized in PyArrow: composite keys are compared as
StructArrays, with semi-join and anti-join operations, falling back to
string-based key serialization for heterogeneous type combinations. See
[Multi-Key API](../reference/multi-key-api.md).

## Interpreting the result

Every `merge()` returns a `MergeResult`. Audit the operation from its fields
rather than re-reading the dataset:

```python
result = io.merge(updates, "customers/", strategy="upsert", key_columns=["customer_id"])

# What changed
print(result.inserted, result.updated, result.deleted)

# Reconciliation check
assert result.target_count_before + result.inserted - result.deleted == result.target_count_after

# File-level audit
print(f"Rewritten: {len(result.rewritten_files)}")
print(f"Inserted: {len(result.inserted_files)}")
print(f"Preserved: {len(result.preserved_files)}")
```

## Key column requirements

- Key columns must be present in both source and target.
- Keys may contain null values. Null key components are compared with
  null-equal semantics (`IS NOT DISTINCT FROM`): `NULL` matches `NULL`, and
  `NULL` never matches a non-null value.
- For a composite key, the full column combination must identify a row
  uniquely. Use `dedup_order_by` (on `deduplicate_parquet_dataset_pyarrow`) to
  control which duplicate survives during deduplication.

## Error handling

Common failures and their fixes:

- **Key column not found**: verify the column names match the source schema
  exactly.
- **`update` on a missing target**: `update` requires an existing dataset. Use
  `insert` or `upsert` to seed a new one.

## Related documentation

- [Merge Datasets](merge-datasets.md) - strategy reference and backend notes.
- [Dataset Handlers](../dataset-handlers.md) - shared interface and result
  types.
- [Multi-Key API](../reference/multi-key-api.md) - composite-key semantics.
