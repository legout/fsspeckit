# Dataset Module Refactor Migration Notes

## Summary
This refactor unifies the dataset API across PyArrow and DuckDB and removes legacy dataset helper functions.

## Removed APIs
The following APIs were removed and are no longer available:

- `fsspeckit.core.ext.dataset.write_pyarrow_dataset`
- `fsspeckit.core.ext.dataset.insert_dataset`
- `fsspeckit.core.ext.dataset.upsert_dataset`
- `fsspeckit.core.ext.dataset.update_dataset`
- `fsspeckit.core.ext.dataset.deduplicate_dataset`
- `DuckDBDatasetIO.merge_parquet_dataset`
- `DuckDBDatasetIO.insert_dataset`, `DuckDBDatasetIO.upsert_dataset`, `DuckDBDatasetIO.update_dataset`, `DuckDBDatasetIO.deduplicate_dataset`

## Replacement API
Use the class-based dataset handlers instead:

```python
from fsspeckit.datasets import PyarrowDatasetIO
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

# PyArrow
pa_io = PyarrowDatasetIO()
pa_io.write_dataset(table, "dataset/", mode="append")
pa_io.merge(table, "dataset/", strategy="upsert", key_columns=["id"])

# DuckDB
conn = create_duckdb_connection()
db_io = DuckDBDatasetIO(conn)
db_io.write_dataset(table, "dataset/", mode="append")
db_io.merge(table, "dataset/", strategy="upsert", key_columns=["id"])
```

## Notes
- `write_dataset` and `merge` now share aligned signatures and defaults across backends.
- `optimize_parquet_dataset` supports optional deduplication arguments on both backends.
- DuckDB write operations now accept `partition_by` to align with PyArrow partitioning.
