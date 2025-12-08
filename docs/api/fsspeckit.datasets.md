# `fsspeckit.datasets` API Reference

> **Package Structure Note:** fsspeckit has been refactored to use package-based structure. DuckDB and PyArrow functionality is now organized under `datasets.duckdb` and `datasets.pyarrow` respectively, while legacy imports still work.

## DuckDB Dataset Operations

### `DuckDBDatasetIO.write_parquet_dataset()`

Write a tabular data to a DuckDB-managed parquet dataset with optional merge strategies.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pd.DataFrame` | Data to write. |
| `path` | `str` | Path to write the data. |
| `basename` | `str | None` | Basename of the files. Defaults to None. |
| `schema` | `pa.Schema | None` | Schema of the data. Defaults to None. |
| `partition_by` | `str` or `list[str]` | Partitioning of the data. Defaults to None. |
| `partitioning_flavor` | `str` | Partitioning flavor. Defaults to 'hive'. |
| `mode` | `str` | Write mode. Defaults to 'append'. |
| `format` | `str | None` | Format of the data. Defaults to 'parquet'. |
| `compression` | `str` | Compression algorithm. Defaults to 'zstd'. |
| `max_rows_per_file` | `int | None` | Maximum number of rows per file. Defaults to 2,500,000. |
| `row_group_size` | `int | None` | Row group size. Defaults to 250,000. |
| `concat` | `bool` | If True, concatenate the DataFrames. Defaults to True. |
| `unique` | `bool` or `str` or `list[str]` | If True, remove duplicates. Defaults to False. |
| `strategy` | `str | None` | Optional merge strategy: 'insert', 'upsert', 'update', 'full_merge', 'deduplicate'. Defaults to None (standard write). |
| `key_columns` | `str | list[str] | None` | Key columns for merge operations. Required for relational strategies. Defaults to None. |
| `dedup_order_by` | `str | list[str] | None` | Columns to order by for deduplication. Defaults to key_columns. |
| `verbose` | `bool` | Print progress information. Defaults to False. |
| `**kwargs` | `Any` | Additional keyword arguments. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata for standard writes, or None for merge-aware writes. |

### `DuckDBDatasetIO.insert_dataset()`

Insert-only dataset write using DuckDB.

Convenience method that calls `write_parquet_dataset` with `strategy='insert'`.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pd.DataFrame` | Data to write. |
| `path` | `str` | Path to write the dataset. |
| `key_columns` | `str` or `list[str]` | Key columns for merge (required). |
| `**kwargs` | `Any` | Additional arguments passed to `write_parquet_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata or None. |

### `DuckDBDatasetIO.upsert_dataset()`

Insert-or-update dataset write using DuckDB.

Convenience method that calls `write_parquet_dataset` with `strategy='upsert'`.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pd.DataFrame` | Data to write. |
| `path` | `str` | Path to write the dataset. |
| `key_columns` | `str` or `list[str]` | Key columns for merge (required). |
| `**kwargs` | `Any` | Additional arguments passed to `write_parquet_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata or None. |

### `DuckDBDatasetIO.update_dataset()`

Update-only dataset write using DuckDB.

Convenience method that calls `write_parquet_dataset` with `strategy='update'`.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pd.DataFrame` | Data to write. |
| `path` | `str` | Path to write the dataset. |
| `key_columns` | `str` or `list[str]` | Key columns for merge (required). |
| `**kwargs` | `Any` | Additional arguments passed to `write_parquet_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata or None. |

### `DuckDBDatasetIO.deduplicate_dataset()`

Deduplicate dataset write using DuckDB.

Convenience method that calls `write_parquet_dataset` with `strategy='deduplicate'`.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pd.DataFrame` | Data to write. |
| `path` | `str` | Path to write the dataset. |
| `key_columns` | `str` or `list[str]` | Optional key columns for deduplication. |
| `dedup_order_by` | `str` or `list[str]` | Columns to order by for deduplication. |
| `**kwargs` | `Any` | Additional arguments passed to `write_parquet_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata or None. |

### `DuckDBParquetHandler`

High-level interface for DuckDB dataset operations that re-exports convenience methods.

| Method | Description |
| :------ | :---------- |
| `write_parquet_dataset()` | Write data with optional merge strategies |
| `insert_dataset()` | Insert-only convenience method |
| `upsert_dataset()` | Insert-or-update convenience method |
| `update_dataset()` | Update-only convenience method |
| `deduplicate_dataset()` | Deduplicate convenience method |

::: fsspeckit.datasets
