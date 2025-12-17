# `fsspeckit.datasets` API Reference

> **Package Structure Note:** fsspeckit has been refactored to use package-based structure. DuckDB and PyArrow functionality is now organized under `datasets.duckdb` and `datasets.pyarrow` respectively, while legacy imports still work.

## DuckDB Dataset Operations

### `DuckDBDatasetIO.write_dataset()`

Write tabular data to a DuckDB-managed parquet dataset with explicit mode configuration.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pd.DataFrame` | Data to write. |
| `path` | `str` | Path to write the data. |
| `mode` | `"append"` or `"overwrite"` | Write mode. Defaults to `"append"`. |
| `basename` | `str | None` | Basename of the files. Defaults to None. |
| `schema` | `pa.Schema | None` | Schema of the data. Defaults to None. |
| `partition_by` | `str` or `list[str]` | Partitioning of the data. Defaults to None. |
| `partitioning_flavor` | `str` | Partitioning flavor. Defaults to 'hive'. |
| `format` | `str | None` | Format of the data. Defaults to 'parquet'. |
| `compression` | `str` | Compression algorithm. Defaults to 'zstd'. |
| `max_rows_per_file` | `int | None` | Maximum number of rows per file. Defaults to 2,500,000. |
| `row_group_size` | `int | None` | Row group size. Defaults to 250,000. |
| `concat` | `bool` | If True, concatenate the DataFrames. Defaults to True. |
| `verbose` | `bool` | Print progress information. Defaults to False. |
| `**kwargs` | `Any` | Additional keyword arguments. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` | List of Parquet file metadata for the write operation. |

### `DuckDBDatasetIO.merge()`

Perform incremental merge operations on existing DuckDB-managed datasets.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pd.DataFrame` | Data to merge. |
| `path` | `str` | Path to the existing dataset. |
| `strategy` | `"insert"` or `"update"` or `"upsert"` | Merge strategy to use. |
| `key_columns` | `str` or `list[str]` | Key columns for merge (required). |
| `basename` | `str | None` | Basename of the files. Defaults to None. |
| `schema` | `pa.Schema | None` | Schema of the data. Defaults to None. |
| `partition_by` | `str` or `list[str]` | Partitioning of the data. Defaults to None. |
| `partitioning_flavor` | `str` | Partitioning flavor. Defaults to 'hive'. |
| `format` | `str | None` | Format of the data. Defaults to 'parquet'. |
| `compression` | `str` | Compression algorithm. Defaults to 'zstd'. |
| `max_rows_per_file` | `int | None` | Maximum number of rows per file. Defaults to 2,500,000. |
| `row_group_size` | `int | None` | Row group size. Defaults to 250,000. |
| `concat` | `bool` | If True, concatenate the DataFrames. Defaults to True. |
| `dedup_order_by` | `str | list[str] | None` | Columns to order by for deduplication. Defaults to key_columns. |
| `verbose` | `bool` | Print progress information. Defaults to False. |
| `**kwargs` | `Any` | Additional keyword arguments. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `MergeStats` | Statistics about the merge operation. |

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

High-level interface for DuckDB dataset operations that provides the core methods.

| Method | Description |
| :------ | :---------- |
| `write_dataset()` | Write data with explicit mode (append/overwrite) |
| `merge()` | Perform merge operations with defined strategies |

## PyArrow Dataset Operations

### `PyarrowDatasetIO.write_dataset()`

Write a PyArrow table to a parquet dataset with explicit mode configuration.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pa.Table` or `list[pa.Table]` | PyArrow table or list of tables to write. |
| `path` | `str` | Output directory path. |
| `mode` | `"append"` or `"overwrite"` | Write mode. Defaults to `"append"`. |
| `basename_template` | `str | None` | Template for file names (default: part-{i}.parquet). |
| `schema` | `pa.Schema | None` | Optional schema to enforce. |
| `partition_by` | `str` or `list[str]` | Column(s) to partition by. |
| `compression` | `str | None` | Compression codec (default: snappy). |
| `max_rows_per_file` | `int | None` | Maximum rows per file (default: 5,000,000). |
| `row_group_size` | `int | None` | Rows per row group (default: 500,000). |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `MergeStats | None` | File metadata for the write operation. |

### `PyarrowDatasetIO.merge()`

Perform incremental merge operations on existing PyArrow datasets.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pa.Table` or `list[pa.Table]` | PyArrow table or list of tables to merge. |
| `path` | `str` | Path to the existing dataset. |
| `strategy` | `"insert"` or `"update"` or `"upsert"` | Merge strategy to use. |
| `key_columns` | `list[str] | str` | Key columns for merge (required). |
| `basename_template` | `str | None` | Template for file names (default: part-{i}.parquet). |
| `schema` | `pa.Schema | None` | Optional schema to enforce. |
| `partition_by` | `str` or `list[str]` | Column(s) to partition by. |
| `compression` | `str | None` | Compression codec (default: snappy). |
| `max_rows_per_file` | `int | None` | Maximum rows per file (default: 5,000,000). |
| `row_group_size` | `int | None` | Rows per row group (default: 500,000). |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `MergeStats` | Statistics about the merge operation. |
| `compression` | `str | None` | Output compression codec. |
| `verbose` | `bool` | Print progress information. |
| `**kwargs` | `Any` | Additional arguments. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `MergeStats` | Merge statistics for the operation. |

### `PyarrowDatasetIO.compact_parquet_dataset()`

Compact a parquet dataset using PyArrow.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Dataset path. |
| `target_mb_per_file` | `int | None` | Target size per file in MB. |
| `target_rows_per_file` | `int | None` | Target rows per file. |
| `partition_filter` | `list[str] | None` | Optional partition filters. |
| `compression` | `str | None` | Compression codec. |
| `dry_run` | `bool` | Whether to perform a dry run. |
| `verbose` | `bool` | Print progress information. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict[str, Any]` | Compaction statistics. |

### `PyarrowDatasetIO.optimize_parquet_dataset()`

Optimize a parquet dataset.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Dataset path. |
| `target_mb_per_file` | `int | None` | Target size per file in MB. |
| `target_rows_per_file` | `int | None` | Target rows per file. |
| `partition_filter` | `list[str] | None` | Optional partition filters. |
| `compression` | `str | None` | Compression codec. |
| `verbose` | `bool` | Print progress information. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict[str, Any]` | Optimization statistics. |

### `PyarrowDatasetIO.read_parquet()`

Read parquet file(s) using PyArrow.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Path to parquet file or directory. |
| `columns` | `list[str] | None` | Optional list of columns to read. |
| `filters` | `Any | None` | Optional row filter expression. |
| `use_threads` | `bool` | Whether to use parallel reading (default: True). |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `pa.Table` | PyArrow table containing the data. |

### `PyarrowDatasetIO.write_parquet()`

Write parquet file using PyArrow.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pa.Table` or `list[pa.Table]` | PyArrow table or list of tables to write. |
| `path` | `str` | Output file path. |
| `compression` | `str | None` | Compression codec (default: snappy). |
| `row_group_size` | `int | None` | Rows per row group. |

### `PyarrowDatasetHandler`

High-level interface for PyArrow dataset operations that inherits all methods from `PyarrowDatasetIO`.

| Method | Description |
| :------ | :---------- |
| `write_parquet_dataset()` | Write data with optional merge strategies |
| `insert_dataset()` | Insert-only convenience method |
| `upsert_dataset()` | Insert-or-update convenience method |
| `update_dataset()` | Update-only convenience method |
| `deduplicate_dataset()` | Deduplicate convenience method |
| `merge_parquet_dataset()` | Merge multiple datasets |
| `compact_parquet_dataset()` | Compact small files |
| `optimize_parquet_dataset()` | Optimize dataset performance |
| `read_parquet()` | Read parquet files and datasets |
| `write_parquet()` | Write single parquet files |

::: fsspeckit.datasets
