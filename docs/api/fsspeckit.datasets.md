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

## Multi-Key Vectorization Support

PyArrow dataset operations now support **vectorized multi-column key processing** for 10-100x performance improvements:

- **Composite Key Deduplication**: `[tenant_id, record_id]` patterns with native Arrow operations
- **Multi-Key Merge Operations**: Upsert/update with composite business keys
- **Automatic Fallback**: Graceful degradation for incompatible type combinations
- **Memory Efficiency**: 5-6x reduction in peak memory usage for large datasets

For detailed API documentation on multi-key helper functions, see [Multi-Key API Reference](../reference/multi-key-api.md).

For practical usage examples, see [Multi-Key Usage Examples](../how-to/multi-key-examples.md).

For performance characteristics and optimization, see [Multi-Key Performance Guide](../how-to/multi-key-performance.md).

## Adaptive Key Tracking

The PyArrow backend includes **AdaptiveKeyTracker** for memory-bounded deduplication during streaming operations. This provides three tiers of key tracking with automatic transitions based on cardinality.

### `AdaptiveKeyTracker`

Tracks unique keys with adaptive memory usage across three tiers:

| Tier | Data Structure | Memory | Accuracy | Use Case |
|------|----------------|--------|----------|----------|
| **EXACT** | Python `set` | ~72 bytes/key | 100% | Low cardinality data |
| **LRU** | `OrderedDict` | ~72 bytes/key | 95-99%* | Medium cardinality with temporal patterns |
| **BLOOM** | `ScalableBloomFilter` | 1.25-2.5 bytes/key | 99.9%+ | High cardinality data |

*\*LRU accuracy depends on access patterns and cache hit ratio*

### Constructor

```python
AdaptiveKeyTracker(
    max_exact_keys: int = 1_000_000,
    max_lru_keys: int = 10_000_000,
    false_positive_rate: float = 0.001,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_exact_keys` | `int` | `1_000_000` | Maximum keys for exact tracking |
| `max_lru_keys` | `int` | `10_000_000` | Maximum keys for LRU cache |
| `false_positive_rate` | `float` | `0.001` | Target false positive rate for Bloom filter |

### Key Features

- **Automatic Tier Transitions**: Seamlessly moves between tiers as data cardinality grows
- **Thread Safety**: All operations are protected by internal locking
- **Memory Bounds**: Guaranteed memory limits regardless of data volume
- **Performance Monitoring**: Comprehensive metrics via `get_metrics()`

### Basic Usage

```python
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

# Create tracker tuned for medium cardinality
tracker = AdaptiveKeyTracker(
    max_exact_keys=500_000,
    max_lru_keys=5_000_000,
    false_positive_rate=0.001
)

# Process streaming data
for record in data_stream:
    key = (record['user_id'], record['timestamp'])
    if key not in tracker:
        tracker.add(key)
        process_new_record(record)

# Monitor performance
metrics = tracker.get_metrics()
print(f"Processed {metrics['unique_keys_estimate']} unique keys")
print(f"Current tier: {metrics['tier']}")
print(f"Tier transitions: {metrics['transitions']}")
```

### Dependencies

- **pybloom-live**: Required for Bloom filter tier (optional)
- **Installation**: `pip install pybloom-live`

### Automatic Integration

The AdaptiveKeyTracker is automatically used in PyArrow streaming operations for efficient deduplication:

```python
from fsspeckit.datasets import PyarrowDatasetHandler

handler = PyarrowDatasetHandler()

# Streaming merge uses AdaptiveKeyTracker internally
result = handler.merge(
    data=new_data,
    path="s3://bucket/dataset/",
    strategy="upsert", 
    key_columns=["user_id", "timestamp"]  # Multi-column keys supported
)

# Deduplication happens with memory-bounded efficiency
# across all three tiers automatically
```

### Advanced Configuration

```python
# Configuration for different workloads

# Small datasets requiring exact accuracy
small_tracker = AdaptiveKeyTracker(
    max_exact_keys=2_000_000,
    max_lru_keys=5_000_000,
    false_positive_rate=0.001
)

# Medium datasets with temporal locality
temporal_tracker = AdaptiveKeyTracker(
    max_exact_keys=100_000,
    max_lru_keys=2_000_000,
    false_positive_rate=0.001
)

# Large datasets requiring high accuracy
large_tracker = AdaptiveKeyTracker(
    max_exact_keys=10_000,
    max_lru_keys=100_000,
    false_positive_rate=0.0001
)

# Memory-constrained environments
memory_tracker = AdaptiveKeyTracker(
    max_exact_keys=10_000,
    max_lru_keys=100_000,
    false_positive_rate=0.01
)
```

### Performance Characteristics

| Operation | Exact Tier | LRU Tier | Bloom Tier |
|-----------|------------|----------|------------|
| **Add Performance** | 50-100K ops/sec | 40-80K ops/sec | 30-60K ops/sec |
| **Lookup Performance** | 200-500K ops/sec | 150-400K ops/sec | 100-300K ops/sec |
| **Memory per Key** | 72 bytes | 72 bytes | 1.25-2.5 bytes |

For detailed documentation:
- [API Reference](./fsspeckit.datasets.pyarrow.adaptive_tracker.md) - Complete API documentation
- [How-to Guide](../how-to/adaptive-key-tracking.md) - Practical usage examples
- [Reference Guide](../reference/adaptive-key-tracking.md) - Technical details and trade-offs

::: fsspeckit.datasets
