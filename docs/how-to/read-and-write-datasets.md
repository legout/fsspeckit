# Read and Write Datasets

This guide covers reading and writing data with fsspeckit. It has two parts:
the extended I/O methods for individual files (JSON, CSV, Parquet), and the
dataset handlers for partitioned parquet datasets.

For the shared handler contract and backend differences, see
[Dataset Handlers](../dataset-handlers.md). For exact I/O signatures, see
[fsspeckit.core](../api/fsspeckit.core.md).

## Reading files

The extended I/O methods are registered on every filesystem instance once
`fsspeckit` is imported.

```python
from fsspeckit import filesystem

fs = filesystem(".")
```

### JSON

```python
# Single file: returns a dict
data = fs.read_json_file("data.json")

# JSON Lines: returns a list of dicts
records = fs.read_json_file("events.jsonl", jsonlines=True)

# Multiple files (glob): returns a Polars DataFrame by default
df = fs.read_json("data/*.json")

# Batch reading with threading
for batch in fs.read_json("data/*.json", batch_size=5, use_threads=True):
    process(batch)

# Keep the source file path as a column
df = fs.read_json("data/*.json", include_file_path=True)
```

### CSV

```python
# Single file
df = fs.read_csv_file("data.csv")

# Multiple files, concatenated
df = fs.read_csv("data/*.csv", concat=True)

# Column selection and dtype optimization
df = fs.read_csv_file("data.csv", columns=["id", "value"])
df = fs.read_csv("data/*.csv", opt_dtypes=True)
```

### Parquet

```python
# Single file: returns a PyArrow Table
table = fs.read_parquet_file("data.parquet")

# Multiple files, schema-unified and concatenated
table = fs.read_parquet("data/*.parquet", concat=True)

# Batch reading for large datasets
for batch in fs.read_parquet("data/*.parquet", batch_size=20):
    process(batch)

# Column selection
table = fs.read_parquet_file("data.parquet", columns=["id", "value"])
```

### Reading any supported format

`read_files()` dispatches to the format-specific reader. Pass `format`
explicitly; supported values are `"json"`, `"csv"`, and `"parquet"`:

```python
df = fs.read_files("data/*.csv", format="csv", use_threads=True)
table = fs.read_files("data/*.parquet", format="parquet", concat=True)
```

## Writing files

The write methods accept a PyArrow Table or a Polars DataFrame:

```python
import pyarrow as pa

table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})

fs.write_parquet(table, "output.parquet")
fs.write_csv(table, "output.csv")
fs.write_json(table, "output.json")

# Compression is configurable
fs.write_parquet(table, "output.parquet", compression="zstd")
```

`write_files()` writes multiple files at once, with optional parallelism:

```python
fs.write_files(
    data=[table1, table2],
    path=["out1.parquet", "out2.parquet"],
    format="parquet",
    use_threads=True,
)
```

## Writing parquet datasets

For partitioned, multi-file parquet datasets, use a dataset handler.
`write_dataset()` writes new parquet files and returns a `WriteDatasetResult`
with per-file metadata. It never modifies existing rows - use
[`merge()`](merge-datasets.md) for incremental reconciliation.

Both backends require the `datasets` extra. See the
[extras matrix](../installation.md#optional-extras).

### PyArrow backend

```python
import pyarrow as pa
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

table = pa.table({
    "year": [2023, 2023, 2024, 2024],
    "month": [1, 2, 1, 2],
    "id": [1, 2, 3, 4],
})

# Partitioned write; mode="overwrite" clears existing parquet files first
result = io.write_dataset(
    table,
    "dataset/",
    mode="overwrite",
    partition_by=["year", "month"],
)

print(f"Wrote {result.total_rows} rows across {len(result.files)} files")
for f in result.files:
    print(f"  {f.path}: {f.row_count} rows, {f.size_bytes} bytes")
```

### DuckDB backend

The DuckDB backend takes a connection created with `create_duckdb_connection()`
and uses DuckDB's SQL engine for writes:

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)

result = io.write_dataset(table, "dataset/", mode="append")
```

### Write modes

`write_dataset()` accepts `mode="append"` (the default) or `mode="overwrite"`:

- **append**: adds new files alongside any existing parquet files.
- **overwrite**: removes existing parquet files under the path first, then
  writes.

Both modes honor `partition_by`, `compression`, `max_rows_per_file`, and
`row_group_size`. For the full parameter list, see
[Dataset Handlers](../dataset-handlers.md).

## Reading datasets back

Use the handler's `read_parquet()` to read an entire dataset directory:

```python
table = io.read_parquet("dataset/")

# With a filter and column projection
table = io.read_parquet("dataset/", columns=["id", "value"])
```

The two backends differ in the filter types they accept (SQL `WHERE` strings
for DuckDB; PyArrow expressions, DNF tuples, or SQL-like strings for
PyArrow). See [Dataset Handlers](../dataset-handlers.md) for the comparison.

## Related documentation

- [Dataset Handlers](../dataset-handlers.md) - shared interface and backend
  differences.
- [Merge Datasets](merge-datasets.md) - incremental merge operations.
- [Work with Filesystems](work-with-filesystems.md) - filesystem and extended
  I/O details.
