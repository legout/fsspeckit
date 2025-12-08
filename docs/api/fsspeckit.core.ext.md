# `fsspeckit.core.ext` API Documentation

This module provides extended functionalities for `fsspec.AbstractFileSystem`, including methods for reading and writing various file formats (JSON, CSV, Parquet) with advanced options like batch processing, parallelization, and data type optimization. It also includes functions for creating PyArrow datasets.

## Optional Dependencies

The extended I/O helpers support multiple data formats through optional dependencies. The core module imports work without these dependencies, but format-specific operations require the corresponding packages to be installed.

### Format Requirements

| Format | Required Package | Install Command | Extras Group |
| :----- | :--------------- | :-------------- | :----------- |
| JSON | `orjson` (recommended) or `json` (built-in) | `pip install orjson` | `sql` |
| CSV | `polars` | `pip install polars` | `datasets` |
| Parquet | `pyarrow` | `pip install pyarrow` | `datasets` |
| Dataset Operations | `polars`, `pyarrow`, `pandas` | `pip install polars pyarrow pandas` | `datasets` |

### Lazy Loading Behavior

The module uses lazy loading to minimize startup dependencies:

```python
# These imports work without optional dependencies
from fsspeckit.core.ext import read_files, write_files, write_file

# Legacy import (still works but deprecated)
# from fsspeckit.core.ext_io import read_files, write_files, write_file

# Using the functions triggers dependency loading
try:
    df = fs.read_files("data.csv", format="csv")  # Requires polars
    table = fs.read_files("data.parquet", format="parquet")  # Requires pyarrow
    fs.write_files(data, "output.json", format="json")  # Requires orjson
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install fsspeckit[datasets]")
```

### Error Messages

When optional dependencies are missing, you'll receive helpful error messages:

```python
# Missing polars for CSV operations
ImportError: polars is required for this function. Install with: pip install fsspeckit[datasets]

# Missing pyarrow for Parquet operations  
ImportError: pyarrow is required for this function. Install with: pip install fsspeckit[datasets]

# Missing orjson for JSON operations
ImportError: orjson is required for this function. Install with: pip install fsspeckit[sql]
```

### Installation Recommendations

**For basic CSV operations:**
```bash
pip install fsspeckit[datasets]
```

**For full functionality including JSON, Parquet, and datasets:**
```bash
pip install fsspeckit[datasets,sql]
```

**For development and testing:**
```bash
pip install fsspeckit[datasets,sql,dev]
```

---

## `path_to_glob()`

Convert a path to a glob pattern for file matching.

Intelligently converts paths to glob patterns that match files of the specified format, handling various directory and wildcard patterns.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Base path to convert. Can include wildcards (`*` or `**`). |
| `format` | `str \| None` | File format to match (without dot). If `None`, inferred from path. |

Common examples:

- Paths: `\"data/\"`, `\"data/*.json\"`, `\"data/**\"`
- Formats: `\"json\"`, `\"csv\"`, `\"parquet\"`

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `str` | `str` | Glob pattern that matches files of the specified format. |

**Example:**

```python
# Basic directory
path_to_glob("data", "json")
# 'data/**/*.json'

# With wildcards
path_to_glob("data/**", "csv")
# 'data/**/*.csv'

# Format inference
path_to_glob("data/file.parquet")
# 'data/file.parquet'
```

---

## `read_json_file()`

Read a single JSON file from any filesystem.

A public wrapper around `_read_json_file` providing a clean interface for reading individual JSON files.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `self` | `AbstractFileSystem` | Filesystem instance to use for reading |
| `path` | `str` | Path to JSON file to read |
| `include_file_path` | `bool` | Whether to return dict with filepath as key |
| `jsonlines` | `bool` | Whether to read as JSON Lines format |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict` or `list[dict]` | `dict` or `list[dict]` | Parsed JSON data. For regular JSON, returns a dict. For JSON Lines, returns a list of dicts. If `include_file_path=True`, returns `{filepath: data}`. |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# Read regular JSON
data = fs.read_json_file("config.json")
print(data["setting"])
# 'value'

# Read JSON Lines with filepath
data = fs.read_json_file(
    "logs.jsonl",
    include_file_path=True,
    jsonlines=True
)
print(list(data.keys())[0])
# 'logs.jsonl'
```

---

## `read_json()`

Read JSON data from one or more files with powerful options.

Provides a flexible interface for reading JSON data with support for:

- Single file or multiple files
- Regular JSON or JSON Lines format
- Batch processing for large datasets
- Parallel processing
- DataFrame conversion
- File path tracking

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` or `list[str]` | Path(s) to JSON file(s). Can be: - Single path string (globs supported) - List of path strings |
| `batch_size` | `int | None` | If set, enables batch reading with this many files per batch |
| `include_file_path` | `bool` | Include source filepath in output |
| `jsonlines` | `bool` | Whether to read as JSON Lines format |
| `as_dataframe` | `bool` | Convert output to Polars DataFrame(s) |
| `concat` | `bool` | Combine multiple files/batches into single result |
| `use_threads` | `bool` | Enable parallel file reading |
| `verbose` | `bool` | Print progress information |
| `opt_dtypes` | `bool` | Optimize DataFrame dtypes for performance |
| `**kwargs` | `Any` | Additional arguments passed to DataFrame conversion |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict` or `list[dict]` or `pl.DataFrame` or `list[pl.DataFrame]` or `Generator` | Various types depending on arguments: - `dict`: Single JSON file as dictionary - `list[dict]`: Multiple JSON files as list of dictionaries - `pl.DataFrame`: Single or concatenated DataFrame - `list[pl.DataFrame]`: List of Dataframes (if `concat=False`) - `Generator`: If `batch_size` set, yields batches of above types |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# Read all JSON files in directory
df = fs.read_json(
    "data/*.json",
    as_dataframe=True,
    concat=True
)
print(df.shape)
# (1000, 5)  # Combined data from all files

# Batch process large dataset
for batch_df in fs.read_json(
    "logs/*.jsonl",
    batch_size=100,
    jsonlines=True,
    include_file_path=True
):
    print(f"Processing {len(batch_df)} records")

# Parallel read with custom options
dfs = fs.read_json(
    ["file1.json", "file2.json"],
    use_threads=True,
    concat=False,
    verbose=True
)
print(f"Read {len(dfs)} files")
```

---

## `read_csv_file()`

Read a single CSV file from any filesystem.

Internal function that handles reading individual CSV files and optionally adds the source filepath as a column.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `self` | `AbstractFileSystem` | Filesystem instance to use for reading |
| `path` | `str` | Path to CSV file |
| `include_file_path` | `bool` | Add source filepath as a column |
| `opt_dtypes` | `bool` | Optimize DataFrame dtypes |
| `**kwargs` | `Any` | Additional arguments passed to `pl.read_csv()` |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `pl.DataFrame` | `pl.DataFrame` | DataFrame containing CSV data |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# This example assumes _read_csv_file is an internal method or needs to be called differently.
# For public use, you would typically use fs.read_csv().
# df = fs.read_csv_file(
#     "data.csv",
#     include_file_path=True,
#     delimiter="|"
# )
# print("file_path" in df.columns)
# True
```

---

## `read_csv()`

Read CSV data from one or more files with powerful options.

Provides a flexible interface for reading CSV files with support for:

- Single file or multiple files
- Batch processing for large datasets
- Parallel
- File path tracking
- Polars DataFrame output

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` or `list[str]` | Path(s) to CSV file(s). Can be: - Single path string (globs supported) - List of path strings |
| `batch_size` | `int | None` | If set, enables batch reading with this many files per batch |
| `include_file_path` | `bool` | Add source filepath as a column |
| `concat` | `bool` | Combine multiple files/batches into single DataFrame |
| `use_threads` | `bool` | Enable parallel file reading |
| `verbose` | `bool` | Print progress information |
| `**kwargs` | `Any` | Additional arguments passed to `pl.read_csv()` |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `pl.DataFrame` or `list[pl.DataFrame]` or `Generator` | Various types depending on arguments: - `pl.DataFrame`: Single or concatenated DataFrame - `list[pl.DataFrame]`: List of DataFrames (if `concat=False`) - `Generator`: If `batch_size` set, yields batches of above types |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# Read all CSVs in directory
df = fs.read_csv(
    "data/*.csv",
    include_file_path=True
)
print(df.columns)
# ['file_path', 'col1', 'col2', ...]

# Batch process large dataset
for batch_df in fs.read_csv(
    "logs/*.csv",
    batch_size=100,
    use_threads=True,
    verbose=True
):
    print(f"Processing {len(batch_df)} rows")

# Multiple files without concatenation
dfs = fs.read_csv(
    ["file1.csv", "file2.csv"],
    concat=False,
    use_threads=True
)
print(f"Read {len(dfs)} files")
```

---

## `read_parquet_file()`

Read a single Parquet file from any filesystem.

Internal function that handles reading individual Parquet files and optionally adds the source filepath as a column.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `self` | `AbstractFileSystem` | Filesystem instance to use for reading |
| `path` | `str` | Path to Parquet file |
| `include_file_path` | `bool` | Add source filepath as a column |
| `opt_dtypes` | `bool` | Optimize DataFrame dtypes |
| `**kwargs` | `Any` | Additional arguments passed to `pq.read_table()` |

| Returns | Type | Description |
| :--- | :--- | :---------- |
| `pa.Table` | `pa.Table` | PyArrow Table containing Parquet data |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# This example assumes _read_parquet_file is an internal method or needs to be called differently.
# For public use, you would typically use fs.read_parquet().
# table = fs.read_parquet_file(
#     "data.parquet",
#     include_file_path=True,
#     use_threads=True
# )
# print("file_path" in table.column_names)
# True
```

---

## `read_parquet()`

Read Parquet data with advanced features and optimizations.

Provides a high-performance interface for reading Parquet files with support for:

- Single file or multiple files
- Batch processing for large datasets
- Parallel processing
- File path tracking
- Automatic concatenation
- PyArrow Table output

The function automatically uses optimal reading strategies:

- Direct dataset reading for simple cases
- Parallel processing for multiple files
- Batched reading for memory efficiency

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` or `list[str]` | Path(s) to Parquet file(s). Can be: - Single path string (globs supported) - List of path strings - Directory containing _metadata file |
| `batch_size` | `int | None` | If set, enables batch reading with this many files per batch |
| `include_file_path` | `bool` | Add source filepath as a column |
| `concat` | `bool` | Combine multiple files/batches into single Table |
| `use_threads` | `bool` | Enable parallel file reading |
| `verbose` | `bool` | Print progress information |
| `opt_dtypes` | `bool` | Optimize Table dtypes for performance |
| `**kwargs` | `Any` | Additional arguments passed to `pq.read_table()` |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `pa.Table` or `list[pa.Table]` or `Generator` | Various types depending on arguments: - `pa.Table`: Single or concatenated Table - `list[pa.Table]`: List of Tables (if `concat=False`) - `Generator`: If `batch_size` set, yields batches of above types |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# Read all Parquet files in directory
table = fs.read_parquet(
    "data/*.parquet",
    include_file_path=True
)
print(table.column_names)
# ['file_path', 'col1', 'col2', ...]

# Batch process large dataset
for batch in fs.read_parquet(
    "data/*.parquet",
    batch_size=100,
    use_threads=True
):
    print(f"Processing {batch.num_rows} rows")

# Read from directory with metadata
table = fs.read_parquet(
    "data/",  # Contains _metadata
    use_threads=True
)
print(f"Total rows: {table.num_rows}")
```

---

## `read_files()`

Universal interface for reading data files of any supported format.

A unified API that automatically delegates to the appropriate reading function based on file format, while preserving all advanced features like:

- Batch processing
- Parallel reading
- File path tracking
- Format-specific optimizations

### Threading Behavior

The `use_threads` parameter controls parallel file reading:

- **`use_threads=True`** (default): Enables parallel processing using threading backend. Files are read concurrently, which significantly improves performance for multiple files.
- **`use_threads=False`**: Sequential processing. Files are read one after another, useful for debugging or when parallel processing causes issues.

**Performance Impact:**
- For single files: No difference in performance
- For multiple files: `use_threads=True` can provide 2-10x speedup depending on file count and size
- Memory usage: Threading mode may use slightly more memory due to concurrent operations

| Parameter | Type | Description |
| | :-------- | :--- | :---------- |
| `path` | `str` or `list[str]` | Path(s) to data file(s). Can be: - Single path string (globs supported) - List of path strings |
| `format` | `str` | File format to read. Supported values: - "json": Regular JSON or JSON Lines - "csv": CSV files - "parquet": Parquet files |
| `batch_size` | `int | None` | If set, enables batch reading with this many files per batch |
| `include_file_path` | `bool` | Add source filepath as column/field |
| `concat` | `bool` | Combine multiple files/batches into single result |
| `jsonlines` | `bool` | For JSON format, whether to read as JSON Lines |
| `use_threads` | `bool` | Enable parallel file reading (default: True) |
| `verbose` | `bool` | Print progress information |
| `opt_dtypes` | `bool` | Optimize DataFrame/Arrow Table dtypes for performance |
| `**kwargs` | `Any` | Additional format-specific arguments |

| Returns | Type | Description |
| | :------ | :--- | :---------- |
| `pl.DataFrame` or `pa.Table` or `list[pl.DataFrame]` or `list[pa.Table]` or `Generator` | Various types depending on format and arguments: - `pl.DataFrame`: For CSV and optionally JSON - `pa.Table`: For Parquet - `list[pl.DataFrame` or `pa.Table]`: Without concatenation - `Generator`: If `batch_size` set, yields batches |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# Read CSV files with parallel processing (default)
df = fs.read_files(
    "data/*.csv",
    format="csv",
    include_file_path=True
)
print(type(df))
# <class 'polars.DataFrame'>

# Batch process Parquet files with threading
for batch in fs.read_files(
    "data/*.parquet",
    format="parquet",
    batch_size=100,
    use_threads=True  # Enable parallel processing
):
    print(f"Batch type: {type(batch)}")

# Sequential processing for debugging
df = fs.read_files(
    "logs/*.jsonl",
    format="json",
    jsonlines=True,
    concat=True,
    use_threads=False  # Sequential processing
)
print(df.columns)
```

---

## `pyarrow_dataset()`

Create a PyArrow dataset from files in any supported format.

Creates a dataset that provides optimized reading and querying capabilities including:

- Schema inference and enforcement
- Partition discovery and pruning
- Predicate pushdown
- Column projection

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Base path to dataset files |
| `format` | `str` | File format. Currently supports: - "parquet" (default) - "csv" - "json" (experimental) |
| `schema` | `pa.Schema | None` | Optional schema to enforce. If None, inferred from data. |
| `partitioning` | `str` or `list[str]` or `pds.Partitioning` | How the dataset is partitioned. Can be: - `str`: Single partition field - `list[str]`: Multiple partition fields - `pds.Partitioning`: Custom partitioning scheme |
| `**kwargs` | `Any` | Additional arguments for dataset creation |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `pds.Dataset` | `pds.Dataset` | PyArrow dataset instance |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# Simple Parquet dataset
ds = fs.pyarrow_dataset("data/")
print(ds.schema)

# Partitioned dataset
ds = fs.pyarrow_dataset(
    "events/",
    partitioning=["year", "month"]
)
# Query with partition pruning
table = ds.to_table(
    filter=(ds.field("year") == 2024)
)

# CSV with schema
ds = fs.pyarrow_dataset(
    "logs/",
    format="csv",
    schema=pa.schema([
        ("timestamp", pa.timestamp("s")),
        ("level", pa.string()),
        ("message", pa.string())
    ])
)
```

---

## `pyarrow_parquet_dataset()`

Create a PyArrow dataset optimized for Parquet files.

Creates a dataset specifically for Parquet data, automatically handling `_metadata` files for optimized reading.

This function is particularly useful for:

- Datasets with existing `_metadata` files
- Multi-file datasets that should be treated as one
- Partitioned Parquet datasets

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Path to dataset directory or `_metadata` file |
| `schema` | `pa.Schema | None` | Optional schema to enforce. If None, inferred from data. |
| `partitioning` | `str` or `list[str]` or `pds.Partitioning` | How the dataset is partitioned. Can be: - `str`: Single partition field - `list[str]`: Multiple partition fields - `pds.Partitioning`: Custom partitioning scheme |
| `**kwargs` | `Any` | Additional dataset arguments |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `pds.Dataset` | `pds.Dataset` | PyArrow dataset instance |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# Dataset with _metadata
ds = fs.pyarrow_parquet_dataset("data/_metadata")
print(ds.files)  # Shows all data files

# Partitioned dataset directory
ds = fs.pyarrow_parquet_dataset(
    "sales/",
    partitioning=["year", "region"]
)
# Query with partition pruning
table = ds.to_table(
    filter=(
        (ds.field("year") == 2024) &
        (ds.field("region") == "EMEA")
    )
)
```

<!-----

## `pydala_dataset()`

Create a Pydala dataset for advanced Parquet operations.

Creates a dataset with additional features beyond PyArrow including:

- Delta table support
- Schema evolution
- Advanced partitioning
- Metadata management
- Sort key optimization

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Path to dataset directory |
| `partitioning` | `str` or `list[str]` or `pds.Partitioning` | How the dataset is partitioned. Can be: - `str`: Single partition field - `list[str]`: Multiple partition fields - `pds.Partitioning`: Custom partitioning scheme |
| `**kwargs` | `Any` | Additional dataset configuration |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `ParquetDataset` | `ParquetDataset` | Pydala dataset instance |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()
# Create dataset
ds = fs.pydala_dataset(
    "data/",
    partitioning=["date"]
)

# Write with delta support
ds.write_to_dataset(
    new_data,
    mode="delta",
    delta_subset=["id"]
)

# Read with metadata
df = ds.to_polars()
print(df.columns)
```-->

---

## `write_parquet()`

Write data to a Parquet file with automatic format conversion.

Handles writing data from multiple input formats to Parquet with:

- Automatic conversion to PyArrow
- Schema validation/coercion
- Metadata collection
- Compression and encoding options

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pl.LazyFrame` or `pa.Table` or `pd.DataFrame` or `dict` or `list[dict]` | Input data in various formats: - Polars DataFrame/LazyFrame - PyArrow Table - Pandas DataFrame - Dict or list of dicts |
| `path` | `str` | Output Parquet file path |
| `schema` | `pa.Schema | None` | Optional schema to enforce on write |
| `**kwargs` | `Any` | Additional arguments for `pq.write_table()` |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `pq.FileMetaData` | `pq.FileMetaData` | Metadata of written Parquet file |

| Raises | Type | Description |
| :----- | :--- | :---------- |
| `SchemaError` | `SchemaError` | If data doesn't match schema |
| `ValueError` | `ValueError` | If data cannot be converted |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem
import polars as pl
import numpy as np
import pyarrow as pa

fs = LocalFileSystem()
# Write Polars DataFrame
df = pl.DataFrame({
    "id": range(1000),
    "value": pl.Series(np.random.randn(1000))
})
metadata = fs.write_parquet(
    df,
    "data.parquet",
    compression="zstd",
    compression_level=3
)
print(f"Rows: {metadata.num_rows}")

# Write with schema
schema = pa.schema([
    ("id", pa.int64()),
    ("value", pa.float64())
])
metadata = fs.write_parquet(
    {"id": [1, 2], "value": [0.1, 0.2]},
    "data.parquet",
    schema=schema
)
```

---

## `write_json()`

Write data to a JSON file with flexible input support.

Handles writing data in various formats to JSON or JSON Lines, with optional appending for streaming writes.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `dict` or `pl.DataFrame` or `pl.LazyFrame` or `pa.Table` or `pd.DataFrame` or `dict` or `list[dict]` | Input data in various formats: - Dict or list of dicts - Polars DataFrame/LazyFrame - PyArrow Table - Pandas DataFrame |
| `path` | `str` | Output JSON file path |
| `append` | `bool` | Whether to append to existing file (JSON Lines mode) |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem
import polars as pl
import pyarrow as pa

fs = LocalFileSystem()
# Write dictionary
data = {"name": "test", "values": [1, 2, 3]}
fs.write_json(data, "config.json")

# Stream records
df1 = pl.DataFrame({"id": [1], "value": ["first"]})
df2 = pl.DataFrame({"id": [2], "value": ["second"]})
fs.write_json(df1, "stream.jsonl", append=False)
fs.write_json(df2, "stream.jsonl", append=True)

# Convert PyArrow
table = pa.table({"a": [1, 2], "b": ["x", "y"]})
fs.write_json(table, "data.json")
```

---

## `write_csv()`

Write data to a CSV file with flexible input support.

Handles writing data from multiple formats to CSV with options for:

- Appending to existing files
- Custom delimiters and formatting
- Automatic type conversion
- Header handling

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pl.LazyFrame` or `pa.Table` or `pd.DataFrame` or `dict` or `list[dict]` | Input data in various formats: - Polars DataFrame/LazyFrame - PyArrow Table - Pandas DataFrame - Dict or list of dicts |
| `path` | `str` | Output CSV file path |
| `append` | `bool` | Whether to append to existing file |
| `**kwargs` | `Any` | Additional arguments for CSV writing: - `delimiter`: Field separator (default ",") - `header`: Whether to write header row - `quote_char`: Character for quoting fields - `date_format`: Format for date/time fields - `float_precision`: Decimal places for floats |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem
import polars as pl
from datetime import datetime
import pyarrow as pa

fs = LocalFileSystem()
# Write Polars DataFrame
df = pl.DataFrame({
    "id": range(100),
    "name": ["item_" + str(i) for i in range(100)]
})
fs.write_csv(df, "items.csv")

# Append records
new_items = pl.DataFrame({
    "id": range(100, 200),
    "name": ["item_" + str(i) for i in range(100, 200)]
})
fs.write_csv(
    new_items,
    "items.csv",
    append=True,
    header=False
)

# Custom formatting
data = pa.table({
    "date": [datetime.now()],
    "value": [123.456]
})
fs.write_csv(
    data,
    "formatted.csv",
    date_format="%Y-%m-%d",
    float_precision=2
)
```

---

## `write_file()`

Write a DataFrame to a file in the given format.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pl.LazyFrame` or `pa.Table` or `pd.DataFrame` or `dict` | Data to write. |
| `path` | `str` | Path to write the data. |
| `format` | `str` | Format of the file. |
| `**kwargs` | `Any` | Additional keyword arguments. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `None` | `None` | |

---

## `write_files()`

Write a DataFrame or a list of DataFrames to a file or a list of files.

### Threading Behavior

The `use_threads` parameter controls parallel file writing:

- **`use_threads=True`** (default): Enables parallel processing using joblib's threading backend. Multiple files are written concurrently, which significantly improves performance for writing many files.
- **`use_threads=False`**: Sequential processing. Files are written one after another, useful for debugging or when parallel processing causes issues.

**Performance Impact:**
- For single files: No difference in performance
- For multiple files: `use_threads=True` can provide 2-8x speedup depending on file count and size
- Memory usage: Threading mode may use slightly more memory due to concurrent operations

### Path Handling

The function intelligently handles different path and data combinations:

- **Single path + Single data**: Writes to the specified path
- **Single path + Multiple data**: Replicates the path for each data item
- **Multiple paths + Single data**: Replicates the data for each path
- **Multiple paths + Multiple data**: Pairs data items with paths by index

| Parameter | Type | Description |
| | :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pl.LazyFrame` or `pa.Table` or `pa.RecordBatch` or `pa.RecordBatchReader` or `pd.DataFrame` or `dict` or `list[pl.DataFrame` or `pl.LazyFrame` or `pa.Table` or `pa.RecordBatch` or `pa.RecordBatchReader` or `pd.DataFrame` or `dict]` | Data to write. |
| `path` | `str` or `list[str]` | Path to write the data. Can be single path or list of paths. |
| `basename` | `str` | Basename of the files. Defaults to None. |
| `format` | `str` | Format of the data. Defaults to None (inferred from path). |
| `concat` | `bool` | If True, concatenate the DataFrames. Defaults to True. |
| `unique` | `bool` or `list[str]` or `str` | If True, remove duplicates. Defaults to False. |
| `mode` | `str` | Write mode. Defaults to 'append'. Options: - 'append': Append to existing files or create numbered variants - 'overwrite': Remove existing files first - 'delete_matching': Delete matching files before writing - 'error_if_exists': Raise error if files exist |
| `use_threads` | `bool` | If True, use parallel processing (default: True). Controls whether files are written concurrently. |
| `verbose` | `bool` | If True, print verbose output. Defaults to False. |
| `**kwargs` | `Any` | Additional keyword arguments. |

| Returns | Type | Description |
| | :------ | :--- | :---------- |
| `None` | `None` | |

| Raises | Type | Description |
| | :----- | :--- | :---------- |
| `FileExistsError` | `FileExistsError` | If file already exists and mode is 'error_if_exists'. |

**Example:**

```python
from fsspec.implementations.local import LocalFileSystem
import polars as pl

fs = LocalFileSystem()

# Create sample data
data1 = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})
data2 = pl.DataFrame({"id": [3, 4], "value": ["c", "d"]})

# Write multiple files with parallel processing (default)
fs.write_files(
    data=[data1, data2],
    path=["output1.json", "output2.json"],
    format="json",
    use_threads=True  # Enable parallel writing
)

# Sequential writing for debugging
fs.write_files(
    data=[data1, data2],
    path=["output1.csv", "output2.csv"],
    format="csv",
    use_threads=False  # Sequential processing
)

# Single path, multiple data (replicates path)
fs.write_files(
    data=[data1, data2],
    path="output.json",
    format="json"
)
# Creates: output.json, output-1.json

# Different write modes
fs.write_files(data1, "output.parquet", mode="overwrite")
fs.write_files(data2, "output.parquet", mode="append")
```

---

## `write_pyarrow_dataset()`

Write a tabular data to a PyArrow dataset.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pa.RecordBatchReader` or `pd.DataFrame` or `list[pl.DataFrame]` or `list[pa.Table]` or `list[pa.RecordBatch]` or `list[pa.RecordBatchReader]` or `list[pd.DataFrame]` | Data to write. |
| `path` | `str` | Path to write the data. |
| `basename` | `str | None` | Basename of the files. Defaults to None. |
| `schema` | `pa.Schema | None` | Schema of the data. Defaults to None. |
| `partition_by` | `str` or `list[str]` or `pds.Partitioning` or `None` | Partitioning of the data. Defaults to None. |
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
| `**kwargs` | `Any` | Additional keyword arguments for `pds.write_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata for standard writes, or None for merge-aware writes. |

<!-----

## `write_pydala_dataset()`

Write a tabular data to a Pydala dataset.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pa.RecordBatchReader` or `pd.DataFrame` or `list[pl.DataFrame]` or `list[pa.Table]` or `list[pa.RecordBatch]` or `list[pa.RecordBatchReader]` or `list[pd.DataFrame]` | Data to write. |
| `path` | `str` | Path to write the data. |
| `mode` | `str` | Write mode. Defaults to 'append'. Options: 'delta', 'overwrite'. |
| `basename` | `str | None` | Basename of the files. Defaults to None. |
| `partition_by` | `str` or `list[str]` or `None` | Partitioning of the data. Defaults to None. |
| `partitioning_flavor` | `str` | Partitioning flavor. Defaults to 'hive'. |
| `max_rows_per_file` | `int | None` | Maximum number of rows per file. Defaults to 2,500,000. |
| `row_group_size` | `int | None` | Row group size. Defaults to 250,000. |
| `compression` | `str` | Compression algorithm. Defaults to 'zstd'. |
| `sort_by` | `str` or `list[str]` or `list[tuple[str, str]]` or `None` | Columns to sort by. Defaults to None. |
| `unique` | `bool` or `str` or `list[str]` | If True, ensure unique values. Defaults to False. |
| `delta_subset` | `str` or `list[str]` or `None` | Subset of columns to include in delta table. Defaults to None. |
| `update_metadata` | `bool` | If True, update metadata. Defaults to True. |

---

## `insert_dataset()`

Insert-only dataset write using PyArrow.

Convenience method that calls `write_pyarrow_dataset` with `strategy='insert'`.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pa.RecordBatchReader` or `pd.DataFrame` or `list[pl.DataFrame]` or `list[pa.Table]` or `list[pa.RecordBatch]` or `list[pa.RecordBatchReader]` or `list[pd.DataFrame]` | Data to write. |
| `path` | `str` | Path to write the dataset. |
| `key_columns` | `str` or `list[str]` | Key columns for merge (required). |
| `**kwargs` | `Any` | Additional arguments passed to `write_pyarrow_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata or None. |

---

## `upsert_dataset()`

Insert-or-update dataset write using PyArrow.

Convenience method that calls `write_pyarrow_dataset` with `strategy='upsert'`.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pa.RecordBatchReader` or `pd.DataFrame` or `list[pl.DataFrame]` or `list[pa.Table]` or `list[pa.RecordBatch]` or `list[pa.RecordBatchReader]` or `list[pd.DataFrame]` | Data to write. |
| `path` | `str` | Path to write the dataset. |
| `key_columns` | `str` or `list[str]` | Key columns for merge (required). |
| `**kwargs` | `Any` | Additional arguments passed to `write_pyarrow_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata or None. |

---

## `update_dataset()`

Update-only dataset write using PyArrow.

Convenience method that calls `write_pyarrow_dataset` with `strategy='update'`.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pa.RecordBatchReader` or `pd.DataFrame` or `list[pl.DataFrame]` or `list[pa.Table]` or `list[pa.RecordBatch]` or `list[pa.RecordBatchReader]` or `list[pd.DataFrame]` | Data to write. |
| `path` | `str` | Path to write the dataset. |
| `key_columns` | `str` or `list[str]` | Key columns for merge (required). |
| `**kwargs` | `Any` | Additional arguments passed to `write_pyarrow_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata or None. |

---

## `deduplicate_dataset()`

Deduplicate dataset write using PyArrow.

Convenience method that calls `write_pyarrow_dataset` with `strategy='deduplicate'`.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `data` | `pl.DataFrame` or `pa.Table` or `pa.RecordBatch` or `pa.RecordBatchReader` or `pd.DataFrame` or `list[pl.DataFrame]` or `list[pa.Table]` or `list[pa.RecordBatch]` or `list[pa.RecordBatchReader]` or `list[pd.DataFrame]` | Data to write. |
| `path` | `str` | Path to write the dataset. |
| `key_columns` | `str` or `list[str]` | Optional key columns for deduplication. |
| `dedup_order_by` | `str` or `list[str]` | Columns to order by for deduplication. |
| `**kwargs` | `Any` | Additional arguments passed to `write_pyarrow_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list[pq.FileMetaData]` or `None` | List of Parquet file metadata or None. |
| `alter_schema` | `bool` | If True, alter schema. Defaults to False. |
| `timestamp_column` | `str` or `None` | Timestamp column. Defaults to None. |
| `verbose` | `bool` | If True, print verbose output. Defaults to True. |
| `**kwargs` | `Any` | Additional keyword arguments for `ParquetDataset.write_to_dataset`. |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `None` | `None` | |-->
