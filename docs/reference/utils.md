# Utils Compatibility

`fsspeckit.utils` is a deprecated backwards-compatible facade. It re-exports a
fixed set of helpers from their canonical domain packages so that existing code
keeps working, but it is not a supported primary import route.

New code should import from the domain packages directly. The full mapping of
legacy imports to canonical imports is on the
[Legacy Imports](legacy-imports.md) page.

## What `utils` re-exports

The facade re-exports these symbols from their current homes:

- Logging: `setup_logging` (from `fsspeckit.common.logging`)
- Parallelism: `run_parallel` (from `fsspeckit.common.parallel`)
- Partitions: `get_partitions_from_path` (from `fsspeckit.common.partitions`)
- Sync: `sync_dir`, `sync_files` (from `fsspeckit.common.sync`)
- Type conversion: `dict_to_dataframe`, `to_pyarrow_table`,
  `convert_large_types_to_normal`, `opt_dtype_pl`, `opt_dtype_pa`,
  `cast_schema` (from `fsspeckit.datasets.types` and
  `fsspeckit.datasets.schema`)
- Polars: `pl` (from `fsspeckit.datasets.polars`)
- Datasets: `DuckDBParquetHandler` (alias for `DuckDBDatasetIO`, from
  `fsspeckit.datasets.duckdb`)
- Progress: `Progress` (from `rich.progress`)

## What `utils` does not cover

The facade does not re-export storage-options classes, SQL filter functions, or
filesystem factories. Import those from their domain packages:

```python
from fsspeckit import filesystem, AwsStorageOptions
from fsspeckit.sql import sql2pyarrow_filter
```

## Status

There is no generated API page for `fsspeckit.utils`. It is documented only
through [Legacy Imports](legacy-imports.md) and this page. Prefer the canonical
imports listed there for all new and maintained code.
