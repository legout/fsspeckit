# Legacy Imports

This page maps old import paths and renamed symbols to their current canonical
imports. Use canonical imports in all new code. Legacy paths still work for the
symbols listed below but emit no long-term support promise.

For the full list of supported public symbols and their status, see the
[Public API Inventory](public-api-inventory.md).

## `fsspeckit.utils` top-level imports

`fsspeckit.utils` is a backwards-compatible facade. The following top-level
imports work and resolve to canonical domain objects:

| Legacy import | Canonical import |
|---------------|------------------|
| `from fsspeckit.utils import setup_logging` | `from fsspeckit.common.logging import setup_logging` |
| `from fsspeckit.utils import run_parallel` | `from fsspeckit.common.parallel import run_parallel` |
| `from fsspeckit.utils import get_partitions_from_path` | `from fsspeckit.common.partitions import get_partitions_from_path` |
| `from fsspeckit.utils import sync_dir` | `from fsspeckit.common.sync import sync_dir` |
| `from fsspeckit.utils import sync_files` | `from fsspeckit.common.sync import sync_files` |
| `from fsspeckit.utils import dict_to_dataframe` | `from fsspeckit.datasets.types import dict_to_dataframe` |
| `from fsspeckit.utils import to_pyarrow_table` | `from fsspeckit.datasets.types import to_pyarrow_table` |
| `from fsspeckit.utils import convert_large_types_to_normal` | `from fsspeckit.datasets.schema import convert_large_types_to_normal` |
| `from fsspeckit.utils import opt_dtype_pl` | `from fsspeckit.datasets.polars import opt_dtype as opt_dtype_pl` |
| `from fsspeckit.utils import opt_dtype_pa` | `from fsspeckit.datasets.schema import opt_dtype as opt_dtype_pa` |
| `from fsspeckit.utils import cast_schema` | `from fsspeckit.datasets.schema import cast_schema` |
| `from fsspeckit.utils import DuckDBParquetHandler` | `from fsspeckit.datasets.duckdb import DuckDBDatasetIO` |
| `from fsspeckit.utils import Progress` | `from rich.progress import Progress` |
| `from fsspeckit.utils import pl` | `import polars as pl` |

## `fsspeckit.utils` submodule paths

Some submodule paths still work as facades:

| Legacy submodule | Canonical import |
|------------------|------------------|
| `fsspeckit.utils.misc.run_parallel` | `from fsspeckit.common.parallel import run_parallel` |
| `fsspeckit.utils.misc.sync_dir` / `sync_files` | `from fsspeckit.common.sync import sync_dir, sync_files` |
| `fsspeckit.utils.misc.get_partitions_from_path` | `from fsspeckit.common.partitions import get_partitions_from_path` |
| `fsspeckit.utils.misc.Progress` | `from rich.progress import Progress` |
| `fsspeckit.utils.polars.opt_dtype_pl` | `from fsspeckit.datasets.polars import opt_dtype as opt_dtype_pl` |
| `fsspeckit.utils.sql.get_table_names` | `from fsspeckit.sql import get_table_names` |

`fsspeckit.utils.pyarrow` is not maintained and may fail to import. Import
schema and type utilities directly from `fsspeckit.datasets.schema` or
`fsspeckit.datasets.pyarrow` instead.

## Renamed dataset symbols

| Old name | Canonical import |
|----------|------------------|
| `DuckDBParquetHandler` | `from fsspeckit.datasets.duckdb import DuckDBDatasetIO` |
| `PyarrowDatasetHandler` | `from fsspeckit.datasets.pyarrow import PyarrowDatasetIO` |
| `pyarrow_dataset` (handler alias) | `from fsspeckit.datasets.pyarrow import PyarrowDatasetIO` |

## What was never part of `utils`

The following were never re-exported from `fsspeckit.utils` and should be
imported from their domain packages in all versions:

- SQL filter functions: `from fsspeckit.sql import sql2pyarrow_filter, sql2polars_filter`
- Storage options classes: `from fsspeckit.storage_options import AwsStorageOptions, GcsStorageOptions, AzureStorageOptions`

## Migration

For the step-by-step package-layout migration, see
[Upgrade from the Pre-refactor Layout](../migration/dataset-module-refactor.md).

For the broader API selection guide, see the [API Guide](api-guide.md).
