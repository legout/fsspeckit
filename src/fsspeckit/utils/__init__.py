"""Utility modules for fsspec-utils."""

from .duckdb import DuckDBParquetHandler
from .logging import setup_logging
from .misc import get_partitions_from_path, run_parallel, sync_dir, sync_files
from .polars import opt_dtype as opt_dtype_pl
from .polars import pl
from .pyarrow import cast_schema, convert_large_types_to_normal
from .pyarrow import opt_dtype as opt_dtype_pa
from .types import dict_to_dataframe, to_pyarrow_table


__all__ = [
    "setup_logging",
    "run_parallel",
    "get_partitions_from_path",
    "to_pyarrow_table",
    "dict_to_dataframe",
    "opt_dtype_pl",
    "opt_dtype_pa",
    "cast_schema",
    "convert_large_types_to_normal",
    "pl",
    "sync_dir",
    "sync_files",
    "DuckDBParquetHandler",
]
