"""Legacy utilities façade for fsspeckit.

This module provides backwards compatibility by re-exporting from new domain packages.
New code should import directly from domain packages.

DEPRECATED: This module exists only for backwards compatibility.
New code should import from:
- fsspeckit.datasets for dataset operations
- fsspeckit.sql for SQL utilities
- fsspeckit.common for cross-cutting utilities

SUPPORTED IMPORTS:
The following imports are supported for backwards compatibility:
- setup_logging
- run_parallel
- get_partitions_from_path
- to_pyarrow_table
- dict_to_dataframe
- opt_dtype_pl
- opt_dtype_pa
- cast_schema
- convert_large_types_to_normal
- pl
- sync_dir
- sync_files
- DuckDBParquetHandler
- Progress (via utils.misc.Progress shim)

All other utils.* imports are deprecated and may be removed in future versions.
"""

# Dataset helpers
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.datasets.pyarrow import cast_schema, convert_large_types_to_normal
from fsspeckit.datasets.pyarrow import opt_dtype as opt_dtype_pa

# Common utilities
from fsspeckit.common.logging import setup_logging
from fsspeckit.common.misc import (
    get_partitions_from_path,
    run_parallel,
    sync_dir,
    sync_files,
)
from fsspeckit.common.polars import opt_dtype as opt_dtype_pl, pl
from fsspeckit.common.types import dict_to_dataframe, to_pyarrow_table

# Re-export Progress from rich for backwards compatibility
# Used by tests that patch fsspeckit.utils.misc.Progress
from rich.progress import Progress

# Maintain existing __all__ for backwards compatibility
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
    "Progress",
]
