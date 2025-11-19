"""Cross-cutting utilities for fsspeckit.

This package contains utilities that are shared across different components:
- Datetime parsing and manipulation utilities
- Logging configuration and helpers
- General purpose utility functions
- Polars DataFrame optimization and manipulation
- Type conversion and data transformation utilities
"""

from .datetime import get_timestamp_column, get_timedelta_str, timestamp_from_string
from .logging import get_logger, setup_logging
from .misc import get_partitions_from_path, run_parallel, sync_dir, sync_files
from .polars import opt_dtype as opt_dtype_pl, pl
from .types import dict_to_dataframe, to_pyarrow_table

__all__ = [
    # datetime utilities
    "get_timestamp_column",
    "get_timedelta_str",
    "timestamp_from_string",
    # logging utilities
    "get_logger",
    "setup_logging",
    # miscellaneous utilities
    "get_partitions_from_path",
    "run_parallel",
    "sync_dir",
    "sync_files",
    # polars utilities
    "opt_dtype_pl",
    "pl",
    # type conversion utilities
    "dict_to_dataframe",
    "to_pyarrow_table",
]