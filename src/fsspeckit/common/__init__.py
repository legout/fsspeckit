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
from .types import dict_to_dataframe, to_pyarrow_table
from .schema import (
    unify_schemas,
    standardize_schema_timezones,
    dominant_timezone_per_column,
    convert_large_types_to_normal,
    cast_schema,
    remove_empty_columns,
)
from .partitions import (
    get_partitions_from_path,
    normalize_partition_value,
    validate_partition_columns,
    build_partition_path,
    extract_partition_filters,
    filter_paths_by_partitions,
    infer_partitioning_scheme,
    get_partition_columns_from_paths,
    create_partition_expression,
    apply_partition_pruning,
)
from .security import (
    validate_path,
    validate_compression_codec,
    scrub_credentials,
    scrub_exception,
    safe_format_error,
    validate_columns,
    VALID_COMPRESSION_CODECS,
)

# Conditionally import polars utilities
try:
    from .polars import opt_dtype as opt_dtype_pl, pl

    _POLARS_UTILS_AVAILABLE = True
except ImportError:
    opt_dtype_pl = None
    pl = None
    _POLARS_UTILS_AVAILABLE = False

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
    # polars utilities (may be None if polars not installed)
    "opt_dtype_pl",
    "pl",
    # type conversion utilities
    "dict_to_dataframe",
    "to_pyarrow_table",
    # security utilities
    "validate_path",
    "validate_compression_codec",
    "scrub_credentials",
    "scrub_exception",
    "safe_format_error",
    "validate_columns",
    "VALID_COMPRESSION_CODECS",
]
