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
from .misc import run_parallel, sync_dir, sync_files
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
# (polars move to datasets is issue #9; keep guarded re-export for now)
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
    # miscellaneous / partition utilities
    "get_partitions_from_path",
    "normalize_partition_value",
    "validate_partition_columns",
    "build_partition_path",
    "extract_partition_filters",
    "filter_paths_by_partitions",
    "infer_partitioning_scheme",
    "get_partition_columns_from_paths",
    "create_partition_expression",
    "apply_partition_pruning",
    "run_parallel",
    "sync_dir",
    "sync_files",
    # polars utilities (may be None if polars not installed)
    "opt_dtype_pl",
    "pl",
    # security utilities
    "validate_path",
    "validate_compression_codec",
    "scrub_credentials",
    "scrub_exception",
    "safe_format_error",
    "validate_columns",
    "VALID_COMPRESSION_CODECS",
]
