"""Cross-cutting utilities for fsspeckit.

This package contains utilities that are shared across different components:
- Datetime parsing and manipulation utilities
- Logging configuration and helpers
- General purpose utility functions
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
    # security utilities
    "validate_path",
    "validate_compression_codec",
    "scrub_credentials",
    "scrub_exception",
    "safe_format_error",
    "validate_columns",
    "VALID_COMPRESSION_CODECS",
]
