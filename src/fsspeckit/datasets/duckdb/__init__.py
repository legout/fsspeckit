"""DuckDB dataset integration for fsspeckit.

This package contains focused submodules for DuckDB functionality:
- dataset: Dataset I/O and maintenance operations
- connection: Connection management and filesystem registration
- helpers: Utility functions for DuckDB operations

All public APIs are re-exported here for convenient access.
"""

from typing import Any, Literal

from fsspec import AbstractFileSystem
from fsspeckit.storage_options.base import BaseStorageOptions

# Re-export connection management
from .connection import (
    DuckDBConnection,
    create_duckdb_connection,
)

# Re-export dataset I/O
from .dataset import (
    DuckDBDatasetIO,
    collect_dataset_stats_duckdb,
    compact_parquet_dataset_duckdb,
)

# Re-export helpers
# from .helpers import (
#     # Add specific helpers here as needed
# )

__all__ = [
    # Connection management
    "DuckDBConnection",
    "create_duckdb_connection",
    # Dataset I/O
    "DuckDBDatasetIO",
    # Dataset operations
    "collect_dataset_stats_duckdb",
    "compact_parquet_dataset_duckdb",
]
