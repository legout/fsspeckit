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
)

# Re-export helpers
# from .helpers import (
#     # Add specific helpers here as needed
# )

# Type alias for backward compatibility
MergeStrategy = Literal["upsert", "insert", "update", "full_merge", "deduplicate"]


# Main DuckDBParquetHandler class for backward compatibility
class DuckDBParquetHandler(DuckDBDatasetIO):
    """Backward compatibility wrapper for DuckDBParquetHandler.

    This class has been refactored into:
    - DuckDBConnection: for connection management
    - DuckDBDatasetIO: for dataset I/O operations

    For new code, consider using DuckDBConnection and DuckDBDatasetIO directly.
    """

    def __init__(
        self,
        storage_options: BaseStorageOptions | dict | None = None,
        filesystem: AbstractFileSystem | None = None,
    ):
        """Initialize DuckDB parquet handler.

        Args:
            storage_options: Storage configuration options (deprecated)
            filesystem: Filesystem instance (deprecated)
        """
        from fsspeckit.datasets.duckdb.connection import create_duckdb_connection

        # Create connection from filesystem
        self._connection = create_duckdb_connection(filesystem=filesystem)

        # Initialize the IO handler
        super().__init__(self._connection)

    def execute_sql(self, query: str, parameters=None):
        """Execute SQL query (deprecated, use connection.execute_sql)."""
        return self._connection.execute_sql(query, parameters)

    def close(self):
        """Close connection (deprecated, use connection.close)."""
        self._connection.close()

    def __enter__(self):
        """Enter context manager (deprecated)."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager (deprecated)."""
        self.close()

    def __del__(self):
        """Destructor (deprecated)."""
        self.close()


__all__ = [
    # Connection management
    "DuckDBConnection",
    "create_duckdb_connection",
    # Dataset I/O
    "DuckDBDatasetIO",
    # Backward compatibility
    "DuckDBParquetHandler",
    "MergeStrategy",
]
