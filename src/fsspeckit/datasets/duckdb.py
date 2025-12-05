"""Re-export module for backward compatibility.

This module has been decomposed into focused submodules:
- duckdb_connection: Connection management and filesystem registration
- duckdb_dataset: Dataset I/O and maintenance operations

All public APIs are re-exported here to maintain backward compatibility.
New code should import directly from the submodules for better organization.
"""

from typing import Any, Literal, Optional, Union

from fsspec import AbstractFileSystem
from fsspeckit.storage_options.base import BaseStorageOptions

# Re-export connection management
from fsspeckit.datasets.duckdb_connection import DuckDBConnection, create_duckdb_connection

# Re-export dataset I/O
from fsspeckit.datasets.duckdb_dataset import DuckDBDatasetIO

# Type alias for backward compatibility
MergeStrategy = Literal["upsert", "insert", "update", "full_merge", "deduplicate"]


# Main DuckDBParquetHandler class - kept for backward compatibility
class DuckDBParquetHandler(DuckDBDatasetIO):
    """Backward compatibility wrapper for DuckDBParquetHandler.

    This class has been refactored into:
    - DuckDBConnection: for connection management
    - DuckDBDatasetIO: for dataset I/O operations

    For new code, consider using DuckDBConnection and DuckDBDatasetIO directly.
    """

    def __init__(
        self,
        storage_options: Optional[Union[BaseStorageOptions, dict]] = None,
        filesystem: Optional[AbstractFileSystem] = None,
    ):
        """Initialize DuckDB parquet handler.

        Args:
            storage_options: Storage configuration options (deprecated)
            filesystem: Filesystem instance (deprecated)
        """
        from fsspeckit.datasets.duckdb_connection import create_duckdb_connection

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
