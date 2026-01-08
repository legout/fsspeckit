"""DuckDB connection and filesystem registration helpers.

This module contains functions and classes for managing DuckDB connections
and integrating with fsspec filesystems.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb
    from fsspeckit.storage_options.base import BaseStorageOptions

from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem

from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _DUCKDB_AVAILABLE
from fsspeckit.datasets.duckdb.exceptions import (
    ConnectionException,
    IOException,
    OperationalError,
)
from fsspeckit.datasets.duckdb.helpers import _unregister_duckdb_table_safely

logger = get_logger(__name__)


class DuckDBConnection:
    """Manages DuckDB connection lifecycle and filesystem registration.

    This class is responsible for:
    - Creating and managing DuckDB connections
    - Registering fsspec filesystems with DuckDB
    - Connection cleanup

    Args:
        filesystem: fsspec filesystem instance to use
    """

    def __init__(self, filesystem: AbstractFileSystem | None = None) -> None:
        """Initialize DuckDB connection manager.

        Args:
            filesystem: Filesystem to use. Defaults to local filesystem.
        """
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._filesystem = filesystem or fsspec_filesystem("file")

    @property
    def connection(self) -> Any:
        """Get active DuckDB connection, creating it if necessary.

        Returns:
            Active DuckDB connection
        """
        if self._connection is None:
            self._connection = self._create_connection()
            self._register_filesystem()
        return self._connection

    @property
    def filesystem(self) -> AbstractFileSystem:
        """Get the filesystem instance.

        Returns:
            Filesystem instance
        """
        return self._filesystem

    def _create_connection(self) -> Any:
        """Create a new DuckDB connection.

        Returns:
            New DuckDB connection
        """
        from fsspeckit.common.optional import _import_duckdb

        duckdb = _import_duckdb()
        return duckdb.connect(":memory:")

    def _register_filesystem(self) -> None:
        """Register the filesystem with DuckDB.

        This allows DuckDB to access files through the fsspec filesystem.
        """
        try:
            self._connection.register_filesystem(self._filesystem)
        except (ConnectionException, IOException) as e:
            logger.warning(
                "Failed to register filesystem with DuckDB during connection setup: %s. "
                "Some operations may not work correctly.",
                e,
            )

    def execute_sql(
        self,
        query: str,
        parameters: list[Any] | None = None,
    ) -> Any:
        """Execute a SQL query.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters

        Returns:
            Query result
        """
        conn = self.connection

        if parameters:
            return conn.execute(query, parameters)
        else:
            return conn.execute(query)

    def close(self) -> None:
        """Close the connection and clean up resources."""
        if self._connection is not None:
            try:
                self._connection.close()
            except (ConnectionException, OperationalError) as e:
                logger.warning("Error closing DuckDB connection: %s", e)
            finally:
                self._connection = None

    def __enter__(self) -> "DuckDBConnection":
        """Enter context manager.

        Returns:
            self
        """
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc_val: Any,
        exc_tb: Any,
    ) -> None:
        """Exit context manager and close connection."""
        self.close()

    def __del__(self) -> None:
        """Destructor to ensure connection is closed."""
        self.close()


def create_duckdb_connection(
    filesystem: AbstractFileSystem | None = None,
) -> DuckDBConnection:
    """Create a DuckDB connection manager.

    Args:
        filesystem: fsspec filesystem to use

    Returns:
        DuckDB connection manager
    """
    return DuckDBConnection(filesystem=filesystem)
