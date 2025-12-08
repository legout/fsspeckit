"""DuckDB cleanup helpers for standardized error handling.

This module provides helper functions for safely unregistering DuckDB tables
with proper error handling and logging, following standardized
error handling patterns defined in change proposal.
"""

from __future__ import annotations

from typing import Any
from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _DUCKDB_AVAILABLE

logger = get_logger(__name__)

# DuckDB exception types for specific error handling
_DUCKDB_EXCEPTIONS = {}
if _DUCKDB_AVAILABLE:
    import duckdb

    _DUCKDB_EXCEPTIONS = {
        "InvalidInputException": duckdb.InvalidInputException,
        "OperationalException": duckdb.OperationalError,
        "CatalogException": duckdb.CatalogException,
        "IOException": duckdb.IOException,
        "OutOfMemoryException": duckdb.OutOfMemoryException,
        "ParserException": duckdb.ParserException,
        "ConnectionException": duckdb.ConnectionException,
        "SyntaxException": duckdb.SyntaxException,
    }


def _unregister_duckdb_table_safely(conn: Any, table_name: str) -> None:
    """Safely unregister a DuckDB table with proper error handling and logging.

    Args:
        conn: DuckDB connection instance
        table_name: Name of table to unregister

    This helper ensures that table unregistration failures are logged but don't
        interrupt overall cleanup process. Partial cleanup failures are visible
        in logs rather than being silently swallowed.
    """
    try:
        conn.unregister(table_name)
    except (_DUCKDB_EXCEPTIONS.get("CatalogException"), _DUCKDB_EXCEPTIONS.get("ConnectionException")) as e:
        # Log the failure but don't raise - cleanup should continue
        logger.warning("Failed to unregister DuckDB table '%s': %s", table_name, e)
