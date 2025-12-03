"""DuckDB cleanup helpers for standardized error handling.

This module provides helper functions for safely unregistering DuckDB tables
with proper error handling and logging, following standardized
error handling patterns defined in change proposal.
"""

from __future__ import annotations

from typing import Any
from fsspeckit.common.logging import get_logger


def _unregister_duckdb_table_safely(conn: Any, table_name: str) -> None:
    """Safely unregister a DuckDB table with proper error handling and logging.

    Args:
        conn: DuckDB connection instance
        table_name: Name of table to unregister

    This helper ensures that table unregistration failures are logged but don't
        interrupt overall cleanup process. Partial cleanup failures are visible
        in logs rather than being silently swallowed.
    """
    logger = get_logger(__name__)
    try:
        conn.unregister(table_name)
    except Exception as e:
        # Log the failure but don't raise - cleanup should continue
        logger.warning(f"Failed to unregister DuckDB table '{table_name}': {e}")
