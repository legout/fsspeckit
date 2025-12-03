def _unregister_duckdb_table(conn, table_name: str, logger) -> None:
    """Helper to safely unregister a DuckDB table with logging.

    Args:
        conn: DuckDB connection
        table_name: Name of the table to unregister
        logger: Logger instance for logging

    Returns:
        None
    """
    try:
        conn.unregister(table_name)
    except Exception as e:
        logger.warning(f"Failed to unregister DuckDB table '{table_name}': {e}")
    except Exception:
        logger.error(f"Unexpected error unregistering DuckDB table '{table_name}': {e}")


def _cleanup_duckdb_tables(conn, table_names: list[str], logger) -> None:
    """Helper to safely unregister multiple DuckDB tables with logging.

    Args:
        conn: DuckDB connection
        table_names: List of table names to unregister
        logger: Logger instance for logging

    Returns:
        None
    """
    for table_name in table_names:
        _unregister_duckdb_table(conn, table_name, logger)
