def _unregister_duckdb_table(conn, table_name: str, logger) -> None:
    """Helper to safely unregister a DuckDB table with logging.

    Args:
        conn: DuckDB connection
        table_name: Name of the table to unregister
        logger: Logger instance for logging

    Returns:
        None
    """
    from fsspeckit.common.optional import _DUCKDB_AVAILABLE

    _DUCKDB_EXCEPTIONS = {}
    if _DUCKDB_AVAILABLE:
        import duckdb

        _DUCKDB_EXCEPTIONS = {
            "CatalogException": duckdb.CatalogException,
            "ConnectionException": duckdb.ConnectionException,
        }

    try:
        conn.unregister(table_name)
    except (_DUCKDB_EXCEPTIONS.get("CatalogException"), _DUCKDB_EXCEPTIONS.get("ConnectionException")) as e:
        logger.warning("Failed to unregister DuckDB table '%s': %s", table_name, e)


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
