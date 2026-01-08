"""DuckDB exception types with fallbacks for when DuckDB is not installed."""

from __future__ import annotations

from fsspeckit.common.optional import _DUCKDB_AVAILABLE


class _NeverRaisedException(Exception):
    """Sentinel exception that is never actually raised.

    Used as a fallback in except clauses when DuckDB is not installed.
    This allows code to be syntactically valid even without DuckDB.
    """

    pass


# Export exception types - real when DuckDB installed, fallback otherwise
if _DUCKDB_AVAILABLE:
    import duckdb

    DuckDBError: type[Exception] = duckdb.Error
    CatalogException: type[Exception] = duckdb.CatalogException
    ConnectionException: type[Exception] = duckdb.ConnectionException
    InvalidInputException: type[Exception] = duckdb.InvalidInputException
    IOException: type[Exception] = duckdb.IOException
    OutOfMemoryException: type[Exception] = duckdb.OutOfMemoryException
    ParserException: type[Exception] = duckdb.ParserException
    SyntaxException: type[Exception] = duckdb.SyntaxException
    OperationalError: type[Exception] = duckdb.OperationalError
else:
    # Fallback placeholders when DuckDB is not available.
    # We use _NeverRaisedException which will never match any real exception,
    # avoiding TypeError at runtime when used in 'except' clauses.
    DuckDBError = _NeverRaisedException  # type: ignore[misc,assignment]
    CatalogException = _NeverRaisedException  # type: ignore[misc,assignment]
    ConnectionException = _NeverRaisedException  # type: ignore[misc,assignment]
    InvalidInputException = _NeverRaisedException  # type: ignore[misc,assignment]
    IOException = _NeverRaisedException  # type: ignore[misc,assignment]
    OutOfMemoryException = _NeverRaisedException  # type: ignore[misc,assignment]
    ParserException = _NeverRaisedException  # type: ignore[misc,assignment]
    SyntaxException = _NeverRaisedException  # type: ignore[misc,assignment]
    OperationalError = _NeverRaisedException  # type: ignore[misc,assignment]
