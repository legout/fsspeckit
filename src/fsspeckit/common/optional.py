"""Optional dependency management utilities.

This module provides utilities for managing optional dependencies in fsspeckit,
implementing lazy loading patterns that allow core functionality to work without
requiring all optional dependencies to be installed.
"""

import importlib.util
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl
    import pandas as pd
    import pyarrow as pa
    import duckdb
    import sqlglot
    import orjson

# Availability flags - check if optional packages are installed
_POLARS_AVAILABLE = importlib.util.find_spec("polars") is not None
_PANDAS_AVAILABLE = importlib.util.find_spec("pandas") is not None
_PYARROW_AVAILABLE = importlib.util.find_spec("pyarrow") is not None
_DUCKDB_AVAILABLE = importlib.util.find_spec("duckdb") is not None
_SQLGLOT_AVAILABLE = importlib.util.find_spec("sqlglot") is not None
_ORJSON_AVAILABLE = importlib.util.find_spec("orjson") is not None
_JOBLIB_AVAILABLE = importlib.util.find_spec("joblib") is not None

# Cached imports
_polars_module = None
_pandas_module = None
_pyarrow_module = None
_duckdb_module = None
_sqlglot_module = None
_orjson_module = None
_joblib_module = None


def _get_install_extra(package_name: str) -> str:
    """Get the pip install extra for a given package."""
    extras_map = {
        "polars": "datasets",
        "pandas": "datasets",
        "pyarrow": "datasets",
        "duckdb": "sql",
        "sqlglot": "sql",
        "orjson": "sql",
        "joblib": "datasets",
    }
    return extras_map.get(package_name, "full")


def _import_polars() -> Any:
    """Import polars with proper error handling.

    Returns:
        polars module

    Raises:
        ImportError: If polars is not installed
    """
    global _polars_module

    if not _POLARS_AVAILABLE:
        raise ImportError(
            "polars is required for this function. "
            "Install with: pip install fsspeckit[datasets]"
        )

    if _polars_module is None:
        import polars as pl

        _polars_module = pl

    return _polars_module


def _import_pandas() -> Any:
    """Import pandas with proper error handling.

    Returns:
        pandas module

    Raises:
        ImportError: If pandas is not installed
    """
    global _pandas_module

    if not _PANDAS_AVAILABLE:
        raise ImportError(
            "pandas is required for this function. "
            "Install with: pip install fsspeckit[datasets]"
        )

    if _pandas_module is None:
        import pandas as pd

        _pandas_module = pd

    return _pandas_module


def _import_pyarrow() -> Any:
    """Import pyarrow with proper error handling.

    Returns:
        pyarrow module

    Raises:
        ImportError: If pyarrow is not installed
    """
    global _pyarrow_module

    if not _PYARROW_AVAILABLE:
        raise ImportError(
            "pyarrow is required for this function. "
            "Install with: pip install fsspeckit[datasets]"
        )

    if _pyarrow_module is None:
        import pyarrow as pa

        _pyarrow_module = pa

    return _pyarrow_module


def _import_pyarrow_parquet() -> Any:
    """Import pyarrow.parquet with proper error handling.

    Returns:
        pyarrow.parquet module

    Raises:
        ImportError: If pyarrow is not installed
    """
    if not _PYARROW_AVAILABLE:
        raise ImportError(
            "pyarrow is required for this function. "
            "Install with: pip install fsspeckit[datasets]"
        )

    import pyarrow.parquet as pq

    return pq


def _import_duckdb() -> Any:
    """Import duckdb with proper error handling.

    Returns:
        duckdb module

    Raises:
        ImportError: If duckdb is not installed
    """
    global _duckdb_module

    if not _DUCKDB_AVAILABLE:
        raise ImportError(
            "duckdb is required for this function. "
            "Install with: pip install fsspeckit[sql]"
        )

    if _duckdb_module is None:
        import duckdb

        _duckdb_module = duckdb

    return _duckdb_module


def _import_sqlglot() -> Any:
    """Import sqlglot with proper error handling.

    Returns:
        sqlglot module

    Raises:
        ImportError: If sqlglot is not installed
    """
    global _sqlglot_module

    if not _SQLGLOT_AVAILABLE:
        raise ImportError(
            "sqlglot is required for this function. "
            "Install with: pip install fsspeckit[sql]"
        )

    if _sqlglot_module is None:
        import sqlglot

        _sqlglot_module = sqlglot

    return _sqlglot_module


def _import_orjson() -> Any:
    """Import orjson with proper error handling.

    Returns:
        orjson module

    Raises:
        ImportError: If orjson is not installed
    """
    global _orjson_module

    if not _ORJSON_AVAILABLE:
        raise ImportError(
            "orjson is required for this function. "
            "Install with: pip install fsspeckit[sql]"
        )

    if _orjson_module is None:
        import orjson

        _orjson_module = orjson

    return _orjson_module


def _import_joblib() -> Any:
    """Import joblib with proper error handling.

    Returns:
        joblib module

    Raises:
        ImportError: If joblib is not installed
    """
    global _joblib_module

    if not _JOBLIB_AVAILABLE:
        raise ImportError(
            "joblib is required for this function. "
            "Install with: pip install fsspeckit[datasets]"
        )

    if _joblib_module is None:
        import joblib

        _joblib_module = joblib

    return _joblib_module


def check_optional_dependency(
    package_name: str, feature_name: str | None = None
) -> None:
    """Check if an optional dependency is available and raise helpful error if not.

    Args:
        package_name: Name of the required package
        feature_name: Name of the feature that requires this package (optional)

    Raises:
        ImportError: If the package is not available
    """
    if not importlib.util.find_spec(package_name):
        extra = _get_install_extra(package_name)
        feature_msg = f" for {feature_name}" if feature_name else ""
        raise ImportError(
            f"{package_name} is required{feature_msg}. "
            f"Install with: pip install fsspeckit[{extra}]"
        )


# Export availability flags
__all__ = [
    "_POLARS_AVAILABLE",
    "_PANDAS_AVAILABLE",
    "_PYARROW_AVAILABLE",
    "_DUCKDB_AVAILABLE",
    "_SQLGLOT_AVAILABLE",
    "_ORJSON_AVAILABLE",
    "_JOBLIB_AVAILABLE",
    "_import_polars",
    "_import_pandas",
    "_import_pyarrow",
    "_import_pyarrow_parquet",
    "_import_duckdb",
    "_import_sqlglot",
    "_import_orjson",
    "_import_joblib",
    "check_optional_dependency",
]
