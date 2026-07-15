"""Dataset-level operations for fsspeckit.

This package contains dataset-specific functionality including:
- DuckDB parquet handlers for high-performance dataset operations
- PyArrow handlers for dataset I/O operations
- PyArrow utilities for schema management and type conversion
- Dataset merging and optimization tools
"""

import warnings
from typing import Any

from .exceptions import (
    DatasetError,
    DatasetFileError,
    DatasetMergeError,
    DatasetOperationError,
    DatasetPathError,
    DatasetSchemaError,
    DatasetValidationError,
)
from .path_utils import normalize_path, validate_dataset_path
from .polars import (
    opt_dtype as opt_dtype_pl,
    pl,
)
from .pyarrow import (
    collect_dataset_stats_pyarrow,
    # Dataset I/O class
    PyarrowDatasetIO,
)
from .schema import (
    cast_schema,
    convert_large_types_to_normal,
    opt_dtype as opt_dtype_pa,
    unify_schemas as unify_schemas_pa,
)

_DEPRECATED_IMPORTS = {
    "duckdb_dataset": ("fsspeckit.datasets.duckdb.dataset", None),
    "duckdb_connection": ("fsspeckit.datasets.duckdb.connection", "DuckDBConnection"),
    "duckdb_helpers": ("fsspeckit.datasets.duckdb.helpers", None),
    "_duckdb_helpers": ("fsspeckit.datasets.duckdb.helpers", None),
    "pyarrow_dataset": ("fsspeckit.datasets.pyarrow.dataset", None),
    "DuckDBParquetHandler": (
        "fsspeckit.datasets.duckdb.dataset",
        "DuckDBDatasetIO",
    ),
    "DuckDBConnection": ("fsspeckit.datasets.duckdb.connection", "DuckDBConnection"),
    "DuckDBDatasetIO": ("fsspeckit.datasets.duckdb.dataset", "DuckDBDatasetIO"),
    "MergeStrategy": ("fsspeckit.core.merge", "MergeStrategy"),
    "PyarrowDatasetHandler": ("fsspeckit.datasets.pyarrow.io", "PyarrowDatasetIO"),
}


def __getattr__(name: str) -> Any:
    if name in _DEPRECATED_IMPORTS:
        module_path, attr = _DEPRECATED_IMPORTS[name]
        warnings.warn(
            f"Importing '{name}' from fsspeckit.datasets is deprecated. "
            f"Use 'from {module_path} import ...' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        import importlib

        module = importlib.import_module(module_path)
        return getattr(module, attr) if attr else module
    raise AttributeError(f"module 'fsspeckit.datasets' has no attribute '{name}'")


__all__ = [
    # Exceptions
    "DatasetError",
    "DatasetFileError",
    "DatasetMergeError",
    "DatasetOperationError",
    "DatasetPathError",
    "DatasetSchemaError",
    "DatasetValidationError",
    # Path utilities
    "normalize_path",
    "validate_dataset_path",
    # Polars utilities
    "opt_dtype_pl",
    "pl",
    # PyArrow handlers
    "PyarrowDatasetIO",
    # PyArrow utilities
    "cast_schema",
    "collect_dataset_stats_pyarrow",
    "convert_large_types_to_normal",
    "opt_dtype_pa",
    "unify_schemas_pa",
]
