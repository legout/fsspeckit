"""Dataset-level operations for fsspeckit.

This package contains dataset-specific functionality including:
- DuckDB parquet handlers for high-performance dataset operations
- PyArrow handlers for dataset I/O operations
- PyArrow utilities for schema management and type conversion
- Dataset merging and optimization tools
"""

import warnings
from typing import Any

_DEPRECATED_IMPORTS = {
    "duckdb_dataset": ("fsspeckit.datasets.duckdb.dataset", None),
    "duckdb_connection": ("fsspeckit.datasets.duckdb.connection", "DuckDBConnection"),
    "duckdb_helpers": ("fsspeckit.datasets.duckdb.helpers", None),
    "_duckdb_helpers": ("fsspeckit.datasets.duckdb.helpers", None),
    "pyarrow_dataset": ("fsspeckit.datasets.pyarrow.dataset", None),
    "pyarrow_schema": ("fsspeckit.datasets.pyarrow.schema", None),
    "DuckDBParquetHandler": ("fsspeckit.datasets.duckdb.dataset", None),
    "DuckDBConnection": ("fsspeckit.datasets.duckdb.connection", "DuckDBConnection"),
    "DuckDBDatasetIO": ("fsspeckit.datasets.duckdb.dataset", None),
    "MergeStrategy": ("fsspeckit.core.merge", None),
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
from .pyarrow import (
    cast_schema,
    collect_dataset_stats_pyarrow,
    compact_parquet_dataset_pyarrow,
    convert_large_types_to_normal,
    optimize_parquet_dataset_pyarrow,
    opt_dtype as opt_dtype_pa,
    unify_schemas as unify_schemas_pa,
    # New handler classes
    PyarrowDatasetIO,
    PyarrowDatasetHandler,
)

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
    # PyArrow handlers
    "PyarrowDatasetIO",
    "PyarrowDatasetHandler",
    # PyArrow utilities
    "cast_schema",
    "collect_dataset_stats_pyarrow",
    "compact_parquet_dataset_pyarrow",
    "convert_large_types_to_normal",
    "optimize_parquet_dataset_pyarrow",
    "opt_dtype_pa",
    "unify_schemas_pa",
]
