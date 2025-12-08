"""Dataset-level operations for fsspeckit.

This package contains dataset-specific functionality including:
- DuckDB parquet handlers for high-performance dataset operations
- PyArrow handlers for dataset I/O operations
- PyArrow utilities for schema management and type conversion
- Dataset merging and optimization tools
"""

from .duckdb import DuckDBParquetHandler, MergeStrategy
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
    # DuckDB handlers
    "DuckDBParquetHandler",
    "MergeStrategy",
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