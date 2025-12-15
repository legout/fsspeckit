"""PyArrow dataset integration for fsspeckit.

This package contains focused submodules for PyArrow functionality:
- dataset: Dataset merge and maintenance operations
- schema: Schema unification, type inference, and optimization
- io: PyarrowDatasetIO class for dataset operations

All public APIs are re-exported here for convenient access.
"""

# Re-export schema utilities from common.schema (canonical location)
from fsspeckit.common.schema import (
    cast_schema,
    convert_large_types_to_normal,
    dominant_timezone_per_column,
    opt_dtype,
    remove_empty_columns,
    standardize_schema_timezones,
    standardize_schema_timezones_by_majority,
    unify_schemas,
)

# Re-export dataset write functions
from fsspeckit.core.ext.dataset import (
    deduplicate_dataset,
    insert_dataset,
    pyarrow_dataset,
    pyarrow_parquet_dataset,
    update_dataset,
    upsert_dataset,
    write_pyarrow_dataset,
)

# Re-export dataset operations
from .dataset import (
    collect_dataset_stats_pyarrow,
    compact_parquet_dataset_pyarrow,
    merge_parquet_dataset_pyarrow,
    optimize_parquet_dataset_pyarrow,
)

# Re-export dataset I/O classes
from .io import (
    PyarrowDatasetHandler,
    PyarrowDatasetIO,
)

__all__ = [
    # Schema utilities
    "cast_schema",
    "collect_dataset_stats_pyarrow",
    "compact_parquet_dataset_pyarrow",
    "convert_large_types_to_normal",
    "dominant_timezone_per_column",
    "merge_parquet_dataset_pyarrow",
    "opt_dtype",
    "optimize_parquet_dataset_pyarrow",
    "pyarrow_dataset",
    "pyarrow_parquet_dataset",
    "remove_empty_columns",
    "standardize_schema_timezones",
    "standardize_schema_timezones_by_majority",
    "unify_schemas",
    "write_pyarrow_dataset",
    "deduplicate_dataset",
    "insert_dataset",
    "update_dataset",
    "upsert_dataset",
    # Dataset I/O classes
    "PyarrowDatasetIO",
    "PyarrowDatasetHandler",
]
