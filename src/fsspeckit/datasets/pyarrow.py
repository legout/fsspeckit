"""Re-export module for backward compatibility.

This module has been decomposed into focused submodules:
- pyarrow_schema: Schema unification, type inference, and optimization
- pyarrow_dataset: Dataset merge and maintenance operations

All public APIs are re-exported here to maintain backward compatibility.
New code should import directly from the submodules for better organization.
"""

# Re-export schema utilities
from fsspeckit.datasets.pyarrow_schema import (
    cast_schema,
    convert_large_types_to_normal,
    opt_dtype,
    remove_empty_columns,
    unify_schemas,
)

# Re-export dataset operations
from fsspeckit.datasets.pyarrow_dataset import (
    collect_dataset_stats_pyarrow,
    compact_parquet_dataset_pyarrow,
    merge_parquet_dataset_pyarrow,
    optimize_parquet_dataset_pyarrow,
)

__all__ = [
    # Schema utilities
    "cast_schema",
    "collect_dataset_stats_pyarrow",
    "compact_parquet_dataset_pyarrow",
    "convert_large_types_to_normal",
    "merge_parquet_dataset_pyarrow",
    "opt_dtype",
    "optimize_parquet_dataset_pyarrow",
    "unify_schemas",
]
