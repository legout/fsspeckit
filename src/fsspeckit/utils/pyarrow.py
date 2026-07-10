"""PyArrow utilities façade.

DEPRECATED: This module exists only for backwards compatibility.
New code should import from fsspeckit.datasets.schema.
"""

# Re-export from canonical locations
from fsspeckit.datasets.schema import (
    cast_schema,
    convert_large_types_to_normal,
    dominant_timezone_per_column,
    opt_dtype,
    remove_empty_columns,
    standardize_schema_timezones,
    standardize_schema_timezones_by_majority,
    unify_schemas,
)

opt_dtype_pa = opt_dtype

__all__ = [
    "cast_schema",
    "convert_large_types_to_normal",
    "dominant_timezone_per_column",
    "opt_dtype_pa",
    "remove_empty_columns",
    "standardize_schema_timezones",
    "standardize_schema_timezones_by_majority",
    "unify_schemas",
]
