"""PyArrow utilities façade.

DEPRECATED: This module exists only for backwards compatibility.
New code should import from fsspeckit.datasets.pyarrow or fsspeckit.common.datetime.
"""

# Re-export from canonical locations
from fsspeckit.datasets.pyarrow import (
    cast_schema,
    convert_large_types_to_normal,
    dominant_timezone_per_column,
    standardize_schema_timezones,
    standardize_schema_timezones_by_majority,
    unify_schemas,
)
from fsspeckit.datasets.pyarrow import (
    opt_dtype as opt_dtype_pa,
)

__all__ = [
    "cast_schema",
    "convert_large_types_to_normal",
    "dominant_timezone_per_column",
    "opt_dtype_pa",
    "standardize_schema_timezones",
    "standardize_schema_timezones_by_majority",
    "unify_schemas",
]
