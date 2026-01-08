"""PyArrow schema utilities for type inference, unification, and optimization.

This module has been refactored to re-export all schema utilities from the canonical
location in fsspeckit.common.schema. This ensures consistency across all backends
and eliminates code duplication.

All public APIs are re-exported from fsspeckit.common.schema.
"""

# Re-export all schema utilities from common.schema (canonical location)
from fsspeckit.common.schema import (
    # Schema unification and casting
    cast_schema,
    unify_schemas,
    remove_empty_columns,

    # Type optimization
    opt_dtype,

    # Type conversion
    convert_large_types_to_normal,

    # Timezone handling
    dominant_timezone_per_column,
    standardize_schema_timezones_by_majority,
    standardize_schema_timezones,
)

__all__ = [
    # Schema utilities
    "cast_schema",
    "unify_schemas",
    "remove_empty_columns",
    "opt_dtype",
    "convert_large_types_to_normal",
    "dominant_timezone_per_column",
    "standardize_schema_timezones_by_majority",
    "standardize_schema_timezones",
]
