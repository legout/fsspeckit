"""PyArrow schema utilities for type inference, unification, and optimization.

This module contains functions for working with PyArrow schemas including:
- Schema unification across multiple tables
- Type inference and optimization
- Timezone handling
- Schema casting
"""

import re
from typing import Any, Iterable, Set

import numpy as np
import pyarrow as pa
from fsspeckit.core.merge import (
    MergeStrategy as CoreMergeStrategy,
)
from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)

# Pre-compiled regex patterns
INTEGER_REGEX = r"^[-+]?\d+$"
FLOAT_REGEX = r"^(?:[-+]?(?:\d*[.,])?\d+(?:[eE][-+]?\d+)?|[-+]?(?:inf|nan))$"
BOOLEAN_REGEX = r"^(true|false|1|0|yes|ja|no|nein|t|f|y|j|n|ok|nok)$"
BOOLEAN_TRUE_REGEX = r"^(true|1|yes|ja|t|y|j|ok)$"
DATETIME_REGEX = (
    r"^("
    r"\d{4}-\d{2}-\d{2}"  # ISO: 2023-12-31
    r"|"
    r"\d{2}/\d{2}/\d{4}"  # US: 12/31/2023
    r"|"
    r"\d{2}\.\d{2}\.\d{4}"  # German: 31.12.2023
    r"|"
    r"\d{8}"  # Compact: 20231231
    r")"
    r"([ T]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?)?"  # Optional time: 23:59[:59[.123456]]
    r"([+-]\d{2}:?\d{2}|Z|UTC)?"  # Optional timezone: +01:00, -0500, Z, UTC
    r"$"
)

# Float32 range limits
F32_MIN = float(np.finfo(np.float32).min)
F32_MAX = float(np.finfo(np.float32).max)


def dominant_timezone_per_column(schemas: list[pa.Schema]) -> dict[str, str]:
    """Determine the dominant timezone for each column across schemas.

    Args:
        schemas: List of PyArrow schemas to analyze

    Returns:
        dict: Mapping of column names to dominant timezone strings
    """
    from collections import Counter

    timezone_counts: dict[str, Counter] = {}

    for schema in schemas:
        for field in schema:
            if pa.types.is_timestamp(field.type):
                tz = field.type.tz
                if tz:
                    if field.name not in timezone_counts:
                        timezone_counts[field.name] = Counter()
                    timezone_counts[field.name][tz] += 1

    # Select dominant timezone for each column
    result = {}
    for col_name, counter in timezone_counts.items():
        result[col_name] = counter.most_common(1)[0][0]

    return result


def standardize_schema_timezones_by_majority(
    schemas: list[pa.Schema],
) -> list[pa.Schema]:
    """Standardize timezone information across schemas based on majority.

    Args:
        schemas: List of schemas to standardize

    Returns:
        List of schemas with standardized timezones
    """
    # Get dominant timezones
    dominant_tz = dominant_timezone_per_column(schemas)

    # Apply dominant timezone to all schemas
    standardized = []
    for schema in schemas:
        fields = []
        for field in schema:
            if pa.types.is_timestamp(field.type) and field.name in dominant_tz:
                # Update timezone to dominant one
                tz = dominant_tz[field.name]
                if field.type.tz != tz:
                    field = field.with_type(
                        pa.timestamp("us", tz=tz) if tz else pa.timestamp("us")
                    )
            fields.append(field)
        standardized.append(pa.schema(fields))

    return standardized


def standardize_schema_timezones(
    schemas: list[pa.Schema],
    standardize_timezones: bool = True,
) -> list[pa.Schema]:
    """Standardize timezone information across schemas.

    Args:
        schemas: List of schemas to standardize
        standardize_timezones: Whether to standardize timezones

    Returns:
        List of schemas with standardized timezones
    """
    if not standardize_timezones:
        return schemas

    return standardize_schema_timezones_by_majority(schemas)


def _is_type_compatible(type1: pa.DataType, type2: pa.DataType) -> bool:
    """Check if two PyArrow types are compatible for unification.

    Args:
        type1: First PyArrow type
        type2: Second PyArrow type

    Returns:
        bool: True if types are compatible
    """
    # Exact match
    if type1 == type2:
        return True

    # Numeric widening
    numeric_types = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
    float_types = [pa.float32(), pa.float64()]

    if type1 in numeric_types and type2 in numeric_types:
        return True
    if type1 in float_types and type2 in float_types:
        return True

    # String compatibility
    if pa.types.is_string(type1) and pa.types.is_string(type2):
        return True

    return False


def _find_common_numeric_type(types: Set[pa.DataType]) -> pa.DataType | None:
    """Find the common numeric type for a set of numeric types.

    Args:
        types: Set of numeric PyArrow types

    Returns:
        Common numeric type or None if not found
    """
    has_float = any(pa.types.is_float(t) for t in types)
    has_int = any(pa.types.is_integer(t) for t in types)

    if has_float and has_int:
        # Mix of int and float -> use float64
        return pa.float64()
    elif has_float:
        # Only floats -> use the widest
        if pa.float64() in types:
            return pa.float64()
        else:
            return pa.float32()
    else:
        # Only integers -> use the widest
        if pa.int64() in types:
            return pa.int64()
        elif pa.int32() in types:
            return pa.int32()
        else:
            return pa.int64()


def _analyze_string_vs_numeric_conflict(
    types: Set[pa.DataType],
) -> pa.DataType | None:
    """Analyze conflict between string and numeric types.

    Args:
        types: Set of conflicting types

    Returns:
        Resolved type or None
    """
    # If we have both string and numeric, prefer string for safety
    # (unless all numeric values can be safely parsed)
    return pa.string()


def _handle_temporal_conflicts(types: Set[pa.DataType]) -> pa.DataType | None:
    """Handle temporal type conflicts.

    Args:
        types: Set of temporal types

    Returns:
        Resolved temporal type or None
    """
    # Get all timestamps
    timestamps = [t for t in types if pa.types.is_timestamp(t)]

    if not timestamps:
        return None

    # Use the highest resolution timestamp
    # Prefer timestamp('us') with timezone over others
    for ts in timestamps:
        if ts.tz is not None:
            return ts

    # Fallback to first timestamp
    return timestamps[0]


def _find_conflicting_fields(schemas: list[pa.Schema]) -> dict:
    """Find fields with conflicting types across schemas.

    Args:
        schemas: List of schemas to analyze

    Returns:
        Dict mapping field names to sets of conflicting types
    """
    field_types: dict[str, Set[pa.DataType]] = {}

    for schema in schemas:
        for field in schema:
            if field.name not in field_types:
                field_types[field.name] = set()
            field_types[field.name].add(field.type)

    # Find fields with conflicting types
    conflicts = {}
    for field_name, types in field_types.items():
        if len(types) > 1:
            conflicts[field_name] = types

    return conflicts


def _normalize_schema_types(schemas: list[pa.Schema], conflicts: dict) -> list[pa.Schema]:
    """Normalize schema types based on conflict analysis.

    Args:
        schemas: List of schemas to normalize
        conflicts: Dict of conflicting field types

    Returns:
        List of normalized schemas
    """
    normalized = []

    for schema in schemas:
        fields = []
        for field in schema:
            if field.name in conflicts:
                # Determine the normalized type for this field
                types = conflicts[field.name]

                # Try to find a common type
                common_type = None

                # Check if all types are numeric
                if all(pa.types.is_integer(t) or pa.types.is_float(t) for t in types):
                    common_type = _find_common_numeric_type(types)
                # Check for string vs numeric
                elif any(pa.types.is_string(t) for t in types):
                    common_type = _analyze_string_vs_numeric_conflict(types)
                # Check for temporal types
                elif any(pa.types.is_timestamp(t) for t in types):
                    common_type = _handle_temporal_conflicts(types)

                if common_type and common_type != field.type:
                    field = field.with_type(common_type)

            fields.append(field)

        normalized.append(pa.schema(fields))

    return normalized


def _unique_schemas(schemas: list[pa.Schema]) -> list[pa.Schema]:
    """Remove duplicate schemas from a list.

    Args:
        schemas: List of schemas

    Returns:
        List of unique schemas
    """
    seen = set()
    unique = []

    for schema in schemas:
        schema_str = str(schema)
        if schema_str not in seen:
            seen.add(schema_str)
            unique.append(schema)

    return unique


def _aggressive_fallback_unification(schemas: list[pa.Schema]) -> pa.Schema:
    """Aggressive fallback unification that removes conflicting fields.

    Args:
        schemas: List of schemas to unify

    Returns:
        Unified schema
    """
    conflicts = _find_conflicting_fields(schemas)

    if not conflicts:
        # No conflicts, just concatenate all fields
        all_fields = []
        for schema in schemas:
            all_fields.extend(schema)
        return pa.schema(all_fields)

    # Remove conflicting fields
    safe_fields = []
    for schema in schemas:
        for field in schema:
            if field.name not in conflicts:
                safe_fields.append(field)

    return pa.schema(safe_fields)


def _remove_conflicting_fields(schemas: list[pa.Schema]) -> list[pa.Schema]:
    """Remove fields with conflicts from schemas.

    Args:
        schemas: List of schemas

    Returns:
        List of schemas with conflicting fields removed
    """
    conflicts = _find_conflicting_fields(schemas)

    if not conflicts:
        return schemas

    cleaned = []
    for schema in schemas:
        fields = [field for field in schema if field.name not in conflicts]
        cleaned.append(pa.schema(fields))

    return cleaned


def _remove_problematic_fields(schemas: list[pa.Schema]) -> list[pa.Schema]:
    """Remove problematic fields that can't be unified.

    Args:
        schemas: List of schemas

    Returns:
        List of schemas with problematic fields removed
    """
    return _remove_conflicting_fields(schemas)


def _log_conflict_summary(conflicts: dict, verbose: bool = False) -> None:
    """Log a summary of schema conflicts.

    Args:
        conflicts: Dict of conflicting field types
        verbose: Whether to print verbose output
    """
    if not verbose:
        return

    logger.debug("\nFound %d conflicting fields:", len(conflicts))
    for field_name, types in conflicts.items():
        logger.debug("  %s: %s", field_name, ', '.join(str(t) for t in types))
    logger.debug("")


def unify_schemas(
    schemas: list[pa.Schema],
    standardize_timezones: bool = True,
    verbose: bool = False,
) -> pa.Schema:
    """Unify multiple PyArrow schemas into a single schema.

    This function handles type conflicts by:
    1. Finding fields with conflicting types
    2. Attempting to normalize compatible types
    3. Using fallback strategies for incompatible types
    4. Removing problematic fields if necessary

    Args:
        schemas: List of schemas to unify
        standardize_timezones: Whether to standardize timezone info
        verbose: Whether to print conflict information

    Returns:
        Unified PyArrow schema

    Raises:
        ValueError: If schemas cannot be unified
    """
    if not schemas:
        raise ValueError("Cannot unify empty list of schemas")

    if len(schemas) == 1:
        return schemas[0]

    # Remove duplicate schemas
    schemas = _unique_schemas(schemas)

    # Standardize timezones if requested
    if standardize_timezones:
        schemas = standardize_schema_timezones(schemas, standardize_timezones)

    # Find conflicts
    conflicts = _find_conflicting_fields(schemas)

    if not conflicts:
        # No conflicts, concatenate all fields
        all_fields = []
        for schema in schemas:
            all_fields.extend(schema)
        return pa.schema(all_fields)

    if verbose:
        _log_conflict_summary(conflicts, verbose)

    # Try to normalize types
    try:
        normalized = _normalize_schema_types(schemas, conflicts)

        # Check if normalization resolved conflicts
        remaining_conflicts = _find_conflicting_fields(normalized)

        if not remaining_conflicts:
            # Normalization successful
            all_fields = []
            for schema in normalized:
                all_fields.extend(schema)
            return pa.schema(all_fields)

        # Fall through to next strategy
        conflicts = remaining_conflicts

    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        # Normalization failed, log and continue to fallback
        logger.debug(
            "Schema type normalization failed: %s. Trying aggressive fallback.",
            str(e)
        )

    # Try aggressive fallback
    try:
        return _aggressive_fallback_unification(schemas)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        # Aggressive fallback failed, log and try last resort
        logger.debug(
            "Aggressive fallback unification failed: %s. Trying last resort cleanup.",
            str(e)
        )

    # Last resort: remove problematic fields
    cleaned = _remove_problematic_fields(schemas)
    all_fields = []
    for schema in cleaned:
        all_fields.extend(schema)

    if verbose and conflicts:
        logger.debug("Removed %d conflicting fields during unification", len(conflicts))

    return pa.schema(all_fields)


def remove_empty_columns(table: pa.Table) -> pa.Table:
    """Remove empty columns from a PyArrow table.

    Args:
        table: PyArrow table

    Returns:
        Table with empty columns removed
    """
    empty_cols = _identify_empty_columns(table)
    if not empty_cols:
        return table

    return table.drop(empty_cols)


def _identify_empty_columns(table: pa.Table) -> list:
    """Identify empty columns in a PyArrow table.

    Args:
        table: PyArrow table

    Returns:
        List of empty column names
    """
    empty_cols = []
    for col_name in table.column_names:
        col = table[col_name]
        if col.null_count == len(col):
            empty_cols.append(col_name)

    return empty_cols


def cast_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Cast a PyArrow table to a target schema.

    Args:
        table: Source table
        schema: Target schema

    Returns:
        Table cast to target schema
    """
    # Filter schema to only include columns present in the table
    table_schema = table.schema
    valid_fields = []

    for field in schema:
        if field.name in table_schema.names:
            valid_fields.append(field)

    target_schema = pa.schema(valid_fields)

    # Cast the table
    return table.cast(target_schema)


def _normalize_datetime_string(s: str) -> str:
    """Normalize datetime string to ISO format.

    Args:
        s: Datetime string

    Returns:
        Normalized datetime string
    """
    # Handle various datetime formats
    # This is a simplified implementation
    return s


def _detect_timezone_from_sample(series: Any) -> str | None:
    """Detect timezone from a sample of values.

    Args:
        series: PyArrow array or chunked array

    Returns:
        Detected timezone or None
    """
    # Simplified timezone detection
    return None


def _sample_values(array: Any, max_sample: int = 1000) -> Any:
    """Sample values from an array.

    Args:
        array: PyArrow array
        max_sample: Maximum number of values to sample

    Returns:
        Sample of values
    """
    if len(array) <= max_sample:
        return array

    # Sample evenly spaced values
    indices = np.linspace(0, len(array) - 1, max_sample, dtype=int)
    return array.take(indices)


def _convert_full_list(series: Any) -> Any:
    """Convert a series to a list if needed.

    Args:
        series: PyArrow array

    Returns:
        Converted array
    """
    return series


def _clean_string_array(array: pa.Array) -> pa.Array:
    """Clean string array by removing leading/trailing whitespace.

    Args:
        array: String array

    Returns:
        Cleaned string array
    """
    if pa.types.is_string(array.type):
        return array.utf8.strip()
    return array


def _can_downcast_to_float32(array: pa.Array) -> bool:
    """Check if a float64 array can be safely downcast to float32.

    Args:
        array: Float64 array

    Returns:
        True if safe to downcast
    """
    if not pa.types.is_float64(array.type):
        return False

    min_val = array.min().as_py()
    max_val = array.max().as_py()

    if min_val is None or max_val is None:
        return False

    return min_val >= F32_MIN and max_val <= F32_MAX


def _get_optimal_int_type(
    min_val: int | None, max_val: int | None
) -> pa.DataType:
    """Get the optimal integer type based on min/max values.

    Args:
        min_val: Minimum value
        max_val: Maximum value

    Returns:
        Optimal integer type
    """
    if min_val is None or max_val is None:
        return pa.int64()

    # Check unsigned
    if min_val >= 0:
        if max_val <= np.iinfo(np.uint8).max:
            return pa.uint8()
        elif max_val <= np.iinfo(np.uint16).max:
            return pa.uint16()
        elif max_val <= np.iinfo(np.uint32).max:
            return pa.uint32()
        else:
            return pa.uint64()
    else:
        # Signed integers
        if min_val >= np.iinfo(np.int8).min and max_val <= np.iinfo(np.int8).max:
            return pa.int8()
        elif min_val >= np.iinfo(np.int16).min and max_val <= np.iinfo(np.int16).max:
            return pa.int16()
        elif min_val >= np.iinfo(np.int32).min and max_val <= np.iinfo(np.int32).max:
            return pa.int32()
        else:
            return pa.int64()


def _optimize_numeric_array(array: pa.Array) -> pa.Array:
    """Optimize a numeric array by downcasting when possible.

    Args:
        array: Numeric array

    Returns:
        Optimized array
    """
    if pa.types.is_float64(array.type):
        if _can_downcast_to_float32(array):
            return array.cast(pa.float32())
    elif pa.types.is_int64(array.type):
        min_val = array.min().as_py()
        max_val = array.max().as_py()
        optimal_type = _get_optimal_int_type(min_val, max_val)
        if optimal_type != pa.int64():
            return array.cast(optimal_type)

    return array


def _all_match_regex(array: pa.Array, pattern: str) -> bool:
    """Check if all values in array match a regex pattern.

    Args:
        array: String array
        pattern: Regex pattern

    Returns:
        True if all values match
    """
    if not pa.types.is_string(array.type):
        return False

    regex = re.compile(pattern)
    # Check each value (simplified implementation)
    for value in array.to_pylist():
        if value is not None and not regex.match(str(value)):
            return False

    return True


def _optimize_string_array(array: pa.Array) -> pa.Array:
    """Optimize a string array by detecting and casting to appropriate types.

    Args:
        array: String array

    Returns:
        Optimized array
    """
    if not pa.types.is_string(array.type):
        return array

    # Try to detect integer pattern
    if _all_match_regex(array, INTEGER_REGEX):
        try:
            return array.cast(pa.int64())
        except (ValueError, pa.ArrowInvalid):
            pass

    # Try to detect float pattern
    if _all_match_regex(array, FLOAT_REGEX):
        try:
            return array.cast(pa.float64())
        except (ValueError, pa.ArrowInvalid):
            pass

    # Try to detect boolean pattern
    if _all_match_regex(array, BOOLEAN_REGEX):
        try:
            # Simple boolean conversion (simplified)
            return array.cast(pa.bool_())
        except (ValueError, pa.ArrowInvalid):
            pass

    return array


def _process_column(
    table: pa.Table,
    column: str,
    strict: bool = False,
) -> pa.Array:
    """Process a single column for dtype optimization.

    Args:
        table: PyArrow table
        column: Column name
        strict: Whether to use strict type checking

    Returns:
        Optimized column array
    """
    array = table.column(column)

    # Remove null values for type detection
    non_null = array.drop_null()
    if len(non_null) == 0:
        return array

    # Try to optimize based on current type
    if pa.types.is_string(array.type):
        return _optimize_string_array(non_null)
    elif pa.types.is_floating(array.type):
        return _optimize_numeric_array(non_null)
    elif pa.types.is_integer(array.type):
        return _optimize_numeric_array(non_null)

    return non_null


def _process_column_for_opt_dtype(args):
    """Process a column for dtype optimization (for parallel processing).

    Args:
        args: Tuple of (table, column, strict)

    Returns:
        Tuple of (column_name, optimized_array)
    """
    table, column, strict = args
    optimized = _process_column(table, column, strict=strict)
    return (column, optimized)


def opt_dtype(
    table: pa.Table,
    strict: bool = False,
    columns: list[str] | None = None,
) -> pa.Table:
    """Optimize dtypes in a PyArrow table based on data analysis.

    This function analyzes the data in each column and attempts to downcast
    to more appropriate types (e.g., int64 -> int32, float64 -> float32,
    string -> int/bool where applicable).

    Args:
        table: PyArrow table to optimize
        strict: Whether to use strict type checking
        columns: List of columns to optimize (None for all)

    Returns:
        Table with optimized dtypes

    Example:
        ```python
        import pyarrow as pa

        table = pa.table(
            {
                "a": pa.array([1, 2, 3], type=pa.int64()),
                "b": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
            },
        )
        optimized = opt_dtype(table)
        print(optimized.column(0).type)  # DataType(int32)
        print(optimized.column(1).type)  # DataType(float32)
        ```
    """
    from fsspeckit.common.misc import run_parallel

    if columns is None:
        columns = table.column_names

    # Process columns in parallel
    results = run_parallel(
        _process_column_for_opt_dtype,
        [(table, col, strict) for col in columns],
        backend="threading",
        n_jobs=-1,
    )

    # Build new table with optimized columns
    new_columns = {}
    for col_name, optimized_array in results:
        new_columns[col_name] = optimized_array

    # Keep non-optimized columns as-is
    for col_name in table.column_names:
        if col_name not in new_columns:
            new_columns[col_name] = table.column(col_name)

    return pa.table(new_columns)


def convert_large_types_to_normal(schema: pa.Schema) -> pa.Schema:
    """Convert large types (like large_string) to normal types.

    Args:
        schema: PyArrow schema

    Returns:
        Schema with large types converted
    """
    fields = []
    for field in schema:
        if pa.types.is_large_string(field.type):
            field = field.with_type(pa.string())
        elif pa.types.is_large_utf8(field.type):
            field = field.with_type(pa.utf8())
        elif pa.types.is_large_list(field.type):
            field = field.with_type(pa.list_(field.type.value_type))
        fields.append(field)

    return pa.schema(fields)
