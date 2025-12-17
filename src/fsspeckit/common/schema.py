"""
Shared schema utilities for fsspeckit.

This module provides canonical implementations for schema compatibility,
unification, timezone handling, and type optimization across all backends.
It consolidates schema-related logic that was previously scattered across
dataset-specific modules.

Key responsibilities:
1. Schema unification with intelligent conflict resolution
2. Timezone standardization and detection
3. Large type conversion to standard types
4. Schema casting and validation
5. Data type optimization
6. Empty column handling

Architecture:
- Functions are designed to work with PyArrow schemas and tables
- All operations preserve metadata when possible
- Timezone handling supports multiple strategies (auto, explicit, removal)
- Type optimization includes safety checks and fallback strategies
- Schema unification uses multiple fallback strategies for maximum compatibility

Usage:
Backend implementations should delegate to this module rather than implementing
their own schema logic. This ensures consistent behavior across DuckDB,
PyArrow, and future backends.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds

from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)

SampleMethod = Literal["first", "random"]

# Pre-compiled regex patterns (identical to original)
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

NULL_LIKE_STRINGS = {
    "",
    "-",
    "None",
    "none",
    "NONE",
    "NaN",
    "Nan",
    "nan",
    "NAN",
    "N/A",
    "n/a",
    "Null",
    "NULL",
    "null",
}

_REGEX_CACHE: dict[str, re.Pattern] = {}


def convert_large_types_to_normal(schema: pa.Schema) -> pa.Schema:
    """
    Convert large types in a PyArrow schema to their standard types.

    Args:
        schema (pa.Schema): The PyArrow schema to convert.

    Returns:
        pa.Schema: A new PyArrow schema with large types converted to standard types.
    """
    # Define mapping of large types to standard types
    type_mapping = {
        pa.large_string(): pa.string(),
        pa.large_binary(): pa.binary(),
        pa.large_utf8(): pa.utf8(),
        pa.large_list(pa.null()): pa.list_(pa.null()),
        pa.large_list_view(pa.null()): pa.list_view(pa.null()),
    }
    # Convert fields
    new_fields = []
    for field in schema:
        field_type = field.type
        # Check if type exists in mapping
        if field_type in type_mapping:
            new_field = pa.field(
                name=field.name,
                type=type_mapping[field_type],
                nullable=field.nullable,
                metadata=field.metadata,
            )
            new_fields.append(new_field)
        # Handle large lists with nested types
        elif isinstance(field_type, pa.LargeListType):
            new_field = pa.field(
                name=field.name,
                type=pa.list_(
                    type_mapping[field_type.value_type]
                    if field_type.value_type in type_mapping
                    else field_type.value_type
                ),
                nullable=field.nullable,
                metadata=field.metadata,
            )
            new_fields.append(new_field)
        # Handle dictionary with large_string, large_utf8, or large_binary values
        elif isinstance(field_type, pa.DictionaryType):
            new_field = pa.field(
                name=field.name,
                type=pa.dictionary(
                    field_type.index_type,
                    type_mapping[field_type.value_type]
                    if field_type.value_type in type_mapping
                    else field_type.value_type,
                    field_type.ordered,
                ),
                metadata=field.metadata,
            )
            new_fields.append(new_field)
        else:
            new_fields.append(field)

    return pa.schema(new_fields)


def dominant_timezone_per_column(
    schemas: list[pa.Schema],
) -> dict[str, tuple[str | None, str | None]]:
    """
    For each timestamp column (by name) across all schemas, detect the most frequent timezone (including None).
    If None and a timezone are tied, prefer the timezone.
    Returns a dict: {column_name: dominant_timezone}
    """
    tz_counts: defaultdict[str, Counter[str | None]] = defaultdict(Counter)
    units: dict[str, str | None] = {}

    for schema in schemas:
        for field in schema:
            if pa.types.is_timestamp(field.type):
                tz = field.type.tz
                name = field.name
                tz_counts[name][tz] += 1
                # Track unit for each column (assume consistent)
                if name not in units:
                    units[name] = field.type.unit

    dominant = {}
    for name, counter in tz_counts.items():
        most_common = counter.most_common()
        if not most_common:
            continue
        top_count = most_common[0][1]
        # Find all with top_count
        top_tzs = [tz for tz, cnt in most_common if cnt == top_count]
        # If tie and one is not None, prefer not-None
        if len(top_tzs) > 1:
            # Sort top_tzs to ensure deterministic behavior (e.g. UTC vs America/New_York)
            # Filter None first
            non_none = [tz for tz in top_tzs if tz is not None]
            if non_none:
                non_none.sort()
                # If UTC is present, prefer it
                if "UTC" in non_none:
                    tz = "UTC"
                else:
                    tz = non_none[0]
            else:
                tz = None
        else:
            tz = most_common[0][0]
        dominant[name] = (units[name], tz)
    return dominant


def standardize_schema_timezones_by_majority(
    schemas: list[pa.Schema],
) -> pa.Schema:
    """
    For each timestamp column (by name) across all schemas, set the timezone to the most frequent (with tie-breaking).
    Returns a new list of schemas with updated timestamp timezones.
    """
    dom = dominant_timezone_per_column(schemas)
    if not schemas:
        return pa.schema([])

    seen: set[str] = set()
    fields: list[pa.Field] = []
    for schema in schemas:
        for field in schema:
            if field.name in seen:
                continue
            seen.add(field.name)
            if pa.types.is_timestamp(field.type) and field.name in dom:
                unit, tz = dom[field.name]
                fields.append(
                    pa.field(
                        field.name,
                        pa.timestamp(unit, tz),
                        field.nullable,
                        field.metadata,
                    )
                )
            else:
                fields.append(field)
    return pa.schema(fields, schemas[0].metadata)


def standardize_schema_timezones(
    schemas: pa.Schema | list[pa.Schema], timezone: str | None = None
) -> pa.Schema | list[pa.Schema]:
    """
    Standardize timezone info for all timestamp columns in a list of PyArrow schemas.

    Args:
        schemas (list of pa.Schema): List of PyArrow schemas.
        timezone (str or None): If None, remove timezone from all timestamp columns.
                                If str, set this timezone for all timestamp columns.
                                If "auto", use the most frequent timezone across schemas.

    Returns:
        list of pa.Schema: New schemas with standardized timezone info.
    """
    if isinstance(schemas, pa.Schema):
        single_input = True
        schema_list: list[pa.Schema] = [schemas]
    else:
        single_input = False
        schema_list = list(schemas)
    if timezone == "auto":
        majority_schema = standardize_schema_timezones_by_majority(schema_list)
        result_list = [majority_schema for _ in schema_list]
        return majority_schema if single_input else result_list
    new_schemas = []
    for schema in schema_list:
        fields = []
        for field in schema:
            if pa.types.is_timestamp(field.type):
                fields.append(
                    pa.field(
                        field.name,
                        pa.timestamp(field.type.unit, timezone),
                        field.nullable,
                        field.metadata,
                    )
                )
            else:
                fields.append(field)
        new_schemas.append(pa.schema(fields, schema.metadata))
    return new_schemas[0] if single_input else new_schemas


def cast_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """
    Cast a PyArrow table to a given schema, updating the schema to match the table's columns.

    Args:
        table (pa.Table): The PyArrow table to cast.
        schema (pa.Schema): The target schema to cast table to.

    Returns:
        pa.Table: A new PyArrow table with the specified schema.
    """
    # Append missing columns as nulls
    table_columns = set(table.schema.names)
    for field in schema:
        if field.name not in table_columns:
            table = table.append_column(
                field.name, pa.nulls(table.num_rows, type=field.type)
            )

    # Identify extra columns in table that are not in schema
    extra_fields = []
    schema_names = set(schema.names)
    for field in table.schema:
        if field.name not in schema_names:
            extra_fields.append(field)
            
    # Create target schema: requested schema + extra columns
    if extra_fields:
        target_schema = pa.schema(list(schema) + extra_fields, metadata=schema.metadata)
    else:
        target_schema = schema

    # Reorder table columns to match target schema
    table = table.select(target_schema.names)
    
    return table.cast(target_schema)


def remove_empty_columns(table: pa.Table) -> pa.Table:
    """Remove columns that are entirely empty from a PyArrow table.

    Args:
        table (pa.Table): The PyArrow table to process.

    Returns:
        pa.Table: A new PyArrow table with empty columns removed.
    """
    if table.num_rows == 0:
        return table

    empty_cols = []
    for col_name in table.column_names:
        column = table.column(col_name)
        if column.null_count == table.num_rows:
            empty_cols.append(col_name)

    if not empty_cols:
        return table
    return table.drop(empty_cols)


def _identify_empty_columns(table: pa.Table) -> list:
    """Identify columns that are entirely empty."""
    if table.num_rows == 0:
        return []

    empty_cols = []
    for col_name in table.column_names:
        column = table.column(col_name)
        if column.null_count == table.num_rows:
            empty_cols.append(col_name)

    return empty_cols


def _is_type_compatible(type1: pa.DataType, type2: pa.DataType) -> bool:
    """
    Check if two PyArrow types can be automatically promoted by pyarrow.unify_schemas.

    Returns True if types are compatible for automatic promotion, False if manual casting is needed.
    """
    # Null types are compatible with everything
    if pa.types.is_null(type1) or pa.types.is_null(type2):
        return True

    # Same types are always compatible
    if type1 == type2:
        return True

    # Numeric type compatibility
    # Integer types can be promoted within signed/unsigned categories and to floats
    if pa.types.is_integer(type1) and pa.types.is_integer(type2):
        # Both signed or both unsigned - can promote to larger type
        type1_signed = not pa.types.is_unsigned_integer(type1)
        type2_signed = not pa.types.is_unsigned_integer(type2)
        return type1_signed == type2_signed

    # Integer to float compatibility
    if (pa.types.is_integer(type1) and pa.types.is_floating(type2)) or (
        pa.types.is_floating(type1) and pa.types.is_integer(type2)
    ):
        return True

    # Float to float compatibility
    if pa.types.is_floating(type1) and pa.types.is_floating(type2):
        return True

    # Temporal type compatibility
    # Date types
    if pa.types.is_date(type1) and pa.types.is_date(type2):
        return True

    # Time types
    if pa.types.is_time(type1) and pa.types.is_time(type2):
        return True

    # Timestamp types (different precisions can be unified)
    if pa.types.is_timestamp(type1) and pa.types.is_timestamp(type2):
        return True

    # String vs Large String - compatible
    if (pa.types.is_string(type1) or pa.types.is_large_string(type1)) and (
        pa.types.is_string(type2) or pa.types.is_large_string(type2)
    ):
        return True

    # Binary vs Large Binary - compatible
    if (pa.types.is_binary(type1) or pa.types.is_large_binary(type1)) and (
        pa.types.is_binary(type2) or pa.types.is_large_binary(type2)
    ):
        return True

    # String vs binary types - these are incompatible
    if (pa.types.is_string(type1) or pa.types.is_large_string(type1)) and (
        pa.types.is_binary(type2) or pa.types.is_large_binary(type2)
    ):
        return False
    if (pa.types.is_binary(type1) or pa.types.is_large_binary(type1)) and (
        pa.types.is_string(type2) or pa.types.is_large_string(type2)
    ):
        return False

    # String/numeric types are incompatible
    if (pa.types.is_string(type1) or pa.types.is_large_string(type1)) and (
        pa.types.is_integer(type1) or pa.types.is_floating(type1)
    ):
        return False
    if (pa.types.is_string(type2) or pa.types.is_large_string(type2)) and (
        pa.types.is_integer(type2) or pa.types.is_floating(type2)
    ):
        return False

    # Struct types - only compatible if field names and types are compatible
    if isinstance(type1, pa.StructType) and isinstance(type2, pa.StructType):
        if len(type1) != len(type2):
            return False
        # Check if field names match
        if set(f.name for f in type1) != set(f.name for f in type2):
            return False
        # Check if corresponding field types are compatible
        for f1, f2 in zip(type1, type2):
            if f1.name != f2.name:
                return False
            if not _is_type_compatible(f1.type, f2.type):
                return False
        return True

    # List types - only compatible if element types are compatible
    if isinstance(type1, pa.ListType) and isinstance(type2, pa.ListType):
        return _is_type_compatible(type1.value_type, type2.value_type)

    # Dictionary types - generally incompatible unless identical
    if isinstance(type1, pa.DictionaryType) and isinstance(type2, pa.DictionaryType):
        return (
            type1.value_type == type2.value_type
            and type1.index_type == type2.index_type
        )

    # All other combinations are considered incompatible
    return False


def _find_common_numeric_type(types: set[pa.DataType]) -> pa.DataType | None:
    """
    Find the optimal common numeric type for a set of numeric types.

    Returns None if types are not numeric or cannot be unified.
    """
    if not types:
        return None

    # Check if ALL types are numeric
    if not all(pa.types.is_integer(t) or pa.types.is_floating(t) for t in types):
        return None

    # Filter only numeric types
    numeric_types = [
        t for t in types if pa.types.is_integer(t) or pa.types.is_floating(t)
    ]
    if not numeric_types:
        return None

    # Check for mixed signed/unsigned integers
    signed_ints = [
        t
        for t in numeric_types
        if pa.types.is_integer(t) and not pa.types.is_unsigned_integer(t)
    ]
    unsigned_ints = [t for t in numeric_types if pa.types.is_unsigned_integer(t)]
    floats = [t for t in numeric_types if pa.types.is_floating(t)]

    # If we have floats, promote to the largest float type
    if floats:
        if any(t == pa.float64() for t in floats):
            return pa.float64()
        return pa.float32()

    # If we have mixed signed and unsigned integers, must promote to float
    if signed_ints and unsigned_ints:
        # Find the largest integer to determine float precision needed
        all_ints = signed_ints + unsigned_ints
        bit_widths = []
        for t in all_ints:
            if t == pa.int8() or t == pa.uint8():
                bit_widths.append(8)
            elif t == pa.int16() or t == pa.uint16():
                bit_widths.append(16)
            elif t == pa.int32() or t == pa.uint32():
                bit_widths.append(32)
            elif t == pa.int64() or t == pa.uint64():
                bit_widths.append(64)

        max_width = max(bit_widths) if bit_widths else 32
        # Use float64 for 64-bit integers to preserve precision
        return pa.float64() if max_width >= 64 else pa.float32()

    # Only signed integers - find largest
    if signed_ints:
        if pa.int64() in signed_ints:
            return pa.int64()
        elif pa.int32() in signed_ints:
            return pa.int32()
        elif pa.int16() in signed_ints:
            return pa.int16()
        else:
            return pa.int8()

    # Only unsigned integers - find largest
    if unsigned_ints:
        if pa.uint64() in unsigned_ints:
            return pa.uint64()
        elif pa.uint32() in unsigned_ints:
            return pa.uint32()
        elif pa.uint16() in unsigned_ints:
            return pa.uint16()
        else:
            return pa.uint8()

    return None


def _analyze_string_vs_numeric_conflict(
    string_type: pa.DataType, numeric_type: pa.DataType
) -> pa.DataType:
    """
    Analyze string vs numeric type conflict and determine best conversion strategy.

    For now, defaults to string type as it's the safest option.
    In a more sophisticated implementation, this could analyze actual data content
    to make an informed decision.
    """
    # Default strategy: convert to string to preserve all information
    # This could be enhanced with data sampling to determine optimal conversion
    return pa.string()


def _handle_temporal_conflicts(types: set[pa.DataType]) -> pa.DataType | None:
    """
    Handle conflicts between temporal types (date, time, timestamp).
    """
    if not types:
        return None

    # Check if ALL types are temporal
    if not all(pa.types.is_temporal(t) for t in types):
        return None

    # Filter temporal types
    temporal_types = [t for t in types if pa.types.is_temporal(t)]
    if not temporal_types:
        return None

    # If we have timestamps, they take precedence
    timestamps = [t for t in temporal_types if pa.types.is_timestamp(t)]
    if timestamps:
        # Find the highest precision timestamp
        # For simplicity, use the first one - in practice might want to find highest precision
        return timestamps[0]

    # If we have times, they take precedence over dates
    times = [t for t in temporal_types if pa.types.is_time(t)]
    if times:
        # Use the higher precision time
        if any(t == pa.time64() for t in times):
            return pa.time64()
        return pa.time32()

    # Only dates remain
    dates = [t for t in temporal_types if pa.types.is_date(t)]
    if dates:
        # Use the higher precision date
        if any(t == pa.date64() for t in dates):
            return pa.date64()
        return pa.date32()

    return None


def _find_conflicting_fields(schemas):
    """Find fields with conflicting types across schemas and categorize them."""
    seen = defaultdict(set)
    for schema in schemas:
        for field in schema:
            seen[field.name].add(field.type)

    conflicts = {}
    for name, types in seen.items():
        if len(types) > 1:
            # Analyze the conflict
            conflicts[name] = {
                "types": types,
                "compatible": True,  # Assume compatible until proven otherwise
                "target_type": None,  # Will be determined by promotion logic
            }

    return conflicts


def _normalize_schema_types(schemas, conflicts, handle_incompatible=True):
    """Normalize schema types based on intelligent promotion rules."""
    # First, analyze all conflicts to determine target types
    promotions = {}

    for field_name, conflict_info in conflicts.items():
        types = conflict_info["types"]

        # Try to find a common type for compatible conflicts
        target_type = None

        # Check if all types are numeric and can be unified
        numeric_type = _find_common_numeric_type(types)
        if numeric_type is not None:
            target_type = numeric_type
            conflict_info["compatible"] = True
        # Check if all types are temporal and can be unified
        else:
            temporal_type = _handle_temporal_conflicts(types)
            if temporal_type is not None:
                target_type = temporal_type
                conflict_info["compatible"] = True
            else:
                # Check if any types are incompatible
                all_compatible = True
                type_list = list(types)
                for i in range(len(type_list)):
                    for j in range(i + 1, len(type_list)):
                        if not _is_type_compatible(type_list[i], type_list[j]):
                            all_compatible = False
                            break
                    if not all_compatible:
                        break

                conflict_info["compatible"] = all_compatible

                if all_compatible:
                    # Types are compatible but we don't have a specific rule
                    # Let PyArrow handle it automatically
                    target_type = None
                else:
                    # Types are incompatible - default to string for safety
                    if handle_incompatible:
                        target_type = pa.string()
                    else:
                        target_type = None

        conflict_info["target_type"] = target_type
        if target_type is not None:
            promotions[field_name] = target_type

    # Apply the promotions to schemas
    normalized = []
    for schema in schemas:
        fields = []
        for field in schema:
            tgt = promotions.get(field.name)
            fields.append(field if tgt is None else field.with_type(tgt))
        normalized.append(pa.schema(fields, metadata=schema.metadata))

    return normalized


def _unique_schemas(schemas: list[pa.Schema]) -> list[pa.Schema]:
    """Get unique schemas from a list of schemas."""
    seen = {}
    unique = []
    for schema in schemas:
        key = schema.serialize().to_pybytes()
        if key not in seen:
            seen[key] = schema
            unique.append(schema)
    return unique


def _aggressive_fallback_unification(schemas: list[pa.Schema]) -> pa.Schema:
    """
    Aggressive fallback strategy for difficult unification scenarios.
    Converts all conflicting fields to strings as a last resort.
    """
    conflicts = _find_conflicting_fields(schemas)
    if not conflicts:
        # No conflicts, try direct unification
        try:
            return pa.unify_schemas(schemas, promote_options="permissive")
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            pass

    # Convert all conflicting fields to strings
    for field_name, conflict_info in conflicts.items():
        conflict_info["target_type"] = pa.string()

    normalized_schemas = _normalize_schema_types(schemas, conflicts)
    try:
        return pa.unify_schemas(normalized_schemas, promote_options="permissive")
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        # If even this fails, return first normalized schema
        return normalized_schemas[0]


def _remove_conflicting_fields(schemas: list[pa.Schema]) -> pa.Schema:
    """
    Remove fields that have type conflicts between schemas.
    Keeps only fields that have the same type across all schemas.

    Args:
        schemas (list[pa.Schema]): List of schemas to process.

    Returns:
        pa.Schema: Schema with only non-conflicting fields.
    """
    if not schemas:
        return pa.schema([])

    # Find conflicts
    conflicts = _find_conflicting_fields(schemas)
    
    # Determine compatibility
    _normalize_schema_types(schemas, conflicts, handle_incompatible=False)
    
    # Identify incompatible fields
    conflicting_field_names = {
        name for name, info in conflicts.items()
        if not info["compatible"]
    }

    # Keep only non-conflicting fields from the first schema
    fields = []
    for field in schemas[0]:
        if field.name not in conflicting_field_names:
            fields.append(field)

    return pa.schema(fields)


def _remove_problematic_fields(schemas: list[pa.Schema]) -> pa.Schema:
    """
    Remove fields that cannot be unified across all schemas.
    This is a last resort when all other strategies fail.
    """
    if not schemas:
        return pa.schema([])

    # Find fields that exist in all schemas
    common_field_names = set(schemas[0].names)
    for schema in schemas[1:]:
        common_field_names &= set(schema.names)

    # Use fields from the first schema for common fields
    fields = []
    for field in schemas[0]:
        if field.name in common_field_names:
            fields.append(field)

    return pa.schema(fields)


def _log_conflict_summary(conflicts: dict, verbose: bool = False) -> None:
    """
    Log a summary of resolved conflicts for debugging purposes.
    """
    if not conflicts or not verbose:
        return

    logger.debug("Schema Unification Conflict Summary:")
    logger.debug("=" * 40)
    for field_name, conflict_info in conflicts.items():
        types_str = ", ".join(str(t) for t in conflict_info["types"])
        compatible = conflict_info["compatible"]
        target_type = conflict_info["target_type"]

        logger.debug("Field: %s", field_name)
        logger.debug("  Types: %s", types_str)
        logger.debug("  Compatible: %s", compatible)
        logger.debug("  Target Type: %s", target_type)
        logger.debug("")


def unify_schemas(
    schemas: list[pa.Schema],
    use_large_dtypes: bool = False,
    timezone: str | None = None,
    standardize_timezones: bool = True,
    verbose: bool = False,
    remove_conflicting_columns: bool = False,
) -> pa.Schema:
    """
    Unify a list of PyArrow schemas into a single schema using intelligent conflict resolution.

    Args:
        schemas (list[pa.Schema]): List of PyArrow schemas to unify.
        use_large_dtypes (bool): If True, keep large types like large_string.
        timezone (str | None): If specified, standardize all timestamp columns to this timezone.
            If "auto", use the most frequent timezone across schemas.
            If None, remove timezone from all timestamp columns.
        standardize_timezones (bool): If True, standardize all timestamp columns to most frequent timezone.
        verbose (bool): If True, print conflict resolution details for debugging.
        remove_conflicting_columns (bool): If True, allows removal of columns with type conflicts as a fallback
            strategy instead of converting them (e.g. to string). Defaults to False.

    Returns:
        pa.Schema: A unified PyArrow schema.

    Raises:
        ValueError: If no schemas are provided.
    """
    if not schemas:
        raise ValueError("At least one schema must be provided for unification")

    # Early exit for single schema
    unique_schemas = _unique_schemas(schemas)
    # Keep a copy of original unique schemas for timezone detection if needed
    original_unique_schemas = unique_schemas
    
    if len(unique_schemas) == 1:
        result_schema = unique_schemas[0]
        if standardize_timezones:
            target_tz = timezone if timezone is not None else "auto"
            result_schema = standardize_schema_timezones([result_schema], target_tz)[0]
        return (
            result_schema
            if use_large_dtypes
            else convert_large_types_to_normal(result_schema)
        )

    # Step 1: Find and resolve conflicts first
    conflicts = _find_conflicting_fields(unique_schemas)
    if conflicts and verbose:
        _log_conflict_summary(conflicts, verbose)

    if conflicts:
        # Pre-process incompatible columns if removal is requested
        if remove_conflicting_columns:
             # Determine compatibility status first
             _normalize_schema_types(unique_schemas, conflicts, handle_incompatible=False)
             
             incompatible_fields = {
                 name for name, info in conflicts.items() 
                 if not info["compatible"]
             }
             
             if incompatible_fields:
                 # Remove conflicting columns from all schemas
                 unique_schemas = [
                     pa.schema([f for f in s if f.name not in incompatible_fields], metadata=s.metadata)
                     for s in unique_schemas
                 ]
                 # Re-evaluate conflicts? No, we removed them.
                 # Update conflicts dict?
                 for field in incompatible_fields:
                     del conflicts[field]

        # Normalize schemas using intelligent promotion rules
        unique_schemas = _normalize_schema_types(
            unique_schemas, conflicts, handle_incompatible=not remove_conflicting_columns
        )

    # Step 2: Attempt unification with conflict-resolved schemas
    try:
        unified_schema = pa.unify_schemas(unique_schemas, promote_options="permissive")

        # Step 3: Apply timezone standardization to the unified result
        if standardize_timezones:
            target_tz = timezone if timezone is not None else "auto"
            
            if target_tz == "auto":
                # Calculate majority from ORIGINAL input schemas, not the unified result
                # (since normalization might have shifted them)
                dom = dominant_timezone_per_column(original_unique_schemas)
                
                # Apply derived timezones to unified schema
                fields = []
                for field in unified_schema:
                    if pa.types.is_timestamp(field.type) and field.name in dom:
                        unit, tz = dom[field.name]
                        # Use unit from unified field to preserve precision promotion
                        current_unit = field.type.unit
                        fields.append(field.with_type(pa.timestamp(current_unit, tz)))
                    else:
                        fields.append(field)
                unified_schema = pa.schema(fields, unified_schema.metadata)
            else:
                # Explicit timezone
                unified_schema = standardize_schema_timezones([unified_schema], target_tz)[0]

        return (
            unified_schema
            if use_large_dtypes
            else convert_large_types_to_normal(unified_schema)
        )

    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        # Step 4: Intelligent fallback strategies
        if verbose:
            logger.debug("Primary unification failed: %s", e)
            logger.debug("Attempting fallback strategies...")

        # Fallback 1: Try aggressive string conversion for remaining conflicts
        try:
            fallback_schema = _aggressive_fallback_unification(unique_schemas)
            if standardize_timezones:
                fallback_schema = standardize_schema_timezones(
                    [fallback_schema], timezone
                )[0]
            if verbose:
                logger.debug("✓ Aggressive fallback succeeded")
            return (
                fallback_schema
                if use_large_dtypes
                else convert_large_types_to_normal(fallback_schema)
            )

        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError) as e:
            if verbose:
                logger.debug("✗ Aggressive fallback failed: %s", str(e), exc_info=True)

        # Fallback 2: Remove conflicting fields (if enabled)
        if remove_conflicting_columns:
            try:
                non_conflicting_schema = _remove_conflicting_fields(unique_schemas)
                if standardize_timezones:
                    non_conflicting_schema = standardize_schema_timezones(
                        [non_conflicting_schema], timezone
                    )[0]
                if verbose:
                    logger.debug("✓ Remove conflicting fields fallback succeeded")
                return (
                    non_conflicting_schema
                    if use_large_dtypes
                    else convert_large_types_to_normal(non_conflicting_schema)
                )

            except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError) as e:
                if verbose:
                    logger.debug(
                        "✗ Remove conflicting fields fallback failed: %s",
                        str(e),
                        exc_info=True,
                    )

        # Fallback 3: Remove problematic fields that can't be unified
        try:
            minimal_schema = _remove_problematic_fields(unique_schemas)
            if standardize_timezones:
                minimal_schema = standardize_schema_timezones(
                    [minimal_schema], timezone
                )[0]
            if verbose:
                logger.debug("✓ Minimal schema (removed problematic fields) succeeded")
            return (
                minimal_schema
                if use_large_dtypes
                else convert_large_types_to_normal(minimal_schema)
            )

        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError) as e:
            if verbose:
                logger.debug(
                    "✗ Minimal schema fallback failed: %s", str(e), exc_info=True
                )

        # Fallback 4: Return first schema as last resort
        if verbose:
            logger.debug("✗ All fallback strategies failed, returning first schema")

        first_schema = unique_schemas[0]
        if standardize_timezones:
            first_schema = standardize_schema_timezones([first_schema], timezone)[0]
        return (
            first_schema
            if use_large_dtypes
            else convert_large_types_to_normal(first_schema)
        )


# ============================================================================
# Data Type Optimization Functions
# ============================================================================

# Float32 range limits for optimization
F32_MIN = float(np.finfo(np.float32).min)
F32_MAX = float(np.finfo(np.float32).max)

# Pre-compiled regex patterns for type detection
INTEGER_REGEX = r"^[-+]?\d+$"
FLOAT_REGEX = r"^(?:[-+]?(?:\d*[.,])?\d+(?:[eE][-+]?\d+)?|[-+]?(?:inf|nan))$"
BOOLEAN_REGEX = r"^(true|false|1|0|yes|ja|no|nein|t|f|y|j|n|ok|nok)$"


def _can_downcast_to_float32(array: pa.Array) -> bool:
    """Check if a float64 array can be safely downcast to float32.

    Args:
        array: Float64 array

    Returns:
        True if safe to downcast
    """
    if not pa.types.is_float64(array.type):
        return False

    min_val = pa.compute.min(array).as_py()
    max_val = pa.compute.max(array).as_py()

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
        min_val = pa.compute.min(array).as_py()
        max_val = pa.compute.max(array).as_py()
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
