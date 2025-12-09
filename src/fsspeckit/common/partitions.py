"""
Shared partition utilities for fsspeckit.

This module provides canonical implementations for partition parsing and related
operations across all backends. It consolidates partition-related logic
that was previously scattered across different modules.

Key responsibilities:
1. Partition extraction from file paths
2. Support for multiple partitioning schemes (Hive, directory-based)
3. Partition validation and normalization
4. Path manipulation for partitioned datasets

Architecture:
- Functions are designed to work with string paths and fsspec filesystems
- Support for common partitioning patterns used in data lakes
- Consistent behavior across all backends
- Extensible design for custom partitioning schemes

Usage:
Backend implementations should delegate to this module rather than implementing
their own partition parsing logic. This ensures consistent behavior across
DuckDB, PyArrow, and future backends.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def get_partitions_from_path(
    path: str, partitioning: str | list[str] | None = None
) -> list[tuple]:
    """
    Extract dataset partitions from a file path.

    Parses file paths to extract partition information based on
    different partitioning schemes. This is the canonical implementation
    used across all fsspeckit backends.

    Args:
        path: File path potentially containing partition information.
        partitioning: Partitioning scheme:
            - "hive": Hive-style partitioning (key=value)
            - str: Single partition column name
            - list[str]: Multiple partition column names
            - None: Return empty list

    Returns:
        List of tuples containing (column, value) pairs.

    Examples:
        >>> # Hive-style partitioning
        >>> get_partitions_from_path("data/year=2023/month=01/file.parquet", "hive")
        [('year', '2023'), ('month', '01')]

        >>> # Single partition column
        >>> get_partitions_from_path("data/2023/01/file.parquet", "year")
        [('year', '2023')]

        >>> # Multiple partition columns
        >>> get_partitions_from_path("data/2023/01/file.parquet", ["year", "month"])
        [('year', '2023'), ('month', '01')]

        >>> # No partitioning
        >>> get_partitions_from_path("data/file.parquet", None)
        []
    """
    if "." in path:
        path = os.path.dirname(path)

    parts = path.split("/")

    if isinstance(partitioning, str):
        if partitioning == "hive":
            return [tuple(p.split("=")) for p in parts if "=" in p]
        else:
            # Single partition column - take the first directory that looks like a value
            # This is a simple heuristic for cases like data/2023/file.parquet
            if parts:
                return [(partitioning, parts[0])]
            return []
    elif isinstance(partitioning, list):
        # Multiple partition columns - map column names to path parts from right to left
        if not parts:
            return []

        # Take the last N parts where N is the number of partition columns
        partition_parts = (
            parts[-len(partitioning) :] if len(parts) >= len(partitioning) else parts
        )
        return list(zip(partitioning, partition_parts))
    else:
        return []


def normalize_partition_value(value: str) -> str:
    """
    Normalize a partition value for consistent comparison.

    Args:
        value: Raw partition value from path.

    Returns:
        Normalized partition value.
    """
    return value.strip().strip("\"'").replace("\\", "")


def validate_partition_columns(
    partitions: list[tuple[str, str]], expected_columns: list[str] | None = None
) -> bool:
    """
    Validate partition columns against expected schema.

    Args:
        partitions: List of (column, value) tuples.
        expected_columns: Optional list of expected column names.

    Returns:
        True if partitions are valid, False otherwise.
    """
    if not partitions:
        return True

    if expected_columns is not None:
        partition_columns = {col for col, _ in partitions}
        expected_set = set(expected_columns)

        # Check if all partition columns are expected
        if not partition_columns.issubset(expected_set):
            return False

        # Check if all expected columns are present (if strict validation needed)
        # This is optional - some datasets might have missing partitions
        # return partition_columns == expected_set

    # Validate that no column names are empty
    for col, val in partitions:
        if not col or not col.strip():
            return False

    return True


def build_partition_path(
    base_path: str, partitions: list[tuple[str, str]], partitioning: str = "hive"
) -> str:
    """
    Build a file path with partition directories.

    Args:
        base_path: Base directory path.
        partitions: List of (column, value) tuples.
        partitioning: Partitioning scheme ("hive" or "directory").

    Returns:
        Path string with partition directories.
    """
    if not partitions:
        return base_path

    if partitioning == "hive":
        # Hive-style: column=value/column=value
        partition_dirs = [f"{col}={val}" for col, val in partitions]
    else:
        # Directory-style: value/value (order matters)
        partition_dirs = [val for _, val in partitions]

    return "/".join([base_path.rstrip("/")] + partition_dirs)


def extract_partition_filters(
    paths: list[str], partitioning: str | list[str] | None = None
) -> dict[str, set[str]]:
    """
    Extract unique partition values from a list of paths.

    Args:
        paths: List of file paths.
        partitioning: Partitioning scheme.

    Returns:
        Dictionary mapping column names to sets of unique values.
    """
    partition_values = {}

    for path in paths:
        partitions = get_partitions_from_path(path, partitioning)
        for col, val in partitions:
            if col not in partition_values:
                partition_values[col] = set()
            partition_values[col].add(val)

    return partition_values


def filter_paths_by_partitions(
    paths: list[str],
    partition_filters: dict[str, str | list[str]],
    partitioning: str | list[str] | None = None,
) -> list[str]:
    """
    Filter paths based on partition values.

    Args:
        paths: List of file paths to filter.
        partition_filters: Dictionary mapping column names to filter values.
        partitioning: Partitioning scheme.

    Returns:
        Filtered list of paths.
    """
    filtered_paths = []

    for path in paths:
        partitions = dict(get_partitions_from_path(path, partitioning))

        # Check if path matches all filters
        matches = True
        for col, filter_val in partition_filters.items():
            if col not in partitions:
                matches = False
                break

            path_val = partitions[col]

            # Handle list of allowed values
            if isinstance(filter_val, list):
                if path_val not in filter_val:
                    matches = False
                    break
            else:
                # Single value comparison
                if path_val != filter_val:
                    matches = False
                    break

        if matches:
            filtered_paths.append(path)

    return filtered_paths


def infer_partitioning_scheme(
    paths: list[str], max_samples: int = 100
) -> dict[str, Any]:
    """
    Infer the partitioning scheme from a sample of paths.

    Args:
        paths: List of file paths to analyze.
        max_samples: Maximum number of paths to sample.

    Returns:
        Dictionary with inferred scheme information.
    """
    if not paths:
        return {"scheme": None, "confidence": 0.0}

    # Sample paths for analysis
    sample_paths = paths[:max_samples] if len(paths) > max_samples else paths

    # Check for Hive-style partitioning
    hive_partitions = []
    directory_partitions = []

    for path in sample_paths:
        # Remove filename and get directory parts
        dir_path = os.path.dirname(path)
        parts = dir_path.split("/")

        # Look for key=value patterns
        hive_parts = [p for p in parts if "=" in p and p.split("=")[0].strip()]
        if hive_parts:
            hive_partitions.append(len(hive_parts))

        # Look for directory-style partitions (numeric dates, etc.)
        dir_parts = [
            p
            for p in parts
            if p.replace("/", "").replace("-", "").replace("_", "").isdigit()
        ]
        if dir_parts:
            directory_partitions.append(len(dir_parts))

    # Determine most likely scheme
    result = {"scheme": None, "confidence": 0.0}

    if hive_partitions:
        avg_hive_parts = sum(hive_partitions) / len(hive_partitions)
        if avg_hive_parts >= 1:  # At least 1 partition level on average
            result["scheme"] = "hive"
            result["confidence"] = min(
                1.0, avg_hive_parts / 3.0
            )  # Normalize by expected max
            result["avg_partitions"] = avg_hive_parts
            return result

    if directory_partitions:
        avg_dir_parts = sum(directory_partitions) / len(directory_partitions)
        if avg_dir_parts >= 1:  # At least 1 partition level on average
            result["scheme"] = "directory"
            result["confidence"] = min(
                1.0, avg_dir_parts / 3.0
            )  # Normalize by expected max
            result["avg_partitions"] = avg_dir_parts
            return result

    # No clear partitioning detected
    return result


def get_partition_columns_from_paths(
    paths: list[str], partitioning: str | list[str] | None = None
) -> list[str]:
    """
    Get all unique partition column names from a list of paths.

    Args:
        paths: List of file paths.
        partitioning: Partitioning scheme.

    Returns:
        List of unique partition column names.
    """
    columns = set()

    for path in paths:
        partitions = get_partitions_from_path(path, partitioning)
        for col, _ in partitions:
            columns.add(col)

    return sorted(list(columns))


def create_partition_expression(
    partitions: list[tuple[str, str]], backend: str = "pyarrow"
) -> Any:
    """
    Create a partition filter expression for different backends.

    Args:
        partitions: List of (column, value) tuples.
        backend: Target backend ("pyarrow", "duckdb").

    Returns:
        Backend-specific filter expression.
    """
    if not partitions:
        return None

    if backend == "pyarrow":
        import pyarrow.dataset as ds

        # Build PyArrow dataset filter expression
        expressions = []
        for col, val in partitions:
            expressions.append(ds.field(col) == val)

        # Combine with AND logic
        result = expressions[0]
        for expr in expressions[1:]:
            result = result & expr
        return result

    elif backend == "duckdb":
        # Build DuckDB WHERE clause
        conditions = []
        for col, val in partitions:
            if isinstance(val, str):
                conditions.append(f"\"{col}\" = '{val}'")
            else:
                conditions.append(f'"{col}" = {val}')

        return " AND ".join(conditions)

    else:
        raise ValueError(f"Unsupported backend: {backend}")


def apply_partition_pruning(
    paths: list[str],
    partition_filters: dict[str, Any],
    partitioning: str | list[str] | None = None,
) -> list[str]:
    """
    Apply partition pruning to reduce the set of files to scan.

    This is an optimization that eliminates files based on partition
    values before any I/O operations.

    Args:
        paths: List of all file paths.
        partition_filters: Dictionary of partition filters to apply.
        partitioning: Partitioning scheme.

    Returns:
        Pruned list of paths.
    """
    if not partition_filters:
        return paths

    return filter_paths_by_partitions(paths, partition_filters, partitioning)
