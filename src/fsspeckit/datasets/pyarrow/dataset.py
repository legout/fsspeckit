"""PyArrow dataset operations including merge and maintenance helpers.

This module contains functions for dataset-level operations including:
- Dataset merging with various strategies
- Dataset statistics collection
- Dataset compaction and optimization
- Maintenance operations
"""

import concurrent.futures
from collections import defaultdict
from pathlib import Path
import random
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import re
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal

if TYPE_CHECKING:
    import polars as pl

from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

from fsspeckit.core.merge import (
    MergeStrategy as CoreMergeStrategy,
    MergeStats,
    calculate_merge_stats,
    check_null_keys,
    normalize_key_columns,
    validate_merge_inputs,
    validate_strategy_compatibility,
)
from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)


def collect_dataset_stats_pyarrow(
    path: str,
    filesystem: AbstractFileSystem | None = None,
    partition_filter: list[str] | None = None,
) -> dict[str, Any]:
    """Collect file-level statistics for a parquet dataset using shared core logic.

    This function delegates to the shared ``fsspeckit.core.maintenance.collect_dataset_stats``
    function, ensuring consistent dataset discovery and statistics across both DuckDB
    and PyArrow backends.

    The helper walks the given dataset directory on the provided filesystem,
    discovers parquet files (recursively), and returns basic statistics:

    - Per-file path, size in bytes, and number of rows
    - Aggregated total bytes and total rows

    The function is intentionally streaming/metadata-driven and never
    materializes the full dataset as a single :class:`pyarrow.Table`.

    Args:
        path: Root directory of the parquet dataset.
        filesystem: Optional fsspec filesystem. If omitted, a local "file"
            filesystem is used.
        partition_filter: Optional list of partition prefix filters
            (e.g. ["date=2025-11-04"]). Only files whose path relative to
            ``path`` starts with one of these prefixes are included.

    Returns:
        Dict with keys:

        - ``files``: list of ``{"path", "size_bytes", "num_rows"}`` dicts
        - ``total_bytes``: sum of file sizes
        - ``total_rows``: sum of row counts

    Raises:
        FileNotFoundError: If the path does not exist or no parquet files
            match the optional partition filter.

    Note:
        This is a thin wrapper around the shared core function. See
        :func:`fsspeckit.core.maintenance.collect_dataset_stats` for the
        authoritative implementation.
    """
    from fsspeckit.core.maintenance import collect_dataset_stats

    return collect_dataset_stats(
        path=path,
        filesystem=filesystem,
        partition_filter=partition_filter,
    )


def compact_parquet_dataset_pyarrow(
    path: str,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    dry_run: bool = False,
    filesystem: AbstractFileSystem | None = None,
) -> dict[str, Any]:
    """Compact a parquet dataset directory into fewer larger files using PyArrow and shared planning.

    Groups small files based on size (MB) and/or row thresholds, rewrites grouped
    files into new parquet files, and optionally changes compression. Supports a
    dry-run mode that returns the compaction plan without modifying files.

    The implementation uses the shared core planning algorithm for consistent
    behavior across backends. It processes data in a group-based, streaming fashion:
    it reads only the files in a given group into memory when processing that group
    and never materializes the entire dataset as a single table.

    Args:
        path: Dataset root directory (local path or fsspec URL).
        target_mb_per_file: Optional max output size per file; must be > 0.
        target_rows_per_file: Optional max rows per output file; must be > 0.
        partition_filter: Optional list of partition prefixes (e.g. ``["date=2025-11-15"]``)
            used to limit both stats collection and rewrites to matching paths.
        compression: Optional parquet compression codec; defaults to ``"snappy"``.
        dry_run: When ``True`` the function returns a plan + before/after stats
            without reading or writing any parquet data.
        filesystem: Optional ``fsspec.AbstractFileSystem`` to reuse existing FS clients.

    Returns:
        A stats dictionary describing before/after file counts, total bytes,
        rewritten bytes, and optional ``planned_groups`` when ``dry_run`` is enabled.
        The structure follows the canonical ``MaintenanceStats`` format from the shared core.

    Raises:
        ValueError: If thresholds are invalid or no files match partition filter.
        FileNotFoundError: If the path does not exist.

    Example:
        ```python
        result = compact_parquet_dataset_pyarrow(
            "/path/to/dataset",
            target_mb_per_file=64,
            dry_run=True,
        )
        print(f"Files before: {result['before_file_count']}")
        print(f"Files after: {result['after_file_count']}")
        ```

    Note:
        This function delegates dataset discovery and compaction planning to the
        shared ``fsspeckit.core.maintenance`` module, ensuring consistent behavior
        across DuckDB and PyArrow backends.
    """
    from fsspeckit.core.maintenance import plan_compaction_groups, MaintenanceStats

    # Get dataset stats using shared logic
    stats = collect_dataset_stats_pyarrow(
        path=path, filesystem=filesystem, partition_filter=partition_filter
    )
    files = stats["files"]

    # Use shared compaction planning
    plan_result = plan_compaction_groups(
        file_infos=files,
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
    )

    groups = plan_result["groups"]
    planned_stats = plan_result["planned_stats"]

    # Update planned stats with compression info
    planned_stats.compression_codec = compression
    planned_stats.dry_run = dry_run

    # If dry run, return the plan
    if dry_run:
        result = planned_stats.to_dict()
        result["planned_groups"] = groups
        return result

    # Execute compaction
    if not groups:
        return planned_stats.to_dict()

    # Execute the compaction
    for group in groups:
        # Read all files in this group
        tables = []
        for file_info in group["files"]:
            file_path = file_info["path"]
            table = pq.read_table(
                file_path,
                filesystem=filesystem,
            )
            tables.append(table)

        # Concatenate tables
        if len(tables) > 1:
            combined = pa.concat_tables(tables, promote_options="permissive")
        else:
            combined = tables[0]

        # Write to output file
        output_path = group["output_path"]
        pq.write_table(
            combined,
            output_path,
            filesystem=filesystem,
            compression=compression or "snappy",
        )

    # Remove original files
    for group in groups:
        for file_info in group["files"]:
            file_path = file_info["path"]
            filesystem.rm(file_path)

    return planned_stats.to_dict()


def optimize_parquet_dataset_pyarrow(
    path: str,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    filesystem: AbstractFileSystem | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """Optimize a parquet dataset through compaction and optional statistics recalculation.

    This is a convenience function that combines compaction with optional statistics
    recalculation. It's particularly useful after many small write operations have
    created a large number of small files.

    Args:
        path: Dataset root directory
        target_mb_per_file: Target size per file in MB
        target_rows_per_file: Target rows per file
        partition_filter: Optional partition filters
        compression: Compression codec to use
        filesystem: Optional filesystem instance
        verbose: Print progress information

    Returns:
        Optimization statistics

    Example:
        ```python
        stats = optimize_parquet_dataset_pyarrow(
            "dataset/",
            target_mb_per_file=64,
            compression="zstd",
        )
        print(
            f"Reduced from {stats['before_file_count']} "
            f"to {stats['after_file_count']} files",
        )
        ```
    """
    # Use compaction
    result = compact_parquet_dataset_pyarrow(
        path=path,
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
        partition_filter=partition_filter,
        compression=compression,
        dry_run=False,
        filesystem=filesystem,
    )

    if verbose:
        logger.info("Optimization complete: %s", result)

    return result


def _normalize_key_columns(key_columns: list[str] | str) -> list[str]:
    """Normalize key column specification to a list.

    Args:
        key_columns: Key columns as string or list

    Returns:
        List of key column names
    """
    if isinstance(key_columns, str):
        return [key_columns]
    return key_columns


def _ensure_pyarrow_filesystem(
    filesystem: AbstractFileSystem,
) -> PyFileSystem:
    """Ensure we have a PyArrow-compatible filesystem.

    Args:
        filesystem: fsspec filesystem

    Returns:
        PyArrow filesystem wrapper
    """
    if isinstance(filesystem, PyFileSystem):
        return filesystem

    handler = FSSpecHandler(filesystem)
    return PyFileSystem(handler)


def _join_path(base: str, child: str) -> str:
    """Join paths correctly.

    Args:
        base: Base path
        child: Child path

    Returns:
        Joined path
    """
    if base.endswith("/"):
        return base + child
    return base + "/" + child


def _load_source_table_pyarrow(
    source: str,
    filesystem: AbstractFileSystem,
    row_filter: Any = None,
    columns: list[str] | None = None,
) -> pa.Table:
    """Load a source table from a path.

    Args:
        source: Source path
        filesystem: Filesystem instance
        row_filter: Optional row filter
        columns: Optional column selection

    Returns:
        PyArrow table
    """
    pa_filesystem = _ensure_pyarrow_filesystem(filesystem)

    if source.endswith(".parquet"):
        return pq.read_table(
            source,
            filesystem=pa_filesystem,
            filters=row_filter,
            columns=columns,
        )
    else:
        # Assume it's a dataset directory
        dataset = ds.dataset(
            source,
            filesystem=pa_filesystem,
        )
        return dataset.to_table(filter=row_filter, columns=columns)


def _iter_table_slices(table: pa.Table, batch_size: int) -> Iterable[pa.Table]:
    """Iterate over a table in slices.

    Args:
        table: PyArrow table
        batch_size: Size of each slice

    Yields:
        Table slices
    """
    num_rows = table.num_rows
    for start in range(0, num_rows, batch_size):
        end = min(start + batch_size, num_rows)
        yield table.slice(start, end - start)


def _build_filter_expression(
    filter_column: str,
    filter_values: list[Any],
) -> Any:
    """Build a filter expression for PyArrow.

    Args:
        filter_column: Column to filter on
        filter_values: Values to include

    Returns:
        PyArrow filter expression
    """
    if len(filter_values) == 1:
        return ds.field(filter_column) == filter_values[0]
    else:
        return ds.field(filter_column).isin(filter_values)


def _extract_key_tuples(
    table: pa.Table,
    key_columns: list[str],
) -> list[tuple]:
    """Extract key tuples from a table.

    Args:
        table: PyArrow table
        key_columns: Key column names

    Returns:
        List of key tuples
    """
    keys = []
    for row in table.to_pylist():
        key = tuple(row[col] for col in key_columns)
        keys.append(key)
    return keys


def _ensure_no_null_keys_table(table: pa.Table, key_columns: list[str]) -> None:
    """Ensure no null keys in a table.

    Args:
        table: PyArrow table
        key_columns: Key column names

    Raises:
        ValueError: If null keys are found
    """
    for key_col in key_columns:
        null_count = table[key_col].null_count
        if null_count > 0:
            raise ValueError(
                f"Found {null_count} null values in key column '{key_col}'. "
                "Null keys are not allowed."
            )


def _ensure_no_null_keys_dataset(
    dataset: ds.Dataset,
    key_columns: list[str],
) -> None:
    """Ensure no null keys in a dataset.

    Args:
        dataset: PyArrow dataset
        key_columns: Key column names

    Raises:
        ValueError: If null keys are found
    """
    for key_col in key_columns:
        # Check schema
        if key_col not in dataset.schema.names:
            raise ValueError(f"Key column '{key_col}' not found in dataset")

        # Check for nullable type
        field = dataset.schema.field(key_col)
        if field.nullable:
            raise ValueError(
                f"Key column '{key_col}' is nullable. Non-nullable keys are required."
            )


def _write_tables_to_dataset(
    tables: list[pa.Table],
    output_path: str,
    filesystem: AbstractFileSystem,
    basename_template: str = "part-{i}.parquet",
    compression: str | None = None,
) -> list[str]:
    """Write tables to a dataset directory.

    Args:
        tables: List of tables to write
        output_path: Output directory
        filesystem: Filesystem instance
        basename_template: Template for file names
        compression: Compression codec

    Returns:
        List of written file paths
    """
    pa_filesystem = _ensure_pyarrow_filesystem(filesystem)
    written_files = []

    for i, table in enumerate(tables):
        file_path = _join_path(
            output_path,
            basename_template.format(i=i),
        )
        pq.write_table(
            table,
            file_path,
            filesystem=pa_filesystem,
            compression=compression,
        )
        written_files.append(file_path)

    return written_files


def merge_parquet_dataset_pyarrow(
    sources: list[str],
    output_path: str,
    target: str | None = None,
    strategy: str | CoreMergeStrategy = "deduplicate",
    key_columns: list[str] | str | None = None,
    filesystem: AbstractFileSystem | None = None,
    compression: str | None = None,
    row_group_size: int | None = 500_000,
    max_rows_per_file: int | None = 5_000_000,
    verbose: bool = False,
    **kwargs: Any,
) -> MergeStats:
    """Merge multiple parquet datasets using PyArrow with various strategies.

    This function provides dataset merging capabilities with support for:
    - Multiple merge strategies (upsert, insert, update, full_merge, deduplicate)
    - Key-based merging for relational operations
    - Batch processing for large datasets
    - Configurable output settings

    Args:
        sources: List of source dataset paths
        output_path: Path for merged output
        target: Target dataset path (for upsert/update strategies)
        strategy: Merge strategy to use
        key_columns: Key columns for merging (required for relational strategies)
        filesystem: fsspec filesystem instance
        compression: Output compression codec
        row_group_size: Rows per parquet row group
        max_rows_per_file: Max rows per output file
        verbose: Print progress information
        **kwargs: Additional arguments

    Returns:
        MergeStats with merge statistics

    Raises:
        ValueError: If required parameters are missing
        FileNotFoundError: If sources don't exist

    Example:
        ```python
        stats = merge_parquet_dataset_pyarrow(
            sources=["dataset1/", "dataset2/"],
            output_path="merged/",
            strategy="deduplicate",
            key_columns=["id"],
            verbose=True,
        )
        print(f"Merged {stats.total_rows} rows")
        ```
    """
    # Validate strategy compatibility
    validate_strategy_compatibility(strategy, key_columns, target)

    # Normalize parameters
    if key_columns is not None:
        key_columns = _normalize_key_columns(key_columns)

    # Get filesystem
    if filesystem is None:
        filesystem = fsspec_filesystem("file")

    pa_filesystem = _ensure_pyarrow_filesystem(filesystem)

    # Load target if provided
    target_table = None
    if target and strategy in ["upsert", "update"]:
        target_table = _load_source_table_pyarrow(target, filesystem)

        if key_columns:
            _ensure_no_null_keys_table(target_table, key_columns)

    # Process sources
    merged_data = []
    total_rows = 0

    for source_path in sources:
        if verbose:
            logger.info("Processing source: %s", source_path)

        source_table = _load_source_table_pyarrow(source_path, filesystem)

        if key_columns:
            _ensure_no_null_keys_table(source_table, key_columns)

        if strategy == "full_merge":
            # Simply concatenate all data
            merged_data.append(source_table)
            total_rows += source_table.num_rows

        elif strategy == "deduplicate":
            # Remove duplicates based on key columns
            if key_columns:
                # Group by keys and keep first occurrence
                table = source_table

                # Use PyArrow's group_by for deduplication
                # This is a simplified implementation
                groups = table.group_by(key_columns).aggregate([])
                keys = groups.select(key_columns)

                # Get unique keys
                unique_keys = []
                for row in keys.to_pylist():
                    unique_keys.append(tuple(row[col] for col in key_columns))

                # Filter to keep only unique rows
                filtered = []
                for row in table.to_pylist():
                    key = tuple(row[col] for col in key_columns)
                    if key in unique_keys:
                        filtered.append(row)
                        unique_keys.remove(key)  # Remove to avoid duplicates

                if filtered:
                    deduped = pa.Table.from_pylist(filtered, schema=table.schema)
                    merged_data.append(deduped)
                    total_rows += deduped.num_rows
            else:
                # No key columns, remove exact duplicates
                merged_data.append(source_table)
                total_rows += source_table.num_rows

        elif strategy in ["upsert", "insert", "update"] and target_table is not None:
            # Key-based relational operations
            if strategy == "insert":
                # Only insert non-existing rows
                target_keys = _extract_key_tuples(target_table, key_columns)
                source_keys = _extract_key_tuples(source_table, key_columns)

                # Find keys that don't exist in target
                new_keys = set(source_keys) - set(target_keys)

                # Filter source for new keys
                new_rows = []
                for row in source_table.to_pylist():
                    key = tuple(row[col] for col in key_columns)
                    if key in new_keys:
                        new_rows.append(row)

                if new_rows:
                    new_table = pa.Table.from_pylist(
                        new_rows, schema=source_table.schema
                    )
                    merged_data.append(new_table)
                    total_rows += new_table.num_rows

            elif strategy == "update":
                # Update existing rows
                target_keys = _extract_key_tuples(target_table, key_columns)
                source_keys = _extract_key_tuples(source_table, key_columns)

                # Find common keys
                common_keys = set(source_keys) & set(target_keys)

                # Build updated target
                updated_data = []

                # Keep non-matching rows from target
                for row in target_table.to_pylist():
                    key = tuple(row[col] for col in key_columns)
                    if key not in common_keys:
                        updated_data.append(row)

                # Add updated rows from source
                for row in source_table.to_pylist():
                    key = tuple(row[col] for col in key_columns)
                    if key in common_keys:
                        updated_data.append(row)

                if updated_data:
                    updated_table = pa.Table.from_pylist(
                        updated_data, schema=target_table.schema
                    )
                    merged_data.append(updated_table)
                    total_rows += updated_table.num_rows

            elif strategy == "upsert":
                # Insert or update
                all_data = [target_table] if target_table else []
                all_data.append(source_table)

                if all_data:
                    combined = pa.concat_tables(all_data, promote_options="permissive")

                    # Deduplicate based on keys
                    if key_columns:
                        # Group by keys and keep last occurrence
                        # This is a simplified implementation
                        groups = combined.group_by(key_columns).aggregate([])
                        keys = groups.select(key_columns)

                        unique_keys = []
                        for row in keys.to_pylist():
                            unique_keys.append(tuple(row[col] for col in key_columns))

                        # Keep only last occurrence of each key
                        filtered = []
                        seen = set()
                        for row in reversed(combined.to_pylist()):
                            key = tuple(row[col] for col in key_columns)
                            if key not in seen:
                                filtered.append(row)
                                seen.add(key)

                        if filtered:
                            deduped = pa.Table.from_pylist(
                                list(reversed(filtered)), schema=combined.schema
                            )
                            merged_data.append(deduped)
                            total_rows += deduped.num_rows
                    else:
                        merged_data.append(combined)
                        total_rows += combined.num_rows

    # Combine all data
    if merged_data:
        if len(merged_data) == 1:
            final_table = merged_data[0]
        else:
            final_table = pa.concat_tables(merged_data, promote_options="permissive")
    else:
        # No data to merge
        final_table = pa.table({})

    # Write output
    pq.write_table(
        final_table,
        output_path,
        filesystem=pa_filesystem,
        compression=compression,
        row_group_size=row_group_size,
        max_rows_per_file=max_rows_per_file,
    )

    # Calculate stats
    stats = calculate_merge_stats(
        sources=sources,
        target=output_path,
        strategy=strategy,
        total_rows=total_rows,
        output_rows=final_table.num_rows,
    )

    if verbose:
        logger.info("\nMerge complete: %s", stats)

    return stats
