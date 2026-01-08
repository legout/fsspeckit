"""PyArrow dataset operations including merge and maintenance helpers.

This module contains functions for dataset-level operations including:
- Dataset merging with various strategies
- Dataset statistics collection
- Dataset compaction and optimization
- Maintenance operations
"""

import concurrent.futures
import random
import re
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

if TYPE_CHECKING:
    import polars as pl

from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

from fsspeckit.common.logging import get_logger
from fsspeckit.core.merge import (
    MergeStats,
    calculate_merge_stats,
    check_null_keys,
    normalize_key_columns,
    validate_merge_inputs,
    validate_strategy_compatibility,
)
from fsspeckit.core.merge import (
    MergeStrategy as CoreMergeStrategy,
)
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor, MemoryPressureLevel
from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

logger = get_logger(__name__)


class PerformanceMonitor:
    """Comprehensive performance monitoring and metrics collection.

    This class tracks various performance metrics including processing time,
    memory usage, throughput, and operation-specific metrics.
    """

    def __init__(
        self,
        max_pyarrow_mb: int = 2048,
        max_process_memory_mb: int | None = None,
        min_system_available_mb: int = 512,
    ):
        self.start_time = time.perf_counter()
        self.operation_breakdown = defaultdict(float)
        self.memory_peak_mb = 0.0
        self.process_memory_peak_mb = 0.0
        self.files_processed = 0
        self.chunks_processed = 0
        self.total_rows_processed = 0
        self.total_bytes_processed = 0
        self.current_op = None
        self.op_start_time = 0.0

        self._memory_monitor = MemoryMonitor(
            max_pyarrow_mb=max_pyarrow_mb,
            max_process_memory_mb=max_process_memory_mb,
            min_system_available_mb=min_system_available_mb,
        )
        self.pressure_counts: dict[str, int] = defaultdict(int)
        self._last_status_time = 0.0

    def start_op(self, name: str):
        """Start tracking a specific operation phase."""
        if self.current_op:
            self.end_op()
        self.current_op = name
        self.op_start_time = time.perf_counter()
        self.track_memory()

    def end_op(self):
        """End tracking the current operation phase."""
        if self.current_op:
            duration = time.perf_counter() - self.op_start_time
            self.operation_breakdown[self.current_op] += duration
            self.current_op = None
        self.track_memory()

    def track_memory(self):
        """Track peak memory usage using MemoryMonitor."""
        now = time.perf_counter()
        # Avoid excessive psutil calls (cache for 100ms)
        if now - self._last_status_time < 0.1:
            return

        status = self._memory_monitor.get_memory_status()
        self._last_status_time = now

        # Track PyArrow peak
        current_pa_mb = status.get("pyarrow_allocated_mb", 0.0)
        if current_pa_mb > self.memory_peak_mb:
            self.memory_peak_mb = current_pa_mb

        # Track Process peak
        current_rss_mb = status.get("process_rss_mb", 0.0)
        if current_rss_mb > self.process_memory_peak_mb:
            self.process_memory_peak_mb = current_rss_mb

        # Track pressure level
        pressure = self._memory_monitor.check_memory_pressure()
        self.pressure_counts[pressure.value] += 1

    def get_memory_status(self) -> dict[str, float]:
        """Get current memory snapshot from MemoryMonitor."""
        return self._memory_monitor.get_memory_status()

    def get_metrics(
        self,
        total_rows_before: int,
        total_rows_after: int,
        total_bytes: int,
    ) -> dict[str, Any]:
        """Generate comprehensive performance metrics report.

        Args:
            total_rows_before: Total rows in the dataset before operation.
            total_rows_after: Total rows in the dataset after operation.
            total_bytes: Total size of the dataset in bytes.

        Returns:
            Dictionary containing performance metrics.
        """
        # Force a final memory track to ensure peaks are captured
        self._last_status_time = 0.0
        self.track_memory()

        total_time = time.perf_counter() - self.start_time
        rows_removed = total_rows_before - total_rows_after
        dedup_efficiency = (
            (rows_removed / total_rows_before) if total_rows_before > 0 else 0.0
        )

        metrics = {
            "total_process_time_sec": total_time,
            "memory_peak_mb": self.memory_peak_mb,
            "process_memory_peak_mb": self.process_memory_peak_mb,
            "throughput_mb_sec": (total_bytes / (1024 * 1024)) / total_time
            if total_time > 0
            else 0.0,
            "rows_per_sec": total_rows_before / total_time if total_time > 0 else 0.0,
            "files_processed": self.files_processed,
            "chunks_processed": self.chunks_processed,
            "dedup_efficiency": dedup_efficiency,
            "operation_breakdown": dict(self.operation_breakdown),
            "memory_pressure_stats": dict(self.pressure_counts),
        }

        # Include system info if available
        status = self._memory_monitor.get_memory_status()
        if "system_available_mb" in status:
            metrics["system_available_mb"] = status["system_available_mb"]

        return metrics


def _table_drop_duplicates(
    table: pa.Table,
    subset: list[str] | None = None,
    keep: Literal["first", "last"] = "first",
) -> pa.Table:
    """Safely drop duplicates from a PyArrow Table.

    Uses Table.drop_duplicates if available (PyArrow >= 12.0.0),
    otherwise falls back to Polars.
    """
    if hasattr(table, "drop_duplicates"):
        # Note: PyArrow Table.drop_duplicates only supports keep='first' in some versions
        # but the kwarg is supported in newer ones.
        try:
            return table.drop_duplicates(subset=subset, keep=keep)  # type: ignore
        except (TypeError, ValueError):
            # Fallback for versions that don't support keep kwarg
            if keep == "first":
                return table.drop_duplicates(subset=subset)  # type: ignore
            # if last requested but not supported, we'll fall back to Polars below

    # Fallback to Polars for older/weird PyArrow environments or if keep='last' not supported
    import polars as pl

    df = pl.from_arrow(table)
    assert isinstance(df, pl.DataFrame)
    return df.unique(subset=subset, keep=keep).to_arrow()  # type: ignore


def _make_struct_safe(table: pa.Table, columns: list[str]) -> pa.Array:
    """Safely create a struct array from table columns.

    Handles ChunkedArrays by combining them.
    """
    arrays = [table[c].combine_chunks() for c in columns]
    return pa.StructArray.from_arrays(arrays, names=columns)


def _create_composite_key_array(table: pa.Table, key_columns: list[str]) -> pa.Array:
    """Create a StructArray representing composite keys for efficient comparison.

    Handles ChunkedArrays by combining them. Uses pa.StructArray.from_arrays()
    to stay in Arrow space.

    Args:
        table: PyArrow table containing the key columns.
        key_columns: List of column names to include in the composite key.

    Returns:
        A StructArray where each element represents a composite key.
    """
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    # Ensure all key columns exist
    missing = [c for c in key_columns if c not in table.column_names]
    if missing:
        raise KeyError(f"Key columns not found in table: {missing}")

    # Combine chunks for each key column and create StructArray
    # This keeps operations in Arrow space for efficient comparison
    try:
        if len(key_columns) == 1:
            return table[key_columns[0]].combine_chunks()

        arrays = [table[c].combine_chunks() for c in key_columns]
        return pa.StructArray.from_arrays(arrays, names=key_columns)
    except Exception as e:
        logger.error("Failed to create composite key array: %s", e)
        raise TypeError(
            f"Failed to create composite key from columns {key_columns}. "
            f"Ensure columns have compatible types for StructArray creation. "
            f"Error: {e}"
        )


def _create_fallback_key_array(table: pa.Table, key_columns: list[str]) -> pa.Array:
    """Create an efficient representation of composite keys as a fallback.

    This is used when StructArray or Join operations fail. It prefers
    memory-efficient binary views to avoid expensive string conversions.

    Args:
        table: PyArrow table containing the key columns.
        key_columns: List of column names to include in the composite key.

    Returns:
        An array where each element represents a composite key.
    """
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    binary_cols = []
    for col_name in key_columns:
        col = table.column(col_name).combine_chunks()
        t = col.type

        # Performance optimization: Use zero-copy binary view for fixed-width types
        # instead of casting to strings.
        try:
            # Check if type has bit_width attribute and is a fixed-width type
            has_bit_width = False
            bit_width_val = 0
            try:
                bit_width_val = t.bit_width
                has_bit_width = True
            except (AttributeError, ValueError):
                has_bit_width = False

            if (
                has_bit_width
                and bit_width_val > 0
                and (
                    pa.types.is_integer(t)
                    or pa.types.is_floating(t)
                    or pa.types.is_timestamp(t)
                    or pa.types.is_duration(t)
                    or pa.types.is_date(t)
                )
            ):
                # zero-copy view as binary, then cast to variable binary for join compatibility
                bin_col = pc.cast(col.view(pa.binary(bit_width_val // 8)), pa.binary())
            else:
                # Fallback to binary cast for others (e.g. strings are already binary-compatible)
                bin_col = pc.cast(col, pa.binary())
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            # Last resort: cast to string then binary
            bin_col = pc.cast(pc.cast(col, pa.string()), pa.binary())

        # Fill nulls with a fixed binary marker to ensure they are tracked
        bin_col = pc.fill_null(bin_col, b"__NULL__")
        binary_cols.append(bin_col)

    if len(binary_cols) == 1:
        return binary_cols[0]

    # Join multiple binary keys with a delimiter
    return pc.binary_join_element_wise(*binary_cols, b"\x1f")


# Alias for backward compatibility and internal consistency
# Deprecated: Use _create_fallback_key_array directly
def _create_string_key_array(*args, **kwargs):
    """Deprecated alias for _create_fallback_key_array.

    This alias is deprecated and will be removed in a future version.
    Use _create_fallback_key_array directly instead.
    """
    import warnings

    warnings.warn(
        "_create_string_key_array is deprecated and will be removed in a future version. "
        "Use _create_fallback_key_array directly instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _create_fallback_key_array(*args, **kwargs)


def _filter_by_key_membership(
    table: pa.Table,
    key_columns: list[str],
    reference_keys: pa.Table,
    keep_matches: bool = True,
) -> pa.Table:
    """Filter table rows based on key membership using PyArrow joins.

    Uses pa.Table.join() with join_type="semi" for matches, "anti" for non-matches.
    Avoids to_pylist() and stays in Arrow space for multi-column keys.
    Falls back to efficient binary keys if native join fails.

    Args:
        table: Table to filter.
        key_columns: List of column names to use as keys.
        reference_keys: Table containing the keys to match against.
        keep_matches: If True, keep rows present in reference_keys (semi-join).
            If False, keep rows NOT present in reference_keys (anti-join).

    Returns:
        Filtered PyArrow Table.
    """
    if not key_columns:
        return table

    try:
        # We only need the key columns from reference_keys for the join
        ref_keys_only = reference_keys.select(key_columns)

        # Perform the join. PyArrow joins handle multi-column keys natively.
        join_type = "left semi" if keep_matches else "left anti"
        return table.join(ref_keys_only, keys=key_columns, join_type=join_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowKeyError) as e:
        logger.warning(
            "Primary join approach failed, falling back to efficient binary keys. "
            "This can happen with heterogeneous type combinations. Error: %s",
            e,
        )

        # Fallback mechanism using efficient binary keys
        table_keys = _create_fallback_key_array(table, key_columns)
        ref_keys = _create_fallback_key_array(reference_keys, key_columns)

        # Use is_in for filtering. value_set must be an array or chunked array.
        mask = pc.is_in(table_keys, value_set=ref_keys)
        if not keep_matches:
            mask = pc.invert(mask)

        return table.filter(mask)


def _vectorized_multi_key_deduplication(
    table: pa.Table, key_columns: list[str]
) -> tuple[pa.Table, dict[str, Any]]:
    """Implement streaming deduplication using AdaptiveKeyTracker for memory-bounded key tracking.

    Args:
        table: Table to deduplicate.
        key_columns: List of column names to use as keys.

    Returns:
        Tuple of (Deduplicated PyArrow Table, Quality Metrics).
    """
    if not key_columns:
        # If no keys specified, use all columns
        key_columns = table.column_names

    tracker = AdaptiveKeyTracker()
    result_batches = []

    logger.debug("Starting vectorized deduplication on %d columns", len(key_columns))

    # Process each batch (chunk) of the table
    for batch_idx, batch in enumerate(table.to_batches()):
        chunk_table = pa.Table.from_batches([batch])

        # 1. Deduplicate within the chunk itself first
        if hasattr(chunk_table, "drop_duplicates"):
            chunk_unique = chunk_table.drop_duplicates(subset=key_columns)
        else:
            chunk_unique = _table_drop_duplicates(chunk_table, subset=key_columns)

        # 2. Extract keys for checking using efficient representation
        # This avoids materializing large tuples of strings/objects.
        key_array = _create_fallback_key_array(chunk_unique, key_columns)
        keys = key_array.to_pylist()

        keep_indices = []
        for i, key in enumerate(keys):
            if key not in tracker:
                tracker.add(key)
                keep_indices.append(i)

        if keep_indices:
            if len(keep_indices) == chunk_unique.num_rows:
                result_batches.extend(chunk_unique.to_batches())
            else:
                # Take only new rows
                new_rows = chunk_unique.take(pa.array(keep_indices))
                result_batches.extend(new_rows.to_batches())

        if (batch_idx + 1) % 10 == 0:
            logger.debug(
                "Processed %d batches, tracker stats: %s",
                batch_idx + 1,
                tracker.get_metrics(),
            )

    if not result_batches:
        return table.schema.empty_table(), tracker.get_metrics()

    return pa.Table.from_batches(result_batches), tracker.get_metrics()


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
    import uuid

    from fsspeckit.core.maintenance import MaintenanceStats, plan_compaction_groups

    fs = filesystem or fsspec_filesystem("file")

    # Get dataset stats using shared logic
    stats = collect_dataset_stats_pyarrow(
        path=path, filesystem=fs, partition_filter=partition_filter
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
        for file_info in group.files:
            file_path = file_info.path
            table = pq.read_table(
                file_path,
                filesystem=fs,
            )
            tables.append(table)

        # Concatenate tables
        if len(tables) > 1:
            combined = pa.concat_tables(tables, promote_options="permissive")
        else:
            combined = tables[0]

        # Write to output file
        output_path = f"{path.rstrip('/')}/compacted-{uuid.uuid4().hex[:16]}.parquet"
        pq.write_table(
            combined,
            output_path,
            filesystem=fs,
            compression=compression or "snappy",
        )

    # Remove original files
    for group in groups:
        for file_info in group.files:
            fs.rm(file_info.path)

    return planned_stats.to_dict()


def optimize_parquet_dataset_pyarrow(
    path: str,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    deduplicate_key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    filesystem: AbstractFileSystem | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """Optimize a parquet dataset with optional deduplication.

    This function combines deduplication (if requested) with compaction for
    comprehensive dataset optimization. It's particularly useful after many
    small write operations have created a large number of small files with duplicates.

    Args:
        path: Dataset root directory
        target_mb_per_file: Target size per file in MB
        target_rows_per_file: Target rows per file
        partition_filter: Optional partition filters
        compression: Compression codec to use
        deduplicate_key_columns: Optional key columns for deduplication before optimization
        dedup_order_by: Columns to order by for deduplication
        filesystem: Optional filesystem instance
        verbose: Print progress information

    Returns:
        Optimization statistics

    Example:
        ```python
        # Optimization with deduplication
        stats = optimize_parquet_dataset_pyarrow(
            "dataset/",
            target_mb_per_file=64,
            compression="zstd",
            deduplicate_key_columns=["id", "timestamp"],
            dedup_order_by=["-timestamp"],
        )
        print(f"Optimized dataset with deduplication")
        ```
    """
    # Perform deduplication first if requested
    if deduplicate_key_columns is not None:
        deduplicate_parquet_dataset_pyarrow(
            path=path,
            key_columns=deduplicate_key_columns,
            dedup_order_by=dedup_order_by,
            partition_filter=partition_filter,
            compression=compression,
            filesystem=filesystem,
            verbose=verbose,
        )

    # Use compaction for optimization
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


def deduplicate_parquet_dataset_pyarrow(
    path: str,
    *,
    key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    dry_run: bool = False,
    filesystem: AbstractFileSystem | None = None,
    verbose: bool = False,
    chunk_size_rows: int = 1_000_000,
    max_memory_mb: int = 2048,
    enable_progress: bool = True,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Deduplicate an existing parquet dataset using PyArrow.

    This method removes duplicate rows from an existing parquet dataset,
    supporting both key-based deduplication and exact duplicate removal.
    It can process datasets larger than memory using chunked processing.

    Args:
        path: Dataset path
        key_columns: Optional key columns for deduplication.
            If provided, keeps one row per key combination.
            If None, removes exact duplicate rows across all columns.
        dedup_order_by: Columns to order by for selecting which
            record to keep when duplicates are found. Defaults to key_columns.
        partition_filter: Optional partition filters to limit scope
        compression: Output compression codec
        dry_run: Whether to perform a dry run (return plan without execution)
        filesystem: Optional filesystem instance
        verbose: Print progress information
        chunk_size_rows: Number of rows per chunk for large datasets.
        max_memory_mb: Peak memory limit in MB.
        enable_progress: Whether to show progress updates.
        progress_callback: Optional callback for progress updates.

    Returns:
        Dictionary containing deduplication statistics and performance metrics.

    Raises:
        ValueError: If key_columns is empty when provided
        FileNotFoundError: If dataset path doesn't exist
        MemoryError: If memory limit is exceeded during processing
    """
    from fsspeckit.core.maintenance import plan_deduplication_groups

    monitor = PerformanceMonitor(max_pyarrow_mb=max_memory_mb)
    monitor.start_op("initialization")

    # Validate inputs
    if key_columns is not None and not key_columns:
        raise ValueError("key_columns cannot be empty when provided")

    # Normalize parameters
    normalized_key_columns: list[str] | None = None
    if key_columns is not None:
        normalized_key_columns = _normalize_key_columns(key_columns)

    normalized_dedup_order_by: list[str] | None = None
    if dedup_order_by is not None:
        normalized_dedup_order_by = _normalize_key_columns(dedup_order_by)
    elif normalized_key_columns is not None:
        normalized_dedup_order_by = normalized_key_columns

    # Get filesystem
    fs = filesystem or fsspec_filesystem("file")

    # Ensure compression is never None
    final_compression = compression or "snappy"

    pa_filesystem = _ensure_pyarrow_filesystem(fs)

    monitor.start_op("planning")
    # Collect dataset stats and plan deduplication
    stats = collect_dataset_stats_pyarrow(
        path=path, filesystem=fs, partition_filter=partition_filter
    )
    files = stats["files"]
    total_bytes_before = stats["total_bytes"]
    total_rows_before = stats["total_rows"]

    # Plan deduplication groups
    plan_result = plan_deduplication_groups(
        file_infos=files,
        key_columns=normalized_key_columns,
        dedup_order_by=normalized_dedup_order_by,
    )

    groups = plan_result["groups"]
    planned_stats = plan_result["planned_stats"]

    # Update planned stats with compression info
    planned_stats.compression_codec = compression
    planned_stats.dry_run = dry_run

    # If dry run, return the plan
    if dry_run:
        result = planned_stats.to_dict()
        result["planned_groups"] = [group.file_paths() for group in groups]
        result["performance_metrics"] = monitor.get_metrics(
            total_rows_before, total_rows_before, total_bytes_before
        )
        return result

    # Execute deduplication
    if not groups:
        logger.info("No files found to deduplicate in %s", path)
        return planned_stats.to_dict()

    logger.info(
        "Starting deduplication of %d files in %d groups", len(files), len(groups)
    )

    # Process each group
    total_deduplicated_rows = 0

    for group_idx, group in enumerate(groups):
        logger.debug(
            "Processing group %d/%d (%d files, %d rows)",
            group_idx + 1,
            len(groups),
            len(group.files),
            group.total_rows,
        )
        monitor.files_processed += len(group.files)
        group_total_rows = group.total_rows
        group_size_bytes = group.total_size_bytes

        # Decide whether to use chunked processing
        use_chunked = (group_total_rows > chunk_size_rows) or (
            group_size_bytes > max_memory_mb * 1024 * 1024 * 0.7
        )

        if not use_chunked:
            monitor.start_op("in_memory_processing")
            # Efficient in-memory logic for small groups
            tables = []
            for file_info in group.files:
                table = pq.read_table(file_info.path, filesystem=pa_filesystem)
                tables.append(table)

            combined = (
                pa.concat_tables(tables, promote_options="permissive")
                if len(tables) > 1
                else tables[0]
            )
            original_count = combined.num_rows

            if normalized_key_columns:
                if (
                    normalized_dedup_order_by
                    and normalized_dedup_order_by != normalized_key_columns
                ):
                    sort_keys = []
                    for col in normalized_dedup_order_by:
                        if col.startswith("-"):
                            sort_keys.append((col[1:], "descending"))
                        else:
                            sort_keys.append((col, "ascending"))
                    combined = combined.sort_by(sort_keys)
                    deduped = _table_drop_duplicates(
                        combined, subset=normalized_key_columns
                    )
                else:
                    deduped = _table_drop_duplicates(
                        combined, subset=normalized_key_columns
                    )
            else:
                deduped = _table_drop_duplicates(combined)

            deduped_count = deduped.num_rows
            total_deduplicated_rows += original_count - deduped_count

            monitor.start_op("writing")
            output_path = group.files[0].path
            pq.write_table(
                deduped,
                output_path,
                filesystem=pa_filesystem,
                compression=final_compression,
            )

            # Remove remaining files
            for file_info in group.files[1:]:
                fs.rm(file_info.path)
        else:
            # Optimized Chunked Processing
            monitor.start_op("chunked_processing_setup")
            if verbose:
                logger.info(
                    "Using optimized chunked processing for group with %d rows",
                    group_total_rows,
                )

            group_files = [f.path for f in group.files]
            group_dataset = ds.dataset(group_files, filesystem=pa_filesystem)
            temp_path = f"{group.files[0].path}.tmp.{uuid.uuid4().hex}"

            # Prepare sort metadata for streaming deduplication if needed
            sort_configs = []
            order_cols = []
            if normalized_dedup_order_by:
                for col in normalized_dedup_order_by:
                    if col.startswith("-"):
                        sort_configs.append(True)  # descending
                        order_cols.append(col[1:])
                    else:
                        sort_configs.append(False)  # ascending
                        order_cols.append(col)

            if (
                not normalized_dedup_order_by
                or normalized_dedup_order_by == normalized_key_columns
            ):
                # One-pass streaming deduplication (keep first seen)
                monitor.start_op("streaming_deduplication")
                logger.debug(
                    "One-pass streaming deduplication for group %d", group_idx + 1
                )
                seen_keys = None  # Will be a pa.Table
                rows_written = 0
                with pq.ParquetWriter(
                    temp_path,
                    group_dataset.schema,
                    filesystem=pa_filesystem,
                    compression=final_compression,
                ) as writer:
                    for chunk in process_in_chunks(
                        group_dataset,
                        chunk_size_rows,
                        max_memory_mb,
                        enable_progress,
                        progress_callback,
                        memory_monitor=monitor._memory_monitor,
                    ):
                        monitor.chunks_processed += 1
                        # Deduplicate within chunk first
                        chunk_deduped = _table_drop_duplicates(
                            chunk, subset=normalized_key_columns
                        )

                        # Extract keys
                        k_cols = (
                            list(normalized_key_columns)
                            if normalized_key_columns is not None
                            else chunk_deduped.column_names
                        )

                        # Use vectorized deduplication against already seen keys
                        if seen_keys is None:
                            final_chunk = chunk_deduped
                            seen_keys = chunk_deduped.select(k_cols)
                        else:
                            final_chunk = _filter_by_key_membership(
                                chunk_deduped, k_cols, seen_keys, keep_matches=False
                            )
                            if final_chunk.num_rows > 0:
                                # Update seen keys with new unique keys from this chunk
                                seen_keys = pa.concat_tables(
                                    [seen_keys, final_chunk.select(k_cols)]
                                )

                        if final_chunk.num_rows > 0:
                            writer.write_table(final_chunk)
                            rows_written += final_chunk.num_rows

                total_deduplicated_rows += group_total_rows - rows_written
            else:
                # Two-pass optimized streaming deduplication for custom ordering
                monitor.start_op("chunked_pass1_find_best")
                best_values_table: pa.Table | None = None
                aggs = []
                for i, col in enumerate(order_cols):
                    op = "max" if sort_configs[i] else "min"
                    aggs.append((col, op))

                k_cols = (
                    list(normalized_key_columns)
                    if normalized_key_columns is not None
                    else []
                )

                for chunk in process_in_chunks(
                    group_dataset,
                    chunk_size_rows,
                    max_memory_mb,
                    enable_progress,
                    progress_callback,
                    memory_monitor=monitor._memory_monitor,
                ):
                    monitor.chunks_processed += 1
                    # Group by keys and find best values in this chunk
                    chunk_best = chunk.group_by(normalized_key_columns).aggregate(aggs)
                    # Rename back to original names immediately for consistent re-aggregation
                    chunk_best = chunk_best.rename_columns(k_cols + order_cols)

                    if best_values_table is None:
                        best_values_table = chunk_best
                    else:
                        # Combine with previous bests and re-aggregate
                        combined = pa.concat_tables([best_values_table, chunk_best])
                        agg_table = combined.group_by(normalized_key_columns).aggregate(
                            aggs
                        )
                        best_values_table = agg_table.rename_columns(
                            k_cols + order_cols
                        )

                if best_values_table is None:
                    continue

                # Pass 2: Write rows that match best values
                monitor.start_op("chunked_pass2_filter_and_write")
                # best_values_table is guaranteed not None here due to check above
                assert best_values_table is not None

                assert best_values_table is not None
                seen_keys = None  # Will be a pa.Table
                rows_written = 0
                with pq.ParquetWriter(
                    temp_path,
                    group_dataset.schema,
                    filesystem=pa_filesystem,
                    compression=final_compression,
                ) as writer:
                    for chunk in process_in_chunks(
                        group_dataset,
                        chunk_size_rows,
                        max_memory_mb,
                        enable_progress,
                        progress_callback,
                        memory_monitor=monitor._memory_monitor,
                    ):
                        monitor.chunks_processed += 1
                        # Inner join with best_values_table to keep only 'best' rows
                        filtered_chunk = chunk.join(
                            best_values_table,
                            keys=k_cols + order_cols,
                            join_type="inner",
                        )

                        # Handle potential duplicates within 'best' rows and across chunks
                        curr_k_cols = k_cols if k_cols else filtered_chunk.column_names

                        # Use vectorized deduplication against already seen keys
                        if seen_keys is None:
                            final_chunk = filtered_chunk
                            seen_keys = filtered_chunk.select(curr_k_cols)
                        else:
                            final_chunk = _filter_by_key_membership(
                                filtered_chunk,
                                curr_k_cols,
                                seen_keys,
                                keep_matches=False,
                            )
                            if final_chunk.num_rows > 0:
                                # Update seen keys with new unique keys from this chunk
                                seen_keys = pa.concat_tables(
                                    [seen_keys, final_chunk.select(curr_k_cols)]
                                )

                        if final_chunk.num_rows > 0:
                            writer.write_table(final_chunk)
                            rows_written += final_chunk.num_rows

                total_deduplicated_rows += group_total_rows - rows_written

            # Cleanup: replace original files with deduplicated temp file
            monitor.start_op("cleanup")
            for f_info in group.files:
                fs.rm(f_info.path)
            fs.mv(temp_path, group.files[0].path)

    monitor.end_op()
    # Update final statistics
    final_stats = planned_stats.to_dict()
    final_stats["deduplicated_rows"] = total_deduplicated_rows
    final_stats["total_rows_before"] = total_rows_before
    final_stats["total_rows_after"] = total_rows_before - total_deduplicated_rows

    total_rows_after = final_stats["total_rows_after"]
    final_stats["performance_metrics"] = monitor.get_metrics(
        total_rows_before, total_rows_after, total_bytes_before
    )

    if verbose:
        logger.info("Deduplication complete: %s", final_stats)

    return final_stats


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


def process_in_chunks(
    dataset: ds.Dataset | pa.Table,
    chunk_size_rows: int = 1_000_000,
    max_memory_mb: int = 2048,
    enable_progress: bool = True,
    progress_callback: Callable[[int, int], None] | None = None,
    memory_monitor: MemoryMonitor | None = None,
) -> Iterable[pa.Table]:
    """Process a dataset or table in configurable chunks to avoid memory overflow.

    This function enables processing of datasets larger than available system memory
    by yielding data in manageable chunks. It monitors memory usage and enforces
    limits to prevent OOM errors.

    Args:
        dataset: PyArrow Dataset or Table to process.
        chunk_size_rows: Number of rows per chunk. Defaults to 1,000,000.
        max_memory_mb: Peak memory limit in MB. Defaults to 2048.
        enable_progress: Whether to track and report progress. Defaults to True.
        progress_callback: Optional callback function(rows_processed, total_rows).
        memory_monitor: Optional MemoryMonitor instance. If None, a new one is created.

    Yields:
        pa.Table: A chunk of data as a PyArrow Table.

    Raises:
        MemoryError: If peak memory usage exceeds max_memory_mb.
    """
    total_rows = (
        dataset.num_rows if isinstance(dataset, pa.Table) else dataset.count_rows()
    )
    rows_processed = 0

    # Initialize memory monitor
    monitor = memory_monitor or MemoryMonitor(max_pyarrow_mb=max_memory_mb)

    if isinstance(dataset, ds.Dataset):
        # For datasets, use a scanner with specified batch size
        scanner = dataset.scanner(batch_size=chunk_size_rows)
        batches = scanner.to_batches()
    else:
        # For tables, slice into chunks
        def _table_iterator():
            for i in range(0, total_rows, chunk_size_rows):
                yield dataset.slice(i, min(chunk_size_rows, total_rows - i))

        batches = _table_iterator()

    for batch in batches:
        # Ensure we have a Table for consistent processing
        chunk = (
            pa.Table.from_batches([batch])
            if isinstance(batch, pa.RecordBatch)
            else batch
        )

        # Monitor memory pressure
        pressure = monitor.check_memory_pressure()
        if pressure == MemoryPressureLevel.EMERGENCY:
            status = monitor.get_detailed_status()
            logger.error(f"Memory limit exceeded (EMERGENCY): {status}")
            raise MemoryError(f"Peak memory usage exceeded limit: {status}")

        yield chunk

        rows_processed += chunk.num_rows
        if enable_progress:
            if progress_callback:
                progress_callback(rows_processed, total_rows)
            else:
                logger.debug(
                    "Processed %d/%d rows (%.1f%%)",
                    rows_processed,
                    total_rows,
                    (rows_processed / total_rows) * 100 if total_rows > 0 else 100,
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
            compression=compression or "snappy",
        )
        written_files.append(file_path)

    return written_files


def merge_upsert_pyarrow(
    existing: pa.Table | ds.Dataset,
    source: pa.Table,
    key_columns: list[str],
    chunk_size: int = 100_000,
    max_memory_mb: int = 1024,
    memory_monitor: MemoryMonitor | None = None,
    writer: pq.ParquetWriter | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pa.Table | None:
    """Perform UPSERT merge using PyArrow operations with streaming support.

    Args:
        existing: Existing data (Table or Dataset)
        source: Source data to merge (Table)
        key_columns: Columns to use as merge keys
        chunk_size: Number of rows per processing chunk
        max_memory_mb: Maximum PyArrow memory to use in MB
        memory_monitor: Optional MemoryMonitor for enhanced tracking
        writer: Optional ParquetWriter for streaming output
        progress_callback: Optional progress callback

    Returns:
        Merged Table if writer is None, else None
    """
    import pyarrow.compute as pc

    from fsspeckit.common.optional import _import_pyarrow

    # Align source schema with existing
    existing_schema = existing.schema
    pa_mod = _import_pyarrow()
    source_aligned = source
    for field in existing_schema:
        if field.name not in source_aligned.column_names:
            source_aligned = source_aligned.append_column(
                field.name, pa_mod.nulls(len(source_aligned), type=field.type)
            )
    source_aligned = source_aligned.select(existing_schema.names).cast(existing_schema)

    # Prepare source keys for filtering
    use_string_fallback = False
    if len(key_columns) == 1:
        source_keys = source_aligned.column(key_columns[0])
    else:
        try:
            # Primary approach: StructArray for vectorized matching
            source_keys = _create_composite_key_array(source_aligned, key_columns)
            # Test if is_in works with this StructArray
            if source_keys.length() > 0:
                pc.is_in(source_keys.slice(0, 1), value_set=source_keys.slice(0, 1))
        except Exception:
            use_string_fallback = True
            source_keys = _create_fallback_key_array(source_aligned, key_columns)

    def _process_chunk(chunk: pa.Table) -> pa.Table:
        if len(key_columns) == 1:
            chunk_keys = chunk.column(key_columns[0])
        elif use_string_fallback:
            chunk_keys = _create_fallback_key_array(chunk, key_columns)
        else:
            chunk_keys = _create_composite_key_array(chunk, key_columns)

        mask = pc.invert(pc.is_in(chunk_keys, source_keys))
        return chunk.filter(mask)

    if writer:
        # Streaming mode
        for chunk in process_in_chunks(
            existing,
            chunk_size,
            max_memory_mb,
            progress_callback=progress_callback,
            memory_monitor=memory_monitor,
        ):
            filtered = _process_chunk(chunk)
            if filtered.num_rows > 0:
                writer.write_table(filtered)
        writer.write_table(source_aligned)
        return None
    else:
        # Batch mode
        if isinstance(existing, pa.Table) and existing.num_rows <= chunk_size:
            filtered_existing = _process_chunk(existing)
        else:
            chunks = []
            for chunk in process_in_chunks(
                existing,
                chunk_size,
                max_memory_mb,
                progress_callback=progress_callback,
                memory_monitor=memory_monitor,
            ):
                chunks.append(_process_chunk(chunk))
            filtered_existing = pa_mod.concat_tables(
                chunks, promote_options="permissive"
            )
        return pa_mod.concat_tables(
            [filtered_existing, source_aligned], promote_options="permissive"
        )


def merge_update_pyarrow(
    existing: pa.Table | ds.Dataset,
    source: pa.Table,
    key_columns: list[str],
    chunk_size: int = 100_000,
    max_memory_mb: int = 1024,
    memory_monitor: MemoryMonitor | None = None,
    writer: pq.ParquetWriter | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pa.Table | None:
    """Perform UPDATE merge using PyArrow operations with streaming support.

    Args:
        existing: Existing data (Table or Dataset)
        source: Source data to merge (Table)
        key_columns: Columns to use as merge keys
        chunk_size: Number of rows per processing chunk
        max_memory_mb: Maximum PyArrow memory to use in MB
        memory_monitor: Optional MemoryMonitor for enhanced tracking
        writer: Optional ParquetWriter for streaming output
        progress_callback: Optional progress callback

    Returns:
        Merged Table if writer is None, else None
    """
    import pyarrow.compute as pc

    from fsspeckit.common.optional import _import_pyarrow

    # Align source schema with existing
    existing_schema = existing.schema
    pa_mod = _import_pyarrow()
    source_aligned = source
    for field in existing_schema:
        if field.name not in source_aligned.column_names:
            source_aligned = source_aligned.append_column(
                field.name, pa_mod.nulls(len(source_aligned), type=field.type)
            )
    source_aligned = source_aligned.select(existing_schema.names).cast(existing_schema)

    # Pass 1: find which source rows are in existing
    existing_keys_table = None
    for chunk in process_in_chunks(
        existing,
        chunk_size,
        max_memory_mb,
        progress_callback=progress_callback,
        memory_monitor=memory_monitor,
    ):
        chunk_keys = chunk.select(key_columns)
        # Deduplicate within chunk to keep existing_keys_table smaller
        chunk_keys = _table_drop_duplicates(chunk_keys, subset=key_columns)

        if existing_keys_table is None:
            existing_keys_table = chunk_keys
        else:
            # Only add keys we haven't seen yet
            new_keys = _filter_by_key_membership(
                chunk_keys, key_columns, existing_keys_table, keep_matches=False
            )
            if new_keys.num_rows > 0:
                existing_keys_table = pa_mod.concat_tables(
                    [existing_keys_table, new_keys]
                )

    # Now filter source to keep only rows that exist in 'existing'
    if existing_keys_table is None:
        source_in_existing = source_aligned.schema.empty_table()
    else:
        source_in_existing = _filter_by_key_membership(
            source_aligned, key_columns, existing_keys_table, keep_matches=True
        )

    def _process_chunk_existing(chunk: pa.Table) -> pa.Table:
        # Rows in existing NOT in source
        return _filter_by_key_membership(
            chunk, key_columns, source_aligned, keep_matches=False
        )

    if writer:
        # Pass 2: Streaming output
        for chunk in process_in_chunks(
            existing,
            chunk_size,
            max_memory_mb,
            progress_callback=progress_callback,
            memory_monitor=memory_monitor,
        ):
            filtered = _process_chunk_existing(chunk)
            if filtered.num_rows > 0:
                writer.write_table(filtered)
        writer.write_table(source_in_existing)
        return None
    else:
        # Pass 2: Batch mode
        chunks = []
        for chunk in process_in_chunks(
            existing,
            chunk_size,
            max_memory_mb,
            progress_callback=progress_callback,
            memory_monitor=memory_monitor,
        ):
            chunks.append(_process_chunk_existing(chunk))
        filtered_existing = pa_mod.concat_tables(chunks, promote_options="permissive")
        return pa_mod.concat_tables(
            [filtered_existing, source_in_existing], promote_options="permissive"
        )
