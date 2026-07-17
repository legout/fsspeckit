"""
Backend-neutral maintenance layer for parquet dataset operations.

This module provides shared functionality for dataset discovery, statistics,
and grouping algorithms used by both DuckDB and PyArrow maintenance operations.
It serves as the authoritative implementation for maintenance planning,
ensuring consistent behavior across different backends.

Key responsibilities:
1. Dataset discovery and file-level statistics
2. Compaction grouping algorithms with streaming execution
3. Optimization planning with z-order validation
4. Canonical statistics structures
5. Partition filtering and edge case handling

Architecture:
- Functions accept both dict format (legacy) and FileInfo objects for backward compatibility
- All planning functions return structured results with canonical MaintenanceStats
- Backend implementations delegate to this core for consistent behavior
- Streaming design avoids materializing entire datasets in memory

Core components:
- FileInfo: Canonical file information with validation
- MaintenanceStats: Canonical statistics structure across backends
- CompactionGroup: Logical grouping of files for processing
- collect_dataset_stats: Dataset discovery with partition filtering
- plan_compaction_groups: Shared compaction planning algorithm
- plan_optimize_groups: Shared optimization planning with z-order validation

Usage:
Backend functions should delegate to this module rather than implementing
their own discovery and planning logic. This ensures that DuckDB and PyArrow
produce identical grouping decisions and statistics structures.
"""

from __future__ import annotations

import os
import posixpath
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem

from fsspeckit.common.logging import get_logger

logger: Any = get_logger(__name__)


@dataclass(frozen=True)
class FileInfo:
    """Information about a single parquet file with validation.

    This canonical data structure represents file metadata across all backends.
    It enables consistent file information handling and size-based planning.

    Attributes:
        path: File path relative to the dataset root.
        size_bytes: File size in bytes; must be >= 0.
        num_rows: Number of rows in the file; must be >= 0.

    Note:
        The size_bytes and num_rows values are validated to be non-negative.
        This class is used throughout the maintenance planning pipeline
        for consistent file metadata representation.
    """

    path: str
    size_bytes: int
    num_rows: int

    def __post_init__(self) -> None:
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be >= 0")
        if self.num_rows < 0:
            raise ValueError("num_rows must be >= 0")


@dataclass
class MaintenanceStats:
    """Canonical statistics structure for maintenance operations.

    This dataclass provides the authoritative statistics format for all maintenance
    operations across DuckDB and PyArrow backends. It ensures consistent reporting
    and enables unified testing and validation.

    Attributes:
        before_file_count: Number of files before the operation.
        after_file_count: Number of files after the operation.
        before_total_bytes: Total bytes before the operation.
        after_total_bytes: Total bytes after the operation.
        compacted_file_count: Number of files that were compacted/rewritten.
        rewritten_bytes: Total bytes rewritten during the operation.
        compression_codec: Compression codec used (None if unchanged).
        dry_run: Whether this was a dry run operation.
        zorder_columns: Z-order columns used (for optimization operations).
        planned_groups: File groupings planned during dry run.

    Note:
        All numeric fields are validated to be non-negative. The to_dict() method
        provides backward compatibility with existing code expecting dictionary format.
    """

    before_file_count: int
    after_file_count: int
    before_total_bytes: int
    after_total_bytes: int
    compacted_file_count: int
    rewritten_bytes: int
    compression_codec: str | None = None
    dry_run: bool = False

    # Optional fields for specific operations
    zorder_columns: list[str] | None = None
    planned_groups: list[list[str]] | None = None
    key_columns: list[str] | None = None
    dedup_order_by: list[str] | None = None
    deduplicated_rows: int | None = None

    def __post_init__(self) -> None:
        if self.before_file_count < 0:
            raise ValueError("before_file_count must be >= 0")
        if self.after_file_count < 0:
            raise ValueError("after_file_count must be >= 0")
        if self.before_total_bytes < 0:
            raise ValueError("before_total_bytes must be >= 0")
        if self.after_total_bytes < 0:
            raise ValueError("after_total_bytes must be >= 0")
        if self.compacted_file_count < 0:
            raise ValueError("compacted_file_count must be >= 0")
        if self.rewritten_bytes < 0:
            raise ValueError("rewritten_bytes must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format for backward compatibility."""
        result: dict[str, Any] = {
            "before_file_count": self.before_file_count,
            "after_file_count": self.after_file_count,
            "before_total_bytes": self.before_total_bytes,
            "after_total_bytes": self.after_total_bytes,
            "compacted_file_count": self.compacted_file_count,
            "rewritten_bytes": self.rewritten_bytes,
            "compression_codec": self.compression_codec,
            "dry_run": self.dry_run,
        }

        if self.zorder_columns is not None:
            result["zorder_columns"] = self.zorder_columns
        if self.planned_groups is not None:
            result["planned_groups"] = self.planned_groups

        return result


@dataclass(frozen=True)
class CompactionGroup:
    """A group of files to be compacted or optimized together.

    This dataclass represents a logical grouping of files that will be processed
    together during maintenance operations. It enables streaming execution by
    bounding the amount of data processed at once.

    Attributes:
        files: Tuple of FileInfo objects in this group.

    Note:
        Must contain at least one file. The total_size_bytes and total_rows
        are computed on demand as properties and used for planning decisions.
        This structure enables per-group streaming processing without
        materializing entire datasets.
    """

    files: tuple[FileInfo, ...]

    def __post_init__(self) -> None:
        if not self.files:
            raise ValueError("CompactionGroup must contain at least one file")

    @property
    def total_size_bytes(self) -> int:
        return sum(f.size_bytes for f in self.files)

    @property
    def total_rows(self) -> int:
        return sum(f.num_rows for f in self.files)

    @property
    def file_count(self) -> int:
        return len(self.files)

    def file_paths(self) -> list[str]:
        return [f.path for f in self.files]


def collect_dataset_stats(
    path: str,
    filesystem: AbstractFileSystem | None = None,
    partition_filter: list[str] | None = None,
) -> dict[str, Any]:
    """
    Collect file-level statistics for a parquet dataset.

    This function walks the given dataset directory on the provided filesystem,
    discovers parquet files (recursively), and returns basic statistics.

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
    """
    import pyarrow.parquet as pq

    fs = filesystem or fsspec_filesystem("file")

    if not fs.exists(path):
        raise FileNotFoundError(f"Dataset path '{path}' does not exist")

    root = Path(path)

    # Discover parquet files recursively via a manual stack walk so we can
    # respect partition_filter prefixes on the logical relative path.
    files: list[str] = []
    stack: list[str] = [path]
    while stack:
        current_dir = stack.pop()
        try:
            entries = fs.ls(current_dir, detail=False)
        except (OSError, PermissionError) as e:
            logger.warning("Failed to list directory '%s': %s", current_dir, e)
            continue

        for entry in entries:
            if entry.endswith(".parquet"):
                files.append(entry)
            else:
                try:
                    if fs.isdir(entry):
                        stack.append(entry)
                except (OSError, PermissionError) as e:
                    logger.warning(
                        "Failed to check if entry '%s' is a directory: %s", entry, e
                    )
                    continue

    if partition_filter:
        normalized_filters = [p.rstrip("/") for p in partition_filter]
        filtered_files: list[str] = []
        for filename in files:
            rel = Path(filename).relative_to(root).as_posix()
            if any(rel.startswith(prefix) for prefix in normalized_filters):
                filtered_files.append(filename)
        files = filtered_files

    if not files:
        raise FileNotFoundError(
            f"No parquet files found under '{path}' matching filter"
        )

    file_infos: list[dict[str, Any]] = []
    total_bytes = 0
    total_rows = 0

    for filename in files:
        size_bytes = 0
        try:
            info = fs.info(filename)
            if isinstance(info, dict):
                size_bytes = int(info.get("size", 0))
        except (OSError, PermissionError) as e:
            logger.warning("Failed to get file info for '%s': %s", filename, e)
            size_bytes = 0

        num_rows = 0
        try:
            with fs.open(filename, "rb") as fh:
                pf = pq.ParquetFile(fh)
                num_rows = pf.metadata.num_rows
        except (OSError, PermissionError, RuntimeError, ValueError) as e:
            # As a fallback, attempt a minimal table read to estimate rows.
            logger.debug(
                "Failed to read parquet metadata from '%s', trying fallback: %s",
                filename,
                e,
            )
            try:
                with fs.open(filename, "rb") as fh:
                    table = pq.read_table(fh)
                num_rows = table.num_rows
            except (OSError, PermissionError, RuntimeError, ValueError) as e:
                logger.debug("Fallback table read failed for '%s': %s", filename, e)
                num_rows = 0

        total_bytes += size_bytes
        total_rows += num_rows
        file_infos.append(
            {"path": filename, "size_bytes": size_bytes, "num_rows": num_rows}
        )

    return {"files": file_infos, "total_bytes": total_bytes, "total_rows": total_rows}


def plan_compaction_groups(
    file_infos: list[dict[str, Any]] | list[FileInfo],
    target_mb_per_file: int | None,
    target_rows_per_file: int | None,
) -> dict[str, Any]:
    """
    Plan compaction groups based on size and row thresholds.

    Args:
        file_infos: List of file information dictionaries or FileInfo objects.
        target_mb_per_file: Target size in megabytes per output file.
        target_rows_per_file: Target number of rows per output file.

    Returns:
        Dictionary with:
        - groups: List of CompactionGroup objects to be compacted
        - untouched_files: List of FileInfo objects not requiring compaction
        - planned_stats: MaintenanceStats object for the planned operation
        - planned_groups: List of file paths per group (for backward compatibility)

    Raises:
        ValueError: If both target_mb_per_file and target_rows_per_file are None or <= 0.
    """
    # Validate inputs
    if target_mb_per_file is None and target_rows_per_file is None:
        raise ValueError(
            "Must provide at least one of target_mb_per_file or target_rows_per_file"
        )
    if target_mb_per_file is not None and target_mb_per_file <= 0:
        raise ValueError("target_mb_per_file must be > 0")
    if target_rows_per_file is not None and target_rows_per_file <= 0:
        raise ValueError("target_rows_per_file must be > 0")

    # Convert to FileInfo objects if needed
    if file_infos and isinstance(file_infos[0], dict):
        dict_files = cast(list[dict[str, Any]], file_infos)
        files = [
            FileInfo(fi["path"], fi["size_bytes"], fi["num_rows"]) for fi in dict_files
        ]
    else:
        files = cast(list[FileInfo], file_infos)

    size_threshold_bytes = (
        target_mb_per_file * 1024 * 1024 if target_mb_per_file is not None else None
    )

    # Separate candidate files (eligible for compaction) from large files.
    candidates: list[FileInfo] = []
    large_files: list[FileInfo] = []
    for file_info in files:
        size_bytes = file_info.size_bytes
        if size_threshold_bytes is None or size_bytes < size_threshold_bytes:
            candidates.append(file_info)
        else:
            large_files.append(file_info)

    # Build groups based on thresholds.
    groups: list[list[FileInfo]] = []
    current_group: list[FileInfo] = []
    current_size = 0
    current_rows = 0

    def flush_group() -> None:
        nonlocal current_group, current_size, current_rows
        if current_group:
            groups.append(current_group)
            current_group = []
            current_size = 0
            current_rows = 0

    for file_info in sorted(candidates, key=lambda x: x.size_bytes):
        size_bytes = file_info.size_bytes
        num_rows = file_info.num_rows
        would_exceed_size = (
            size_threshold_bytes is not None
            and current_size + size_bytes > size_threshold_bytes
            and current_group
        )
        would_exceed_rows = (
            target_rows_per_file is not None
            and current_rows + num_rows > target_rows_per_file
            and current_group
        )
        if would_exceed_size or would_exceed_rows:
            flush_group()
        current_group.append(file_info)
        current_size += size_bytes
        current_rows += num_rows
    flush_group()

    # Compact multi-file groups, and also singleton groups whose single file
    # exceeds the hard row-count bound — the writer must split those so no
    # published file exceeds max_rows_per_file. Byte-size targets are advisory
    # (CONTEXT.md: "Hard row-count compaction bound") and do not force a split.
    finalized_groups: list[CompactionGroup] = [
        CompactionGroup(files=tuple(group))
        for group in groups
        if len(group) > 1
        or (
            target_rows_per_file is not None
            and group[0].num_rows > target_rows_per_file
        )
    ]

    # Calculate statistics
    before_file_count = len(files)
    before_total_bytes = sum(f.size_bytes for f in files)

    compacted_file_count = sum(len(group.files) for group in finalized_groups)
    untouched_files = large_files + [
        file_info
        for file_info in candidates
        if not any(file_info in group.files for group in finalized_groups)
    ]

    after_file_count = len(untouched_files) + len(finalized_groups)

    # Estimate after_total_bytes (assume minimal compression change for planning)
    compacted_bytes = sum(group.total_size_bytes for group in finalized_groups)
    untouched_bytes = sum(f.size_bytes for f in untouched_files)
    after_total_bytes = untouched_bytes + compacted_bytes  # Rough estimate

    rewritten_bytes = compacted_bytes

    # Create compatibility structures
    planned_groups = [group.file_paths() for group in finalized_groups]

    planned_stats = MaintenanceStats(
        before_file_count=before_file_count,
        after_file_count=after_file_count,
        before_total_bytes=before_total_bytes,
        after_total_bytes=after_total_bytes,
        compacted_file_count=compacted_file_count,
        rewritten_bytes=rewritten_bytes,
        compression_codec=None,  # Will be set by backend
        dry_run=True,
        planned_groups=planned_groups,
    )

    return {
        "groups": finalized_groups,
        "untouched_files": untouched_files,
        "planned_stats": planned_stats,
        "planned_groups": planned_groups,
    }


def plan_optimize_groups(
    file_infos: list[dict[str, Any]] | list[FileInfo],
    zorder_columns: list[str],
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    sample_schema: Any = None,
) -> dict[str, Any]:
    """
    Plan optimization groups with z-order validation.

    Args:
        file_infos: List of file information dictionaries or FileInfo objects.
        zorder_columns: List of columns to use for z-order clustering.
        target_mb_per_file: Target size in megabytes per output file.
        target_rows_per_file: Target number of rows per output file.
        sample_schema: PyArrow schema or object with column_names method for validation.
                      If None, schema validation will be skipped.

    Returns:
        Dictionary with:
        - groups: List of CompactionGroup objects to be optimized
        - untouched_files: List of FileInfo objects not requiring optimization
        - planned_stats: MaintenanceStats object for the planned operation
        - planned_groups: List of file paths per group (for backward compatibility)

    Raises:
        ValueError: If thresholds are invalid or zorder_columns is empty.
    """
    # Validate inputs
    if not zorder_columns:
        raise ValueError("zorder_columns must be a non-empty list")
    if target_mb_per_file is not None and target_mb_per_file <= 0:
        raise ValueError("target_mb_per_file must be > 0")
    if target_rows_per_file is not None and target_rows_per_file <= 0:
        raise ValueError("target_rows_per_file must be > 0")

    # Validate zorder columns against schema if provided
    if sample_schema is not None:
        try:
            available_cols = set(sample_schema.column_names)
            missing = [col for col in zorder_columns if col not in available_cols]
            if missing:
                raise ValueError(
                    f"Missing z-order columns: {', '.join(missing)}. "
                    f"Available columns: {', '.join(sorted(available_cols))}"
                )
        except AttributeError:
            # sample_schema doesn't have column_names, skip validation
            pass

    # Convert to FileInfo objects if needed
    if file_infos and isinstance(file_infos[0], dict):
        dict_files = cast(list[dict[str, Any]], file_infos)
        files = [
            FileInfo(fi["path"], fi["size_bytes"], fi["num_rows"]) for fi in dict_files
        ]
    else:
        files = cast(list[FileInfo], file_infos)

    # For optimization, we typically want to process all files unless they're
    # already large enough to be left alone
    size_threshold_bytes = (
        target_mb_per_file * 1024 * 1024 if target_mb_per_file is not None else None
    )

    # Separate candidate files from large files
    candidates: list[FileInfo] = []
    large_files: list[FileInfo] = []
    for file_info in files:
        size_bytes = file_info.size_bytes
        if size_threshold_bytes is None or size_bytes < size_threshold_bytes:
            candidates.append(file_info)
        else:
            large_files.append(file_info)

    # Group files for optimization - similar to compaction but more aggressive
    # since optimization typically rewrites all eligible files
    groups: list[list[FileInfo]] = []
    current_group: list[FileInfo] = []
    current_size = 0
    current_rows = 0

    def flush_group() -> None:
        nonlocal current_group, current_size, current_rows
        if current_group:
            groups.append(current_group)
            current_group = []
            current_size = 0
            current_rows = 0

    # Sort files for more consistent optimization
    for file_info in sorted(candidates, key=lambda x: x.size_bytes):
        size_bytes = file_info.size_bytes
        num_rows = file_info.num_rows
        would_exceed_size = (
            size_threshold_bytes is not None
            and current_size + size_bytes > size_threshold_bytes
            and current_group
        )
        would_exceed_rows = (
            target_rows_per_file is not None
            and current_rows + num_rows > target_rows_per_file
            and current_group
        )
        if would_exceed_size or would_exceed_rows:
            flush_group()
        current_group.append(file_info)
        current_size += size_bytes
        current_rows += num_rows
    flush_group()

    # Include single-file groups for optimization (unlike compaction)
    # because optimization needs to reorder all eligible files
    finalized_groups: list[CompactionGroup] = []
    for group in groups:
        if len(group) > 0:  # Include single files too
            finalized_groups.append(CompactionGroup(files=tuple(group)))

    # Calculate statistics
    before_file_count = len(files)
    before_total_bytes = sum(f.size_bytes for f in files)

    optimized_file_count = sum(len(group.files) for group in finalized_groups)
    untouched_files = large_files  # Only large files are left untouched in optimization

    after_file_count = len(untouched_files) + len(finalized_groups)

    # Estimate after_total_bytes (optimization may improve compression)
    optimized_bytes = sum(group.total_size_bytes for group in finalized_groups)
    untouched_bytes = sum(f.size_bytes for f in untouched_files)
    after_total_bytes = untouched_bytes + optimized_bytes  # Rough estimate

    rewritten_bytes = optimized_bytes

    # Create compatibility structures
    planned_groups = [group.file_paths() for group in finalized_groups]

    planned_stats = MaintenanceStats(
        before_file_count=before_file_count,
        after_file_count=after_file_count,
        before_total_bytes=before_total_bytes,
        after_total_bytes=after_total_bytes,
        compacted_file_count=optimized_file_count,
        rewritten_bytes=rewritten_bytes,
        compression_codec=None,  # Will be set by backend
        dry_run=True,
        zorder_columns=zorder_columns,
        planned_groups=planned_groups,
    )

    return {
        "groups": finalized_groups,
        "untouched_files": untouched_files,
        "planned_stats": planned_stats,
        "planned_groups": planned_groups,
    }


def plan_deduplication_groups(
    file_infos: list[dict[str, Any]] | list[FileInfo],
    key_columns: list[str] | None = None,
    dedup_order_by: list[str] | None = None,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
) -> dict[str, Any]:
    """
    Plan deduplication groups for existing parquet datasets.

    This function groups files for deduplication operations, supporting both
    key-based deduplication and exact duplicate removal. It integrates with
    the existing compaction planning to produce optimized file layouts.

    Args:
        file_infos: List of file information dictionaries or FileInfo objects
        key_columns: Optional key columns for deduplication (None for exact duplicates)
        dedup_order_by: Columns to order by for preferred record selection
        target_mb_per_file: Target size per output file
        target_rows_per_file: Target rows per output file

    Returns:
        Dictionary with:
        - groups: List of CompactionGroup objects to be processed
        - untouched_files: List of FileInfo objects not requiring processing
        - planned_stats: MaintenanceStats object for the planned operation
        - planned_groups: List of file paths per group (for backward compatibility)

    Raises:
        ValueError: If thresholds are invalid or key_columns is empty when provided
    """
    # Validate inputs
    if target_mb_per_file is not None and target_mb_per_file <= 0:
        raise ValueError("target_mb_per_file must be > 0")
    if target_rows_per_file is not None and target_rows_per_file <= 0:
        raise ValueError("target_rows_per_file must be > 0")
    if key_columns is not None and not key_columns:
        raise ValueError("key_columns cannot be empty when provided")

    # Convert to FileInfo objects if needed
    if file_infos and isinstance(file_infos[0], dict):
        dict_files = cast(list[dict[str, Any]], file_infos)
        files = [
            FileInfo(fi["path"], fi["size_bytes"], fi["num_rows"]) for fi in dict_files
        ]
    else:
        files = cast(list[FileInfo], file_infos)

    size_threshold_bytes = (
        target_mb_per_file * 1024 * 1024 if target_mb_per_file is not None else None
    )

    # For deduplication, we typically want to process all files since the goal
    # is to remove duplicates across the entire dataset
    # Only exclude files that are already large enough to be left alone
    candidates: list[FileInfo] = []
    large_files: list[FileInfo] = []
    for file_info in files:
        size_bytes = file_info.size_bytes
        if size_threshold_bytes is None or size_bytes < size_threshold_bytes:
            candidates.append(file_info)
        else:
            large_files.append(file_info)

    # Group files for deduplication - similar to optimization but more aggressive
    # since deduplication typically processes all files
    groups: list[list[FileInfo]] = []
    current_group: list[FileInfo] = []
    current_size = 0
    current_rows = 0

    def flush_group() -> None:
        nonlocal current_group, current_size, current_rows
        if current_group:
            groups.append(current_group)
            current_group = []
            current_size = 0
            current_rows = 0

    # Sort files for more consistent deduplication
    for file_info in sorted(candidates, key=lambda x: x.size_bytes):
        size_bytes = file_info.size_bytes
        num_rows = file_info.num_rows
        would_exceed_size = (
            size_threshold_bytes is not None
            and current_size + size_bytes > size_threshold_bytes
            and current_group
        )
        would_exceed_rows = (
            target_rows_per_file is not None
            and current_rows + num_rows > target_rows_per_file
            and current_group
        )
        if would_exceed_size or would_exceed_rows:
            flush_group()
        current_group.append(file_info)
        current_size += size_bytes
        current_rows += num_rows
    flush_group()

    # Include all groups for deduplication (unlike compaction which skips singletons)
    # because we need to deduplicate even single files to handle duplicates within them
    finalized_groups: list[CompactionGroup] = []
    for group in groups:
        if len(group) > 0:  # Include all groups
            finalized_groups.append(CompactionGroup(files=tuple(group)))

    # Calculate statistics
    before_file_count = len(files)
    before_total_bytes = sum(f.size_bytes for f in files)

    deduplicated_file_count = sum(len(group.files) for group in finalized_groups)
    untouched_files = large_files  # Only large files are left untouched

    after_file_count = len(untouched_files) + len(finalized_groups)

    # Estimate after_total_bytes (deduplication may reduce data size)
    deduplicated_bytes = sum(group.total_size_bytes for group in finalized_groups)
    untouched_bytes = sum(f.size_bytes for f in untouched_files)
    after_total_bytes = untouched_bytes + deduplicated_bytes  # Rough estimate

    rewritten_bytes = deduplicated_bytes

    # Create compatibility structures
    planned_groups = [group.file_paths() for group in finalized_groups]

    planned_stats = MaintenanceStats(
        before_file_count=before_file_count,
        after_file_count=after_file_count,
        before_total_bytes=before_total_bytes,
        after_total_bytes=after_total_bytes,
        compacted_file_count=deduplicated_file_count,
        rewritten_bytes=rewritten_bytes,
        compression_codec=None,  # Will be set by backend
        dry_run=True,
        key_columns=key_columns,
        dedup_order_by=dedup_order_by,
        deduplicated_rows=None,  # Will be calculated during execution
        planned_groups=planned_groups,
    )

    return {
        "groups": finalized_groups,
        "untouched_files": untouched_files,
        "planned_stats": planned_stats,
        "planned_groups": planned_groups,
    }


def validate_deduplication_inputs(
    key_columns: list[str] | None = None,
    dedup_order_by: list[str] | None = None,
) -> tuple[list[str] | None, list[str] | None]:
    """
    Validate and normalize deduplication input parameters.

    Args:
        key_columns: Optional key columns for deduplication
        dedup_order_by: Optional ordering columns for deduplication

    Returns:
        Tuple of (normalized_key_columns, normalized_dedup_order_by)

    Raises:
        ValueError: If parameters are invalid
    """
    from fsspeckit.core.merge import normalize_key_columns

    # Validate key columns
    normalized_key_columns = None
    if key_columns is not None:
        if not key_columns:
            raise ValueError("key_columns cannot be empty when provided")
        normalized_key_columns = normalize_key_columns(key_columns)

    # An omitted business order intentionally preserves snapshot-local physical
    # row order.  It must not silently become key-column ordering: callers who
    # need a business winner must provide it explicitly.
    normalized_dedup_order_by = (
        normalize_key_columns(dedup_order_by) if dedup_order_by is not None else None
    )
    return normalized_key_columns, normalized_dedup_order_by


def prepare_deduplication_stats(
    planned_stats: MaintenanceStats,
    compression: str | None,
    dry_run: bool,
) -> MaintenanceStats:
    """
    Prepare maintenance stats for deduplication operation.

    Args:
        planned_stats: Initial planned stats
        compression: Compression codec to use
        dry_run: Whether this is a dry run

    Returns:
        Updated MaintenanceStats object
    """
    updated_stats = MaintenanceStats(
        before_file_count=planned_stats.before_file_count,
        after_file_count=planned_stats.after_file_count,
        before_total_bytes=planned_stats.before_total_bytes,
        after_total_bytes=planned_stats.after_total_bytes,
        compacted_file_count=planned_stats.compacted_file_count,
        rewritten_bytes=planned_stats.rewritten_bytes,
        compression_codec=compression,
        dry_run=dry_run,
        key_columns=planned_stats.key_columns,
        dedup_order_by=planned_stats.dedup_order_by,
        deduplicated_rows=None,  # Will be set during execution
        planned_groups=planned_stats.planned_groups,
    )

    return updated_stats


def execute_deduplication_template(
    groups: list[CompactionGroup],
    planned_stats: MaintenanceStats,
    backend_executor: Callable[[CompactionGroup], tuple[int, dict[str, Any]]],
    dry_run: bool,
) -> dict[str, Any]:
    """
    Template method for executing deduplication across groups.

    Args:
        groups: List of compaction groups to process
        planned_stats: Planned statistics
        backend_executor: Backend-specific execution function
        dry_run: Whether this is a dry run

    Returns:
        Dictionary with execution results
    """
    if not groups:
        result = planned_stats.to_dict()
        result["execution_results"] = []
        return result

    if dry_run:
        result = planned_stats.to_dict()
        result["execution_results"] = [
            {"planned_groups": [group.file_paths() for group in groups]}
        ]
        return result

    # Execute deduplication for each group
    total_deduplicated_rows = 0
    execution_results = []

    for group in groups:
        group_result = backend_executor(group)
        deduplicated_rows = group_result[0] if isinstance(group_result, tuple) else 0
        group_stats = group_result[1] if isinstance(group_result, tuple) else {}

        total_deduplicated_rows += deduplicated_rows
        execution_results.append(
            {
                "group": group.file_paths(),
                "deduplicated_rows": deduplicated_rows,
                "stats": group_stats,
            }
        )

    # Update final statistics
    final_stats = planned_stats.to_dict()
    final_stats["deduplicated_rows"] = total_deduplicated_rows
    final_stats["execution_results"] = execution_results

    return final_stats


def execute_compaction_template(
    groups: list[CompactionGroup],
    planned_stats: MaintenanceStats,
    dataset_path: str,
    compact_group_fn: Callable[[CompactionGroup, str], None],
    filesystem: AbstractFileSystem,
    dry_run: bool,
) -> dict[str, Any]:
    """Template method for executing compaction across groups.

    Args:
        groups: List of compaction groups to process.
        planned_stats: Planned statistics for the operation.
        dataset_path: Root dataset path used for generated output files.
        compact_group_fn: Backend-specific function that compacts a group
            into the provided output path.
        filesystem: Filesystem object used to remove original files.
        dry_run: When ``True``, returns the plan without modifying files.

    Returns:
        Dictionary with execution statistics.
    """
    if dry_run:
        result = planned_stats.to_dict()
        result["planned_groups"] = [group.file_paths() for group in groups]
        return result

    if not groups:
        return planned_stats.to_dict()

    for group in groups:
        output_path = posixpath.join(
            dataset_path, f"compacted-{uuid.uuid4().hex[:16]}.parquet"
        )
        compact_group_fn(group, output_path)

    for group in groups:
        for file_info in group.files:
            filesystem.rm(file_info.path)

    return planned_stats.to_dict()


# --------------------------------------------------------------------------- #
# Typed maintenance planning and guarantee classification
# --------------------------------------------------------------------------- #


class MaintenanceOperation(str, Enum):
    """Supported maintenance operations."""

    COMPACTION = "compaction"
    PARTITION_LOCAL_DEDUPLICATION = "partition_local_deduplication"
    GLOBAL_REPARTITION_DEDUPLICATION = "global_repartition_deduplication"
    COORDINATED_OPTIMIZATION = "coordinated_optimization"


class GuaranteeLevel(str, Enum):
    """Publication guarantee levels for maintenance plans."""

    ATOMIC_LOCAL = "atomic_local"
    BEST_EFFORT_OBJECT_STORE = "best_effort_object_store"


class ValidationLevel(str, Enum):
    """Validation levels for maintenance plans."""

    DEFAULT = "default"
    FULL_DISTINCT_KEY_SCAN = "full_distinct_key_scan"


class SchemaOutcome(str, Enum):
    """Outcome of schema reconciliation during maintenance."""

    LOSSLESS_PRESERVED = "lossless_preserved"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class PartitionScopeType(str, Enum):
    """Partition scope classification for a maintenance plan."""

    FULL = "full"
    FILTERED = "filtered"
    REPARTITION = "repartition"


class MaintenanceBackend(str, Enum):
    """Supported maintenance backends."""

    PYARROW = "pyarrow"
    DUCKDB = "duckdb"


@dataclass(frozen=True)
class SourceFileInfo:
    """A single file entry in a maintenance source snapshot.

    Attributes:
        relative_path: Path relative to the dataset root.
        absolute_path: Absolute path as reported by the filesystem.
        size_bytes: File size in bytes at snapshot time.
        num_rows: Number of rows according to the latest Parquet metadata read.
        content_token: A best-effort content token for drift detection.
    """

    relative_path: str
    absolute_path: str
    size_bytes: int
    num_rows: int
    content_token: str


@dataclass(frozen=True)
class SourceSnapshot:
    """Immutable snapshot of the source dataset captured at plan time.

    Attributes:
        dataset_path: Root path of the dataset.
        filesystem_protocol: Canonical fsspec protocol of the filesystem.
        files: Tuple of SourceFileInfo entries.
        total_bytes: Sum of file sizes.
        total_rows: Sum of row counts.
        captured_at: ISO-8601 timestamp of snapshot capture.
    """

    dataset_path: str
    filesystem_protocol: str
    files: tuple[SourceFileInfo, ...]
    total_bytes: int
    total_rows: int
    captured_at: str


@dataclass(frozen=True)
class PartitionScope:
    """Partition scope for a maintenance plan.

    Attributes:
        scope_type: Classification of the scope.
        partition_filter: Optional filter prefixes that constrained planning.
        partition_paths: Optional paths of the affected partitions.
        partition_columns: Output partition columns for a repartition operation.
    """

    scope_type: PartitionScopeType
    partition_filter: tuple[str, ...] | None = None
    partition_paths: tuple[str, ...] | None = None
    partition_columns: tuple[str, ...] | None = None


@dataclass(frozen=True)
class MaintenancePlan:
    """Base immutable maintenance plan.

    Attributes:
        operation: The maintenance operation this plan represents.
        source_snapshot: Snapshot of the source dataset at plan time.
        selected_backend: Backend pinned to this plan.
        guarantee_level: Automatic guarantee classification.
        partition_scope: Partition scope of the operation.
        schema_outcome: Lossless schema-reconciliation outcome.
        selected_codec: Selected compression codec.
        max_rows_per_file: Hard upper bound on output rows per file.
        target_byte_size: Advisory target for output bytes per file.
        validation_level: Validation level requested for the operation.
        schema: Optional captured source schema.
    """

    operation: MaintenanceOperation
    source_snapshot: SourceSnapshot
    selected_backend: str
    guarantee_level: GuaranteeLevel
    partition_scope: PartitionScope
    schema_outcome: SchemaOutcome
    selected_codec: str
    max_rows_per_file: int | None
    target_byte_size: int | None
    validation_level: ValidationLevel
    schema: pa.Schema | None = None


@dataclass(frozen=True)
class CompactionPlan(MaintenancePlan):
    """Immutable plan for a compaction operation."""

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.COMPACTION, init=False
    )
    compaction_groups: tuple[CompactionGroup, ...] = ()


@dataclass(frozen=True)
class PartitionLocalDeduplicationPlan(MaintenancePlan):
    """Immutable plan for partition-local deduplication."""

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.PARTITION_LOCAL_DEDUPLICATION, init=False
    )
    dedup_key_columns: tuple[str, ...] | None = None
    dedup_order_by: tuple[str, ...] | None = None
    dedup_groups: tuple[CompactionGroup, ...] = ()


@dataclass(frozen=True)
class GlobalRepartitionDeduplicationPlan(MaintenancePlan):
    """Immutable plan for global-repartitioning deduplication."""

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.GLOBAL_REPARTITION_DEDUPLICATION, init=False
    )
    partition_columns: tuple[str, ...] = ()
    dedup_key_columns: tuple[str, ...] | None = None
    dedup_order_by: tuple[str, ...] | None = None
    dedup_groups: tuple[CompactionGroup, ...] = ()


@dataclass(frozen=True)
class CoordinatedOptimizationPlan(MaintenancePlan):
    """Immutable plan for coordinated optimization (optional dedup + compaction)."""

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.COORDINATED_OPTIMIZATION, init=False
    )
    dedup_key_columns: tuple[str, ...] | None = None
    dedup_order_by: tuple[str, ...] | None = None
    optimization_groups: tuple[CompactionGroup, ...] = ()


# Strict native local/POSIX protocol allowlist that earns the atomic_local
# guarantee. Every other fsspec filesystem (S3, GCS, Azure, memory, ...) is
# classified best_effort_object_store.
_ATOMIC_LOCAL_PROTOCOLS = frozenset({"local", "file", "os"})


def _is_local_filesystem(filesystem: AbstractFileSystem) -> bool:
    """Return True if the filesystem is a strict native local/POSIX filesystem.

    Classification is protocol-based so the allowlist matches fsspec's native
    local backends rather than two concrete classes. A directory view (or any
    wrapper exposing ``.fs``) is classified by its inner filesystem, so a DirFileSystem
    over a local fs keeps the atomic_local guarantee.
    """
    inner = getattr(filesystem, "fs", None)
    if inner is not None and inner is not filesystem:
        return _is_local_filesystem(inner)
    protocol = getattr(filesystem, "protocol", "unknown")
    if isinstance(protocol, (list, tuple)):
        protocol = protocol[0] if protocol else "unknown"
    return str(protocol) in _ATOMIC_LOCAL_PROTOCOLS


def _classify_guarantee(filesystem: AbstractFileSystem) -> GuaranteeLevel:
    """Classify the publication guarantee for a filesystem."""
    if _is_local_filesystem(filesystem):
        return GuaranteeLevel.ATOMIC_LOCAL
    return GuaranteeLevel.BEST_EFFORT_OBJECT_STORE


def _protocol_str(filesystem: AbstractFileSystem) -> str:
    """Return the canonical protocol string for a filesystem."""
    inner = getattr(filesystem, "fs", None)
    if inner is not None and inner is not filesystem:
        return _protocol_str(inner)
    protocol = getattr(filesystem, "protocol", "unknown")
    if isinstance(protocol, (list, tuple)):
        return protocol[0] if protocol else "unknown"
    return str(protocol)


def _content_token(info: dict[str, Any], size_bytes: int) -> str:
    """Build a best-effort content token for drift detection."""
    mtime = info.get("mtime")
    if mtime is None:
        mtime = info.get("created")
    if mtime is not None and hasattr(mtime, "isoformat"):
        mtime = mtime.isoformat()
    return f"{size_bytes}:{mtime}"


def _relative_file_path(absolute_path: str, dataset_root: str) -> str:
    """Return the path of a file relative to the dataset root."""
    if absolute_path == dataset_root:
        return ""
    try:
        rel = posixpath.relpath(absolute_path, dataset_root)
        if rel.startswith(".."):
            return posixpath.basename(absolute_path)
        return rel
    except ValueError:
        return posixpath.basename(absolute_path)


def _partition_paths(
    file_stats: list[dict[str, Any]],
    dataset_root: str,
) -> tuple[str, ...]:
    """Return the sorted unique partition directory paths relative to the dataset root."""
    paths: set[str] = set()
    for file_stat in file_stats:
        rel = _relative_file_path(file_stat["path"], dataset_root)
        dir_path = posixpath.dirname(rel)
        if dir_path:
            paths.add(dir_path)
    return tuple(sorted(paths))


def _resolve_dataset_root(filesystem: AbstractFileSystem, dataset_path: str) -> str:
    """Resolve a stable dataset root for source snapshots."""
    if _is_local_filesystem(filesystem):
        return os.path.abspath(dataset_path)
    return dataset_path


def _capture_source_snapshot(
    filesystem: AbstractFileSystem,
    dataset_path: str,
    file_stats: list[dict[str, Any]],
) -> SourceSnapshot:
    """Capture a lock-free source snapshot for drift detection."""
    root = _resolve_dataset_root(filesystem, dataset_path)
    source_files: list[SourceFileInfo] = []
    total_bytes = 0
    total_rows = 0

    for file_stat in file_stats:
        absolute_path = file_stat["path"]
        size_bytes = file_stat["size_bytes"]
        num_rows = file_stat["num_rows"]
        relative_path = _relative_file_path(absolute_path, root)

        try:
            info = filesystem.info(absolute_path)
        except Exception:
            info = {}
        token = _content_token(info, size_bytes)

        source_files.append(
            SourceFileInfo(
                relative_path=relative_path,
                absolute_path=absolute_path,
                size_bytes=size_bytes,
                num_rows=num_rows,
                content_token=token,
            )
        )
        total_bytes += size_bytes
        total_rows += num_rows

    return SourceSnapshot(
        dataset_path=root,
        filesystem_protocol=_protocol_str(filesystem),
        files=tuple(source_files),
        total_bytes=total_bytes,
        total_rows=total_rows,
        captured_at=datetime.now(tz=timezone.utc).isoformat(),
    )


def _partition_scope(
    operation: MaintenanceOperation,
    partition_filter: list[str] | None,
    partition_columns: list[str] | None = None,
    partition_paths: tuple[str, ...] | None = None,
) -> PartitionScope:
    """Build a partition scope descriptor for a plan."""
    if operation == MaintenanceOperation.GLOBAL_REPARTITION_DEDUPLICATION:
        return PartitionScope(
            scope_type=PartitionScopeType.REPARTITION,
            partition_columns=tuple(partition_columns) if partition_columns else None,
        )
    if partition_filter:
        return PartitionScope(
            scope_type=PartitionScopeType.FILTERED,
            partition_filter=tuple(partition_filter),
            partition_paths=partition_paths,
        )
    return PartitionScope(
        scope_type=PartitionScopeType.FULL,
        partition_paths=partition_paths,
    )


def _coerce_validation_level(
    value: ValidationLevel | str | None,
) -> ValidationLevel:
    """Coerce a validation level argument to the enum type."""
    if value is None:
        return ValidationLevel.DEFAULT
    if isinstance(value, ValidationLevel):
        return value
    return ValidationLevel(value)


def _coerce_backend(value: str | MaintenanceBackend) -> str:
    """Coerce a backend argument to its string value."""
    if isinstance(value, MaintenanceBackend):
        return value.value
    return value


def _reconcile_schema(
    filesystem: AbstractFileSystem,
    file_stats: list[dict[str, Any]],
) -> tuple[SchemaOutcome, Any]:
    """Reconcile source schemas across all discovered files.

    Planning is lock-free and reads only metadata; it does not rewrite files.
    If any file cannot be read or its schema differs from the first file, the
    outcome is ``RECONCILIATION_REQUIRED``.
    """
    if not file_stats:
        return SchemaOutcome.LOSSLESS_PRESERVED, None
    import pyarrow.parquet as pq

    def _read_schema(path: str) -> Any:
        with filesystem.open(path, "rb") as fh:
            return pq.ParquetFile(fh).schema_arrow

    try:
        base_schema = _read_schema(file_stats[0]["path"])
    except Exception:
        return SchemaOutcome.RECONCILIATION_REQUIRED, None

    for fi in file_stats[1:]:
        try:
            other_schema = _read_schema(fi["path"])
        except Exception:
            return SchemaOutcome.RECONCILIATION_REQUIRED, None
        if not base_schema.equals(other_schema, check_metadata=True):
            return SchemaOutcome.RECONCILIATION_REQUIRED, None

    return SchemaOutcome.LOSSLESS_PRESERVED, base_schema


def _prepare_plan_inputs(
    operation: MaintenanceOperation,
    dataset_path: str,
    filesystem: AbstractFileSystem | None,
    partition_filter: list[str] | None,
    partition_columns: list[str] | None,
    target_mb_per_file: int | None,
    validation_level: ValidationLevel | str | None,
    codec: str | None,
) -> tuple[
    AbstractFileSystem,
    list[dict[str, Any]],
    SourceSnapshot,
    PartitionScope,
    SchemaOutcome,
    Any,
    str,
    int | None,
    ValidationLevel,
]:
    """Collect the common, lock-free planning inputs shared by all operations."""
    if target_mb_per_file is not None and target_mb_per_file <= 0:
        raise ValueError("target_mb_per_file must be > 0")
    fs = filesystem or fsspec_filesystem("file")
    file_stats = collect_dataset_stats(
        dataset_path, fs, partition_filter=partition_filter
    )["files"]
    snapshot = _capture_source_snapshot(fs, dataset_path, file_stats)
    schema_outcome, schema = _reconcile_schema(fs, file_stats)
    if schema_outcome == SchemaOutcome.RECONCILIATION_REQUIRED:
        raise ValueError(
            "Schema reconciliation required; the dataset contains incompatible schemas. "
            "Maintenance cannot proceed without a lossless reconciliation."
        )
    partition_paths = _partition_paths(file_stats, snapshot.dataset_path)
    scope = _partition_scope(
        operation, partition_filter, partition_columns, partition_paths
    )
    selected_codec = codec or "snappy"
    target_byte_size = target_mb_per_file * 1024 * 1024 if target_mb_per_file else None
    validation = _coerce_validation_level(validation_level)
    return (
        fs,
        file_stats,
        snapshot,
        scope,
        schema_outcome,
        schema,
        selected_codec,
        target_byte_size,
        validation,
    )


def _group_file_stats_by_partition(
    file_stats: list[dict[str, Any]],
    dataset_root: str,
) -> dict[str, list[dict[str, Any]]]:
    """Bucket file stats by their partition directory relative to *dataset_root*.

    The partition directory is ``posixpath.dirname`` of each file's path
    relative to the root; files living directly in the root share the empty
    string. Returned dict iteration order is insertion order; callers sort the
    keys when deterministic ordering is required.
    """
    partitions: dict[str, list[dict[str, Any]]] = {}
    for file_stat in file_stats:
        rel = _relative_file_path(file_stat["path"], dataset_root)
        partition_dir = posixpath.dirname(rel)
        partitions.setdefault(partition_dir, []).append(file_stat)
    return partitions


def _plan_partition_local_deduplication_groups(
    file_stats: list[dict[str, Any]],
    dataset_root: str,
) -> tuple[CompactionGroup, ...]:
    """Plan one deduplication group per physical partition.

    Unlike compaction, deduplication must inspect every source file in a
    partition as one logical input.  Thresholds govern only output splitting;
    they must never split a partition's input and allow cross-file duplicates
    to survive.
    """
    partitions = _group_file_stats_by_partition(file_stats, dataset_root)
    return tuple(
        CompactionGroup(
            files=tuple(
                FileInfo(fi["path"], fi["size_bytes"], fi["num_rows"])
                for fi in partitions[partition]
            )
        )
        for partition in sorted(partitions)
    )


def _plan_partition_local_compaction_groups(
    file_stats: list[dict[str, Any]],
    dataset_root: str,
    target_mb_per_file: int | None,
    target_rows_per_file: int | None,
) -> tuple[CompactionGroup, ...]:
    """Plan compaction groups that never cross a partition boundary.

    Files are grouped by their partition directory (the directory containing
    the file relative to the dataset root); each partition is planned
    independently so a compacted output never mixes rows from two physical
    partitions. A flat dataset (every file directly under the root) collapses
    to a single partition and is therefore planned identically to
    :func:`plan_compaction_groups`.
    """
    partitions = _group_file_stats_by_partition(file_stats, dataset_root)

    groups: list[CompactionGroup] = []
    for partition_dir in sorted(partitions):
        partition_files = partitions[partition_dir]
        result = plan_compaction_groups(
            partition_files, target_mb_per_file, target_rows_per_file
        )
        groups.extend(result["groups"])
    return tuple(groups)


# --------------------------------------------------------------------------- #
# Typed execution result types (#37)
# --------------------------------------------------------------------------- #


class LockAcquisitionError(Exception):
    """Raised when a bounded advisory lock acquisition times out."""


@dataclass(frozen=True)
class PhaseOutcome:
    """Outcome of a single execution phase.

    Attributes:
        phase: Name of the lifecycle phase (stage, write, validate, lock,
            drift_check, publish, cleanup).
        succeeded: True when the phase completed successfully.
        error: Human-readable description of the failure, or None on success.
    """

    phase: str
    succeeded: bool
    error: str | None = None


@dataclass(frozen=True)
class ValidationOutcome:
    """Outcome of staged-output validation before publication.

    Attributes:
        succeeded: True when all validation checks passed.
        staged_row_count: Total rows counted across all staged files.
        expected_row_count: Row count expected from the source snapshot.
        error: Human-readable failure description, or None on success.
    """

    succeeded: bool
    staged_row_count: int | None = None
    expected_row_count: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class PublicationOutcome:
    """Outcome of the atomic-rename publication step.

    Attributes:
        succeeded: True when all staged files were published.
        published_files: Absolute paths of files written into the live dataset.
        removed_source_files: Source files that were replaced during publication.
        error: Human-readable description of the failure, or None on success.
        rollback_succeeded: Whether automatic rollback completed after failure.
        rollback_error: Any rollback errors retained for manual recovery.

    """

    succeeded: bool
    published_files: tuple[str, ...] = ()
    removed_source_files: tuple[str, ...] = ()
    error: str | None = None
    rollback_succeeded: bool | None = None
    rollback_error: str | None = None


@dataclass(frozen=True)
class RecoveryArtifacts:
    """Recovery information left after a failed or partial execution.

    Attributes:
        workspace_path: Path of the sibling maintenance workspace, if it still
            exists (None after successful cleanup).
        backup_paths: Paths of source-file backups in the workspace (present
            only when rollback preserved them for manual recovery).
        recovered: True when an automatic rollback restored the dataset.
        error: Human-readable description of a cleanup/recovery failure.
    """

    workspace_path: str | None = None
    backup_paths: tuple[str, ...] = ()
    recovered: bool = False
    error: str | None = None


@dataclass(frozen=True)
class ActualMetrics:
    """Actual output metrics recorded after a successful execution.

    Attributes:
        row_count: Total rows written across all output files.
        file_count: Number of output files published.
        total_bytes: Sum of output file sizes in bytes.
    """

    row_count: int
    file_count: int
    total_bytes: int


@dataclass(frozen=True)
class MaintenanceResult:
    """Typed result returned by DatasetMaintenanceCoordinator.execute().

    Attributes:
        plan: The plan that was executed.
        succeeded: True when the operation completed successfully end-to-end.
        guarantee_level: Guarantee level that governed this execution.
        phase_outcomes: Ordered per-phase outcomes (stage, write, validate,
            lock, drift_check, publish, cleanup).
        validation: Staged-output validation outcome, populated after the
            write phase.
        publication: Atomic-rename publication outcome.
        recovery: Recovery artifact details; workspace_path is None after a
            successful cleanup.
        actual_metrics: Row/file/byte counts of the published output.  Only
            populated on success.
        error: Top-level error summary, or None on success.
    """

    plan: MaintenancePlan
    succeeded: bool
    guarantee_level: GuaranteeLevel
    phase_outcomes: tuple[PhaseOutcome, ...]
    validation: ValidationOutcome | None = None
    publication: PublicationOutcome | None = None
    recovery: RecoveryArtifacts | None = None
    actual_metrics: ActualMetrics | None = None
    error: str | None = None


# --------------------------------------------------------------------------- #
# Atomic-local execution helpers (#37)
# --------------------------------------------------------------------------- #


class _BoundedAdvisoryLock:
    """POSIX advisory lock (fcntl.flock) with bounded acquisition.

    Callers poll with ``LOCK_NB`` until the timeout expires so the operation
    never hangs forever.  Only available on POSIX; a ``RuntimeError`` is raised
    on construction if the platform does not provide ``fcntl``.

    Args:
        lock_path: Path to the lock file (created if absent).
        exclusive: Acquire an exclusive (write) lock when True; shared
            (read) lock otherwise.
        timeout_s: Maximum seconds to wait before returning ``False``.
        retry_interval_s: Sleep duration between acquisition attempts.
    """

    def __init__(
        self,
        lock_path: str,
        exclusive: bool = True,
        timeout_s: float = 30.0,
        retry_interval_s: float = 0.05,
    ) -> None:
        try:
            import fcntl as _fcntl  # noqa: PLC0415

            self._fcntl = _fcntl
        except ImportError as exc:
            raise RuntimeError(
                "atomic_local locking requires a POSIX platform with fcntl"
            ) from exc
        self._path = lock_path
        self._exclusive = exclusive
        self._timeout_s = timeout_s
        self._retry_interval_s = retry_interval_s
        self._fh: Any = None

    def acquire(self) -> bool:
        """Attempt to acquire the lock within the timeout.

        Returns:
            True on success, False on timeout.
        """
        import time  # noqa: PLC0415

        op = self._fcntl.LOCK_EX if self._exclusive else self._fcntl.LOCK_SH
        op |= self._fcntl.LOCK_NB
        deadline = time.monotonic() + self._timeout_s
        self._fh = open(self._path, "a+")  # noqa: SIM115
        while True:
            try:
                self._fcntl.flock(self._fh, op)
                return True
            except OSError:
                if time.monotonic() >= deadline:
                    self._fh.close()
                    self._fh = None
                    return False
                time.sleep(self._retry_interval_s)

    def release(self) -> None:
        """Release the lock unconditionally."""
        if self._fh is not None:
            try:
                self._fcntl.flock(self._fh, self._fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None

    def __enter__(self) -> _BoundedAdvisoryLock:
        if not self.acquire():
            raise LockAcquisitionError(
                f"Could not acquire {'exclusive' if self._exclusive else 'shared'} "
                f"lock on {self._path!r} within {self._timeout_s}s"
            )
        return self

    def __exit__(self, *_: Any) -> None:
        self.release()


def _make_workspace(dataset_root: str) -> tuple[str, str, str]:
    """Create a sibling maintenance workspace with staged/ and backup/ subdirs.

    The workspace is placed in the same parent directory as the dataset so that
    ``os.rename`` moves stay on the same filesystem.

    Returns:
        (workspace_path, staged_dir, backup_dir)
    """
    parent = os.path.dirname(dataset_root)
    name = os.path.basename(dataset_root)
    workspace = os.path.join(parent, f".maintenance_{name}_{uuid.uuid4().hex[:16]}")
    staged_dir = os.path.join(workspace, "staged")
    backup_dir = os.path.join(workspace, "backup")
    os.makedirs(staged_dir, exist_ok=False)
    os.makedirs(backup_dir, exist_ok=True)
    return workspace, staged_dir, backup_dir


def _validate_staged_output(
    staged_files: list[str],
    expected_schema: pa.Schema | None,
    expected_rows: int,
) -> ValidationOutcome:
    """Validate staged Parquet files before publication.

    Checks that the total row count across all staged files equals
    *expected_rows* and that every file's schema matches *expected_schema*
    (when provided).
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    try:
        total_rows = 0
        for path in staged_files:
            meta = pq.read_metadata(path)
            total_rows += meta.num_rows
            if expected_schema is not None:
                schema = pq.read_schema(path)
                if not schema.equals(expected_schema, check_metadata=True):
                    return ValidationOutcome(
                        succeeded=False,
                        staged_row_count=total_rows,
                        expected_row_count=expected_rows,
                        error=f"Schema mismatch in staged file {path}",
                    )
        if total_rows != expected_rows:
            return ValidationOutcome(
                succeeded=False,
                staged_row_count=total_rows,
                expected_row_count=expected_rows,
                error=(
                    f"Row count mismatch: staged={total_rows}, expected={expected_rows}"
                ),
            )
        return ValidationOutcome(
            succeeded=True,
            staged_row_count=total_rows,
            expected_row_count=expected_rows,
        )
    except Exception as exc:
        return ValidationOutcome(
            succeeded=False,
            staged_row_count=None,
            expected_row_count=expected_rows,
            error=f"Validation error: {exc}",
        )


def _check_source_drift(snapshot: SourceSnapshot) -> str | None:
    """Return an error string if any source file has drifted, or None.

    Local plans compare both size and the snapshot content token's mtime when
    available.  Older/manual snapshots with a ``:None`` token retain their
    size-only behavior for compatibility.
    """
    for file_info in snapshot.files:
        path = file_info.absolute_path
        try:
            stat = os.stat(path)
        except OSError:
            return f"Source file not accessible: {path}"
        current_size = stat.st_size
        if current_size != file_info.size_bytes:
            return (
                f"Source file size changed: {path} "
                f"(expected {file_info.size_bytes}, got {current_size})"
            )
        expected_token = file_info.content_token
        if not expected_token.endswith(":None"):
            current_token = _content_token({"mtime": stat.st_mtime}, current_size)
            if current_token != expected_token:
                return (
                    f"Source file content token changed: {path} "
                    f"(expected {expected_token}, got {current_token})"
                )
    return None


def _rollback_publication(
    swaps: list[tuple[list[tuple[str, str]], list[str]]],
) -> tuple[bool, tuple[str, ...]]:
    """Restore swapped subtrees and report any failed restore step."""
    errors: list[str] = []
    for backed_up, published in swaps:
        for dest in published:
            try:
                os.remove(dest)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"remove {dest}: {exc}")
        for original, backup in backed_up:
            try:
                os.rename(backup, original)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"restore {backup} -> {original}: {exc}")
    return not errors, tuple(errors)


def _publish_atomic_local(
    source_files: list[str],
    staged_files: list[str],
    dataset_root: str,
    backup_dir: str,
    staged_partition_dirs: list[str] | None = None,
) -> PublicationOutcome:
    """Publish staged files via the per-subtree backup-then-rename protocol.

    When *staged_partition_dirs* is provided it must be parallel to
    *staged_files*; entry *i* is the partition directory (relative to
    *dataset_root*) beneath which staged file *i* is published. Source files
    are grouped by the partition directory implied by their path relative to
    *dataset_root*.

    Each partition subtree is swapped independently — back up its source
    files, then rename its staged outputs into place — while one caller-held
    exclusive window protects the whole multi-directory transaction. On any
    failure every already-swapped subtree (including the in-progress one) is
    rolled back before returning, so a failed multi-subtree publication never
    leaves a partially visible dataset (#38).

    When *staged_partition_dirs* is ``None`` the publication is flat: every
    output lands directly beneath *dataset_root* and every backup lands
    directly beneath *backup_dir*, matching the original single-directory
    contract.

    A ``PublicationOutcome`` with ``succeeded=False`` is returned (not raised)
    so callers can attach it to the ``MaintenanceResult`` without losing
    per-phase context.
    """
    if staged_partition_dirs is None:
        staged_partition_dirs = [""] * len(staged_files)

    # Group source files by their partition directory relative to the root.
    source_subtrees: dict[str, list[str]] = {}
    for src in source_files:
        rel = _relative_file_path(src, dataset_root)
        subtree = posixpath.dirname(rel)
        source_subtrees.setdefault(subtree, []).append(src)

    # Group staged outputs by their target partition directory.
    staged_subtrees: dict[str, list[str]] = {}
    for staged, subtree in zip(staged_files, staged_partition_dirs, strict=True):
        staged_subtrees.setdefault(subtree, []).append(staged)

    all_subtrees = sorted(set(source_subtrees) | set(staged_subtrees))

    completed: list[tuple[list[tuple[str, str]], list[str]]] = []
    current_backed_up: list[tuple[str, str]] = []
    current_published: list[str] = []

    try:
        for subtree in all_subtrees:
            current_backed_up = []
            current_published = []

            # Step 1 — back up this subtree's source files (hide from readers).
            for src in source_subtrees.get(subtree, []):
                backup_rel = (
                    os.path.join(subtree, os.path.basename(src))
                    if subtree
                    else os.path.basename(src)
                )
                backup_path = os.path.join(backup_dir, backup_rel)
                os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                os.rename(src, backup_path)
                current_backed_up.append((src, backup_path))

            # Step 2 — publish this subtree's staged outputs into place.
            target_dir = (
                os.path.join(dataset_root, subtree) if subtree else dataset_root
            )
            # Destination partition directories may not exist yet (global
            # repartition writes to declared destination tuples that can differ
            # from every source partition). ``exist_ok=True`` keeps this a no-op
            # for compaction and partition-local dedup, whose source and
            # destination partition directories already match (#42).
            os.makedirs(target_dir, exist_ok=True)
            for staged in staged_subtrees.get(subtree, []):
                dest = os.path.join(target_dir, os.path.basename(staged))
                os.rename(staged, dest)
                current_published.append(dest)

            completed.append((current_backed_up, current_published))

        return PublicationOutcome(
            succeeded=True,
            published_files=tuple(
                dest for _, published in completed for dest in published
            ),
            removed_source_files=tuple(
                src for backed_up, _ in completed for src, _ in backed_up
            ),
        )

    except Exception as exc:
        # Roll back the in-progress subtree, then every completed subtree.
        current_ok, current_errors = _rollback_publication(
            [(current_backed_up, current_published)]
        )
        completed_ok, completed_errors = _rollback_publication(completed)
        rollback_errors = current_errors + completed_errors
        return PublicationOutcome(
            succeeded=False,
            error=f"Publication rename failed: {exc}",
            rollback_succeeded=current_ok and completed_ok,
            rollback_error="; ".join(rollback_errors) if rollback_errors else None,
        )


def _group_partition_dir(group: CompactionGroup, dataset_root: str) -> str:
    """Return the partition directory shared by a compaction group's files.

    The directory is relative to *dataset_root* and empty for files that live
    directly in the root. Partition-local planning guarantees every file in a
    group shares one partition tuple, so the first file is representative.
    """
    for file_info in group.files:
        return posixpath.dirname(_relative_file_path(file_info.path, dataset_root))
    return ""


def _execute_atomic_local_compaction(
    plan: CompactionPlan,
    lock_timeout_s: float = 30.0,
    lock_retry_interval_s: float = 0.05,
) -> MaintenanceResult:
    """Execute the atomic_local lifecycle for a flat local compaction plan.

    Lifecycle phases: stage, write, validate, lock, drift_check, publish,
    cleanup, report.

    Staged files are written to a sibling maintenance workspace.  An exclusive
    advisory lock gates the publish phase.  On any failure after backup has
    begun the protocol rolls back before returning so the live dataset is never
    left in a partial state.

    Args:
        plan: An accepted :class:`CompactionPlan` with
            ``guarantee_level == GuaranteeLevel.ATOMIC_LOCAL``.
        lock_timeout_s: Maximum seconds to wait for the exclusive lock.
        lock_retry_interval_s: Sleep interval between lock-acquisition retries.

    Returns:
        A :class:`MaintenanceResult` describing every phase and the actual
        output metrics.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    dataset_root = plan.source_snapshot.dataset_path
    phase_outcomes: list[PhaseOutcome] = []
    workspace: str | None = None
    validation: ValidationOutcome | None = None
    publication: PublicationOutcome | None = None
    backup_dir = ""
    staged_dir = ""

    # Short-circuit when there is nothing to compact.
    if not plan.compaction_groups:
        return MaintenanceResult(
            plan=plan,
            succeeded=True,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=(PhaseOutcome(phase="write", succeeded=True),),
            actual_metrics=ActualMetrics(row_count=0, file_count=0, total_bytes=0),
        )

    # ------------------------------------------------------------------ #
    # Phase: stage
    # ------------------------------------------------------------------ #
    try:
        workspace, staged_dir, backup_dir = _make_workspace(dataset_root)
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="stage", succeeded=False, error=str(exc))
        )
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Stage phase failed: {exc}",
        )

    staged_files: list[str] = []
    staged_partition_dirs: list[str] = []
    total_rows_written = 0
    source_files_in_groups: list[str] = []
    expected_rows = sum(
        sum(fi.num_rows for fi in group.files) for group in plan.compaction_groups
    )

    # ------------------------------------------------------------------ #
    # Phase: write
    # ------------------------------------------------------------------ #
    try:
        for group in plan.compaction_groups:
            source_files_in_groups.extend(fi.path for fi in group.files)
            group_partition_dir = _group_partition_dir(group, dataset_root)
            tables = []
            for fi in group.files:
                with open(fi.path, "rb") as fh:
                    tables.append(pq.read_table(fh))

            combined = pa.concat_tables(tables)
            if plan.schema is not None:
                combined = combined.cast(plan.schema)

            num_rows = combined.num_rows
            # max_rows_per_file is a HARD output bound; None means no splitting.
            chunk_size = plan.max_rows_per_file if plan.max_rows_per_file else num_rows
            if chunk_size <= 0:
                chunk_size = num_rows if num_rows > 0 else 1

            offset = 0
            while offset < max(num_rows, 1):
                chunk = combined.slice(offset, chunk_size)
                if chunk.num_rows == 0:
                    break
                out_name = f"compacted_{uuid.uuid4().hex[:16]}.parquet"
                out_path = os.path.join(staged_dir, out_name)
                pq.write_table(chunk, out_path, compression=plan.selected_codec)
                staged_files.append(out_path)
                staged_partition_dirs.append(group_partition_dir)
                total_rows_written += chunk.num_rows
                offset += chunk_size

        phase_outcomes.append(PhaseOutcome(phase="write", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="write", succeeded=False, error=str(exc))
        )
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Write phase failed: {exc}",
        )

    # ------------------------------------------------------------------ #
    # Phase: validate
    # ------------------------------------------------------------------ #
    validation = _validate_staged_output(staged_files, plan.schema, expected_rows)
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate",
            succeeded=validation.succeeded,
            error=validation.error,
        )
    )
    if not validation.succeeded:
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Validation failed: {validation.error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: lock  (bounded exclusive advisory lock)
    # ------------------------------------------------------------------ #
    lock_path = os.path.join(dataset_root, ".fsspeckit_maintenance.lock")
    lock = _BoundedAdvisoryLock(
        lock_path,
        exclusive=True,
        timeout_s=lock_timeout_s,
        retry_interval_s=lock_retry_interval_s,
    )
    if not lock.acquire():
        phase_outcomes.append(
            PhaseOutcome(
                phase="lock",
                succeeded=False,
                error=(
                    f"Could not acquire exclusive lock on {lock_path!r} "
                    f"within {lock_timeout_s}s"
                ),
            )
        )
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error="Lock acquisition timed out",
        )

    phase_outcomes.append(PhaseOutcome(phase="lock", succeeded=True))

    try:
        # -------------------------------------------------------------- #
        # Phase: drift_check
        # -------------------------------------------------------------- #
        drift_error = _check_source_drift(plan.source_snapshot)
        if drift_error is not None:
            phase_outcomes.append(
                PhaseOutcome(phase="drift_check", succeeded=False, error=drift_error)
            )
            return MaintenanceResult(
                plan=plan,
                succeeded=False,
                guarantee_level=plan.guarantee_level,
                phase_outcomes=tuple(phase_outcomes),
                validation=validation,
                recovery=RecoveryArtifacts(workspace_path=workspace),
                error=f"Source drift detected: {drift_error}",
            )
        phase_outcomes.append(PhaseOutcome(phase="drift_check", succeeded=True))

        # -------------------------------------------------------------- #
        # Phase: publish  (backup-then-rename)
        # -------------------------------------------------------------- #
        publication = _publish_atomic_local(
            source_files=source_files_in_groups,
            staged_files=staged_files,
            dataset_root=dataset_root,
            backup_dir=backup_dir,
            staged_partition_dirs=staged_partition_dirs,
        )
        phase_outcomes.append(
            PhaseOutcome(
                phase="publish",
                succeeded=publication.succeeded,
                error=publication.error,
            )
        )
    finally:
        lock.release()

    if not publication.succeeded:
        # Backups may or may not have been restored; surface workspace info.
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            publication=publication,
            recovery=RecoveryArtifacts(
                workspace_path=workspace,
                backup_paths=tuple(
                    path
                    for path in (
                        os.path.join(
                            backup_dir,
                            _relative_file_path(f, dataset_root),
                        )
                        for f in source_files_in_groups
                    )
                    if os.path.exists(path)
                ),
                recovered=publication.rollback_succeeded is True,
                error=publication.rollback_error,
            ),
            error=f"Publication failed: {publication.error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: cleanup  (non-fatal; success not contingent on it)
    # ------------------------------------------------------------------ #
    import shutil  # noqa: PLC0415

    cleanup_error: str | None = None
    try:
        shutil.rmtree(workspace)
        workspace = None
        phase_outcomes.append(PhaseOutcome(phase="cleanup", succeeded=True))
    except Exception as exc:
        cleanup_error = str(exc)
        phase_outcomes.append(
            PhaseOutcome(phase="cleanup", succeeded=False, error=cleanup_error)
        )
    retained_backup_paths = (
        tuple(
            path
            for path in (
                os.path.join(
                    backup_dir,
                    _relative_file_path(f, dataset_root),
                )
                for f in source_files_in_groups
            )
            if os.path.exists(path)
        )
        if workspace is not None
        else ()
    )

    total_bytes = sum(
        os.path.getsize(f) for f in publication.published_files if os.path.exists(f)
    )

    return MaintenanceResult(
        plan=plan,
        succeeded=True,
        guarantee_level=plan.guarantee_level,
        phase_outcomes=tuple(phase_outcomes),
        validation=validation,
        publication=publication,
        recovery=RecoveryArtifacts(
            workspace_path=workspace,
            backup_paths=retained_backup_paths,
            error=cleanup_error,
        ),
        actual_metrics=ActualMetrics(
            row_count=total_rows_written,
            file_count=len(publication.published_files),
            total_bytes=total_bytes,
        ),
    )


# --------------------------------------------------------------------------- #
def _validate_staged_partition_placement(
    staged_files: list[str],
    staged_partition_dirs: list[str],
    staged_root: str,
) -> str | None:
    """Validate that each staged file is beneath its planned partition tuple."""
    if len(staged_files) != len(staged_partition_dirs):
        return "Staged partition metadata does not match staged files"
    for staged, partition in zip(staged_files, staged_partition_dirs, strict=True):
        relative = posixpath.relpath(staged, staged_root)
        actual = posixpath.dirname(relative)
        if actual != partition:
            return (
                f"Staged file {staged!r} has partition {actual!r}; "
                f"expected {partition!r}"
            )
    return None


def _validate_staged_duplicate_keys(
    staged_files: list[str],
    staged_partition_dirs: list[str],
    key_columns: tuple[str, ...] | None,
) -> str | None:
    """Perform the opt-in full duplicate-key scan across each partition."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    tables_by_partition: dict[str, list[pa.Table]] = {}
    try:
        for staged, partition in zip(staged_files, staged_partition_dirs, strict=True):
            tables_by_partition.setdefault(partition, []).append(pq.read_table(staged))
        for partition, tables in tables_by_partition.items():
            table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
            deduplicated = _deduplicate_partition_table(table, key_columns, None)
            if deduplicated.num_rows != table.num_rows:
                return f"Duplicate keys remain in staged partition {partition!r}"
    except Exception as exc:
        return f"Duplicate-key validation failed: {exc}"
    return None


# Shared Hive-style partition path helpers (used by global-repartition
# execution in both the atomic_local and best_effort_object_store lanes).
# --------------------------------------------------------------------------- #
_RESERVED_PARTITION_VALUES = frozenset(
    {"nan", "__HIVE_DEFAULT_PARTITION__", "__HIVE_NAN_PARTITION__"}
)
_PARTITION_ESCAPE_PREFIX = "__fsspeckit_value__"


def _hive_partition_component(value: Any) -> str:
    """Encode one partition column value into a Hive path component.

    Null maps to ``__HIVE_DEFAULT_PARTITION__``, NaN to
    ``__HIVE_NAN_PARTITION__``, and values that collide with those sentinels
    (or the escape prefix) are disambiguated by prepending the prefix.
    """
    from urllib.parse import quote  # noqa: PLC0415

    if value is None:
        return "__HIVE_DEFAULT_PARTITION__"
    if isinstance(value, float) and value != value:
        return "__HIVE_NAN_PARTITION__"
    encoded = quote(str(value), safe="")
    if encoded in _RESERVED_PARTITION_VALUES or encoded.startswith(
        _PARTITION_ESCAPE_PREFIX
    ):
        encoded = _PARTITION_ESCAPE_PREFIX + encoded
    return encoded


def _hive_partition_path(columns: tuple[str, ...], values: tuple[Any, ...]) -> str:
    """Build a Hive-style ``col=val/...`` partition path from raw values."""
    from urllib.parse import quote  # noqa: PLC0415

    return "/".join(
        f"{quote(col, safe='')}={_hive_partition_component(val)}"
        for col, val in zip(columns, values, strict=True)
    )


def _validate_global_partition_placement(
    staged_files: list[str],
    staged_partition_dirs: list[str],
    staged_partition_values: list[tuple[Any, ...]],
    staged_root: str,
    partition_columns: tuple[str, ...],
) -> str | None:
    """Validate global-repartition staged files land under their declared tuple.

    Each staged file must live beneath the Hive-style destination path built
    from its declared partition values, AND every row inside it must carry
    those exact partition values.  This is the repartitioning analogue of
    :func:`_validate_staged_partition_placement` plus a per-row data check.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    if len(staged_files) != len(staged_partition_dirs):
        return "Staged partition metadata does not match staged files"
    try:
        for staged, partition_dir, values in zip(
            staged_files, staged_partition_dirs, staged_partition_values, strict=True
        ):
            relative = posixpath.relpath(staged, staged_root)
            actual_dir = posixpath.dirname(relative)
            if actual_dir != partition_dir:
                return (
                    f"Staged file {staged!r} has partition {actual_dir!r}; "
                    f"expected {partition_dir!r}"
                )
            expected_path = _hive_partition_path(partition_columns, values)
            if actual_dir != expected_path:
                return (
                    f"Staged file {staged!r} path {actual_dir!r} does not match "
                    f"declared partition values {expected_path!r}"
                )
            # Per-row check: every row must carry the declared partition values.
            with open(staged, "rb") as fh:
                staged_table = pq.read_table(fh)
            for row_index in range(staged_table.num_rows):
                for column, expected_value in zip(
                    partition_columns, values, strict=True
                ):
                    actual_value = staged_table[column][row_index].as_py()
                    if _canonical_deduplication_value(
                        actual_value
                    ) != _canonical_deduplication_value(expected_value):
                        return (
                            f"Destination partition mismatch in staged file "
                            f"{staged!r}: column {column!r} expected "
                            f"{expected_value!r}, got {actual_value!r}"
                        )
    except Exception as exc:
        return f"Global partition placement validation failed: {exc}"
    return None


def _validate_global_duplicate_keys(
    staged_files: list[str],
    key_columns: tuple[str, ...] | None,
) -> str | None:
    """Perform the opt-in full distinct-key scan across ALL staged files.

    Unlike the partition-local scan, this reads every staged file into one
    table and re-deduplicates the entire global retained set.  The result must
    be identical to the already-deduplicated input.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    try:
        if not staged_files:
            return None
        tables = []
        for path in staged_files:
            with open(path, "rb") as fh:
                tables.append(pq.read_table(fh))
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        deduplicated = _deduplicate_partition_table(combined, key_columns, None)
        if deduplicated.num_rows != combined.num_rows:
            return (
                f"Duplicate keys remain across staged files: "
                f"{deduplicated.num_rows} unique of {combined.num_rows} rows"
            )
    except Exception as exc:
        return f"Global duplicate-key validation failed: {exc}"
    return None


def _execute_atomic_local_partition_local_deduplication(
    plan: PartitionLocalDeduplicationPlan,
    lock_timeout_s: float = 30.0,
    lock_retry_interval_s: float = 0.05,
) -> MaintenanceResult:
    """Execute partition-local deduplication with atomic local publication.

    Every physical partition is read as one logical input, regardless of the
    planner's output grouping thresholds, so duplicate keys can never survive
    merely because they were split across source files.  Staged outputs retain
    their source partition directory and are published through the same
    backup-then-rename transaction as local compaction.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415
    import shutil  # noqa: PLC0415

    dataset_root = plan.source_snapshot.dataset_path
    phase_outcomes: list[PhaseOutcome] = []
    workspace: str | None = None
    staged_dir = ""
    backup_dir = ""
    validation: ValidationOutcome | None = None
    publication: PublicationOutcome | None = None

    if not plan.source_snapshot.files:
        return MaintenanceResult(
            plan=plan,
            succeeded=True,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=(PhaseOutcome(phase="write", succeeded=True),),
            actual_metrics=ActualMetrics(row_count=0, file_count=0, total_bytes=0),
        )

    try:
        workspace, staged_dir, backup_dir = _make_workspace(dataset_root)
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="stage", succeeded=False, error=str(exc))
        )
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Stage phase failed: {exc}",
        )

    sources_by_partition: dict[str, list[SourceFileInfo]] = {}
    for source in plan.source_snapshot.files:
        partition = posixpath.dirname(source.relative_path)
        sources_by_partition.setdefault(partition, []).append(source)

    staged_files: list[str] = []
    staged_partition_dirs: list[str] = []
    source_files: list[str] = []
    expected_rows = 0

    try:
        for partition in sorted(sources_by_partition):
            partition_sources = sources_by_partition[partition]
            source_files.extend(source.absolute_path for source in partition_sources)
            tables: list[pa.Table] = []
            for source in partition_sources:
                with open(source.absolute_path, "rb") as fh:
                    tables.append(pq.read_table(fh))
            combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
            deduplicated = _deduplicate_partition_table(
                combined, plan.dedup_key_columns, plan.dedup_order_by
            )
            if plan.schema is not None:
                deduplicated = deduplicated.cast(plan.schema)
            expected_rows += deduplicated.num_rows
            output_dir = (
                os.path.join(staged_dir, partition) if partition else staged_dir
            )
            os.makedirs(output_dir, exist_ok=True)
            for chunk_index, chunk in enumerate(
                _split_table_by_rows(deduplicated, plan.max_rows_per_file)
            ):
                if chunk.num_rows == 0:
                    continue
                output_path = os.path.join(
                    output_dir,
                    f"deduplicated_{uuid.uuid4().hex[:16]}_{chunk_index}.parquet",
                )
                pq.write_table(chunk, output_path, compression=plan.selected_codec)
                staged_files.append(output_path)
                staged_partition_dirs.append(partition)
        phase_outcomes.append(PhaseOutcome(phase="write", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="write", succeeded=False, error=str(exc))
        )
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Write phase failed: {exc}",
        )

    placement_error = _validate_staged_partition_placement(
        staged_files, staged_partition_dirs, staged_dir
    )
    if placement_error is not None:
        validation = ValidationOutcome(
            succeeded=False, expected_row_count=expected_rows, error=placement_error
        )
    else:
        validation = _validate_staged_output(staged_files, plan.schema, expected_rows)
        if (
            validation.succeeded
            and plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN
        ):
            duplicate_error = _validate_staged_duplicate_keys(
                staged_files, staged_partition_dirs, plan.dedup_key_columns
            )
            if duplicate_error is not None:
                validation = ValidationOutcome(
                    succeeded=False,
                    staged_row_count=validation.staged_row_count,
                    expected_row_count=expected_rows,
                    error=duplicate_error,
                )
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate", succeeded=validation.succeeded, error=validation.error
        )
    )
    if not validation.succeeded:
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Validation failed: {validation.error}",
        )

    lock_path = os.path.join(dataset_root, ".fsspeckit_maintenance.lock")
    lock = _BoundedAdvisoryLock(
        lock_path,
        exclusive=True,
        timeout_s=lock_timeout_s,
        retry_interval_s=lock_retry_interval_s,
    )
    if not lock.acquire():
        error = (
            f"Could not acquire exclusive lock on {lock_path!r} "
            f"within {lock_timeout_s}s"
        )
        phase_outcomes.append(PhaseOutcome(phase="lock", succeeded=False, error=error))
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error="Lock acquisition timed out",
        )
    phase_outcomes.append(PhaseOutcome(phase="lock", succeeded=True))
    try:
        drift_error = _check_source_drift(plan.source_snapshot)
        if drift_error is not None:
            phase_outcomes.append(
                PhaseOutcome(phase="drift_check", succeeded=False, error=drift_error)
            )
            return MaintenanceResult(
                plan=plan,
                succeeded=False,
                guarantee_level=plan.guarantee_level,
                phase_outcomes=tuple(phase_outcomes),
                validation=validation,
                recovery=RecoveryArtifacts(workspace_path=workspace),
                error=f"Source drift detected: {drift_error}",
            )
        phase_outcomes.append(PhaseOutcome(phase="drift_check", succeeded=True))
        publication = _publish_atomic_local(
            source_files,
            staged_files,
            dataset_root,
            backup_dir,
            staged_partition_dirs,
        )
        phase_outcomes.append(
            PhaseOutcome(
                phase="publish",
                succeeded=publication.succeeded,
                error=publication.error,
            )
        )
    finally:
        lock.release()

    if publication is None or not publication.succeeded:
        error = (
            publication.error if publication is not None else None
        ) or "Publication did not run"
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            publication=publication,
            recovery=RecoveryArtifacts(
                workspace_path=workspace,
                backup_paths=tuple(
                    path
                    for path in (
                        os.path.join(backup_dir, source.relative_path)
                        for source in plan.source_snapshot.files
                    )
                    if os.path.exists(path)
                ),
                recovered=publication.rollback_succeeded is True,
                error=publication.rollback_error,
            ),
            error=f"Publication failed: {error}",
        )

    cleanup_error: str | None = None
    try:
        shutil.rmtree(workspace)
        workspace = None
        phase_outcomes.append(PhaseOutcome(phase="cleanup", succeeded=True))
    except Exception as exc:
        cleanup_error = str(exc)
        phase_outcomes.append(
            PhaseOutcome(phase="cleanup", succeeded=False, error=cleanup_error)
        )
    retained_backup_paths = (
        tuple(
            path
            for path in (
                os.path.join(backup_dir, source.relative_path)
                for source in plan.source_snapshot.files
            )
            if os.path.exists(path)
        )
        if workspace is not None
        else ()
    )

    total_bytes = sum(
        os.path.getsize(path)
        for path in publication.published_files
        if os.path.exists(path)
    )
    return MaintenanceResult(
        plan=plan,
        succeeded=True,
        guarantee_level=plan.guarantee_level,
        phase_outcomes=tuple(phase_outcomes),
        validation=validation,
        publication=publication,
        recovery=RecoveryArtifacts(
            workspace_path=workspace,
            backup_paths=retained_backup_paths,
            error=cleanup_error,
        ),
        actual_metrics=ActualMetrics(
            row_count=expected_rows,
            file_count=len(publication.published_files),
            total_bytes=total_bytes,
        ),
    )


# --------------------------------------------------------------------------- #
# Atomic-local global-repartitioning deduplication (#42)
# --------------------------------------------------------------------------- #


def _execute_atomic_local_global_repartition_deduplication(
    plan: GlobalRepartitionDeduplicationPlan,
    lock_timeout_s: float = 30.0,
    lock_retry_interval_s: float = 0.05,
) -> MaintenanceResult:
    """Execute global-repartitioning deduplication with atomic local publication.

    Unlike partition-local deduplication, this operation reads every source
    file into one snapshot-local table and removes duplicate keys globally
    across all source partitions.  Retained rows are then written under
    Hive-style paths for exactly the declared destination partition columns,
    which may differ entirely from the source partition layout.  Publication
    uses the same backup-then-rename protocol as local compaction so a
    cooperating fsspeckit reader never observes a partial directory rewrite.

    Full validation (schema, partition placement, row count, optional full
    distinct-key scan) runs before publication.  On any failure the workspace
    and backups are retained as recovery artifacts and every swapped subtree
    is rolled back under one caller-held exclusive lock.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415
    import shutil  # noqa: PLC0415

    dataset_root = plan.source_snapshot.dataset_path
    phase_outcomes: list[PhaseOutcome] = []
    workspace: str | None = None
    staged_dir = ""
    backup_dir = ""
    validation: ValidationOutcome | None = None
    publication: PublicationOutcome | None = None

    if not plan.source_snapshot.files:
        return MaintenanceResult(
            plan=plan,
            succeeded=True,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=(PhaseOutcome(phase="write", succeeded=True),),
            actual_metrics=ActualMetrics(row_count=0, file_count=0, total_bytes=0),
        )

    try:
        workspace, staged_dir, backup_dir = _make_workspace(dataset_root)
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="stage", succeeded=False, error=str(exc))
        )
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Stage phase failed: {exc}",
        )

    source_files: list[str] = []
    staged_files: list[str] = []
    staged_partition_dirs: list[str] = []
    staged_partition_values: list[tuple[Any, ...]] = []
    staged_rows: list[int] = []
    run_id = uuid.uuid4().hex[:16]
    expected_rows = 0

    try:
        # Read every source into one snapshot-local table.  The physical
        # fallback order is partition path, file path, row offset — so sort
        # the snapshot by relative_path before reading, matching the
        # object-store lane and making winner selection deterministic.
        sorted_sources = sorted(
            plan.source_snapshot.files, key=lambda source: source.relative_path
        )
        source_files.extend(source.absolute_path for source in sorted_sources)
        tables: list[pa.Table] = []
        for source in sorted_sources:
            with open(source.absolute_path, "rb") as fh:
                tables.append(pq.read_table(fh))
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        deduplicated = _deduplicate_partition_table(
            combined, plan.dedup_key_columns, plan.dedup_order_by
        )
        if plan.schema is not None:
            deduplicated = deduplicated.cast(plan.schema)
        expected_rows = deduplicated.num_rows

        # Group retained rows by their declared destination partition tuple.
        partition_arrays = [
            deduplicated[column].to_pylist() for column in plan.partition_columns
        ]
        partition_groups: dict[tuple[Any, ...], tuple[tuple[Any, ...], list[int]]] = {}
        for row_index in range(expected_rows):
            raw_values = tuple(values[row_index] for values in partition_arrays)
            canonical_values = tuple(
                _canonical_deduplication_value(value) for value in raw_values
            )
            entry = partition_groups.get(canonical_values)
            if entry is None:
                entry = (raw_values, [])
                partition_groups[canonical_values] = entry
            entry[1].append(row_index)

        sorted_partitions = sorted(
            partition_groups.values(),
            key=lambda item: tuple(_hive_partition_component(v) for v in item[0]),
        )
        for partition_index, (raw_values, row_indices) in enumerate(sorted_partitions):
            partition_dir = _hive_partition_path(plan.partition_columns, raw_values)
            partition_table = deduplicated.take(pa.array(row_indices, type=pa.int64()))
            for chunk_index, chunk in enumerate(
                _split_table_by_rows(partition_table, plan.max_rows_per_file)
            ):
                if chunk.num_rows == 0:
                    continue
                filename = (
                    f"deduplicated-{run_id}-{partition_index:04d}-"
                    f"{chunk_index:04d}.parquet"
                )
                output_dir = os.path.join(staged_dir, partition_dir)
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, filename)
                pq.write_table(chunk, output_path, compression=plan.selected_codec)
                staged_files.append(output_path)
                staged_partition_dirs.append(partition_dir)
                staged_partition_values.append(raw_values)
                staged_rows.append(chunk.num_rows)
        phase_outcomes.append(PhaseOutcome(phase="write", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="write", succeeded=False, error=str(exc))
        )
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Write phase failed: {exc}",
        )

    # ------------------------------------------------------------------ #
    # Phase: validate — partition placement, schema, row count, and an
    # optional full distinct-key scan across the *global* retained set.
    # ------------------------------------------------------------------ #
    placement_error = _validate_global_partition_placement(
        staged_files,
        staged_partition_dirs,
        staged_partition_values,
        staged_dir,
        plan.partition_columns,
    )
    if placement_error is not None:
        validation = ValidationOutcome(
            succeeded=False, expected_row_count=expected_rows, error=placement_error
        )
    else:
        validation = _validate_staged_output(staged_files, plan.schema, expected_rows)
        if (
            validation.succeeded
            and plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN
        ):
            duplicate_error = _validate_global_duplicate_keys(
                staged_files, plan.dedup_key_columns
            )
            if duplicate_error is not None:
                validation = ValidationOutcome(
                    succeeded=False,
                    staged_row_count=validation.staged_row_count,
                    expected_row_count=expected_rows,
                    error=duplicate_error,
                )
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate", succeeded=validation.succeeded, error=validation.error
        )
    )
    if not validation.succeeded:
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Validation failed: {validation.error}",
        )

    lock_path = os.path.join(dataset_root, ".fsspeckit_maintenance.lock")
    lock = _BoundedAdvisoryLock(
        lock_path,
        exclusive=True,
        timeout_s=lock_timeout_s,
        retry_interval_s=lock_retry_interval_s,
    )
    if not lock.acquire():
        error = (
            f"Could not acquire exclusive lock on {lock_path!r} "
            f"within {lock_timeout_s}s"
        )
        phase_outcomes.append(PhaseOutcome(phase="lock", succeeded=False, error=error))
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error="Lock acquisition timed out",
        )
    phase_outcomes.append(PhaseOutcome(phase="lock", succeeded=True))
    try:
        drift_error = _check_source_drift(plan.source_snapshot)
        if drift_error is not None:
            phase_outcomes.append(
                PhaseOutcome(phase="drift_check", succeeded=False, error=drift_error)
            )
            return MaintenanceResult(
                plan=plan,
                succeeded=False,
                guarantee_level=plan.guarantee_level,
                phase_outcomes=tuple(phase_outcomes),
                validation=validation,
                recovery=RecoveryArtifacts(workspace_path=workspace),
                error=f"Source drift detected: {drift_error}",
            )
        phase_outcomes.append(PhaseOutcome(phase="drift_check", succeeded=True))
        publication = _publish_atomic_local(
            source_files,
            staged_files,
            dataset_root,
            backup_dir,
            staged_partition_dirs,
        )
        phase_outcomes.append(
            PhaseOutcome(
                phase="publish",
                succeeded=publication.succeeded,
                error=publication.error,
            )
        )
    finally:
        lock.release()

    if publication is None or not publication.succeeded:
        error = (
            publication.error if publication is not None else None
        ) or "Publication did not run"
        return MaintenanceResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            publication=publication,
            recovery=RecoveryArtifacts(
                workspace_path=workspace,
                backup_paths=tuple(
                    path
                    for path in (
                        os.path.join(backup_dir, source.relative_path)
                        for source in plan.source_snapshot.files
                    )
                    if os.path.exists(path)
                ),
                recovered=publication.rollback_succeeded is True,
                error=publication.rollback_error,
            ),
            error=f"Publication failed: {error}",
        )

    cleanup_error: str | None = None
    try:
        shutil.rmtree(workspace)
        workspace = None
        phase_outcomes.append(PhaseOutcome(phase="cleanup", succeeded=True))
    except Exception as exc:
        cleanup_error = str(exc)
        phase_outcomes.append(
            PhaseOutcome(phase="cleanup", succeeded=False, error=cleanup_error)
        )
    retained_backup_paths = (
        tuple(
            path
            for path in (
                os.path.join(backup_dir, source.relative_path)
                for source in plan.source_snapshot.files
            )
            if os.path.exists(path)
        )
        if workspace is not None
        else ()
    )

    total_bytes = sum(
        os.path.getsize(path)
        for path in publication.published_files
        if os.path.exists(path)
    )
    return MaintenanceResult(
        plan=plan,
        succeeded=True,
        guarantee_level=plan.guarantee_level,
        phase_outcomes=tuple(phase_outcomes),
        validation=validation,
        publication=publication,
        recovery=RecoveryArtifacts(
            workspace_path=workspace,
            backup_paths=retained_backup_paths,
            error=cleanup_error,
        ),
        actual_metrics=ActualMetrics(
            row_count=expected_rows,
            file_count=len(publication.published_files),
            total_bytes=total_bytes,
        ),
    )


def _execute_atomic_local_coordinated_optimization(
    plan: CoordinatedOptimizationPlan,
    lock_timeout_s: float = 30.0,
    lock_retry_interval_s: float = 0.05,
) -> CoordinatedOptimizationResult:
    """Execute local optimization as one atomic deduplication/compaction rewrite.

    The optional partition-local deduplication phase and the compaction phase
    share one staged workspace, validation pass, advisory-lock window, and
    backup-then-rename publication.  Consequently a failed second phase never
    exposes a dataset in which only the first phase was published.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415
    import shutil  # noqa: PLC0415

    dataset_root = plan.source_snapshot.dataset_path
    phase_outcomes: list[PhaseOutcome] = []
    workspace: str | None = None
    validation: ValidationOutcome | None = None
    publication: PublicationOutcome | None = None
    backup_dir = ""
    staged_dir = ""
    dedup_phase_executed = plan.dedup_key_columns is not None
    dedup_rows_removed: int | None = None

    if not plan.optimization_groups:
        outcomes: list[PhaseOutcome] = []
        if dedup_phase_executed:
            outcomes.append(PhaseOutcome(phase="dedup", succeeded=True))
        outcomes.append(PhaseOutcome(phase="compaction", succeeded=True))
        return CoordinatedOptimizationResult(
            plan=plan,
            succeeded=True,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(outcomes),
            actual_metrics=ActualMetrics(row_count=0, file_count=0, total_bytes=0),
            dedup_phase_executed=dedup_phase_executed,
            dedup_rows_removed=0 if dedup_phase_executed else None,
        )

    try:
        workspace, staged_dir, backup_dir = _make_workspace(dataset_root)
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="stage", succeeded=False, error=str(exc))
        )
        return CoordinatedOptimizationResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            dedup_phase_executed=dedup_phase_executed,
            error=f"Stage phase failed: {exc}",
        )

    tables_by_partition: list[tuple[pa.Table, str]] = []
    source_files: list[str] = []

    def read_group(group: CompactionGroup) -> tuple[pa.Table, str]:
        source_files.extend(source.path for source in group.files)
        tables = []
        for source in group.files:
            with open(source.path, "rb") as fh:
                tables.append(pq.read_table(fh))
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        return combined, _group_partition_dir(group, dataset_root)

    try:
        if dedup_phase_executed:
            input_rows = 0
            output_rows = 0
            for group in plan.optimization_groups:
                combined, partition_dir = read_group(group)
                input_rows += combined.num_rows
                deduplicated = _deduplicate_partition_table(
                    combined, plan.dedup_key_columns, plan.dedup_order_by
                )
                output_rows += deduplicated.num_rows
                tables_by_partition.append((deduplicated, partition_dir))
            dedup_rows_removed = input_rows - output_rows
            phase_outcomes.append(PhaseOutcome(phase="dedup", succeeded=True))
        else:
            for group in plan.optimization_groups:
                tables_by_partition.append(read_group(group))
    except Exception as exc:
        failed_phase = "dedup" if dedup_phase_executed else "compaction"
        phase_outcomes.append(
            PhaseOutcome(failed_phase, succeeded=False, error=str(exc))
        )
        return CoordinatedOptimizationResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            dedup_phase_executed=dedup_phase_executed,
            dedup_rows_removed=dedup_rows_removed,
            error=f"{failed_phase.capitalize()} phase failed: {exc}",
        )

    staged_files: list[str] = []
    staged_partition_dirs: list[str] = []
    expected_rows = 0
    try:
        for table, partition_dir in tables_by_partition:
            if plan.schema is not None:
                table = table.cast(plan.schema)
            expected_rows += table.num_rows
            output_dir = (
                os.path.join(staged_dir, partition_dir) if partition_dir else staged_dir
            )
            os.makedirs(output_dir, exist_ok=True)
            for chunk_index, chunk in enumerate(
                _split_table_by_rows(table, plan.max_rows_per_file)
            ):
                if chunk.num_rows == 0:
                    continue
                output_path = os.path.join(
                    output_dir,
                    f"optimized_{uuid.uuid4().hex[:16]}_{chunk_index}.parquet",
                )
                pq.write_table(chunk, output_path, compression=plan.selected_codec)
                staged_files.append(output_path)
                staged_partition_dirs.append(partition_dir)
        phase_outcomes.append(PhaseOutcome(phase="compaction", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="compaction", succeeded=False, error=str(exc))
        )
        return CoordinatedOptimizationResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            dedup_phase_executed=dedup_phase_executed,
            dedup_rows_removed=dedup_rows_removed,
            error=f"Compaction phase failed: {exc}",
        )

    placement_error = _validate_staged_partition_placement(
        staged_files, staged_partition_dirs, staged_dir
    )
    validation = (
        ValidationOutcome(
            succeeded=False, expected_row_count=expected_rows, error=placement_error
        )
        if placement_error is not None
        else _validate_staged_output(staged_files, plan.schema, expected_rows)
    )
    if (
        validation.succeeded
        and dedup_phase_executed
        and plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN
    ):
        duplicate_error = _validate_staged_duplicate_keys(
            staged_files, staged_partition_dirs, plan.dedup_key_columns
        )
        if duplicate_error is not None:
            validation = ValidationOutcome(
                succeeded=False,
                staged_row_count=validation.staged_row_count,
                expected_row_count=expected_rows,
                error=duplicate_error,
            )
    phase_outcomes.append(
        PhaseOutcome("validate", validation.succeeded, validation.error)
    )
    if not validation.succeeded:
        return CoordinatedOptimizationResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            dedup_phase_executed=dedup_phase_executed,
            dedup_rows_removed=dedup_rows_removed,
            error=f"Validation failed: {validation.error}",
        )

    lock_path = os.path.join(dataset_root, ".fsspeckit_maintenance.lock")
    lock = _BoundedAdvisoryLock(
        lock_path,
        exclusive=True,
        timeout_s=lock_timeout_s,
        retry_interval_s=lock_retry_interval_s,
    )
    if not lock.acquire():
        error = f"Could not acquire exclusive lock on {lock_path!r} within {lock_timeout_s}s"
        phase_outcomes.append(PhaseOutcome(phase="lock", succeeded=False, error=error))
        return CoordinatedOptimizationResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            dedup_phase_executed=dedup_phase_executed,
            dedup_rows_removed=dedup_rows_removed,
            error="Lock acquisition timed out",
        )
    phase_outcomes.append(PhaseOutcome(phase="lock", succeeded=True))
    try:
        drift_error = _check_source_drift(plan.source_snapshot)
        if drift_error is not None:
            phase_outcomes.append(
                PhaseOutcome(phase="drift_check", succeeded=False, error=drift_error)
            )
            return CoordinatedOptimizationResult(
                plan=plan,
                succeeded=False,
                guarantee_level=plan.guarantee_level,
                phase_outcomes=tuple(phase_outcomes),
                validation=validation,
                recovery=RecoveryArtifacts(workspace_path=workspace),
                dedup_phase_executed=dedup_phase_executed,
                dedup_rows_removed=dedup_rows_removed,
                error=f"Source drift detected: {drift_error}",
            )
        phase_outcomes.append(PhaseOutcome(phase="drift_check", succeeded=True))
        publication = _publish_atomic_local(
            source_files,
            staged_files,
            dataset_root,
            backup_dir,
            staged_partition_dirs,
        )
        phase_outcomes.append(
            PhaseOutcome("publish", publication.succeeded, publication.error)
        )
    finally:
        lock.release()

    if publication is None or not publication.succeeded:
        error = (
            publication.error if publication is not None else None
        ) or "Publication did not run"
        return CoordinatedOptimizationResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            publication=publication,
            recovery=RecoveryArtifacts(
                workspace_path=workspace,
                backup_paths=tuple(
                    os.path.join(backup_dir, _relative_file_path(source, dataset_root))
                    for source in source_files
                    if os.path.exists(
                        os.path.join(
                            backup_dir, _relative_file_path(source, dataset_root)
                        )
                    )
                ),
                recovered=publication is not None
                and publication.rollback_succeeded is True,
                error=publication.rollback_error if publication is not None else None,
            ),
            dedup_phase_executed=dedup_phase_executed,
            dedup_rows_removed=dedup_rows_removed,
            error=f"Publication failed: {error}",
        )

    cleanup_error: str | None = None
    try:
        shutil.rmtree(workspace)
        workspace = None
        phase_outcomes.append(PhaseOutcome(phase="cleanup", succeeded=True))
    except Exception as exc:
        cleanup_error = str(exc)
        phase_outcomes.append(
            PhaseOutcome(phase="cleanup", succeeded=False, error=cleanup_error)
        )
    backup_paths = (
        tuple(
            os.path.join(backup_dir, _relative_file_path(source, dataset_root))
            for source in source_files
            if os.path.exists(
                os.path.join(backup_dir, _relative_file_path(source, dataset_root))
            )
        )
        if workspace is not None
        else ()
    )
    total_bytes = sum(
        os.path.getsize(path)
        for path in publication.published_files
        if os.path.exists(path)
    )
    return CoordinatedOptimizationResult(
        plan=plan,
        succeeded=True,
        guarantee_level=plan.guarantee_level,
        phase_outcomes=tuple(phase_outcomes),
        validation=validation,
        publication=publication,
        recovery=RecoveryArtifacts(
            workspace_path=workspace,
            backup_paths=backup_paths,
            error=cleanup_error,
        ),
        actual_metrics=ActualMetrics(
            row_count=expected_rows,
            file_count=len(publication.published_files),
            total_bytes=total_bytes,
        ),
        dedup_phase_executed=dedup_phase_executed,
        dedup_rows_removed=dedup_rows_removed,
    )


# Best-effort object-store execution helpers (#39)
# --------------------------------------------------------------------------- #

#: Explicit concurrency disclaimer for best_effort_object_store results.
BEST_EFFORT_CONCURRENCY_DISCLAIMER = (
    "best_effort_object_store: no distributed lock, no atomic visibility, "
    "no automatic rollback. Staging locations and partial live outputs are "
    "preserved as recovery artifacts after failure."
)


@dataclass(frozen=True)
class BestEffortCompactionResult(MaintenanceResult):
    """Typed result for best_effort_object_store compaction execution.

    Extends :class:`MaintenanceResult` with object-store-specific publication
    fields that explicitly disclose staged keys, copy failures, untouched
    sources, drift detection, and concurrency limitations.

    No distributed lock, atomic visibility, or automatic rollback is claimed.
    Staging locations and partial live outputs are preserved as recovery
    artifacts after any failure.

    Attributes:
        staging_prefix: The object-store prefix used for staged output files.
        staged_keys: Absolute paths of all staged output objects.
        copied_live_keys: Absolute paths of live objects successfully copied
            from staged output (populated after the publish phase).
        failed_copies: Live key paths where the copy operation failed.
        untouched_source_keys: Source keys that were NOT deleted — either
            because drift was detected, copies failed, or they were not part
            of any compaction group.
        drift_detected: True if any source revalidation found a changed
            content token; when True no source objects are deleted.
        concurrency_disclaimer: Explicit statement that no distributed lock,
            atomic visibility, or automatic rollback is provided.
    """

    staging_prefix: str = ""
    staged_keys: tuple[str, ...] = ()
    copied_live_keys: tuple[str, ...] = ()
    failed_copies: tuple[str, ...] = ()
    untouched_source_keys: tuple[str, ...] = ()
    drift_detected: bool = False
    concurrency_disclaimer: str = BEST_EFFORT_CONCURRENCY_DISCLAIMER


@dataclass(frozen=True)
class BestEffortGlobalRepartitionDeduplicationResult(BestEffortCompactionResult):
    """Typed result for global-repartitioning object-store deduplication.

    This result keeps the common best-effort recovery fields while making the
    destination partition contract explicit for callers inspecting a run.
    """

    destination_partition_columns: tuple[str, ...] = ()
    destination_partition_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class CoordinatedOptimizationResult(MaintenanceResult):
    """Typed result for coordinated optimization.

    Optimization composes optional partition-local deduplication with
    compaction through the selected staged lifecycle. This result makes the
    actual phase composition explicit without weakening or misrepresenting
    the selected guarantee level.

    Attributes:
        dedup_phase_executed: True when the deduplication phase ran.
        dedup_rows_removed: Duplicate rows removed by the dedup phase
            (None when dedup did not run).
    """

    dedup_phase_executed: bool = False
    dedup_rows_removed: int | None = None


@dataclass(frozen=True)
class BestEffortCoordinatedOptimizationResult(
    BestEffortCompactionResult, CoordinatedOptimizationResult
):
    """Coordinated optimization result with object-store recovery details."""


def _split_table_by_rows(table: pa.Table, max_rows: int | None) -> list[pa.Table]:
    """Split a PyArrow table into chunks with at most *max_rows* rows each.

    Args:
        table: The table to split.
        max_rows: Hard upper-bound on rows per chunk.  When *None* the whole
            table is returned as a single-element list.

    Returns:
        A non-empty list of table slices.  An empty source table produces a
        single empty-table slice.
    """
    if max_rows is None:
        return [table]
    n = table.num_rows
    if n == 0:
        return [table]
    chunks: list[pa.Table] = []
    offset = 0
    while offset < n:
        end = min(offset + max_rows, n)
        chunks.append(table.slice(offset, end - offset))
        offset = end
    return chunks


def _revalidate_source_token(
    filesystem: AbstractFileSystem,
    file_info: SourceFileInfo,
) -> str | None:
    """Return a drift-error string if *file_info*'s content token has changed.

    Compares the token recorded at plan time against a freshly computed one
    from :func:`filesystem.info`.  A missing file is always a drift signal.

    Returns:
        None when the token matches (no drift); an error string otherwise.
    """
    try:
        info = filesystem.info(file_info.absolute_path)
    except Exception as exc:
        return f"Source file not accessible: {file_info.absolute_path}: {exc}"
    current_size: int = info.get("size", info.get("Size", 0))
    current_token = _content_token(info, current_size)
    if current_token != file_info.content_token:
        return (
            f"Source drift detected: {file_info.absolute_path} "
            f"(token {file_info.content_token!r} → {current_token!r})"
        )
    return None


def _execute_best_effort_compaction(
    plan: CompactionPlan,
    filesystem: AbstractFileSystem,
) -> BestEffortCompactionResult:
    """Execute the best_effort_object_store lifecycle for a compaction plan.

    Lifecycle phases (in order):

    1. **stage** — write every compaction-group output under a staging prefix;
       enforce ``max_rows_per_file`` as a HARD upper bound.
    2. **validate** — read metadata of every staged file and confirm the total
       row count equals the source snapshot row count for those groups.  No
       live-key copy begins until this passes.
    3. **publish** — copy each staged file to its planned live key; validate
       EVERY live key individually after copy (prefix listing is not proof).
    4. **drift_check** — revalidate every planned source object immediately
       before deletion; if ANY drift is detected, delete NO source objects.
    5. **cleanup** — delete source objects that were successfully compacted;
       remove staging prefix on full success (retained on failure).

    On any failure the staging prefix and any partial live outputs are reported
    in the returned :class:`BestEffortCompactionResult` as recovery artifacts;
    no automatic rollback is performed.

    Args:
        plan: An accepted :class:`CompactionPlan` with
            ``guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE``.
        filesystem: The fsspec filesystem used to read and write all files.

    Returns:
        A :class:`BestEffortCompactionResult` describing every phase, staged
        keys, copied live keys, copy failures, untouched source keys, and
        whether source drift was detected.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    run_id = uuid.uuid4().hex[:16]
    dataset_root = plan.source_snapshot.dataset_path
    staging_prefix = posixpath.join(dataset_root, "_maintenance_staging", run_id)

    phase_outcomes: list[PhaseOutcome] = []

    # Collect which source paths are involved in compaction groups.
    source_paths_in_groups: set[str] = {
        fi.path for group in plan.compaction_groups for fi in group.files
    }
    # Source files not in any group are always untouched.
    all_source_paths = {fi.absolute_path for fi in plan.source_snapshot.files}
    always_untouched = sorted(all_source_paths - source_paths_in_groups)

    # Expected total rows from compacted sources.
    expected_rows = sum(
        sum(fi.num_rows for fi in group.files) for group in plan.compaction_groups
    )

    # ------------------------------------------------------------------ #
    # Phase: stage — write all outputs under the staging prefix
    # ------------------------------------------------------------------ #
    staged_keys: list[str] = []
    staged_key_rows: dict[str, int] = {}
    staged_to_live: dict[str, str] = {}

    try:
        for group_idx, group in enumerate(plan.compaction_groups):
            tables = []
            for fi in group.files:
                with filesystem.open(fi.path, "rb") as fh:
                    tables.append(pq.read_table(fh))

            combined = pa.concat_tables(tables)
            if plan.schema is not None:
                combined = combined.cast(plan.schema)

            chunks = _split_table_by_rows(combined, plan.max_rows_per_file)
            for chunk_idx, chunk in enumerate(chunks):
                if chunk.num_rows == 0:
                    continue
                staged_path = posixpath.join(
                    staging_prefix,
                    f"output-{group_idx:04d}-{chunk_idx:04d}.parquet",
                )
                live_path = posixpath.join(
                    dataset_root,
                    f"compacted-{run_id}-{group_idx:04d}-{chunk_idx:04d}.parquet",
                )
                buf = BytesIO()
                pq.write_table(chunk, buf, compression=plan.selected_codec)
                filesystem.pipe(staged_path, buf.getvalue())
                staged_keys.append(staged_path)
                staged_key_rows[staged_path] = chunk.num_rows
                staged_to_live[staged_path] = live_path

        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=True))
    except Exception as exc:
        err = f"Stage phase failed: {exc}"
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=False, error=err))
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            untouched_source_keys=tuple(
                sorted(source_paths_in_groups | set(always_untouched))
            ),
            error=err,
        )

    # ------------------------------------------------------------------ #
    # Phase: validate — check ALL staged files before any live copy
    #
    # Spec: "fully validate staged output BEFORE any live-key copy begins."
    # Checks: file readability, row-count match, schema compatibility.
    # ------------------------------------------------------------------ #
    total_staged_rows = 0
    validation_error: str | None = None

    try:
        for staged_path in staged_keys:
            with filesystem.open(staged_path, "rb") as fh:
                staged_file = pq.ParquetFile(fh)
                staged_meta = staged_file.metadata
                total_staged_rows += staged_meta.num_rows
                # Schema check: every staged file must be compatible with
                # the plan's reconciled schema (when one was captured).
                if plan.schema is not None:
                    staged_schema = staged_file.schema_arrow
                    if not staged_schema.equals(plan.schema):
                        validation_error = (
                            f"Schema mismatch in staged file {staged_path}: "
                            f"expected {plan.schema}, got {staged_schema}"
                        )
                        break
        if validation_error is None and total_staged_rows != expected_rows:
            validation_error = (
                f"Staged row count mismatch: got {total_staged_rows}, "
                f"expected {expected_rows}"
            )
    except Exception as exc:
        validation_error = f"Staged validation error: {exc}"

    validation_outcome = ValidationOutcome(
        succeeded=validation_error is None,
        staged_row_count=total_staged_rows,
        expected_row_count=expected_rows,
        error=validation_error,
    )
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate",
            succeeded=validation_error is None,
            error=validation_error,
        )
    )

    if validation_error is not None:
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation_outcome,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            untouched_source_keys=tuple(
                sorted(source_paths_in_groups | set(always_untouched))
            ),
            error=f"Staged validation failed: {validation_error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: publish — copy staged → live keys, validate each live key
    # ------------------------------------------------------------------ #
    copied_live_keys: list[str] = []
    failed_copies: list[str] = []

    for staged_path in staged_keys:
        live_path = staged_to_live[staged_path]
        try:
            content = filesystem.cat(staged_path)
            filesystem.pipe(live_path, content)
            # Validate this live key individually (prefix listing is NOT proof).
            with filesystem.open(live_path, "rb") as fh:
                pq.read_metadata(fh)
            copied_live_keys.append(live_path)
        except Exception as exc:
            logger.warning(
                "Failed to copy/validate staged key %s → %s: %s",
                staged_path,
                live_path,
                exc,
            )
            failed_copies.append(live_path)

    copy_succeeded = not failed_copies
    publish_error = (
        f"Failed to copy or validate live keys: {failed_copies}"
        if failed_copies
        else None
    )
    phase_outcomes.append(
        PhaseOutcome(
            phase="publish",
            succeeded=copy_succeeded,
            error=publish_error,
        )
    )

    if not copy_succeeded:
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation_outcome,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            copied_live_keys=tuple(copied_live_keys),
            failed_copies=tuple(failed_copies),
            untouched_source_keys=tuple(
                sorted(source_paths_in_groups | set(always_untouched))
            ),
            error="Copy or live-key validation failed; staging and partial live "
            "outputs retained as recovery artifacts.",
        )

    # ------------------------------------------------------------------ #
    # Phase: drift_check — revalidate sources before any deletion
    # ------------------------------------------------------------------ #
    snapshot_map = {fi.absolute_path: fi for fi in plan.source_snapshot.files}
    drift_error: str | None = None

    for src_path in sorted(source_paths_in_groups):
        snap_fi = snapshot_map.get(src_path)
        if snap_fi is None:
            drift_error = f"Source file not in snapshot: {src_path}"
            break
        err_msg = _revalidate_source_token(filesystem, snap_fi)
        if err_msg is not None:
            drift_error = err_msg
            break

    drift_detected = drift_error is not None
    phase_outcomes.append(
        PhaseOutcome(
            phase="drift_check",
            succeeded=not drift_detected,
            error=drift_error,
        )
    )

    if drift_detected:
        # Zero source deletions when any drift is detected.
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation_outcome,
            publication=PublicationOutcome(
                succeeded=False,
                published_files=tuple(copied_live_keys),
                error=f"Source drift detected; no sources deleted: {drift_error}",
            ),
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            copied_live_keys=tuple(copied_live_keys),
            untouched_source_keys=tuple(
                sorted(source_paths_in_groups | set(always_untouched))
            ),
            drift_detected=True,
            error=f"Source drift detected, no sources deleted: {drift_error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: cleanup — delete compacted source objects
    #
    # Spec: "failures retain staging plus partial live artifacts WITHOUT
    # DELETING REMAINING SOURCES."  On the first delete failure we stop
    # immediately; every not-yet-deleted source is left untouched.
    # ------------------------------------------------------------------ #
    source_keys_deleted: list[str] = []
    delete_failed: list[str] = []
    remaining_to_delete = sorted(source_paths_in_groups)

    for src_path in remaining_to_delete:
        try:
            filesystem.rm(src_path)
            source_keys_deleted.append(src_path)
        except Exception as exc:
            logger.warning("Failed to delete source %s: %s", src_path, exc)
            delete_failed.append(src_path)
            # Stop immediately — do not attempt to delete any further sources.
            remaining_after_failure = [
                p
                for p in remaining_to_delete
                if p not in source_keys_deleted and p != src_path
            ]
            delete_failed.extend(remaining_after_failure)
            break

    delete_succeeded = not delete_failed
    phase_outcomes.append(
        PhaseOutcome(
            phase="cleanup",
            succeeded=delete_succeeded,
            error=(
                f"Failed to delete sources: {delete_failed}" if delete_failed else None
            ),
        )
    )

    # Remove staging prefix only after complete success.
    staging_prefix_in_result: str | None
    if delete_succeeded:
        try:
            filesystem.rm(staging_prefix, recursive=True)
        except Exception:  # noqa: BLE001
            pass
        staging_prefix_in_result = None
    else:
        staging_prefix_in_result = staging_prefix

    # Compute actual bytes written.
    total_bytes_written = 0
    for live_path in copied_live_keys:
        try:
            total_bytes_written += filesystem.info(live_path).get("size", 0)
        except Exception:  # noqa: BLE001
            pass

    remaining_untouched = tuple(sorted(set(delete_failed) | set(always_untouched)))
    succeeded = delete_succeeded

    return BestEffortCompactionResult(
        plan=plan,
        succeeded=succeeded,
        guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
        phase_outcomes=tuple(phase_outcomes),
        validation=validation_outcome,
        publication=PublicationOutcome(
            succeeded=succeeded,
            published_files=tuple(copied_live_keys),
            removed_source_files=tuple(source_keys_deleted),
            error=(
                f"Failed to delete sources: {delete_failed}" if delete_failed else None
            ),
        ),
        recovery=(
            RecoveryArtifacts(workspace_path=staging_prefix_in_result)
            if not succeeded
            else None
        ),
        actual_metrics=(
            ActualMetrics(
                row_count=sum(staged_key_rows[k] for k in staged_keys),
                file_count=len(copied_live_keys),
                total_bytes=total_bytes_written,
            )
            if succeeded
            else None
        ),
        staging_prefix=staging_prefix,
        staged_keys=tuple(staged_keys),
        copied_live_keys=tuple(copied_live_keys),
        untouched_source_keys=remaining_untouched,
        drift_detected=False,
    )


def _canonical_deduplication_value(value: Any) -> Any:
    """Return a hashable exact-value representation for key comparison.

    Maintenance key equality is deliberately stricter than Python's generic
    equality: nulls compare equal to nulls, NaNs compare equal to NaNs, and
    strings retain their exact stored values (no case folding, trimming, or
    normalization).  Type tags keep values such as ``1`` and ``True`` from
    collapsing merely because Python considers them equal.
    """
    if value is None:
        return ("null",)
    if isinstance(value, float) and value != value:
        return ("nan", type(value).__qualname__)
    if isinstance(value, (list, tuple)):
        return (
            type(value).__qualname__,
            tuple(_canonical_deduplication_value(item) for item in value),
        )
    if isinstance(value, dict):
        entries = [
            (
                _canonical_deduplication_value(key),
                _canonical_deduplication_value(item),
            )
            for key, item in value.items()
        ]
        return (
            type(value).__qualname__,
            tuple(sorted(entries, key=repr)),
        )
    try:
        hash(value)
    except TypeError:
        return (type(value).__qualname__, repr(value))
    return (type(value).__qualname__, value)


@dataclass(frozen=True)
class DedupSortKey:
    """One parsed deduplication ordering column and its sort direction.

    Only a leading ``-`` in the raw name is special: it marks the column
    descending so that the keep-first-wins rule selects the most recent row
    (the legacy ``-column`` pattern). Every other name — including one with a
    leading ``+`` or any other character — is a literal ascending column, so
    a column actually named e.g. ``+ts`` stays addressable.
    """

    column: str
    descending: bool = False


def parse_dedup_order_by(
    order_by: tuple[str, ...] | list[str] | None,
) -> tuple[DedupSortKey, ...]:
    """Parse raw ``dedup_order_by`` names into typed sort keys.

    This is the single place that interprets column-name ordering sigils: a
    leading ``-`` yields a descending ``DedupSortKey``; every other name is
    returned verbatim as an ascending key. Returns an empty tuple when no
    ordering is requested.
    """
    if not order_by:
        return ()
    parsed: list[DedupSortKey] = []
    for name in order_by:
        if name.startswith("-"):
            parsed.append(DedupSortKey(column=name[1:], descending=True))
        else:
            parsed.append(DedupSortKey(column=name, descending=False))
    return tuple(parsed)


def _deduplicate_partition_table(
    table: pa.Table,
    key_columns: tuple[str, ...] | None,
    order_by: tuple[str, ...] | None,
) -> pa.Table:
    """Retain one deterministic row for each key within one physical partition.

    Physical row order is the default winner order.  When ``order_by`` is
    explicit, Arrow's ordering is primary and original physical indices break
    equal-order ties.
    """
    keys = key_columns or tuple(table.column_names)
    physical_indices = list(range(table.num_rows))
    sort_keys = parse_dedup_order_by(order_by)
    if sort_keys:
        import pyarrow.compute as pc  # noqa: PLC0415

        # PyArrow adaptation stays local: the typed ``DedupSortKey`` values are
        # the single source of truth for column and direction.
        arrow_sort_keys = [
            (key.column, "descending" if key.descending else "ascending")
            for key in sort_keys
        ]
        order_columns = [key.column for key in sort_keys]

        sorted_indices = pc.sort_indices(
            table,
            sort_keys=arrow_sort_keys,
        ).to_pylist()
        ordered: list[int] = []
        start = 0
        while start < len(sorted_indices):
            end = start + 1
            first = sorted_indices[start]
            first_values = tuple(
                _canonical_deduplication_value(table[column][first].as_py())
                for column in order_columns
            )
            while end < len(sorted_indices):
                candidate = sorted_indices[end]
                candidate_values = tuple(
                    _canonical_deduplication_value(table[column][candidate].as_py())
                    for column in order_columns
                )
                if candidate_values != first_values:
                    break
                end += 1
            ordered.extend(sorted(sorted_indices[start:end]))
            start = end
        physical_indices = ordered

    seen: set[tuple[Any, ...]] = set()
    retained_indices: list[int] = []
    columns = [table[column].to_pylist() for column in keys]
    for row_index in physical_indices:
        key = tuple(
            _canonical_deduplication_value(column[row_index]) for column in columns
        )
        if key not in seen:
            seen.add(key)
            retained_indices.append(row_index)
    return table.take(pa.array(retained_indices, type=pa.int64()))


def _execute_best_effort_partition_local_deduplication(
    plan: PartitionLocalDeduplicationPlan,
    filesystem: AbstractFileSystem,
) -> BestEffortCompactionResult:
    """Execute partition-local deduplication through best-effort object publication."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    run_id = uuid.uuid4().hex[:16]
    dataset_root = plan.source_snapshot.dataset_path
    staging_prefix = posixpath.join(dataset_root, "_maintenance_staging", run_id)
    phase_outcomes: list[PhaseOutcome] = []
    sources_by_partition: dict[str, list[SourceFileInfo]] = {}
    for source in plan.source_snapshot.files:
        partition = posixpath.dirname(source.relative_path)
        sources_by_partition.setdefault(partition, []).append(source)
    staged_keys: list[str] = []
    staged_to_live: dict[str, str] = {}
    staged_rows: dict[str, int] = {}
    try:
        for partition_index, partition in enumerate(sorted(sources_by_partition)):
            tables: list[pa.Table] = []
            for source in sources_by_partition[partition]:
                with filesystem.open(source.absolute_path, "rb") as fh:
                    tables.append(pq.read_table(fh))
            deduplicated = _deduplicate_partition_table(
                pa.concat_tables(tables),
                plan.dedup_key_columns,
                plan.dedup_order_by,
            )
            if plan.schema is not None:
                deduplicated = deduplicated.cast(plan.schema)
            for chunk_index, chunk in enumerate(
                _split_table_by_rows(deduplicated, plan.max_rows_per_file)
            ):
                if chunk.num_rows == 0:
                    continue
                name = f"deduplicated-{run_id}-{partition_index:04d}-{chunk_index:04d}.parquet"
                staged_path = posixpath.join(staging_prefix, partition, name)
                live_path = posixpath.join(dataset_root, partition, name)
                buffer = BytesIO()
                pq.write_table(chunk, buffer, compression=plan.selected_codec)
                filesystem.pipe(staged_path, buffer.getvalue())
                staged_keys.append(staged_path)
                staged_to_live[staged_path] = live_path
                staged_rows[staged_path] = chunk.num_rows
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=True))
    except Exception as exc:
        error = f"Stage phase failed: {exc}"
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=False, error=error))
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            untouched_source_keys=tuple(
                source.absolute_path for source in plan.source_snapshot.files
            ),
            error=error,
        )

    try:
        for staged_path in staged_keys:
            with filesystem.open(staged_path, "rb") as fh:
                parquet_file = pq.ParquetFile(fh)
                if plan.schema is not None and not parquet_file.schema_arrow.equals(
                    plan.schema
                ):
                    raise ValueError(f"Schema mismatch in staged file {staged_path}")
        validation = ValidationOutcome(
            succeeded=True,
            staged_row_count=sum(staged_rows.values()),
        )
        phase_outcomes.append(PhaseOutcome(phase="validate", succeeded=True))
    except Exception as exc:
        error = f"Staged validation failed: {exc}"
        validation = ValidationOutcome(succeeded=False, error=error)
        phase_outcomes.append(
            PhaseOutcome(phase="validate", succeeded=False, error=error)
        )
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            untouched_source_keys=tuple(
                source.absolute_path for source in plan.source_snapshot.files
            ),
            error=error,
        )

    copied_live_keys: list[str] = []
    failed_copies: list[str] = []
    for staged_path, live_path in staged_to_live.items():
        try:
            filesystem.pipe(live_path, filesystem.cat(staged_path))
            with filesystem.open(live_path, "rb") as fh:
                pq.read_metadata(fh)
            copied_live_keys.append(live_path)
        except Exception:
            failed_copies.append(live_path)
    phase_outcomes.append(
        PhaseOutcome(
            phase="publish",
            succeeded=not failed_copies,
            error=f"Failed live copies: {failed_copies}" if failed_copies else None,
        )
    )
    all_sources = tuple(source.absolute_path for source in plan.source_snapshot.files)
    if failed_copies:
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            copied_live_keys=tuple(copied_live_keys),
            failed_copies=tuple(failed_copies),
            untouched_source_keys=all_sources,
            error="Live copy failed; source objects were not deleted.",
        )

    drift_errors: list[str] = []
    for source_file in plan.source_snapshot.files:
        drift_error = _revalidate_source_token(filesystem, source_file)
        if drift_error is not None:
            drift_errors.append(drift_error)
    if drift_errors:
        error = drift_errors[0]
        phase_outcomes.append(
            PhaseOutcome(phase="drift_check", succeeded=False, error=error)
        )
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            publication=PublicationOutcome(
                succeeded=False,
                published_files=tuple(copied_live_keys),
                error=error,
            ),
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            copied_live_keys=tuple(copied_live_keys),
            untouched_source_keys=all_sources,
            drift_detected=True,
            error=f"Source drift detected; no sources deleted: {error}",
        )
    phase_outcomes.append(PhaseOutcome(phase="drift_check", succeeded=True))
    deleted: list[str] = []
    try:
        for source_key in all_sources:
            filesystem.rm(source_key)
            deleted.append(source_key)
    except Exception as exc:
        error = f"Source deletion failed: {exc}"
        phase_outcomes.append(
            PhaseOutcome(phase="cleanup", succeeded=False, error=error)
        )
        return BestEffortCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            copied_live_keys=tuple(copied_live_keys),
            untouched_source_keys=tuple(
                source_key for source_key in all_sources if source_key not in deleted
            ),
            error=error,
        )
    filesystem.rm(staging_prefix, recursive=True)
    phase_outcomes.append(PhaseOutcome(phase="cleanup", succeeded=True))
    return BestEffortCompactionResult(
        plan=plan,
        succeeded=True,
        guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
        phase_outcomes=tuple(phase_outcomes),
        validation=validation,
        publication=PublicationOutcome(
            succeeded=True,
            published_files=tuple(copied_live_keys),
            removed_source_files=tuple(deleted),
        ),
        actual_metrics=ActualMetrics(
            row_count=sum(staged_rows.values()),
            file_count=len(copied_live_keys),
            total_bytes=sum(
                filesystem.info(path).get("size", 0) for path in copied_live_keys
            ),
        ),
        staging_prefix=staging_prefix,
        staged_keys=tuple(staged_keys),
        copied_live_keys=tuple(copied_live_keys),
    )


def _execute_best_effort_global_repartition_deduplication(
    plan: GlobalRepartitionDeduplicationPlan,
    filesystem: AbstractFileSystem,
) -> BestEffortGlobalRepartitionDeduplicationResult:
    """Execute global deduplication through best-effort staged publication.

    Unlike partition-local deduplication, this operation reads every source
    object into one snapshot-local table before selecting winners.  Retained
    rows are then written under Hive-style paths for exactly the declared
    destination partition columns.  No source object is removed until all
    staged output has been validated, every planned live key has been copied
    and validated, and every source object has been revalidated.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    run_id = uuid.uuid4().hex[:16]
    dataset_root = plan.source_snapshot.dataset_path
    staging_prefix = posixpath.join(dataset_root, "_maintenance_staging", run_id)
    source_keys = tuple(source.absolute_path for source in plan.source_snapshot.files)
    destination_keys: list[str] = []
    phase_outcomes: list[PhaseOutcome] = []
    staged_keys: list[str] = []
    staged_to_live: dict[str, str] = {}
    staged_rows: dict[str, int] = {}
    staged_partition_values: dict[str, tuple[Any, ...]] = {}
    retained_rows = 0

    def result(
        *,
        succeeded: bool,
        validation: ValidationOutcome | None = None,
        publication: PublicationOutcome | None = None,
        recovery: RecoveryArtifacts | None = None,
        actual_metrics: ActualMetrics | None = None,
        copied_live_keys: tuple[str, ...] = (),
        failed_copies: tuple[str, ...] = (),
        untouched_source_keys: tuple[str, ...] = source_keys,
        drift_detected: bool = False,
        error: str | None = None,
    ) -> BestEffortGlobalRepartitionDeduplicationResult:
        return BestEffortGlobalRepartitionDeduplicationResult(
            plan=plan,
            succeeded=succeeded,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            publication=publication,
            recovery=recovery,
            actual_metrics=actual_metrics,
            error=error,
            staging_prefix=staging_prefix,
            staged_keys=tuple(staged_keys),
            copied_live_keys=copied_live_keys,
            failed_copies=failed_copies,
            untouched_source_keys=untouched_source_keys,
            drift_detected=drift_detected,
            destination_partition_columns=plan.partition_columns,
            destination_partition_keys=tuple(destination_keys),
        )

    from urllib.parse import quote  # noqa: PLC0415

    reserved_partition_values = {
        "nan",
        "__HIVE_DEFAULT_PARTITION__",
        "__HIVE_NAN_PARTITION__",
    }
    partition_escape_prefix = "__fsspeckit_value__"

    def partition_component(value: Any) -> str:
        if value is None:
            return "__HIVE_DEFAULT_PARTITION__"
        if isinstance(value, float) and value != value:
            return "__HIVE_NAN_PARTITION__"
        encoded = quote(str(value), safe="")
        if encoded in reserved_partition_values or encoded.startswith(
            partition_escape_prefix
        ):
            # Keep ordinary Hive paths readable while making literal values
            # unambiguous with null/NaN sentinels.  Values beginning with the
            # escape prefix are escaped recursively by this marker.
            encoded = partition_escape_prefix + encoded
        return encoded

    # ------------------------------------------------------------------ #
    try:
        tables: list[pa.Table] = []
        # The physical fallback order is partition path, file path, row offset.
        # SourceSnapshot preserves discovery metadata but fsspec listings are
        # not required to return keys in a stable order.
        source_files = sorted(
            plan.source_snapshot.files,
            key=lambda source: source.relative_path,
        )
        for source in source_files:
            with filesystem.open(source.absolute_path, "rb") as fh:
                tables.append(pq.read_table(fh))
        if not tables:
            raise ValueError("Global deduplication requires at least one source table")
        combined = pa.concat_tables(tables)
        deduplicated = _deduplicate_partition_table(
            combined,
            plan.dedup_key_columns,
            plan.dedup_order_by,
        )
        if plan.schema is not None:
            deduplicated = deduplicated.cast(plan.schema)
        retained_rows = deduplicated.num_rows

        partition_arrays = [
            deduplicated[column].to_pylist() for column in plan.partition_columns
        ]
        partition_groups: dict[tuple[Any, ...], tuple[tuple[Any, ...], list[int]]] = {}
        for row_index in range(retained_rows):
            raw_values = tuple(values[row_index] for values in partition_arrays)
            canonical_values = tuple(
                _canonical_deduplication_value(value) for value in raw_values
            )
            entry = partition_groups.get(canonical_values)
            if entry is None:
                entry = (raw_values, [])
                partition_groups[canonical_values] = entry
            entry[1].append(row_index)

        sorted_partitions = sorted(
            partition_groups.values(),
            key=lambda item: tuple(partition_component(value) for value in item[0]),
        )
        for partition_index, (raw_values, row_indices) in enumerate(sorted_partitions):
            partition_path = "/".join(
                f"{quote(column, safe='')}={partition_component(value)}"
                for column, value in zip(
                    plan.partition_columns, raw_values, strict=True
                )
            )
            destination_keys.append(partition_path)
            partition_table = deduplicated.take(pa.array(row_indices, type=pa.int64()))
            for chunk_index, chunk in enumerate(
                _split_table_by_rows(partition_table, plan.max_rows_per_file)
            ):
                if chunk.num_rows == 0:
                    continue
                filename = (
                    f"deduplicated-{run_id}-{partition_index:04d}-"
                    f"{chunk_index:04d}.parquet"
                )
                staged_path = posixpath.join(staging_prefix, partition_path, filename)
                live_path = posixpath.join(dataset_root, partition_path, filename)
                buffer = BytesIO()
                pq.write_table(chunk, buffer, compression=plan.selected_codec)
                filesystem.pipe(staged_path, buffer.getvalue())
                staged_keys.append(staged_path)
                staged_to_live[staged_path] = live_path
                staged_rows[staged_path] = chunk.num_rows
                staged_partition_values[staged_path] = raw_values
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=True))
    except Exception as exc:
        error = f"Stage phase failed: {exc}"
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=False, error=error))
        return result(
            succeeded=False,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            error=error,
        )

    # ------------------------------------------------------------------ #
    # Phase: validate — every staged object must be readable, schema-compatible,
    # and collectively contain exactly the globally retained rows.
    # ------------------------------------------------------------------ #
    total_staged_rows = 0
    validation_error: str | None = None
    try:
        for staged_path in staged_keys:
            with filesystem.open(staged_path, "rb") as fh:
                parquet_file = pq.ParquetFile(fh)
                rows = parquet_file.metadata.num_rows
                total_staged_rows += rows
                if rows != staged_rows[staged_path]:
                    validation_error = (
                        f"Row count mismatch in staged file {staged_path}: "
                        f"got {rows}, expected {staged_rows[staged_path]}"
                    )
                    break
                if plan.schema is not None and not parquet_file.schema_arrow.equals(
                    plan.schema
                ):
                    validation_error = f"Schema mismatch in staged file {staged_path}"
                    break
                expected_partition_path = "/".join(
                    f"{quote(column, safe='')}={partition_component(value)}"
                    for column, value in zip(
                        plan.partition_columns,
                        staged_partition_values[staged_path],
                        strict=True,
                    )
                )
                staged_relative = posixpath.relpath(staged_path, staging_prefix)
                actual_partition_path = posixpath.dirname(staged_relative)
                if actual_partition_path != expected_partition_path:
                    validation_error = (
                        "Destination path mismatch in staged file "
                        f"{staged_path}: expected {expected_partition_path!r}, "
                        f"got {actual_partition_path!r}"
                    )
                    break
                if validation_error is None:
                    expected_partition = tuple(
                        _canonical_deduplication_value(value)
                        for value in staged_partition_values[staged_path]
                    )
                    with filesystem.open(staged_path, "rb") as partition_handle:
                        staged_table = pq.read_table(partition_handle)
                    for row_index in range(staged_table.num_rows):
                        actual_partition = tuple(
                            _canonical_deduplication_value(
                                staged_table[column][row_index].as_py()
                            )
                            for column in plan.partition_columns
                        )
                        if actual_partition != expected_partition:
                            validation_error = (
                                "Destination partition mismatch in staged file "
                                f"{staged_path}: expected {expected_partition}, "
                                f"got {actual_partition}"
                            )
                            break
        if validation_error is None and total_staged_rows != retained_rows:
            validation_error = (
                f"Staged row count mismatch: got {total_staged_rows}, "
                f"expected {retained_rows}"
            )
    except Exception as exc:
        validation_error = f"Staged validation error: {exc}"
    validation = ValidationOutcome(
        succeeded=validation_error is None,
        staged_row_count=total_staged_rows,
        expected_row_count=retained_rows,
        error=validation_error,
    )
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate",
            succeeded=validation_error is None,
            error=validation_error,
        )
    )
    if validation_error is not None:
        return result(
            succeeded=False,
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            error=f"Staged validation failed: {validation_error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: publish — copy and exactly validate each planned live key.
    # ------------------------------------------------------------------ #
    copied_live_keys: list[str] = []
    failed_copies: list[str] = []
    for staged_path, live_path in staged_to_live.items():
        try:
            content = filesystem.cat(staged_path)
            filesystem.pipe(live_path, content)
            if filesystem.cat(live_path) != content:
                raise ValueError(f"Live key content mismatch for {live_path}")
            with filesystem.open(live_path, "rb") as fh:
                parquet_file = pq.ParquetFile(fh)
                if parquet_file.metadata.num_rows != staged_rows[staged_path]:
                    raise ValueError(f"Live key row-count mismatch for {live_path}")
                if plan.schema is not None and not parquet_file.schema_arrow.equals(
                    plan.schema
                ):
                    raise ValueError(f"Live key schema mismatch for {live_path}")
            copied_live_keys.append(live_path)
        except Exception as exc:
            logger.warning(
                "Failed to copy/validate staged key %s → %s: %s",
                staged_path,
                live_path,
                exc,
            )
            failed_copies.append(live_path)
    copy_error = (
        f"Failed to copy or validate live keys: {failed_copies}"
        if failed_copies
        else None
    )
    phase_outcomes.append(
        PhaseOutcome(
            phase="publish",
            succeeded=not failed_copies,
            error=copy_error,
        )
    )
    if failed_copies:
        return result(
            succeeded=False,
            validation=validation,
            publication=PublicationOutcome(
                succeeded=False,
                published_files=tuple(copied_live_keys),
                error=copy_error,
            ),
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            copied_live_keys=tuple(copied_live_keys),
            failed_copies=tuple(failed_copies),
            error=(
                "Copy or live-key validation failed; staging and partial live "
                "outputs retained as recovery artifacts."
            ),
        )

    # ------------------------------------------------------------------ #
    # Phase: drift_check — revalidate every source before any deletion.
    # ------------------------------------------------------------------ #
    drift_error: str | None = None
    for source in plan.source_snapshot.files:
        drift_error = _revalidate_source_token(filesystem, source)
        if drift_error is not None:
            break
    phase_outcomes.append(
        PhaseOutcome(
            phase="drift_check",
            succeeded=drift_error is None,
            error=drift_error,
        )
    )
    if drift_error is not None:
        return result(
            succeeded=False,
            validation=validation,
            publication=PublicationOutcome(
                succeeded=False,
                published_files=tuple(copied_live_keys),
                error=f"Source drift detected; no sources deleted: {drift_error}",
            ),
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            copied_live_keys=tuple(copied_live_keys),
            drift_detected=True,
            error=f"Source drift detected, no sources deleted: {drift_error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: cleanup — remove all sources only after the preceding gates pass.
    # A delete failure stops immediately and leaves every remaining source
    # untouched; no automatic rollback is claimed for an already successful
    # delete on an object store.
    # ------------------------------------------------------------------ #
    deleted_sources: list[str] = []
    delete_failures: list[str] = []
    for source_path in source_keys:
        try:
            filesystem.rm(source_path)
            deleted_sources.append(source_path)
        except Exception as exc:
            logger.warning("Failed to delete source %s: %s", source_path, exc)
            delete_failures.append(source_path)
            delete_failures.extend(
                path
                for path in source_keys
                if path not in deleted_sources and path != source_path
            )
            break
    if delete_failures:
        error = f"Failed to delete sources: {delete_failures}"
        phase_outcomes.append(
            PhaseOutcome(phase="cleanup", succeeded=False, error=error)
        )
        return result(
            succeeded=False,
            validation=validation,
            publication=PublicationOutcome(
                succeeded=False,
                published_files=tuple(copied_live_keys),
                removed_source_files=tuple(deleted_sources),
                error=error,
            ),
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            copied_live_keys=tuple(copied_live_keys),
            untouched_source_keys=tuple(
                path for path in source_keys if path not in deleted_sources
            ),
            error=error,
        )

    try:
        if filesystem.exists(staging_prefix):
            filesystem.rm(staging_prefix, recursive=True)
    except Exception as exc:
        error = f"Failed to remove staging prefix: {exc}"
        phase_outcomes.append(
            PhaseOutcome(phase="cleanup", succeeded=False, error=error)
        )
        return result(
            succeeded=False,
            validation=validation,
            publication=PublicationOutcome(
                succeeded=False,
                published_files=tuple(copied_live_keys),
                removed_source_files=tuple(deleted_sources),
                error=error,
            ),
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            copied_live_keys=tuple(copied_live_keys),
            untouched_source_keys=(),
            error=error,
        )

    phase_outcomes.append(PhaseOutcome(phase="cleanup", succeeded=True))
    total_bytes = 0
    for live_path in copied_live_keys:
        try:
            total_bytes += int(filesystem.info(live_path).get("size", 0))
        except Exception:
            pass
    return result(
        succeeded=True,
        validation=validation,
        publication=PublicationOutcome(
            succeeded=True,
            published_files=tuple(copied_live_keys),
            removed_source_files=tuple(deleted_sources),
        ),
        actual_metrics=ActualMetrics(
            row_count=retained_rows,
            file_count=len(copied_live_keys),
            total_bytes=total_bytes,
        ),
        copied_live_keys=tuple(copied_live_keys),
        untouched_source_keys=(),
    )


def _execute_best_effort_coordinated_optimization(
    plan: CoordinatedOptimizationPlan,
    filesystem: AbstractFileSystem,
) -> BestEffortCoordinatedOptimizationResult:
    """Execute best-effort coordinated optimization: optional dedup + compaction.

    Coordinated optimization composes partition-local deduplication (when
    ``dedup_key_columns`` is set) with compaction through one unified staged
    lifecycle.  Both phases are reported separately in ``phase_outcomes``.
    A failure in either phase prevents unsafe deletion of still-valid sources.

    Lifecycle phases (in order):

    1. **dedup** *(optional)* — read each optimization group, deduplicate
       within the group, report rows removed.  Skipped when no dedup keys.
    2. **stage** — split each (deduplicated or raw) group table by
       ``max_rows_per_file`` and write every chunk under the staging prefix.
    3. **validate** — read every staged file, confirm readability and schema
       compatibility.
    4. **publish** — copy each staged file to its planned live key; validate
       every live key individually after copy.
    5. **drift_check** — revalidate every planned source object immediately
       before deletion; if any drift is detected, delete no source objects.
    6. **cleanup** — delete source objects; stop on first failure.  Remove
       staging prefix on full success.

    On any failure the staging prefix and any partial live outputs are
    reported as recovery artifacts; no automatic rollback is performed.

    Args:
        plan: An accepted :class:`CoordinatedOptimizationPlan` with
            ``guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE``.
        filesystem: The fsspec filesystem used to read and write all files.

    Returns:
        A :class:`CoordinatedOptimizationResult` describing every phase,
        staged keys, copied live keys, copy failures, untouched source keys,
        whether source drift was detected, and whether the dedup phase ran.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    run_id = uuid.uuid4().hex[:16]
    dataset_root = plan.source_snapshot.dataset_path
    staging_prefix = posixpath.join(dataset_root, "_maintenance_staging", run_id)
    phase_outcomes: list[PhaseOutcome] = []

    source_paths_in_groups: set[str] = {
        fi.path for group in plan.optimization_groups for fi in group.files
    }
    all_source_paths = {fi.absolute_path for fi in plan.source_snapshot.files}
    always_untouched = sorted(all_source_paths - source_paths_in_groups)
    snapshot_map = {fi.absolute_path: fi for fi in plan.source_snapshot.files}

    dedup_phase_executed = plan.dedup_key_columns is not None
    dedup_rows_removed: int | None = None

    # ------------------------------------------------------------------ #
    # Helper: build a failure result that preserves all recovery context.
    # ------------------------------------------------------------------ #
    def _failure(
        *,
        validation: ValidationOutcome | None = None,
        publication: PublicationOutcome | None = None,
        staged_keys_tuple: tuple[str, ...] = (),
        copied_live_keys: tuple[str, ...] = (),
        failed_copies: tuple[str, ...] = (),
        drift_detected: bool = False,
        error: str | None = None,
    ) -> BestEffortCoordinatedOptimizationResult:
        return BestEffortCoordinatedOptimizationResult(
            plan=plan,
            succeeded=False,
            guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            publication=publication,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            staging_prefix=staging_prefix,
            staged_keys=staged_keys_tuple,
            copied_live_keys=copied_live_keys,
            failed_copies=failed_copies,
            untouched_source_keys=tuple(
                sorted(source_paths_in_groups | set(always_untouched))
            ),
            drift_detected=drift_detected,
            dedup_phase_executed=dedup_phase_executed,
            dedup_rows_removed=dedup_rows_removed,
            error=error,
        )

    # ------------------------------------------------------------------ #
    # Phase: dedup (optional) — read and deduplicate within each group.
    # ------------------------------------------------------------------ #
    group_tables: list[tuple[pa.Table, str]] = []  # (table, partition_dir)
    expected_output_rows = 0

    if dedup_phase_executed:
        try:
            total_input_rows = 0
            total_output_rows = 0
            for group in plan.optimization_groups:
                tables: list[pa.Table] = []
                for fi in group.files:
                    with filesystem.open(fi.path, "rb") as fh:
                        tables.append(pq.read_table(fh))
                combined = pa.concat_tables(tables)
                total_input_rows += combined.num_rows
                deduped = _deduplicate_partition_table(
                    combined,
                    plan.dedup_key_columns,
                    plan.dedup_order_by,
                )
                total_output_rows += deduped.num_rows
                partition_dir = posixpath.dirname(
                    _relative_file_path(group.files[0].path, dataset_root)
                )
                group_tables.append((deduped, partition_dir))
            dedup_rows_removed = total_input_rows - total_output_rows
            expected_output_rows = total_output_rows
            phase_outcomes.append(PhaseOutcome(phase="dedup", succeeded=True))
        except Exception as exc:
            err = f"Dedup phase failed: {exc}"
            phase_outcomes.append(
                PhaseOutcome(phase="dedup", succeeded=False, error=err)
            )
            return _failure(error=err)
    else:
        # No dedup: read and concat each group; expected output rows equals
        # total source rows.
        for group in plan.optimization_groups:
            tables = []
            for fi in group.files:
                with filesystem.open(fi.path, "rb") as fh:
                    tables.append(pq.read_table(fh))
            combined = pa.concat_tables(tables)
            partition_dir = posixpath.dirname(
                _relative_file_path(group.files[0].path, dataset_root)
            )
            group_tables.append((combined, partition_dir))
        expected_output_rows = sum(t.num_rows for t, _ in group_tables)

    # ------------------------------------------------------------------ #
    # Phase: stage — write compaction outputs under the staging prefix.
    # ------------------------------------------------------------------ #
    staged_keys: list[str] = []
    staged_key_rows: dict[str, int] = {}
    staged_to_live: dict[str, str] = {}

    try:
        for group_idx, (table, partition_dir) in enumerate(group_tables):
            if plan.schema is not None:
                table = table.cast(plan.schema)
            for chunk_idx, chunk in enumerate(
                _split_table_by_rows(table, plan.max_rows_per_file)
            ):
                if chunk.num_rows == 0:
                    continue
                name = f"optimized-{run_id}-{group_idx:04d}-{chunk_idx:04d}.parquet"
                staged_path = posixpath.join(staging_prefix, partition_dir, name)
                live_path = posixpath.join(dataset_root, partition_dir, name)
                buf = BytesIO()
                pq.write_table(chunk, buf, compression=plan.selected_codec)
                filesystem.pipe(staged_path, buf.getvalue())
                staged_keys.append(staged_path)
                staged_key_rows[staged_path] = chunk.num_rows
                staged_to_live[staged_path] = live_path
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=True))
    except Exception as exc:
        err = f"Stage phase failed: {exc}"
        phase_outcomes.append(PhaseOutcome(phase="stage", succeeded=False, error=err))
        return _failure(staged_keys_tuple=tuple(staged_keys), error=err)

    # ------------------------------------------------------------------ #
    # Phase: validate — check ALL staged files before any live copy.
    # ------------------------------------------------------------------ #
    total_staged_rows = 0
    validation_error: str | None = None

    try:
        for staged_path in staged_keys:
            with filesystem.open(staged_path, "rb") as fh:
                staged_file = pq.ParquetFile(fh)
                total_staged_rows += staged_file.metadata.num_rows
                if plan.schema is not None:
                    staged_schema = staged_file.schema_arrow
                    if not staged_schema.equals(plan.schema):
                        validation_error = (
                            f"Schema mismatch in staged file {staged_path}: "
                            f"expected {plan.schema}, got {staged_schema}"
                        )
                        break
        if validation_error is None and total_staged_rows != expected_output_rows:
            validation_error = (
                f"Staged row count mismatch: got {total_staged_rows}, "
                f"expected {expected_output_rows}"
            )
    except Exception as exc:
        validation_error = f"Staged validation error: {exc}"

    validation_outcome = ValidationOutcome(
        succeeded=validation_error is None,
        staged_row_count=total_staged_rows,
        expected_row_count=expected_output_rows,
        error=validation_error,
    )
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate",
            succeeded=validation_error is None,
            error=validation_error,
        )
    )

    if validation_error is not None:
        return _failure(
            validation=validation_outcome,
            staged_keys_tuple=tuple(staged_keys),
            error=f"Staged validation failed: {validation_error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: publish — copy staged → live keys, validate each live key.
    # ------------------------------------------------------------------ #
    copied_live_keys: list[str] = []
    failed_copies: list[str] = []

    for staged_path in staged_keys:
        live_path = staged_to_live[staged_path]
        try:
            content = filesystem.cat(staged_path)
            filesystem.pipe(live_path, content)
            with filesystem.open(live_path, "rb") as fh:
                pq.read_metadata(fh)
            copied_live_keys.append(live_path)
        except Exception as exc:
            logger.warning(
                "Failed to copy/validate staged key %s → %s: %s",
                staged_path,
                live_path,
                exc,
            )
            failed_copies.append(live_path)

    copy_succeeded = not failed_copies
    publish_error = (
        f"Failed to copy or validate live keys: {failed_copies}"
        if failed_copies
        else None
    )
    phase_outcomes.append(
        PhaseOutcome(
            phase="publish",
            succeeded=copy_succeeded,
            error=publish_error,
        )
    )

    if not copy_succeeded:
        return _failure(
            validation=validation_outcome,
            staged_keys_tuple=tuple(staged_keys),
            copied_live_keys=tuple(copied_live_keys),
            failed_copies=tuple(failed_copies),
            error=(
                "Copy or live-key validation failed; staging and partial live "
                "outputs retained as recovery artifacts."
            ),
        )

    # ------------------------------------------------------------------ #
    # Phase: drift_check — revalidate sources before any deletion.
    # ------------------------------------------------------------------ #
    drift_error: str | None = None

    for src_path in sorted(source_paths_in_groups):
        snap_fi = snapshot_map.get(src_path)
        if snap_fi is None:
            drift_error = f"Source file not in snapshot: {src_path}"
            break
        err_msg = _revalidate_source_token(filesystem, snap_fi)
        if err_msg is not None:
            drift_error = err_msg
            break

    drift_detected = drift_error is not None
    phase_outcomes.append(
        PhaseOutcome(
            phase="drift_check",
            succeeded=not drift_detected,
            error=drift_error,
        )
    )

    if drift_detected:
        return _failure(
            validation=validation_outcome,
            publication=PublicationOutcome(
                succeeded=False,
                published_files=tuple(copied_live_keys),
                error=f"Source drift detected; no sources deleted: {drift_error}",
            ),
            staged_keys_tuple=tuple(staged_keys),
            copied_live_keys=tuple(copied_live_keys),
            drift_detected=True,
            error=f"Source drift detected, no sources deleted: {drift_error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: cleanup — delete source objects; stop on first failure.
    # ------------------------------------------------------------------ #
    source_keys_deleted: list[str] = []
    delete_failed: list[str] = []
    remaining_to_delete = sorted(source_paths_in_groups)

    for src_path in remaining_to_delete:
        try:
            filesystem.rm(src_path)
            source_keys_deleted.append(src_path)
        except Exception as exc:
            logger.warning("Failed to delete source %s: %s", src_path, exc)
            delete_failed.append(src_path)
            remaining_after_failure = [
                p
                for p in remaining_to_delete
                if p not in source_keys_deleted and p != src_path
            ]
            delete_failed.extend(remaining_after_failure)
            break

    delete_succeeded = not delete_failed
    phase_outcomes.append(
        PhaseOutcome(
            phase="cleanup",
            succeeded=delete_succeeded,
            error=(
                f"Failed to delete sources: {delete_failed}" if delete_failed else None
            ),
        )
    )

    staging_prefix_in_result: str | None
    if delete_succeeded:
        try:
            filesystem.rm(staging_prefix, recursive=True)
        except Exception:  # noqa: BLE001
            pass
        staging_prefix_in_result = None
    else:
        staging_prefix_in_result = staging_prefix

    total_bytes_written = 0
    for live_path in copied_live_keys:
        try:
            total_bytes_written += filesystem.info(live_path).get("size", 0)
        except Exception:  # noqa: BLE001
            pass

    remaining_untouched = tuple(sorted(set(delete_failed) | set(always_untouched)))
    succeeded = delete_succeeded

    return BestEffortCoordinatedOptimizationResult(
        plan=plan,
        succeeded=succeeded,
        guarantee_level=GuaranteeLevel.BEST_EFFORT_OBJECT_STORE,
        phase_outcomes=tuple(phase_outcomes),
        validation=validation_outcome,
        publication=PublicationOutcome(
            succeeded=succeeded,
            published_files=tuple(copied_live_keys),
            removed_source_files=tuple(source_keys_deleted),
            error=(
                f"Failed to delete sources: {delete_failed}" if delete_failed else None
            ),
        ),
        recovery=(
            RecoveryArtifacts(workspace_path=staging_prefix_in_result)
            if not succeeded
            else None
        ),
        actual_metrics=(
            ActualMetrics(
                row_count=sum(staged_key_rows[k] for k in staged_keys),
                file_count=len(copied_live_keys),
                total_bytes=total_bytes_written,
            )
            if succeeded
            else None
        ),
        staging_prefix=staging_prefix,
        staged_keys=tuple(staged_keys),
        copied_live_keys=tuple(copied_live_keys),
        untouched_source_keys=remaining_untouched,
        drift_detected=False,
        dedup_phase_executed=dedup_phase_executed,
        dedup_rows_removed=dedup_rows_removed,
    )


class DatasetMaintenanceCoordinator:
    """Direct coordinator that creates immutable, backend-pinned maintenance plans.

    Planning is lock-free and does not modify dataset files.  Execution
    implements the full staged-rename lifecycle for ``atomic_local``
    :class:`CompactionPlan` (flat local datasets, issue #37) and the
    staged-copy lifecycle for ``best_effort_object_store``
    :class:`CompactionPlan` (issue #39).  Other plan types are left as
    typed seams with descriptive ``NotImplementedError`` messages.
    """

    def __init__(self, backend: str | MaintenanceBackend) -> None:
        self.backend = _coerce_backend(backend)

    # ------------------------------------------------------------------ #
    # Planning API
    # ------------------------------------------------------------------ #

    def _common_plan_inputs(
        self,
        operation: MaintenanceOperation,
        dataset_path: str,
        filesystem: AbstractFileSystem | None,
        partition_filter: list[str] | None,
        target_mb_per_file: int | None,
        validation_level: ValidationLevel | str | None,
        codec: str | None,
    ) -> tuple[
        AbstractFileSystem,
        list[dict[str, Any]],
        SourceSnapshot,
        PartitionScope,
        SchemaOutcome,
        Any,
        str,
        int | None,
        ValidationLevel,
    ]:
        """Internal helper that delegates to the module-level input builder."""
        return _prepare_plan_inputs(
            operation,
            dataset_path,
            filesystem,
            partition_filter,
            None,
            target_mb_per_file,
            validation_level,
            codec,
        )

    def plan_compaction(
        self,
        dataset_path: str,
        filesystem: AbstractFileSystem | None = None,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        validation_level: ValidationLevel | str | None = None,
        codec: str | None = None,
    ) -> CompactionPlan:
        """Create an immutable compaction plan without modifying files."""
        if target_rows_per_file is not None and target_rows_per_file <= 0:
            raise ValueError("target_rows_per_file must be > 0")
        (
            fs,
            file_stats,
            snapshot,
            scope,
            schema_outcome,
            schema,
            selected_codec,
            target_byte_size,
            validation,
        ) = self._common_plan_inputs(
            MaintenanceOperation.COMPACTION,
            dataset_path,
            filesystem,
            partition_filter,
            target_mb_per_file,
            validation_level,
            codec,
        )
        # Partition-local planning (#38) is scoped to the atomic_local path,
        # whose per-subtree publisher preserves partition tuples. The
        # best_effort_object_store publisher (#39) flattens outputs and keeps
        # its own planning policy until its partition-subtree work lands.
        guarantee_level = _classify_guarantee(fs)
        if guarantee_level == GuaranteeLevel.ATOMIC_LOCAL:
            compaction_groups = _plan_partition_local_compaction_groups(
                file_stats,
                snapshot.dataset_path,
                target_mb_per_file,
                target_rows_per_file,
            )
        else:
            compaction_groups = tuple(
                plan_compaction_groups(
                    file_stats, target_mb_per_file, target_rows_per_file
                )["groups"]
            )
        return CompactionPlan(
            source_snapshot=snapshot,
            selected_backend=self.backend,
            guarantee_level=guarantee_level,
            partition_scope=scope,
            schema_outcome=schema_outcome,
            selected_codec=selected_codec,
            max_rows_per_file=target_rows_per_file,
            target_byte_size=target_byte_size,
            validation_level=validation,
            schema=schema,
            compaction_groups=compaction_groups,
        )

    def plan_partition_local_deduplication(
        self,
        dataset_path: str,
        filesystem: AbstractFileSystem | None = None,
        key_columns: list[str] | None = None,
        dedup_order_by: list[str] | None = None,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        validation_level: ValidationLevel | str | None = None,
        codec: str | None = None,
    ) -> PartitionLocalDeduplicationPlan:
        """Create an immutable partition-local deduplication plan."""
        if target_rows_per_file is not None and target_rows_per_file <= 0:
            raise ValueError("target_rows_per_file must be > 0")
        (
            fs,
            file_stats,
            snapshot,
            scope,
            schema_outcome,
            schema,
            selected_codec,
            target_byte_size,
            validation,
        ) = self._common_plan_inputs(
            MaintenanceOperation.PARTITION_LOCAL_DEDUPLICATION,
            dataset_path,
            filesystem,
            partition_filter,
            target_mb_per_file,
            validation_level,
            codec,
        )
        normalized_key_columns, normalized_dedup_order_by = (
            validate_deduplication_inputs(key_columns, dedup_order_by)
        )
        groups = _plan_partition_local_deduplication_groups(
            file_stats, snapshot.dataset_path
        )
        return PartitionLocalDeduplicationPlan(
            source_snapshot=snapshot,
            selected_backend=self.backend,
            guarantee_level=_classify_guarantee(fs),
            partition_scope=scope,
            schema_outcome=schema_outcome,
            selected_codec=selected_codec,
            max_rows_per_file=target_rows_per_file,
            target_byte_size=target_byte_size,
            validation_level=validation,
            schema=schema,
            dedup_key_columns=(
                tuple(normalized_key_columns) if normalized_key_columns else None
            ),
            dedup_order_by=(
                tuple(normalized_dedup_order_by) if normalized_dedup_order_by else None
            ),
            dedup_groups=groups,
        )

    def plan_global_repartition_deduplication(
        self,
        dataset_path: str,
        partition_columns: list[str],
        filesystem: AbstractFileSystem | None = None,
        key_columns: list[str] | None = None,
        dedup_order_by: list[str] | None = None,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        validation_level: ValidationLevel | str | None = None,
        codec: str | None = None,
    ) -> GlobalRepartitionDeduplicationPlan:
        """Create an immutable global-repartitioning deduplication plan."""
        if target_rows_per_file is not None and target_rows_per_file <= 0:
            raise ValueError("target_rows_per_file must be > 0")
        if not partition_columns:
            raise ValueError("partition_columns must be a non-empty list")
        if any(
            not isinstance(column, str) or not column for column in partition_columns
        ):
            raise ValueError("partition_columns must contain non-empty strings")
        if len(set(partition_columns)) != len(partition_columns):
            raise ValueError("partition_columns must not contain duplicates")
        (
            fs,
            file_stats,
            snapshot,
            scope,
            schema_outcome,
            schema,
            selected_codec,
            target_byte_size,
            validation,
        ) = _prepare_plan_inputs(
            MaintenanceOperation.GLOBAL_REPARTITION_DEDUPLICATION,
            dataset_path,
            filesystem,
            None,
            partition_columns,
            target_mb_per_file,
            validation_level,
            codec,
        )
        if schema is None or any(
            column not in schema.names for column in partition_columns
        ):
            raise ValueError(
                "partition_columns must name columns present in the source schema"
            )
        normalized_key_columns, normalized_dedup_order_by = (
            validate_deduplication_inputs(key_columns, dedup_order_by)
        )
        # Global deduplication always includes every source file.  Output
        # chunking is applied after one complete-dataset winner selection.
        global_group = CompactionGroup(
            files=tuple(
                FileInfo(
                    path=file_stat["path"],
                    size_bytes=file_stat["size_bytes"],
                    num_rows=file_stat["num_rows"],
                )
                for file_stat in file_stats
            )
        )
        groups = (global_group,)
        return GlobalRepartitionDeduplicationPlan(
            source_snapshot=snapshot,
            selected_backend=self.backend,
            guarantee_level=_classify_guarantee(fs),
            partition_scope=scope,
            schema_outcome=schema_outcome,
            selected_codec=selected_codec,
            max_rows_per_file=target_rows_per_file,
            target_byte_size=target_byte_size,
            validation_level=validation,
            schema=schema,
            partition_columns=tuple(partition_columns),
            dedup_key_columns=(
                tuple(normalized_key_columns) if normalized_key_columns else None
            ),
            dedup_order_by=(
                tuple(normalized_dedup_order_by) if normalized_dedup_order_by else None
            ),
            dedup_groups=groups,
        )

    def plan_coordinated_optimization(
        self,
        dataset_path: str,
        filesystem: AbstractFileSystem | None = None,
        dedup_key_columns: list[str] | None = None,
        dedup_order_by: list[str] | None = None,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        validation_level: ValidationLevel | str | None = None,
        codec: str | None = None,
    ) -> CoordinatedOptimizationPlan:
        """Create an immutable coordinated optimization plan.

        Coordinated optimization is defined as optional deduplication followed
        by compaction. No z-ordering, sorting, or implicit repartitioning is
        planned.
        """
        if target_rows_per_file is not None and target_rows_per_file <= 0:
            raise ValueError("target_rows_per_file must be > 0")
        (
            fs,
            file_stats,
            snapshot,
            scope,
            schema_outcome,
            schema,
            selected_codec,
            target_byte_size,
            validation,
        ) = self._common_plan_inputs(
            MaintenanceOperation.COORDINATED_OPTIMIZATION,
            dataset_path,
            filesystem,
            partition_filter,
            target_mb_per_file,
            validation_level,
            codec,
        )
        if dedup_key_columns is not None:
            normalized_key_columns, normalized_dedup_order_by = (
                validate_deduplication_inputs(dedup_key_columns, dedup_order_by)
            )
            groups = _plan_partition_local_deduplication_groups(
                file_stats, snapshot.dataset_path
            )
        else:
            normalized_key_columns = None
            normalized_dedup_order_by = None
            groups = _plan_partition_local_compaction_groups(
                file_stats,
                snapshot.dataset_path,
                target_mb_per_file,
                target_rows_per_file,
            )
        return CoordinatedOptimizationPlan(
            source_snapshot=snapshot,
            selected_backend=self.backend,
            guarantee_level=_classify_guarantee(fs),
            partition_scope=scope,
            schema_outcome=schema_outcome,
            selected_codec=selected_codec,
            max_rows_per_file=target_rows_per_file,
            target_byte_size=target_byte_size,
            validation_level=validation,
            schema=schema,
            dedup_key_columns=(
                tuple(normalized_key_columns) if normalized_key_columns else None
            ),
            dedup_order_by=(
                tuple(normalized_dedup_order_by) if normalized_dedup_order_by else None
            ),
            optimization_groups=groups,
        )

    # ------------------------------------------------------------------ #
    # Execution seam (#37)
    # ------------------------------------------------------------------ #

    def execute(
        self,
        plan: MaintenancePlan,
        filesystem: AbstractFileSystem | None = None,
        lock_timeout_s: float = 30.0,
        lock_retry_interval_s: float = 0.05,
    ) -> MaintenanceResult:
        """Execute an accepted maintenance plan and return a typed result.

        Implemented paths:

        - ``atomic_local`` :class:`CompactionPlan` — full staged-rename
          lifecycle with advisory locking and partition-subtree preservation.
        - ``atomic_local`` :class:`PartitionLocalDeduplicationPlan` —
          partition-local staged-rename deduplication with rollback.
        - ``atomic_local`` :class:`GlobalRepartitionDeduplicationPlan` —
          global cross-partition staged-rename deduplication with rollback
          (#42).
        - ``best_effort_object_store`` :class:`CompactionPlan` — staged-copy
          lifecycle with per-key validation, source-drift revalidation, and
          recovery artifact reporting.  *filesystem* is **required** for this
          path.
        - ``best_effort_object_store`` :class:`PartitionLocalDeduplicationPlan`
          — staged-copy deduplication with per-key validation and source-drift
          revalidation.
        - ``best_effort_object_store`` :class:`GlobalRepartitionDeduplicationPlan`
          — staged-copy global repartitioning deduplication.
        - ``atomic_local`` :class:`CoordinatedOptimizationPlan` — optional
          partition-local deduplication followed by compaction through one
          staged, validated, locked, and rollback-capable publication.
        - ``best_effort_object_store`` :class:`CoordinatedOptimizationPlan`
          — optional partition-local deduplication followed by compaction,
          reported as separate phases.  *filesystem* is **required** for this
          path.

        Args:
            plan: An accepted plan created by one of the ``plan_*`` methods.
            filesystem: Required for ``best_effort_object_store`` plans.
                Ignored for ``atomic_local`` plans.
            lock_timeout_s: Maximum seconds to wait for the publication lock
                (``atomic_local`` only).
            lock_retry_interval_s: Sleep between lock-acquisition retries
                (``atomic_local`` only).

        Returns:
            A :class:`MaintenanceResult` (or :class:`BestEffortCompactionResult`
            subclass) carrying per-phase outcomes, validation, publication,
            recovery artifacts, and actual metrics.

        Raises:
            NotImplementedError: For plan types or guarantee levels not yet
                implemented in this release.
            ValueError: When *filesystem* is not provided for a
                ``best_effort_object_store`` plan.
        """
        if plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL and isinstance(
            plan, PartitionLocalDeduplicationPlan
        ):
            return _execute_atomic_local_partition_local_deduplication(
                plan,
                lock_timeout_s=lock_timeout_s,
                lock_retry_interval_s=lock_retry_interval_s,
            )

        if plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL and isinstance(
            plan, GlobalRepartitionDeduplicationPlan
        ):
            return _execute_atomic_local_global_repartition_deduplication(
                plan,
                lock_timeout_s=lock_timeout_s,
                lock_retry_interval_s=lock_retry_interval_s,
            )

        if plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL and isinstance(
            plan, CoordinatedOptimizationPlan
        ):
            return _execute_atomic_local_coordinated_optimization(
                plan,
                lock_timeout_s=lock_timeout_s,
                lock_retry_interval_s=lock_retry_interval_s,
            )

        if plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL and isinstance(
            plan, CompactionPlan
        ):
            return _execute_atomic_local_compaction(
                plan,
                lock_timeout_s=lock_timeout_s,
                lock_retry_interval_s=lock_retry_interval_s,
            )

        # ---------------------------------------------------------- #
        # best_effort_object_store compaction (#39)
        # ---------------------------------------------------------- #
        if (
            plan.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
            and isinstance(plan, CompactionPlan)
        ):
            if filesystem is None:
                # Keep a NotImplementedError seam that callers can detect until
                # they update to pass the filesystem argument.  The "#39" token
                # lets existing tests and tooling identify the seam.
                raise NotImplementedError(
                    "best_effort_object_store execute() requires a filesystem "
                    "argument (pass the fsspec filesystem used to create the plan). "
                    "See issue #39."
                )
            return _execute_best_effort_compaction(plan, filesystem)

        if (
            plan.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
            and isinstance(plan, PartitionLocalDeduplicationPlan)
        ):
            if filesystem is None:
                raise ValueError(
                    "best_effort_object_store deduplication requires the filesystem "
                    "used to create the plan."
                )
            return _execute_best_effort_partition_local_deduplication(plan, filesystem)

        if (
            plan.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
            and isinstance(plan, GlobalRepartitionDeduplicationPlan)
        ):
            if filesystem is None:
                raise ValueError(
                    "best_effort_object_store deduplication requires the filesystem "
                    "used to create the plan."
                )
            return _execute_best_effort_global_repartition_deduplication(
                plan, filesystem
            )

        # ---------------------------------------------------------- #
        # best_effort_object_store coordinated optimization (#45)
        # ---------------------------------------------------------- #
        if (
            plan.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
            and isinstance(plan, CoordinatedOptimizationPlan)
        ):
            if filesystem is None:
                raise ValueError(
                    "best_effort_object_store optimization requires the filesystem "
                    "used to create the plan."
                )
            return _execute_best_effort_coordinated_optimization(plan, filesystem)

        # ---------------------------------------------------------- #
        # Seams for downstream issues — raise descriptive errors
        # ---------------------------------------------------------- #

        raise NotImplementedError(
            f"execute() is not implemented for plan type {type(plan).__name__!r} "
            f"with guarantee_level={plan.guarantee_level!r}."
        )
