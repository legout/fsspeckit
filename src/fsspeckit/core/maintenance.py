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

    # Normalize dedup order by
    normalized_dedup_order_by = None
    if dedup_order_by is not None:
        normalized_dedup_order_by = normalize_key_columns(dedup_order_by)
    elif normalized_key_columns is not None:
        normalized_dedup_order_by = normalized_key_columns

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
        if not base_schema.equals(other_schema):
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


def _plan_partition_local_deduplication_groups(
    file_stats: list[dict[str, Any]],
    dataset_root: str,
    key_columns: list[str] | None,
    dedup_order_by: list[str] | None,
    target_mb_per_file: int | None,
    target_rows_per_file: int | None,
) -> tuple[CompactionGroup, ...]:
    """Plan deduplication groups that never cross a partition boundary.

    Files are grouped by their partition directory (the directory containing the
    file relative to the dataset root). Each partition is planned independently
    so retained rows are never written into another physical partition.
    """
    partitions: dict[str, list[dict[str, Any]]] = {}
    for file_stat in file_stats:
        rel = _relative_file_path(file_stat["path"], dataset_root)
        partition_dir = posixpath.dirname(rel)
        partitions.setdefault(partition_dir, []).append(file_stat)

    groups: list[CompactionGroup] = []
    for partition_dir in sorted(partitions):
        partition_files = partitions[partition_dir]
        groups.extend(
            plan_deduplication_groups(
                partition_files,
                key_columns=key_columns,
                dedup_order_by=dedup_order_by,
                target_mb_per_file=target_mb_per_file,
                target_rows_per_file=target_rows_per_file,
            )["groups"]
        )
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
        error: Human-readable failure description, or None on success.
    """

    succeeded: bool
    published_files: tuple[str, ...] = ()
    removed_source_files: tuple[str, ...] = ()
    error: str | None = None


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

    def __enter__(self) -> "_BoundedAdvisoryLock":
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
                if not schema.equals(expected_schema, check_metadata=False):
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
                    f"Row count mismatch: staged={total_rows}, "
                    f"expected={expected_rows}"
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

    Drift is detected by comparing each file's current on-disk size against
    the size recorded in the snapshot.  Missing files are always a drift signal.
    """
    for file_info in snapshot.files:
        path = file_info.absolute_path
        try:
            current_size = os.path.getsize(path)
        except OSError:
            return f"Source file not accessible: {path}"
        if current_size != file_info.size_bytes:
            return (
                f"Source file size changed: {path} "
                f"(expected {file_info.size_bytes}, got {current_size})"
            )
    return None


def _publish_atomic_local(
    source_files: list[str],
    staged_files: list[str],
    dataset_root: str,
    backup_dir: str,
) -> PublicationOutcome:
    """Publish staged files via the backup-then-rename protocol.

    Steps:
    1. Move every source file into *backup_dir* (atomic rename within the same
       filesystem).
    2. Move every staged file into *dataset_root* (atomic rename).

    On any failure after step 1 has begun:
    - Every published staged file is removed from the dataset root.
    - Every backed-up source file is restored to its original location.

    A ``PublicationOutcome`` with ``succeeded=False`` is returned (not raised)
    so callers can attach it to the ``MaintenanceResult`` without losing
    per-phase context.
    """
    backed_up: list[tuple[str, str]] = []
    published: list[str] = []

    try:
        # Step 1 — move source files to backup (makes them invisible to readers)
        for src in source_files:
            bak = os.path.join(backup_dir, os.path.basename(src))
            os.rename(src, bak)
            backed_up.append((src, bak))

        # Step 2 — move staged files into the live dataset root
        for staged in staged_files:
            dest = os.path.join(dataset_root, os.path.basename(staged))
            os.rename(staged, dest)
            published.append(dest)

        return PublicationOutcome(
            succeeded=True,
            published_files=tuple(published),
            removed_source_files=tuple(src for src, _ in backed_up),
        )

    except Exception as exc:
        # Rollback: remove staged files already placed in dataset root, then
        # restore backed-up source files to their original paths.
        for dest in published:
            try:
                os.remove(dest)
            except Exception:  # noqa: BLE001
                pass
        for original, bak in backed_up:
            try:
                os.rename(bak, original)
            except Exception:  # noqa: BLE001
                pass
        return PublicationOutcome(
            succeeded=False,
            error=f"Publication rename failed: {exc}",
        )


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
            phase_outcomes=(
                PhaseOutcome(phase="write", succeeded=True),
            ),
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
                    os.path.join(backup_dir, os.path.basename(f))
                    for f in source_files_in_groups
                ),
                recovered=False,
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

    total_bytes = sum(
        os.path.getsize(f)
        for f in publication.published_files
        if os.path.exists(f)
    )

    return MaintenanceResult(
        plan=plan,
        succeeded=True,
        guarantee_level=plan.guarantee_level,
        phase_outcomes=tuple(phase_outcomes),
        validation=validation,
        publication=publication,
        recovery=RecoveryArtifacts(workspace_path=workspace),
        actual_metrics=ActualMetrics(
            row_count=total_rows_written,
            file_count=len(publication.published_files),
            total_bytes=total_bytes,
        ),
    )


# --------------------------------------------------------------------------- #
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
        phase_outcomes.append(
            PhaseOutcome(phase="stage", succeeded=False, error=err)
        )
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
                p for p in remaining_to_delete if p not in source_keys_deleted and p != src_path
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
        groups = plan_compaction_groups(
            file_stats, target_mb_per_file, target_rows_per_file
        )
        return CompactionPlan(
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
            compaction_groups=tuple(groups["groups"]),
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
            file_stats,
            snapshot.dataset_path,
            normalized_key_columns,
            normalized_dedup_order_by,
            target_mb_per_file,
            target_rows_per_file,
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
        if not partition_columns:
            raise ValueError("partition_columns must be a non-empty list")
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
        normalized_key_columns, normalized_dedup_order_by = (
            validate_deduplication_inputs(key_columns, dedup_order_by)
        )
        groups = plan_deduplication_groups(
            file_stats,
            key_columns=normalized_key_columns,
            dedup_order_by=normalized_dedup_order_by,
            target_mb_per_file=target_mb_per_file,
            target_rows_per_file=target_rows_per_file,
        )
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
            dedup_groups=tuple(groups["groups"]),
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
                file_stats,
                snapshot.dataset_path,
                normalized_key_columns,
                normalized_dedup_order_by,
                target_mb_per_file,
                target_rows_per_file,
            )
        else:
            normalized_key_columns = None
            normalized_dedup_order_by = None
            compaction_result = plan_compaction_groups(
                file_stats, target_mb_per_file, target_rows_per_file
            )
            groups = tuple(compaction_result["groups"])
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
          lifecycle with advisory locking (flat, non-partitioned local
          datasets).  *filesystem* is ignored for this path.
        - ``best_effort_object_store`` :class:`CompactionPlan` — staged-copy
          lifecycle with per-key validation, source-drift revalidation, and
          recovery artifact reporting.  *filesystem* is **required** for this
          path.

        Deferred paths (leave clean seams):

        - Partitioned atomic_local compaction: TODO(#38)
        - Deduplication execution: TODO(#40)
        - Coordinated optimization execution: TODO(#44)

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
        if (
            plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
            and isinstance(plan, CompactionPlan)
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

        # ---------------------------------------------------------- #
        # Seams for downstream issues — raise descriptive errors
        # ---------------------------------------------------------- #
        if isinstance(plan, (PartitionLocalDeduplicationPlan, GlobalRepartitionDeduplicationPlan)):
            # TODO(#40): implement deduplication execution.
            raise NotImplementedError(
                "Deduplication execute() is deferred to issues #40-#43; "
                "plan is available for inspection."
            )

        if isinstance(plan, CoordinatedOptimizationPlan):
            # TODO(#44): implement coordinated optimization execution.
            raise NotImplementedError(
                "CoordinatedOptimization execute() is deferred to issues #44-#45; "
                "plan is available for inspection."
            )

        raise NotImplementedError(
            f"execute() is not implemented for plan type {type(plan).__name__!r} "
            f"with guarantee_level={plan.guarantee_level!r}."
        )

