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
from collections.abc import Callable, Iterator
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


class CompactionSkipReason(str, Enum):
    """Why a source file does not need compaction."""

    AT_TARGET_SIZE = "at_target_size"
    OVER_TARGET_SIZE = "over_target_size"
    SINGLETON_PARTITION = "singleton_partition"


@dataclass(frozen=True)
class CompactionSkip:
    """A source file intentionally excluded from a compaction plan."""

    file: FileInfo
    reason: CompactionSkipReason


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


def _footer_codecs(metadata: Any) -> frozenset[str]:
    """Return the normalized per-column compression codecs from a Parquet footer.

    Each column's compression is lower-cased so callers can compare against a
    normalized target codec (e.g. ``"snappy"``, ``"uncompressed"``).
    """
    return frozenset(
        metadata.row_group(row_group).column(column).compression.lower()
        for row_group in range(metadata.num_row_groups)
        for column in range(metadata.num_columns)
    )


def _discover_parquet_files(
    fs: AbstractFileSystem,
    path: str,
    partition_filter: list[str] | None,
) -> list[str]:
    """Discover parquet files under *path*, honoring optional partition filters.

    A manual stack walk is used so partition filters apply to the logical
    relative path. Raises ``FileNotFoundError`` when *path* is missing or no
    parquet files match the filter.
    """
    if not fs.exists(path):
        raise FileNotFoundError(f"Dataset path '{path}' does not exist")

    root = Path(path)
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
        files = [
            filename
            for filename in files
            if any(
                Path(filename).relative_to(root).as_posix().startswith(prefix)
                for prefix in normalized_filters
            )
        ]

    if not files:
        raise FileNotFoundError(
            f"No parquet files found under '{path}' matching filter"
        )
    return files


def _file_size_bytes(fs: AbstractFileSystem, filename: str) -> int:
    """Best-effort file size in bytes (0 when the filesystem cannot report it)."""
    try:
        info = fs.info(filename)
        if isinstance(info, dict):
            return int(info.get("size", 0))
    except (OSError, PermissionError) as e:
        logger.warning("Failed to get file info for '%s': %s", filename, e)
    return 0


def _read_parquet_footer(
    fs: AbstractFileSystem,
    filename: str,
    *,
    capture_metadata: bool,
) -> tuple[int, Any, frozenset[str] | None]:
    """Read a parquet footer once; return ``(num_rows, schema_arrow, codecs)``.

    The row count, the Arrow schema, and the per-column codecs all come from a
    single footer open (#66). *schema_arrow* and *codecs* are populated only
    when *capture_metadata* is true and the footer is readable; otherwise they
    are ``None`` and a ``read_table`` fallback still recovers the row count.
    Consumers fall back to opening the footer themselves when the cache is
    absent.
    """
    import pyarrow.parquet as pq

    try:
        with fs.open(filename, "rb") as fh:
            parquet_file = pq.ParquetFile(fh)
            metadata = parquet_file.metadata
            num_rows = metadata.num_rows
            if capture_metadata:
                return num_rows, parquet_file.schema_arrow, _footer_codecs(metadata)
            return num_rows, None, None
    except (OSError, PermissionError, RuntimeError, ValueError) as e:
        # As a fallback, attempt a minimal table read to estimate rows.
        logger.debug(
            "Failed to read parquet metadata from '%s', trying fallback: %s",
            filename,
            e,
        )
        try:
            with fs.open(filename, "rb") as fh:
                return pq.read_table(fh).num_rows, None, None
        except (OSError, PermissionError, RuntimeError, ValueError) as e:
            logger.debug("Fallback table read failed for '%s': %s", filename, e)
            return 0, None, None


def _collect_dataset_stats(
    fs: AbstractFileSystem,
    path: str,
    partition_filter: list[str] | None,
    *,
    capture_footer_metadata: bool,
) -> dict[str, Any]:
    """Internal dataset-stats core shared by the public stats and planning paths.

    When *capture_footer_metadata* is true, each file dict additionally carries
    ``schema_arrow`` and ``codecs`` harvested from the single footer open, so
    the schema-reconciliation and codec consumers (#66) need not re-open it;
    otherwise those keys are ``None`` and only the row count is read.
    """
    files = _discover_parquet_files(fs, path, partition_filter)
    file_infos: list[dict[str, Any]] = []
    total_bytes = 0
    total_rows = 0

    for filename in files:
        size_bytes = _file_size_bytes(fs, filename)
        num_rows, schema_arrow, codecs = _read_parquet_footer(
            fs, filename, capture_metadata=capture_footer_metadata
        )
        total_bytes += size_bytes
        total_rows += num_rows
        file_infos.append(
            {
                "path": filename,
                "size_bytes": size_bytes,
                "num_rows": num_rows,
                "schema_arrow": schema_arrow,
                "codecs": codecs,
            }
        )

    return {
        "files": file_infos,
        "total_bytes": total_bytes,
        "total_rows": total_rows,
    }


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
    fs = filesystem or fsspec_filesystem("file")
    result = _collect_dataset_stats(
        fs, path, partition_filter, capture_footer_metadata=False
    )
    # The public contract exposes only path/size/rows per file. Footer schema
    # and codec metadata are an internal planning cache (#66) and are stripped
    # here so the supported stats surface stays unchanged.
    return {
        "files": [
            {
                "path": file_info["path"],
                "size_bytes": file_info["size_bytes"],
                "num_rows": file_info["num_rows"],
            }
            for file_info in result["files"]
        ],
        "total_bytes": result["total_bytes"],
        "total_rows": result["total_rows"],
    }


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
        exceeds_row_bound = (
            target_rows_per_file is not None
            and file_info.num_rows > target_rows_per_file
        )
        if (
            exceeds_row_bound
            or size_threshold_bytes is None
            or size_bytes < size_threshold_bytes
        ):
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
    skipped_files = tuple(
        CompactionSkip(
            file=file_info,
            reason=(
                CompactionSkipReason.AT_TARGET_SIZE
                if size_threshold_bytes is not None
                and file_info.size_bytes == size_threshold_bytes
                else CompactionSkipReason.OVER_TARGET_SIZE
                if size_threshold_bytes is not None
                and file_info.size_bytes > size_threshold_bytes
                else CompactionSkipReason.SINGLETON_PARTITION
            ),
        )
        for file_info in untouched_files
    )

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
        "skipped_files": skipped_files,
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
    REPARTITION = "repartition"
    SCHEMA_REWRITE = "schema_rewrite"
    ORDERED_COMPACTION = "ordered_compaction"
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
    """Outcome of schema reconciliation during maintenance.

    - :attr:`LOSSLESS_PRESERVED`: every source file shared one exact schema.
    - :attr:`LOSSLESS_PROMOTED`: source schemas differed but reconciled to a
      defined lossless common target (e.g. ``string`` plus ``large_string``
      promote to ``large_string``); the target schema is stored on the plan.
    - :attr:`RECONCILIATION_REQUIRED`: the schemas are incompatible or
      ambiguous and the plan cannot be created without a separate migration.
    """

    LOSSLESS_PRESERVED = "lossless_preserved"
    LOSSLESS_PROMOTED = "lossless_promoted"
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


class CastPolicy(str, Enum):
    """Cast policy for caller-directed schema rewrite (#62).

    - ``STRICT``: allow only value-preserving promotions (a superset of
      ADR-0006 lossless reconciliation).
    - ``SAFE``: add lossless narrowing (e.g. ``int64 → int32`` when every
      value fits).
    - ``LOOSE``: allow narrowing that may truncate, but validate every
      value across the full scope before publication; any value that would
      overflow or become null aborts the plan before any live mutation.
    """

    STRICT = "strict"
    SAFE = "safe"
    LOOSE = "loose"


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
class DerivedPartitionKey:
    """A validated timestamp-derived hive partition key."""

    name: str
    function: str
    source_column: str
    timezone: str
    format: str | None = None


@dataclass(frozen=True)
class CompactionPlan(MaintenancePlan):
    """Immutable plan for a compaction operation."""

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.COMPACTION, init=False
    )
    compaction_groups: tuple[CompactionGroup, ...] = ()
    skipped_files: tuple[CompactionSkip, ...] = ()


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
    derived_partition_keys: tuple[DerivedPartitionKey, ...] = ()
    dedup_groups: tuple[CompactionGroup, ...] = ()


@dataclass(frozen=True)
class RepartitionPlan(MaintenancePlan):
    """Immutable plan for a pure full-dataset repartition (#60).

    A pure physical rewrite that preserves every source row, including exact
    duplicates. Performs no winner selection and carries no deduplication
    fields. Destination partition columns may be source columns or validated
    :class:`DerivedPartitionKey` values; partition columns are path metadata
    only and are not stored in physical file schemas.

    ``repartition_memory_budget_mb`` records the bounded-memory strategy
    selected at plan time. When it is ``None`` the operation is group-bounded
    like :class:`GlobalRepartitionDeduplicationPlan`: source files are read
    into one snapshot-local table and destination-partition buckets are
    written directly. When it is set, destination-partition buckets whose
    materialized size exceeds the budget spill to a per-bucket temporary file
    under the maintenance workspace and are re-read through a row-batch
    reader during output writing.
    """

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.REPARTITION, init=False
    )
    partition_columns: tuple[str, ...] = ()
    derived_partition_keys: tuple[DerivedPartitionKey, ...] = ()
    repartition_memory_budget_mb: int | None = None
    repartition_groups: tuple[CompactionGroup, ...] = ()


# --------------------------------------------------------------------------- #
# Partition-ordered compaction (#61)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SortKey:
    """One sort column for partition-ordered compaction (#61).

    A typed sort key carries the column name, sort direction, and explicit
    null placement. The literal field default is ``nulls_first=False`` (nulls
    sort last); callers that want SQL-standard null placement on a descending
    key (nulls first) must pass ``nulls_first=True`` explicitly or use the
    string ``-col`` convention, which the planner normalizes to
    ``SortKey(column, descending=True, nulls_first=True)``.

    Attributes:
        column: Source column name sorted by this key.
        descending: Sort this column descending when True (ascending default).
        nulls_first: Place nulls (and NaN) before non-null values when True.
    """

    column: str
    descending: bool = False
    nulls_first: bool = False


@dataclass(frozen=True)
class OrderedCompactionPlan(MaintenancePlan):
    """Immutable plan for partition-ordered compaction (#61).

    Output chunks are contiguous slices of one partition-level sorted
    sequence per affected physical partition. Adjacent output files form a
    single sorted run; the result does not claim global business ordering
    across partitions.

    Sorting is stable and deterministic for equal caller sort keys. The
    physical tie-breaker is the ADR-0006 tuple ``(partition path, file path,
    row offset)`` captured in the source snapshot, applied only after the
    caller-supplied sort keys tie.

    ``sort_memory_budget_mb`` records the bounded-memory strategy selected at
    plan time. When it is ``None`` the operation sorts each affected physical
    partition in memory and is partition-bounded; the planner rejects a
    partition whose estimated decoded size exceeds the default budget unless a
    spill directory is supplied. When it is set and a partition exceeds the
    budget, each source file is sorted in memory and written as a sorted run
    under ``sort_spill_directory``; runs are merged through a k-way merge that
    streams output into ``max_rows_per_file`` chunks. For ``atomic_local``
    the spill directory must be on the same filesystem as the dataset root so
    rename-into-place stays atomic.
    """

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.ORDERED_COMPACTION, init=False
    )
    sort_keys: tuple[SortKey, ...] = ()
    ordered_groups: tuple[CompactionGroup, ...] = ()
    sort_memory_budget_mb: int | None = None
    sort_spill_directory: str | None = None


@dataclass(frozen=True)
class CoordinatedOptimizationPlan(MaintenancePlan):
    """Immutable plan for coordinated optimization (optional dedup + compaction)."""

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.COORDINATED_OPTIMIZATION, init=False
    )
    dedup_key_columns: tuple[str, ...] | None = None
    dedup_order_by: tuple[str, ...] | None = None
    optimization_groups: tuple[CompactionGroup, ...] = ()


@dataclass(frozen=True)
class SchemaRewritePlan(MaintenancePlan):
    """Immutable plan for a caller-directed schema rewrite (#62).

    The target schema is supplied by the caller. Dtype inference is not
    invoked. The plan exposes the source schema, the target schema, and the
    tuple of fields whose type changes.

    ``schema_rewrite_memory_budget_mb`` records the bounded-memory strategy
    selected at plan time. When it is ``None`` the operation uses the
    PyArrow scanner default batch size and is row-batch-bounded. When set,
    the budget sets the batch size for both reading and casting; no
    whole-column materialization occurs.
    """

    operation: MaintenanceOperation = field(
        default=MaintenanceOperation.SCHEMA_REWRITE, init=False
    )
    target_schema: pa.Schema | None = None
    source_schema: pa.Schema | None = None
    cast_policy: CastPolicy = CastPolicy.SAFE
    changed_fields: tuple[str, ...] = ()
    schema_rewrite_groups: tuple[CompactionGroup, ...] = ()
    schema_rewrite_memory_budget_mb: int | None = None


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
    if operation in (
        MaintenanceOperation.GLOBAL_REPARTITION_DEDUPLICATION,
        MaintenanceOperation.REPARTITION,
    ):
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


_SIGNED_INT_RANK: dict[Any, int] = {
    pa.int8(): 8,
    pa.int16(): 16,
    pa.int32(): 32,
    pa.int64(): 64,
}
_UNSIGNED_INT_RANK: dict[Any, int] = {
    pa.uint8(): 8,
    pa.uint16(): 16,
    pa.uint32(): 32,
    pa.uint64(): 64,
}
_FLOAT_RANK: dict[Any, int] = {
    pa.float16(): 16,
    pa.float32(): 32,
    pa.float64(): 64,
}
_WIDTH_TO_SIGNED_INT: dict[int, Any] = {v: k for k, v in _SIGNED_INT_RANK.items()}
_WIDTH_TO_UNSIGNED_INT: dict[int, Any] = {v: k for k, v in _UNSIGNED_INT_RANK.items()}
_RANK_TO_FLOAT: dict[int, Any] = {v: k for k, v in _FLOAT_RANK.items()}


def _lossless_common_type(left: Any, right: Any) -> Any | None:
    """Return a lossless common Arrow type for two compatible types.

    Reconciliation is constrained to *defined* lossless promotions; anything
    ambiguous (mixed signedness, differing decimal precision/scale, timezone
    mismatches, distinct nested shapes) returns ``None`` so the caller can
    invalidate the plan. Dtype inference is never invoked here.
    """
    if left.equals(right):
        return left

    # Offset-width string / binary promote toward the wider representation.
    left_is_string = pa.types.is_string(left)
    right_is_string = pa.types.is_string(right)
    left_is_large_string = pa.types.is_large_string(left)
    right_is_large_string = pa.types.is_large_string(right)
    if {left_is_string, right_is_large_string} == {True} or {
        left_is_large_string,
        right_is_string,
    } == {True}:
        return pa.large_string()
    left_is_binary = pa.types.is_binary(left)
    right_is_binary = pa.types.is_binary(right)
    left_is_large_binary = pa.types.is_large_binary(left)
    right_is_large_binary = pa.types.is_large_binary(right)
    if {left_is_binary, right_is_large_binary} == {True} or {
        left_is_large_binary,
        right_is_binary,
    } == {True}:
        return pa.large_binary()

    # Integer widening within the same signedness family.
    left_int = _SIGNED_INT_RANK.get(left) or _UNSIGNED_INT_RANK.get(left)
    right_int = _SIGNED_INT_RANK.get(right) or _UNSIGNED_INT_RANK.get(right)
    if left_int is not None and right_int is not None:
        left_signed = left in _SIGNED_INT_RANK
        right_signed = right in _SIGNED_INT_RANK
        if left_signed == right_signed:
            width = max(left_int, right_int)
            return (
                _WIDTH_TO_SIGNED_INT[width]
                if left_signed
                else _WIDTH_TO_UNSIGNED_INT[width]
            )
        return None

    # Float widening (half < single < double).
    left_float = _FLOAT_RANK.get(left)
    right_float = _FLOAT_RANK.get(right)
    if left_float is not None and right_float is not None:
        return _RANK_TO_FLOAT[max(left_float, right_float)]

    # Nested list / large_list: promote the offset width and recurse items.
    # Reconcile the child *field* (not just its type) so nested metadata and
    # nullability are validated rather than silently discarded.
    left_list = pa.types.is_list(left)
    right_list = pa.types.is_list(right)
    left_large_list = pa.types.is_large_list(left)
    right_large_list = pa.types.is_large_list(right)
    if (left_list or left_large_list) and (right_list or right_large_list):
        common_value_field = _lossless_common_field(left.value_field, right.value_field)
        if common_value_field is None:
            return None
        # Prefer the large variant when either side is large_list, matching
        # the offset-width promotion policy.
        use_large = left_large_list or right_large_list
        builder = pa.large_list if use_large else pa.list_
        return builder(common_value_field)

    # Fixed-size lists of equal length: recurse the child field.
    if pa.types.is_fixed_size_list(left) and pa.types.is_fixed_size_list(right):
        if left.list_size == right.list_size:
            common_value_field = _lossless_common_field(
                left.value_field, right.value_field
            )
            if common_value_field is not None:
                return pa.list_(common_value_field.type, left.list_size)
        return None

    # Structs with matching field names/order: recurse each field through the
    # field-level reconciler so metadata and nullability are preserved.
    if pa.types.is_struct(left) and pa.types.is_struct(right):
        if len(left) != len(right):
            return None
        fields: list[Any] = []
        for lf, rf in zip(left, right, strict=True):
            common_field = _lossless_common_field(lf, rf)
            if common_field is None:
                return None
            fields.append(common_field)
        return pa.struct(fields)

    return None


def _lossless_common_field(left: Any, right: Any) -> Any | None:
    """Return a lossless common field, or ``None`` if incompatible.

    The policy is metadata-preserving and order-independent:
    * field names must match;
    * schema/field metadata must be identical (a conflict invalidates the
      plan rather than being silently dropped or unioned);
    * nullability must be identical — promoting a nullable field to
      non-nullable could drop nulls, and is an ambiguous change the caller
      should resolve via schema rewrite, not silent reconciliation.
    The returned field carries the reconciled type with the shared metadata.
    """
    if left.name != right.name:
        return None
    if left.metadata != right.metadata:
        return None
    if left.nullable != right.nullable:
        return None
    common_type = _lossless_common_type(left.type, right.type)
    if common_type is None:
        return None
    return pa.field(
        left.name, common_type, nullable=left.nullable, metadata=left.metadata
    )


def _lossless_common_schema(schemas: list[Any]) -> Any | None:
    """Compute a lossless common schema across compatible raw schemas.

    Returns ``None`` when the schemas are structurally incompatible, when a
    field type has no defined lossless promotion, or when schema/field
    metadata conflicts. Identical schemas are handled by the caller's
    fast path.
    """
    base = schemas[0]
    names = base.names
    metadata = base.metadata
    for other in schemas[1:]:
        if other.names != names:
            return None
        if other.metadata != metadata:
            return None
    fields: list[Any] = []
    for index in range(len(base)):
        # Accumulate a widening across all schemas at this field position so
        # more than two inputs compose into one target type.
        accumulated = base.field(index)
        for other in schemas[1:]:
            accumulated = _lossless_common_field(accumulated, other.field(index))
            if accumulated is None:
                return None
        fields.append(accumulated)
    return pa.schema(fields, metadata=metadata)


def _reconcile_schema(
    filesystem: AbstractFileSystem,
    file_stats: list[dict[str, Any]],
) -> tuple[SchemaOutcome, Any]:
    """Reconcile source schemas across all discovered files.

    Planning is lock-free and reads only metadata; it does not rewrite files.

    * Identical schemas (including metadata) take the exact-schema fast path
      and return :attr:`SchemaOutcome.LOSSLESS_PRESERVED`.
    * Compatible schemas with a defined lossless common representation
      (e.g. ``string`` plus ``large_string``) return
      :attr:`SchemaOutcome.LOSSLESS_PROMOTED` with the reconciled target
      schema.
    * Unreadable files, structural mismatches, ambiguous type conflicts, or
      metadata conflicts return :attr:`SchemaOutcome.RECONCILIATION_REQUIRED`.
    """
    if not file_stats:
        return SchemaOutcome.LOSSLESS_PRESERVED, None
    import pyarrow.parquet as pq

    def _read_schema(path: str) -> Any:
        with filesystem.open(path, "rb") as fh:
            return pq.ParquetFile(fh).schema_arrow

    schemas: list[Any] = []
    for fi in file_stats:
        # Reuse the schema captured during dataset-stats collection (#66) and
        # only re-open the footer when that cache is absent.
        cached_schema = fi.get("schema_arrow")
        try:
            if cached_schema is not None:
                schemas.append(cached_schema)
            else:
                schemas.append(_read_schema(fi["path"]))
        except Exception:
            return SchemaOutcome.RECONCILIATION_REQUIRED, None

    # Exact-schema fast path (metadata-aware).
    base_schema = schemas[0]
    if all(
        base_schema.equals(other_schema, check_metadata=True)
        for other_schema in schemas[1:]
    ):
        return SchemaOutcome.LOSSLESS_PRESERVED, base_schema

    # Lossless promotion path.
    target_schema = _lossless_common_schema(schemas)
    if target_schema is None:
        return SchemaOutcome.RECONCILIATION_REQUIRED, None
    return SchemaOutcome.LOSSLESS_PROMOTED, target_schema


def _read_input_table(file_handle: Any, target_schema: Any | None) -> Any:
    """Read a Parquet source table cast to the planned target schema.

    Casting each input *before* concatenation guarantees that compatible raw
    schemas (e.g. ``string`` plus ``large_string``) concatenate into one table
    that exactly matches the reconciled ``plan.schema``. Lossless promotions
    use PyArrow's safe cast; an input that cannot be safely cast raises before
    any concatenation occurs.
    """
    import pyarrow.parquet as pq

    table = pq.read_table(file_handle)
    if target_schema is not None and not table.schema.equals(target_schema):
        table = table.cast(target_schema)
    return table


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
    file_stats = _collect_dataset_stats(
        fs, dataset_path, partition_filter, capture_footer_metadata=True
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


def _plan_partition_local_compaction(
    file_stats: list[dict[str, Any]],
    dataset_root: str,
    target_mb_per_file: int | None,
    target_rows_per_file: int | None,
    force_rewrite_paths: set[str] | None = None,
) -> tuple[tuple[CompactionGroup, ...], tuple[CompactionSkip, ...]]:
    """Plan partition-local groups and record intentional skip decisions."""
    partitions = _group_file_stats_by_partition(file_stats, dataset_root)
    forced = force_rewrite_paths or set()

    groups: list[CompactionGroup] = []
    skipped: list[CompactionSkip] = []
    for partition_dir in sorted(partitions):
        result = plan_compaction_groups(
            partitions[partition_dir], target_mb_per_file, target_rows_per_file
        )
        groups.extend(result["groups"])
        for record in result["skipped_files"]:
            if record.file.path in forced:
                groups.append(CompactionGroup(files=(record.file,)))
            else:
                skipped.append(record)
    return tuple(groups), tuple(skipped)


def _plan_partition_local_compaction_groups(
    file_stats: list[dict[str, Any]],
    dataset_root: str,
    target_mb_per_file: int | None,
    target_rows_per_file: int | None,
) -> tuple[CompactionGroup, ...]:
    """Compatibility wrapper returning only partition-local groups."""
    groups, _ = _plan_partition_local_compaction(
        file_stats,
        dataset_root,
        target_mb_per_file,
        target_rows_per_file,
    )
    return groups


def _parquet_codecs(
    filesystem: AbstractFileSystem,
    path: str,
    cached_codecs: frozenset[str] | None = None,
) -> frozenset[str]:
    """Read normalized compression codecs from a Parquet footer.

    When *cached_codecs* is supplied (captured during dataset-stats
    collection), it is returned directly without re-opening the footer; the
    footer is opened only when the cache is absent (#66).
    """
    if cached_codecs is not None:
        return cached_codecs
    import pyarrow.parquet as pq  # noqa: PLC0415

    with filesystem.open(path, "rb") as fh:
        return _footer_codecs(pq.ParquetFile(fh).metadata)


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
                    tables.append(_read_input_table(fh, plan.schema))

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


def _repartition_file_schema(
    schema: pa.Schema | None,
    partition_columns: tuple[str, ...],
) -> pa.Schema | None:
    """Return the physical file schema for a hive-partitioned rewrite."""
    if schema is None:
        return None
    partition_names = set(partition_columns)
    return pa.schema(
        [field for field in schema if field.name not in partition_names],
        metadata=schema.metadata,
    )


def _normalize_derived_partition_keys(
    schema: pa.Schema,
    definitions: dict[str, tuple[str, ...]] | None,
    timezone_name: str,
) -> tuple[DerivedPartitionKey, ...]:
    """Validate and normalize timestamp-derived partition definitions."""
    from zoneinfo import ZoneInfo  # noqa: PLC0415

    if not definitions:
        return ()
    try:
        ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError(f"Unknown partition timezone: {timezone_name!r}") from exc

    timestamp_columns = [
        field.name for field in schema if pa.types.is_timestamp(field.type)
    ]
    allowed = {"year", "month", "day", "date", "strftime"}
    result: list[DerivedPartitionKey] = []
    for name, definition in definitions.items():
        if not isinstance(name, str) or not name:
            raise ValueError("Derived partition names must be non-empty strings")
        if name in schema.names:
            raise ValueError(
                f"Derived partition column {name!r} collides with the source schema"
            )
        if not isinstance(definition, tuple) or len(definition) not in (2, 3):
            raise ValueError(
                f"Derived partition {name!r} must be (function, source) or "
                "(strftime, source, format)"
            )
        function, source_column, *format_values = definition
        if function not in allowed:
            raise ValueError(
                f"Unknown derived partition function {function!r}; "
                f"expected one of {sorted(allowed)}"
            )
        if source_column == "auto":
            if len(timestamp_columns) != 1:
                raise ValueError(
                    "Timestamp source 'auto' requires exactly one timestamp column; "
                    f"candidates: {timestamp_columns}"
                )
            source_column = timestamp_columns[0]
        if source_column not in schema.names:
            raise ValueError(
                f"Derived partition source column {source_column!r} does not exist"
            )
        if not pa.types.is_timestamp(schema.field(source_column).type):
            raise ValueError(
                f"Derived partition source column {source_column!r} must be timestamp"
            )
        format_string = format_values[0] if format_values else None
        if function == "strftime":
            if not isinstance(format_string, str) or not format_string:
                raise ValueError(
                    "strftime derived partitions require a non-empty format"
                )
        elif format_string is not None:
            raise ValueError(f"{function!r} derived partitions do not accept a format")
        result.append(
            DerivedPartitionKey(
                name=name,
                function=function,
                source_column=source_column,
                timezone=timezone_name,
                format=format_string,
            )
        )
    return tuple(result)


def _apply_derived_partition_keys(
    table: pa.Table,
    keys: tuple[DerivedPartitionKey, ...],
) -> pa.Table:
    """Append validated derived keys after deduplication and before partitioning."""
    import pyarrow.compute as pc  # noqa: PLC0415

    for key in keys:
        source = table[key.source_column]
        source_type = table.schema.field(key.source_column).type
        target_type = pa.timestamp(source_type.unit, tz=key.timezone)
        if source_type.tz is None:
            localized = pc.assume_timezone(source, key.timezone)
        else:
            localized = pc.cast(source, target_type)
        if key.function == "year":
            derived = pc.year(localized)
        elif key.function == "month":
            derived = pc.month(localized)
        elif key.function == "day":
            derived = pc.day(localized)
        elif key.function == "date":
            derived = pc.cast(localized, pa.date32())
        else:
            derived = pc.strftime(localized, format=key.format)
        table = table.append_column(key.name, derived)
    return table


def _validate_global_partition_placement(
    staged_files: list[str],
    staged_partition_dirs: list[str],
    staged_partition_values: list[tuple[Any, ...]],
    staged_root: str,
    partition_columns: tuple[str, ...],
) -> str | None:
    """Validate staged paths and hive-partitioned physical schemas.

    Each staged file must live beneath the Hive-style destination path built
    from its declared partition values. Destination partition columns must be
    absent from the physical file schema because the path supplies them.
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
            with open(staged, "rb") as fh:
                staged_schema = pq.read_schema(fh)
            duplicated_columns = [
                column for column in partition_columns if column in staged_schema.names
            ]
            if duplicated_columns:
                return (
                    f"Staged file {staged!r} physically contains hive partition "
                    f"columns {duplicated_columns!r}"
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
                    tables.append(_read_input_table(fh, plan.schema))
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
                tables.append(_read_input_table(fh, plan.schema))
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        deduplicated = _deduplicate_partition_table(
            combined, plan.dedup_key_columns, plan.dedup_order_by
        )
        if plan.schema is not None:
            deduplicated = deduplicated.cast(plan.schema)
        expected_rows = deduplicated.num_rows

        # Group retained rows by their declared destination partition tuple.
        deduplicated = _apply_derived_partition_keys(
            deduplicated, plan.derived_partition_keys
        )
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
            # Partition values are encoded as hive path metadata; the physical
            # file schema must NOT duplicate them (#56) or hive reads crash on
            # type mismatches (e.g. int64 file vs int32 path-inferred column).
            partition_table = deduplicated.take(pa.array(row_indices, type=pa.int64()))
            partition_table = partition_table.drop_columns(
                [c for c in plan.partition_columns if c in partition_table.column_names]
            )
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
        validation = _validate_staged_output(
            staged_files,
            _repartition_file_schema(plan.schema, plan.partition_columns),
            expected_rows,
        )
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


def _repartition_spill_bucket(
    bucket_table: pa.Table,
    bucket_index: int,
    run_id: str,
    spill_dir: str,
    memory_budget_mb: int | None,
    codec: str,
) -> tuple[pa.Table | None, str | None]:
    """Spill a destination-partition bucket when it exceeds the memory budget.

    Returns ``(bucket_table, None)`` when the bucket stays in memory — either
    because no budget was declared (group-bounded behavior) or because the
    bucket fits the declared budget — and ``(None, spill_path)`` when the
    bucket is written to a per-bucket temporary file under *spill_dir* so the
    caller can stream it back through a row-batch reader.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    if memory_budget_mb is None:
        return bucket_table, None
    budget_bytes = memory_budget_mb * 1024 * 1024
    if bucket_table.nbytes <= budget_bytes:
        return bucket_table, None
    os.makedirs(spill_dir, exist_ok=True)
    spill_path = os.path.join(
        spill_dir, f"repartition-bucket-{run_id}-{bucket_index:04d}.parquet"
    )
    pq.write_table(bucket_table, spill_path, compression=codec)
    return None, spill_path


def _stream_spilled_chunks(
    spill_path: str,
    max_rows: int | None,
) -> Iterator[pa.Table]:
    """Yield row-bounded table chunks from a spilled parquet bucket file."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    parquet_file = pq.ParquetFile(spill_path)
    batch_size = max_rows if (max_rows is not None and max_rows > 0) else 1 << 15
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        if batch.num_rows == 0:
            continue
        yield pa.Table.from_batches([batch])


def _execute_atomic_local_repartition(
    plan: RepartitionPlan,
    lock_timeout_s: float = 30.0,
    lock_retry_interval_s: float = 0.05,
) -> RepartitionResult:
    """Execute a pure full-dataset repartition with atomic local publication (#60).

    Unlike global-repartitioning deduplication, this operation preserves every
    source row, including exact duplicates. It reads every source file into
    one snapshot-local table, applies any declared derived partition keys,
    hashes rows into destination-partition buckets, and writes each bucket
    under its Hive-style destination path with ``max_rows_per_file`` honored
    as a hard per-file bound.

    When ``plan.repartition_memory_budget_mb`` is set and a destination
    bucket's materialized size exceeds the budget, the bucket is spilled to
    a per-bucket temporary file under the maintenance workspace and re-read
    through a row-batch reader during output writing. When the budget is
    ``None`` the operation is group-bounded and writes each bucket directly,
    matching :class:`GlobalRepartitionDeduplicationPlan`.

    Full validation (schema, partition placement, row count) runs before
    publication. On any failure the workspace and backups are retained as
    recovery artifacts and every swapped subtree is rolled back under one
    caller-held exclusive lock.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415
    import shutil  # noqa: PLC0415

    if plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN:
        # Defense-in-depth: plan_repartition already rejects this. Keep the
        # check so an externally constructed plan cannot bypass the invariant.
        raise ValueError(
            "FULL_DISTINCT_KEY_SCAN is not applicable to pure repartition: "
            "the operation has no key semantics."
        )

    dataset_root = plan.source_snapshot.dataset_path
    phase_outcomes: list[PhaseOutcome] = []
    workspace: str | None = None
    staged_dir = ""
    backup_dir = ""
    validation: ValidationOutcome | None = None
    publication: PublicationOutcome | None = None

    if not plan.source_snapshot.files:
        return RepartitionResult(
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
        return RepartitionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Stage phase failed: {exc}",
        )

    spill_dir = os.path.join(workspace, "spill")
    source_files: list[str] = []
    staged_files: list[str] = []
    staged_partition_dirs: list[str] = []
    staged_partition_values: list[tuple[Any, ...]] = []
    staged_rows: list[int] = []
    run_id = uuid.uuid4().hex[:16]
    expected_rows = 0

    try:
        # Read every source row; pure repartition performs no winner selection.
        # The physical fallback order is partition path, file path, row offset
        # — sort the snapshot by relative_path before reading so output is
        # deterministic, matching the object-store lane.
        sorted_sources = sorted(
            plan.source_snapshot.files, key=lambda source: source.relative_path
        )
        source_files.extend(source.absolute_path for source in sorted_sources)
        tables: list[pa.Table] = []
        for source in sorted_sources:
            with open(source.absolute_path, "rb") as fh:
                tables.append(_read_input_table(fh, plan.schema))
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        if plan.schema is not None:
            combined = combined.cast(plan.schema)
        # No deduplication: apply derived keys directly to the concatenated
        # source table before partitioning.
        combined = _apply_derived_partition_keys(combined, plan.derived_partition_keys)
        expected_rows = combined.num_rows

        partition_arrays = [
            combined[column].to_pylist() for column in plan.partition_columns
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
            # Partition values are encoded as hive path metadata; the physical
            # file schema must NOT duplicate them (#56) or hive reads crash on
            # type mismatches (e.g. int64 file vs int32 path-inferred column).
            bucket_table = combined.take(pa.array(row_indices, type=pa.int64()))
            bucket_table = bucket_table.drop_columns(
                [c for c in plan.partition_columns if c in bucket_table.column_names]
            )
            in_memory_bucket, spill_path = _repartition_spill_bucket(
                bucket_table,
                partition_index,
                run_id,
                spill_dir,
                plan.repartition_memory_budget_mb,
                plan.selected_codec,
            )
            if in_memory_bucket is not None:
                chunks = _split_table_by_rows(in_memory_bucket, plan.max_rows_per_file)
            else:
                chunks = _stream_spilled_chunks(spill_path, plan.max_rows_per_file)
            for chunk_index, chunk in enumerate(chunks):
                if chunk.num_rows == 0:
                    continue
                filename = (
                    f"repartitioned-{run_id}-{partition_index:04d}-"
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
        return RepartitionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Write phase failed: {exc}",
        )

    # ------------------------------------------------------------------ #
    # Phase: validate — partition placement, schema, and the per-source
    # and per-destination row-count invariant (every source row appears
    # exactly once in the output). Pure repartition has no key semantics;
    # FULL_DISTINCT_KEY_SCAN is rejected above.
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
        validation = _validate_staged_output(
            staged_files,
            _repartition_file_schema(plan.schema, plan.partition_columns),
            expected_rows,
        )
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate", succeeded=validation.succeeded, error=validation.error
        )
    )
    if not validation.succeeded:
        return RepartitionResult(
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
        return RepartitionResult(
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
            return RepartitionResult(
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
        return RepartitionResult(
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
    return RepartitionResult(
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


def _execute_atomic_local_ordered_compaction(
    plan: OrderedCompactionPlan,
    lock_timeout_s: float = 30.0,
    lock_retry_interval_s: float = 0.05,
) -> OrderedCompactionResult:
    """Execute partition-ordered compaction with atomic local publication (#61).

    Each physical partition in ``plan.ordered_groups`` is sorted by the plan's
    sort keys (plus the ADR-0006 physical tie-breaker), split into contiguous
    ``max_rows_per_file``-bounded chunks, and published through the same
    partition-subtree backup-then-rename protocol as compaction. Validation
    checks per-file sort order, sort order across adjacent output-file
    boundaries within each partition, partition placement, schema, and the
    row-count invariant before publication.

    When ``plan.sort_memory_budget_mb`` is set and a partition's materialized
    size exceeds the budget, each source file is sorted in memory and written
    as a sorted run under ``plan.sort_spill_directory``; runs are merged
    through a streaming k-way merge. When the budget is ``None`` the partition
    is sorted in memory (the planner has already rejected partitions that
    exceed the default budget without a spill directory).
    """
    import pyarrow.parquet as pq  # noqa: PLC0415
    import shutil  # noqa: PLC0415

    if plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN:
        # Defense-in-depth: the planner rejects this too.
        raise ValueError(
            "FULL_DISTINCT_KEY_SCAN is not applicable to ordered compaction: "
            "the operation has no key semantics."
        )

    dataset_root = plan.source_snapshot.dataset_path
    phase_outcomes: list[PhaseOutcome] = []
    workspace: str | None = None
    staged_dir = ""
    backup_dir = ""
    validation: ValidationOutcome | None = None
    publication: PublicationOutcome | None = None

    if not plan.ordered_groups:
        return OrderedCompactionResult(
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
        return OrderedCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Stage phase failed: {exc}",
        )

    source_files_in_groups: list[str] = []
    staged_files: list[str] = []
    staged_partition_dirs: list[str] = []
    expected_rows = 0
    run_id = uuid.uuid4().hex[:16]

    try:
        for group_index, group in enumerate(plan.ordered_groups):
            partition_dir = _group_partition_dir(group, dataset_root)
            # Read source files in physical (snapshot-relative) order so the
            # stable sort's tie-break reproduces (partition path, file path,
            # row offset). Sort the group's files by their relative path.
            sorted_files = sorted(
                group.files,
                key=lambda fi: _relative_file_path(fi.path, dataset_root),
            )
            source_files_in_groups.extend(fi.path for fi in sorted_files)
            source_tables: list[pa.Table] = []
            for fi in sorted_files:
                with open(fi.path, "rb") as fh:
                    source_tables.append(_read_input_table(fh, plan.schema))
            expected_rows += sum(t.num_rows for t in source_tables)
            combined = (
                pa.concat_tables(source_tables)
                if len(source_tables) > 1
                else source_tables[0]
            )
            if plan.schema is not None:
                combined = combined.cast(plan.schema)

            spill_required = _ordered_compaction_spill_required(
                combined.nbytes, plan.sort_memory_budget_mb
            )
            if not spill_required:
                chunks = _ordered_compaction_in_memory_chunks(
                    combined, plan.sort_keys, plan.max_rows_per_file
                )
            else:
                chunks = _ordered_compaction_local_spill_chunks(
                    source_tables,
                    plan.sort_keys,
                    plan.max_rows_per_file,
                    plan.sort_spill_directory,
                    run_id,
                    group_index,
                    plan.selected_codec,
                )
            for chunk_index, chunk in enumerate(chunks):
                if chunk.num_rows == 0:
                    continue
                filename = (
                    f"ordered-{run_id}-{group_index:04d}-{chunk_index:04d}.parquet"
                )
                output_dir = os.path.join(staged_dir, partition_dir)
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, filename)
                pq.write_table(chunk, output_path, compression=plan.selected_codec)
                staged_files.append(output_path)
                staged_partition_dirs.append(partition_dir)
        phase_outcomes.append(PhaseOutcome(phase="write", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="write", succeeded=False, error=str(exc))
        )
        return OrderedCompactionResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Write phase failed: {exc}",
        )

    # ------------------------------------------------------------------ #
    # Phase: validate — partition placement, schema, row count, and the
    # per-file + adjacent-file sort order invariant. Ordered compaction has
    # no key semantics; FULL_DISTINCT_KEY_SCAN is rejected above.
    # ------------------------------------------------------------------ #
    placement_error = _validate_staged_partition_placement(
        staged_files, staged_partition_dirs, staged_dir
    )
    if placement_error is not None:
        validation = ValidationOutcome(
            succeeded=False, expected_row_count=expected_rows, error=placement_error
        )
    else:
        order_error = _validate_ordered_compaction_order(
            staged_files,
            staged_partition_dirs,
            plan.sort_keys,
            lambda path: pq.ParquetFile(path).read(),
        )
        if order_error is not None:
            validation = ValidationOutcome(
                succeeded=False,
                expected_row_count=expected_rows,
                error=order_error,
            )
        else:
            validation = _validate_staged_output(
                staged_files, plan.schema, expected_rows
            )
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate", succeeded=validation.succeeded, error=validation.error
        )
    )
    if not validation.succeeded:
        return OrderedCompactionResult(
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
        return OrderedCompactionResult(
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
            return OrderedCompactionResult(
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

    if publication is None or not publication.succeeded:
        error = (
            publication.error if publication is not None else None
        ) or "Publication did not run"
        return OrderedCompactionResult(
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
                        os.path.join(backup_dir, _relative_file_path(p, dataset_root))
                        for p in source_files_in_groups
                    )
                    if os.path.exists(path)
                ),
                recovered=publication.rollback_succeeded is True
                if publication
                else False,
                error=publication.rollback_error if publication else None,
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
                os.path.join(backup_dir, _relative_file_path(p, dataset_root))
                for p in source_files_in_groups
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
    return OrderedCompactionResult(
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
                tables.append(_read_input_table(fh, plan.schema))
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


# --------------------------------------------------------------------------- #
# Schema rewrite helpers (#62)
# --------------------------------------------------------------------------- #


def _coerce_cast_policy(cast_policy: CastPolicy | str) -> CastPolicy:
    """Coerce a cast-policy argument to the :class:`CastPolicy` enum."""
    if isinstance(cast_policy, CastPolicy):
        return cast_policy
    try:
        return CastPolicy(cast_policy)
    except ValueError:
        raise ValueError(f"Unknown cast_policy: {cast_policy!r}")


def _schema_rewrite_changed_fields(
    source_schema: pa.Schema,
    target_schema: pa.Schema,
) -> tuple[str, ...]:
    """Return the tuple of field names whose Arrow type changes."""
    source_types: dict[str, pa.DataType] = {f.name: f.type for f in source_schema}
    changed: list[str] = []
    for f in target_schema:
        source_type = source_types.get(f.name)
        if source_type is not None and not source_type.equals(f.type):
            changed.append(f.name)
    return tuple(changed)


def _is_type_promotion(
    source_type: pa.DataType,
    target_type: pa.DataType,
) -> bool:
    """Return True if *target_type* is a value-preserving promotion of *source_type*.

    A promotion is a widening cast that can never lose information regardless
    of the data values (a superset of ADR-0006 lossless reconciliation).
    """
    if source_type.equals(target_type):
        return True
    # Integer widening (same signedness, strictly wider).
    if pa.types.is_integer(source_type) and pa.types.is_integer(target_type):
        source_signed = bool(pa.types.is_signed_integer(source_type))
        target_signed = bool(pa.types.is_signed_integer(target_type))
        if source_signed and target_signed:
            return bool(target_type.bit_width > source_type.bit_width)
        if not source_signed and not target_signed:
            return bool(target_type.bit_width > source_type.bit_width)
        # unsigned → signed must be strictly wider (e.g. uint8→int16, not
        # uint8→int8, because uint8 values 128–255 are not int8-representable).
        if not source_signed and target_signed:
            return bool(target_type.bit_width > source_type.bit_width)
        return False
    # Float widening.
    if pa.types.is_floating(source_type) and pa.types.is_floating(target_type):
        return bool(target_type.bit_width > source_type.bit_width)
    # Timestamp unit widening (same timezone, strictly finer unit).
    if pa.types.is_timestamp(source_type) and pa.types.is_timestamp(target_type):
        if source_type.tz != target_type.tz:
            return False
        unit_order = {"s": 1, "ms": 2, "us": 3, "ns": 4}
        return bool(
            unit_order.get(target_type.unit, 0) > unit_order.get(source_type.unit, 0)
        )
    # date32 → date64.
    if pa.types.is_date32(source_type) and pa.types.is_date64(target_type):
        return True
    # string → large_string.
    if pa.types.is_string(source_type) and pa.types.is_large_string(target_type):
        return True
    # binary → large_binary.
    if pa.types.is_binary(source_type) and pa.types.is_large_binary(target_type):
        return True
    # list → large_list.
    if pa.types.is_list(source_type) and pa.types.is_large_list(target_type):
        return True
    return False


def _validate_strict_promotion(
    source_schema: pa.Schema,
    target_schema: pa.Schema,
    changed_fields: tuple[str, ...],
) -> None:
    """Reject any STRICT-plan field change that is not a type-level promotion."""
    for field_name in changed_fields:
        source_type = source_schema.field(field_name).type
        target_type = target_schema.field(field_name).type
        if not _is_type_promotion(source_type, target_type):
            raise ValueError(
                f"STRICT cast_policy requires value-preserving promotions; "
                f"field {field_name!r} changes {source_type} → {target_type}, "
                f"which is not a widening promotion."
            )


def _schema_rewrite_batch_size(memory_budget_mb: int | None) -> int:
    """Compute the RecordBatch row count from the memory budget.

    When the budget is ``None`` the PyArrow scanner default (64 K rows) is
    used and the operation is row-batch-bounded. When set, the budget caps
    both reading and casting; no whole-column materialization occurs.
    """
    if memory_budget_mb is None:
        return 1 << 16  # 65 536 — PyArrow scanner default.
    # Heuristic: ~128 bytes/row → 8 K rows per megabyte, minimum 1 K.
    return max(1024, memory_budget_mb * 8192)


def _validate_loose_narrowing(
    source_col: pa.Array,
    casted_col: pa.Array,
    source_type: pa.DataType,
    field_name: str,
) -> None:
    """Validate that a LOOSE narrowing did not overflow or produce new nulls.

    PyArrow ``safe=False`` allows the cast to proceed silently. This guard
    confirms that no value became null unexpectedly (failed string→int, out-
    of-range decimal) and that integer narrowing did not wrap.
    """
    if casted_col.null_count > source_col.null_count:
        raise ValueError(
            f"LOOSE cast of field {field_name!r} produced new nulls; "
            f"aborting before publication."
        )
    # Integer narrowing: detect overflow by casting back and comparing.
    if (
        pa.types.is_integer(source_type)
        and pa.types.is_integer(casted_col.type)
        and source_type.bit_width > casted_col.type.bit_width
    ):
        back = casted_col.cast(source_type)
        if not back.equals(source_col):
            raise ValueError(
                f"LOOSE cast of field {field_name!r} from {source_type} "
                f"to {casted_col.type} would overflow; aborting before "
                f"publication."
            )


def _cast_schema_rewrite_batch(
    table: pa.Table,
    target_schema: pa.Schema,
    source_schema: pa.Schema,
    cast_policy: CastPolicy,
    changed_fields: tuple[str, ...],
) -> pa.Table:
    """Cast one RecordBatch-sized table to the target schema under the policy.

    STRICT and SAFE use PyArrow ``safe=True`` which raises on the first value
    that does not fit the target type. LOOSE uses ``safe=False`` and then
    validates per-field that no value overflowed or became null.
    """
    safe = cast_policy != CastPolicy.LOOSE
    casted = table.cast(target_schema, safe=safe)
    if cast_policy == CastPolicy.LOOSE and changed_fields:
        for field_name in changed_fields:
            source_col = table.column(field_name)
            casted_col = casted.column(field_name)
            source_type = source_schema.field(field_name).type
            _validate_loose_narrowing(source_col, casted_col, source_type, field_name)
    return casted


def _execute_atomic_local_schema_rewrite(
    plan: SchemaRewritePlan,
    lock_timeout_s: float = 30.0,
    lock_retry_interval_s: float = 0.05,
) -> SchemaRewriteResult:
    """Execute a caller-directed schema rewrite with atomic local publication (#62).

    Source files are read in ``RecordBatch``-sized chunks; each chunk is cast
    in place under the configured :class:`CastPolicy` and appended to a staged
    output writer. No whole-column materialization occurs; the ``opt_dtype``
    Python-side regex loops are explicitly not in the publication path.

    Full validation (target schema exactly matches, row-count invariant) runs
    before publication. LOOSE narrowing is validated per-batch during the
    write phase so any overflow or unexpected null aborts before any live
    mutation. On any failure the workspace and backups are retained as
    recovery artifacts and every swapped subtree is rolled back under one
    caller-held exclusive lock.
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
        return SchemaRewriteResult(
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
        return SchemaRewriteResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Stage phase failed: {exc}",
        )

    run_id = uuid.uuid4().hex[:16]
    batch_size = _schema_rewrite_batch_size(plan.schema_rewrite_memory_budget_mb)
    staged_files: list[str] = []
    staged_partition_dirs: list[str] = []
    source_files_in_groups: list[str] = []
    expected_rows = 0

    # ------------------------------------------------------------------ #
    # Phase: write — batch-stream each source file, cast, stage output.
    # ------------------------------------------------------------------ #
    try:
        for group in plan.schema_rewrite_groups:
            group_partition_dir = _group_partition_dir(group, dataset_root)
            source_files_in_groups.extend(fi.path for fi in group.files)
            writer: pq.ParquetWriter | None = None
            rows_in_file = 0
            file_index = 0

            for fi in group.files:
                with open(fi.path, "rb") as fh:
                    pf = pq.ParquetFile(fh)
                    for record_batch in pf.iter_batches(batch_size=batch_size):
                        batch_table = pa.Table.from_batches([record_batch])
                        casted = _cast_schema_rewrite_batch(
                            batch_table,
                            plan.target_schema,
                            plan.source_schema,
                            plan.cast_policy,
                            plan.changed_fields,
                        )
                        expected_rows += casted.num_rows
                        # Write the casted batch, splitting it to honor
                        # max_rows_per_file as a hard per-file bound.  Small
                        # batches accumulate in the current file until full.
                        offset = 0
                        while offset < casted.num_rows:
                            if plan.max_rows_per_file is not None:
                                capacity = plan.max_rows_per_file - rows_in_file
                            else:
                                capacity = casted.num_rows - offset
                            if capacity <= 0:
                                assert writer is not None
                                writer.close()
                                writer = None
                                rows_in_file = 0
                                capacity = plan.max_rows_per_file or (
                                    casted.num_rows - offset
                                )
                            chunk_rows = min(capacity, casted.num_rows - offset)
                            chunk = casted.slice(offset, chunk_rows)
                            if writer is None:
                                filename = (
                                    f"schema-rewrite-{run_id}-{file_index:04d}.parquet"
                                )
                                output_dir = os.path.join(
                                    staged_dir, group_partition_dir
                                )
                                os.makedirs(output_dir, exist_ok=True)
                                output_path = os.path.join(output_dir, filename)
                                writer = pq.ParquetWriter(
                                    output_path,
                                    plan.target_schema,
                                    compression=plan.selected_codec,
                                )
                                staged_files.append(output_path)
                                staged_partition_dirs.append(group_partition_dir)
                                rows_in_file = 0
                                file_index += 1
                            assert writer is not None
                            writer.write_table(chunk)
                            rows_in_file += chunk_rows
                            offset += chunk_rows
            if writer is not None:
                writer.close()
        phase_outcomes.append(PhaseOutcome(phase="write", succeeded=True))
    except Exception as exc:
        phase_outcomes.append(
            PhaseOutcome(phase="write", succeeded=False, error=str(exc))
        )
        return SchemaRewriteResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Write phase failed: {exc}",
        )

    # ------------------------------------------------------------------ #
    # Phase: validate — target schema exactly matches; row-count invariant.
    # ------------------------------------------------------------------ #
    validation = _validate_staged_output(
        staged_files, plan.target_schema, expected_rows
    )
    phase_outcomes.append(
        PhaseOutcome(
            phase="validate",
            succeeded=validation.succeeded,
            error=validation.error,
        )
    )
    if not validation.succeeded:
        return SchemaRewriteResult(
            plan=plan,
            succeeded=False,
            guarantee_level=plan.guarantee_level,
            phase_outcomes=tuple(phase_outcomes),
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=workspace),
            error=f"Validation failed: {validation.error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: lock, drift_check, publish, cleanup — identical to compaction.
    # ------------------------------------------------------------------ #
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
        return SchemaRewriteResult(
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
            return SchemaRewriteResult(
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

    if publication is None or not publication.succeeded:
        error = (
            publication.error if publication is not None else None
        ) or "Publication did not run"
        return SchemaRewriteResult(
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
                recovered=publication.rollback_succeeded is True
                if publication is not None
                else False,
                error=publication.rollback_error if publication is not None else None,
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
        os.path.getsize(path)
        for path in publication.published_files
        if os.path.exists(path)
    )
    return SchemaRewriteResult(
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
class RepartitionResult(MaintenanceResult):
    """Typed result of executing a :class:`RepartitionPlan` (#60).

    Pure repartition carries no key semantics, so this result inherits the
    common maintenance result fields without adding deduplication-specific
    data. The per-source and per-destination row-count invariant (every
    source row appears exactly once in the output) is enforced by
    :func:`_validate_staged_output` against ``expected_rows`` before
    publication.
    """


@dataclass(frozen=True)
class BestEffortRepartitionResult(BestEffortCompactionResult, RepartitionResult):
    """Repartition result with object-store recovery details (#60).

    Carries the standard best-effort recovery fields (staging prefix,
    staged keys, copied live keys, failed copies, untouched sources,
    drift flag, concurrency disclaimer) inherited from
    :class:`BestEffortCompactionResult`. Destination partition columns and
    keys are available from ``plan.partition_columns`` and the copied live
    keys' paths.
    """


@dataclass(frozen=True)
class OrderedCompactionResult(MaintenanceResult):
    """Typed result of executing an :class:`OrderedCompactionPlan` (#61).

    Partition-ordered compaction carries no key semantics. The per-file and
    adjacent-file sort-order invariants are enforced by
    :func:`_validate_ordered_compaction_order` before publication, in addition
    to the standard readable/schema/row-count/placement validation.
    """


@dataclass(frozen=True)
class BestEffortOrderedCompactionResult(
    BestEffortCompactionResult, OrderedCompactionResult
):
    """Ordered compaction result with object-store recovery details (#61).

    Carries the standard best-effort recovery fields (staging prefix,
    staged keys, copied live keys, failed copies, untouched sources,
    drift flag, concurrency disclaimer) inherited from
    :class:`BestEffortCompactionResult`. Per-partition sort order is
    validated on staged output before any live key is copied.
    """


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


@dataclass(frozen=True)
class SchemaRewriteResult(MaintenanceResult):
    """Typed result of executing a :class:`SchemaRewritePlan` (#62).

    Schema rewrite preserves row multiplicity and the partition layout. The
    target schema exactly matches the output; per-field cast validation,
    row-count and null-count invariants, and partition column type
    compatibility are enforced before publication.
    """


@dataclass(frozen=True)
class BestEffortSchemaRewriteResult(BestEffortCompactionResult, SchemaRewriteResult):
    """Schema rewrite result with object-store recovery details (#62).

    Carries the standard best-effort recovery fields (staging prefix,
    staged keys, copied live keys, failed copies, untouched sources,
    drift flag, concurrency disclaimer) inherited from
    :class:`BestEffortCompactionResult`.
    """


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


# --------------------------------------------------------------------------- #
# Partition-ordered compaction sort helpers (#61)
# --------------------------------------------------------------------------- #
#
# The ordered-compaction sort contract (PRD §2, ADR-0007) is:
#
# - Ordering scope is the complete affected physical partition.
# - Output chunks are contiguous slices of one partition-level sorted run.
# - Sorting is stable; the physical tie-breaker
#   ``(partition path, file path, row offset)`` from the source snapshot is
#   applied only after caller sort keys tie. Because source files are read in
#   physical order and concatenated in that order, a stable sort over caller
#   keys preserves the physical tie-breaker for free.
# - Nulls (and NaN) are placed per ``SortKey.nulls_first``; the string
#   ``+col`` / ``-col`` convention is normalized into typed keys with the
#   SQL-standard null placement (nulls last for ascending, nulls first for
#   descending).


# Default in-memory budget (MB) when ``sort_memory_budget_mb`` is None. A
# partition whose estimated decoded size exceeds this without a configured
# spill directory is rejected at plan time.
_DEFAULT_ORDERED_COMPACTION_MEMORY_BUDGET_MB = 512


def _normalize_sort_keys(
    sort_keys: list[SortKey | str],
) -> tuple[SortKey, ...]:
    """Normalize raw sort-key arguments into typed :class:`SortKey` values.

    String items accept the existing ``+col`` / ``-col`` convention used by
    :func:`parse_dedup_order_by`: a leading ``-`` marks the column descending,
    every other name (including a leading ``+``) is ascending. String keys
    also receive the SQL-standard null placement: ``nulls_last`` for ascending
    and ``nulls_first`` for descending. A typed :class:`SortKey` overrides
    direction and supplies explicit null placement verbatim.
    """
    if not sort_keys:
        raise ValueError("sort_keys must be a non-empty list")
    normalized: list[SortKey] = []
    seen: set[str] = set()
    for item in sort_keys:
        if isinstance(item, SortKey):
            key = item
        elif isinstance(item, str) and item:
            if item.startswith("-"):
                column = item[1:]
                if not column:
                    raise ValueError(
                        "sort_keys string items must name a column after the "
                        "leading direction sigil"
                    )
                # SQL standard: descending keys place nulls first.
                key = SortKey(column=column, descending=True, nulls_first=True)
            elif item.startswith("+"):
                column = item[1:]
                if not column:
                    raise ValueError(
                        "sort_keys string items must name a column after the "
                        "leading direction sigil"
                    )
                key = SortKey(column=column, descending=False, nulls_first=False)
            else:
                key = SortKey(column=item, descending=False, nulls_first=False)
        else:
            raise ValueError(
                "sort_keys items must be SortKey instances or non-empty strings; "
                f"got {item!r}"
            )
        if not key.column:
            raise ValueError("sort_keys columns must be non-empty strings")
        if key.column in seen:
            raise ValueError(
                f"sort_keys must not repeat columns; duplicate: {key.column!r}"
            )
        seen.add(key.column)
        normalized.append(key)
    return tuple(normalized)


def _sort_value_is_null(value: Any) -> bool:
    """Return True for SQL null or float NaN (treated as null by ordering)."""
    if value is None:
        return True
    # NaN compares unequal to itself; treat it as null for ordering so that
    # null placement governs both None and NaN uniformly.
    return isinstance(value, float) and value != value


def _compare_sort_values(
    a: Any,
    b: Any,
    key: SortKey,
) -> int:
    """Compare two scalar sort-key values under one :class:`SortKey`.

    Returns -1/0/1. Null (and NaN) placement honors ``key.nulls_first``;
    non-null values compare ascending or descending per ``key.descending``.
    """
    a_null = _sort_value_is_null(a)
    b_null = _sort_value_is_null(b)
    if a_null and b_null:
        return 0
    if a_null:
        return -1 if key.nulls_first else 1
    if b_null:
        return 1 if key.nulls_first else -1
    try:
        if a < b:
            base = -1
        elif b < a:
            base = 1
        else:
            base = 0
    except TypeError:
        sa, sb = str(a), str(b)
        base = -1 if sa < sb else (1 if sb < sa else 0)
    return -base if key.descending else base


def _compare_ordered_rows(
    a_values: tuple[Any, ...],
    b_values: tuple[Any, ...],
    sort_keys: tuple[SortKey, ...],
) -> int:
    """Compare two rows by sort keys only (no physical tie-break).

    Returns -1/0/1. Equal caller sort keys return 0 so callers can apply the
    physical tie-breaker separately.
    """
    for key, a, b in zip(sort_keys, a_values, b_values, strict=True):
        cmp = _compare_sort_values(a, b, key)
        if cmp != 0:
            return cmp
    return 0


def _sort_table_ordered(
    table: pa.Table,
    sort_keys: tuple[SortKey, ...],
) -> pa.Table:
    """Stable sort *table* by caller sort keys with the physical tie-break.

    The physical tie-breaker ``(partition path, file path, row offset)`` is
    preserved automatically: callers pass a table whose rows are already in
    physical order (source files concatenated in snapshot-physical order), and
    Python's stable sort keeps equal-key rows in that input order.
    """
    from functools import cmp_to_key  # noqa: PLC0415

    if not sort_keys or table.num_rows == 0:
        return table
    columns = [table.column(key.column).to_pylist() for key in sort_keys]

    def compare(i: int, j: int) -> int:
        for col, key in zip(columns, sort_keys, strict=True):
            cmp = _compare_sort_values(col[i], col[j], key)
            if cmp != 0:
                return cmp
        return 0

    indices = sorted(range(table.num_rows), key=cmp_to_key(compare))
    return table.take(pa.array(indices, type=pa.int64()))


def _ordered_merge_key(
    sort_keys: tuple[SortKey, ...],
) -> Any:
    """Build a comparison key function for ``heapq.merge`` over sort keys.

    Returns a callable mapping ``(values, physical_index)`` to a wrapper whose
    ``__lt__`` encodes the per-key direction and null placement, then breaks
    ties by ``physical_index`` (a ``(run_index, row_offset)`` tuple assigned in
    snapshot order). The physical tie-break makes the comparison a *total*
    order: ``heapq.merge``'s ``key=`` argument is **not** stable across input
    iterables for equal keys, so folding the physical tuple into the comparator
    (rather than relying on stream order) is what reproduces the ADR-0006
    ``(partition path, file path, row offset)`` ordering exactly.
    """

    class _Key:
        __slots__ = ("values", "physical")

        def __init__(self, values: tuple[Any, ...], physical: tuple[int, int]) -> None:
            self.values = values
            self.physical = physical

        def __lt__(self, other: _Key) -> bool:
            cmp = _compare_ordered_rows(self.values, other.values, sort_keys)
            if cmp != 0:
                return cmp < 0
            return self.physical < other.physical

    return _Key


def _row_sort_values(
    row: dict[str, Any],
    sort_keys: tuple[SortKey, ...],
) -> tuple[Any, ...]:
    """Extract the comparable sort-key value tuple from a pylist row."""
    return tuple(row[key.column] for key in sort_keys)


def _stream_merge_runs(
    run_row_generators: list[Iterator[tuple[Any, dict[str, Any]]]],
    max_rows: int | None,
    column_names: list[str],
) -> Iterator[pa.Table]:
    """k-way merge of sorted run row-generators into ``max_rows`` chunks.

    Each generator yields ``(key, row_dict)`` pairs in sorted order, where
    ``key`` is a wrapper whose ``__lt__`` encodes the per-key direction and
    null placement. ``heapq.merge`` is stable across iterables, so equal-key
    ties fall back to generator order — runs must be passed in physical
    (snapshot) order so the tie-break reproduces the ADR-0006 tuple. Generators
    are expected to read their run lazily (batched) so peak memory is bounded
    by one batch per run plus the output chunk.
    """
    import heapq  # noqa: PLC0415
    import operator  # noqa: PLC0415

    chunk_size = max_rows if (max_rows is not None and max_rows > 0) else 1 << 15
    merged = heapq.merge(*run_row_generators, key=operator.itemgetter(0))
    buffer: list[dict[str, Any]] = []
    for _, row in merged:
        buffer.append(row)
        if len(buffer) >= chunk_size:
            yield _rows_to_table(buffer, column_names)
            buffer = []
    if buffer:
        yield _rows_to_table(buffer, column_names)


def _rows_to_table(
    rows: list[dict[str, Any]],
    column_names: list[str],
) -> pa.Table:
    """Build a PyArrow table from pylist rows preserving column order."""
    return pa.table({name: [row[name] for row in rows] for name in column_names})


def _run_row_generator(
    iter_batches: Iterator[pa.RecordBatch],
    sort_keys: tuple[SortKey, ...],
    run_index: int = 0,
) -> Iterator[tuple[Any, dict[str, Any]]]:
    """Yield ``(key, row_dict)`` per row of a sorted run, batched.

    Reads the run through *iter_batches* so only one record batch is
    materialized at a time. Each row's sort-key values are wrapped by the
    comparator returned from :func:`_ordered_merge_key`, together with a
    ``(run_index, row_offset)`` physical index that makes the comparator a
    total order and reproduces the ADR-0006 tie-breaker.
    """
    key_fn = _ordered_merge_key(sort_keys)
    offset = 0
    for batch in iter_batches:
        names = batch.schema.names
        key_columns = [batch.column(k.column).to_pylist() for k in sort_keys]
        all_columns = [batch.column(name).to_pylist() for name in names]
        for r in range(batch.num_rows):
            yield (
                key_fn(
                    tuple(col[r] for col in key_columns),
                    (run_index, offset),
                ),
                {name: col[r] for name, col in zip(names, all_columns)},
            )
            offset += 1


def _ordered_compaction_in_memory_chunks(
    combined: pa.Table,
    sort_keys: tuple[SortKey, ...],
    max_rows: int | None,
) -> list[pa.Table]:
    """Sort the partition in memory and slice into ``max_rows`` chunks."""
    return _split_table_by_rows(_sort_table_ordered(combined, sort_keys), max_rows)


def _ordered_compaction_spill_required(
    materialized_bytes: int,
    memory_budget_mb: int | None,
) -> bool:
    """Return True when a partition's materialized size exceeds the budget.

    When ``memory_budget_mb`` is ``None`` the partition is in-memory-bounded
    (the planner has already rejected partitions exceeding the default budget
    without a spill directory), so this returns False.
    """
    if memory_budget_mb is None:
        return False
    return materialized_bytes > memory_budget_mb * 1024 * 1024


def _ordered_compaction_local_spill_chunks(
    source_tables: list[pa.Table],
    sort_keys: tuple[SortKey, ...],
    max_rows: int | None,
    spill_directory: str | None,
    run_id: str,
    group_index: int,
    codec: str,
) -> list[pa.Table]:
    """External merge sort for one partition on the local filesystem.

    Each source table is sorted in memory and written as a sorted run file
    under *spill_directory*; runs are streamed back through a k-way merge that
    respects the physical tie-breaker (runs are passed in physical order).
    Run files are removed after the merge completes.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    if spill_directory is None:
        raise ValueError(
            "Ordered compaction spill is required but no spill_directory is "
            "configured; the planner should have rejected this plan."
        )
    os.makedirs(spill_directory, exist_ok=True)
    run_paths: list[str] = []
    column_names: list[str] = []
    try:
        for file_index, table in enumerate(source_tables):
            if table.num_rows == 0:
                continue
            if not column_names:
                column_names = list(table.column_names)
            sorted_run = _sort_table_ordered(table, sort_keys)
            run_path = os.path.join(
                spill_directory,
                f"ordered-run-{run_id}-{group_index:04d}-{file_index:04d}.parquet",
            )
            pq.write_table(sorted_run, run_path, compression=codec)
            run_paths.append(run_path)
        generators = [
            _run_row_generator(
                pq.ParquetFile(path).iter_batches(), sort_keys, run_index
            )
            for run_index, path in enumerate(run_paths)
        ]
        return list(_stream_merge_runs(generators, max_rows, column_names))
    finally:
        for path in run_paths:
            try:
                os.remove(path)
            except OSError:
                pass


def _ordered_compaction_object_store_spill_chunks(
    filesystem: AbstractFileSystem,
    source_tables: list[pa.Table],
    sort_keys: tuple[SortKey, ...],
    max_rows: int | None,
    spill_prefix: str,
    run_id: str,
    group_index: int,
    codec: str,
) -> list[pa.Table]:
    """External merge sort for one partition on an object-store filesystem.

    Each source table is sorted in memory and written as a sorted run object
    under *spill_prefix*; runs are streamed back through a k-way merge. The
    spill prefix is caller-managed and is not part of recovery artifacts; run
    objects are removed after the merge completes.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    run_paths: list[str] = []
    column_names: list[str] = []
    try:
        for file_index, table in enumerate(source_tables):
            if table.num_rows == 0:
                continue
            if not column_names:
                column_names = list(table.column_names)
            sorted_run = _sort_table_ordered(table, sort_keys)
            run_path = posixpath.join(
                spill_prefix,
                f"ordered-run-{run_id}-{group_index:04d}-{file_index:04d}.parquet",
            )
            buffer = BytesIO()
            pq.write_table(sorted_run, buffer, compression=codec)
            filesystem.pipe(run_path, buffer.getvalue())
            run_paths.append(run_path)

        def _read_run_batches(path: str) -> Iterator[pa.RecordBatch]:
            with filesystem.open(path, "rb") as handle:
                parquet_file = pq.ParquetFile(handle)
                yield from parquet_file.iter_batches()

        generators = [
            _run_row_generator(_read_run_batches(path), sort_keys, run_index)
            for run_index, path in enumerate(run_paths)
        ]
        return list(_stream_merge_runs(generators, max_rows, column_names))
    finally:
        for path in run_paths:
            try:
                filesystem.rm(path)
            except Exception:
                pass


def _validate_ordered_compaction_order(
    staged_files: list[str],
    staged_partition_dirs: list[str],
    sort_keys: tuple[SortKey, ...],
    read_table: Any,
) -> str | None:
    """Validate per-file and adjacent-file sort order within each partition.

    *staged_files* and *staged_partition_dirs* are parallel and already in
    publication order within each partition. *read_table* is a callable that
    reads a staged file path into a ``pa.Table`` (local path handle or
    fsspec filesystem, depending on the lane).
    """
    if not sort_keys:
        return None
    # Group staged files by partition, preserving publication order.
    by_partition: dict[str, list[str]] = {}
    for path, partition in zip(staged_files, staged_partition_dirs, strict=True):
        by_partition.setdefault(partition, []).append(path)
    for partition, files in by_partition.items():
        prev_values: tuple[Any, ...] | None = None
        for path in files:
            try:
                table = read_table(path)
            except Exception as exc:
                return (
                    f"Could not read staged file for order validation {path!r}: {exc}"
                )
            columns = [table.column(k.column).to_pylist() for k in sort_keys]
            for r in range(table.num_rows):
                values = tuple(col[r] for col in columns)
                if prev_values is not None:
                    if _compare_ordered_rows(prev_values, values, sort_keys) > 0:
                        return (
                            f"Sort order violation in partition {partition!r}: "
                            f"{prev_values!r} precedes {values!r}"
                        )
                prev_values = values
        # Reset between partitions: cross-partition order is not claimed.
    return None


def _first_existing_ancestor_device(path: str) -> int:
    """Return the device id of the longest existing ancestor of *path*.

    Used to verify a caller-supplied spill directory lives on the same
    filesystem as the dataset root without creating it (planning is
    metadata-only). Walks up until an existing ancestor is found.
    """
    current = os.path.abspath(path)
    while True:
        try:
            return os.stat(current).st_dev
        except OSError:
            parent = os.path.dirname(current)
            if parent == current:
                # Reached the root without a stat-able ancestor; fall back to
                # a stat of the original path so the error surfaces clearly.
                return os.stat(path).st_dev
            current = parent


def _ordered_spill_same_filesystem(spill_directory: str, dataset_root: str) -> bool:
    """Return True if *spill_directory* is on the same device as *dataset_root*.

    For ``atomic_local`` ordered compaction the spill directory must share the
    dataset root's filesystem so rename-into-place publication stays atomic.
    """
    try:
        return (
            _first_existing_ancestor_device(spill_directory)
            == os.stat(dataset_root).st_dev
        )
    except OSError:
        return False


def _validate_ordered_compaction_spill_contract(
    groups: tuple[CompactionGroup, ...],
    memory_budget_mb: int | None,
    spill_directory: str | None,
    guarantee_level: GuaranteeLevel,
    dataset_root: str,
) -> None:
    """Enforce the ordered-compaction memory/spill contract at plan time.

    When a partition's estimated size exceeds the effective budget (the
    declared ``memory_budget_mb`` or the default when ``None``) a spill
    directory is required. For ``atomic_local`` the spill directory must be on
    the same filesystem as the dataset root so rename-into-place stays atomic.
    """
    effective_budget_mb = (
        memory_budget_mb
        if memory_budget_mb is not None
        else _DEFAULT_ORDERED_COMPACTION_MEMORY_BUDGET_MB
    )
    budget_bytes = effective_budget_mb * 1024 * 1024
    for group in groups:
        estimated = group.total_size_bytes
        if estimated > budget_bytes and spill_directory is None:
            label = "default " if memory_budget_mb is None else ""
            raise ValueError(
                f"Physical partition (estimated {estimated} bytes) exceeds the "
                f"{label}memory budget ({effective_budget_mb} MB) for ordered "
                "compaction; supply spill_directory to enable the external "
                "merge sort path."
            )
    if (
        spill_directory is not None
        and guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        and not _ordered_spill_same_filesystem(spill_directory, dataset_root)
    ):
        raise ValueError(
            "For atomic_local ordered compaction the spill directory must be "
            "on the same filesystem as the dataset root so rename-into-place "
            "publication stays atomic."
        )


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
            partition_dir = _group_partition_dir(group, dataset_root)
            tables = []
            for fi in group.files:
                with filesystem.open(fi.path, "rb") as fh:
                    tables.append(_read_input_table(fh, plan.schema))

            combined = pa.concat_tables(tables)
            if plan.schema is not None:
                combined = combined.cast(plan.schema)

            chunks = _split_table_by_rows(combined, plan.max_rows_per_file)
            for chunk_idx, chunk in enumerate(chunks):
                if chunk.num_rows == 0:
                    continue
                staged_path = posixpath.join(
                    staging_prefix,
                    partition_dir,
                    f"output-{group_idx:04d}-{chunk_idx:04d}.parquet",
                )
                live_path = posixpath.join(
                    dataset_root,
                    partition_dir,
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
                    tables.append(_read_input_table(fh, plan.schema))
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
                tables.append(_read_input_table(fh, plan.schema))
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

        deduplicated = _apply_derived_partition_keys(
            deduplicated, plan.derived_partition_keys
        )
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
            # Partition values live in the hive path; drop them from the file
            # schema so hive reads see each column exactly once (#56).
            partition_table = deduplicated.take(pa.array(row_indices, type=pa.int64()))
            partition_table = partition_table.drop_columns(
                [c for c in plan.partition_columns if c in partition_table.column_names]
            )
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
    expected_file_schema = _repartition_file_schema(plan.schema, plan.partition_columns)
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
                if (
                    expected_file_schema is not None
                    and not parquet_file.schema_arrow.equals(expected_file_schema)
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
                duplicated_columns = [
                    column
                    for column in plan.partition_columns
                    if column in parquet_file.schema_arrow.names
                ]
                if duplicated_columns:
                    validation_error = (
                        f"Staged file {staged_path} physically contains hive "
                        f"partition columns {duplicated_columns!r}"
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
                if (
                    expected_file_schema is not None
                    and not parquet_file.schema_arrow.equals(expected_file_schema)
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


def _execute_best_effort_repartition(
    plan: RepartitionPlan,
    filesystem: AbstractFileSystem,
) -> BestEffortRepartitionResult:
    """Execute a pure full-dataset repartition through best-effort publication (#60).

    Unlike global-repartitioning deduplication, this operation preserves every
    source row, including exact duplicates. It reads every source object into
    one snapshot-local table, applies any declared derived partition keys,
    hashes rows into destination-partition buckets, and writes each bucket
    under its Hive-style destination path. No source object is removed until
    all staged output has been validated, every planned live key has been
    copied and validated, and every source object has been revalidated.

    When ``plan.repartition_memory_budget_mb`` is set and a destination
    bucket's materialized size exceeds the budget, the bucket is spilled to
    a per-bucket object under a ``_spill`` prefix beneath the staging prefix
    and re-read through a row-batch reader during output writing. When the
    budget is ``None`` the operation is group-bounded.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    if plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN:
        raise ValueError(
            "FULL_DISTINCT_KEY_SCAN is not applicable to pure repartition: "
            "the operation has no key semantics."
        )

    run_id = uuid.uuid4().hex[:16]
    dataset_root = plan.source_snapshot.dataset_path
    staging_prefix = posixpath.join(dataset_root, "_maintenance_staging", run_id)
    spill_prefix = posixpath.join(staging_prefix, "_spill")
    source_keys = tuple(source.absolute_path for source in plan.source_snapshot.files)
    phase_outcomes: list[PhaseOutcome] = []
    staged_keys: list[str] = []
    staged_to_live: dict[str, str] = {}
    staged_rows: dict[str, int] = {}
    staged_partition_values: dict[str, tuple[Any, ...]] = {}
    spill_keys: list[str] = []
    source_row_count = 0

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
    ) -> BestEffortRepartitionResult:
        return BestEffortRepartitionResult(
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
        )

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
                tables.append(_read_input_table(fh, plan.schema))
        if not tables:
            raise ValueError("Pure repartition requires at least one source table")
        combined = pa.concat_tables(tables)
        if plan.schema is not None:
            combined = combined.cast(plan.schema)
        # No deduplication: apply derived keys directly before partitioning.
        combined = _apply_derived_partition_keys(combined, plan.derived_partition_keys)
        source_row_count = combined.num_rows

        partition_arrays = [
            combined[column].to_pylist() for column in plan.partition_columns
        ]
        partition_groups: dict[tuple[Any, ...], tuple[tuple[Any, ...], list[int]]] = {}
        for row_index in range(source_row_count):
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
            partition_path = _hive_partition_path(plan.partition_columns, raw_values)
            # Partition values live in the hive path; drop them from the file
            # schema so hive reads see each column exactly once (#56).
            bucket_table = combined.take(pa.array(row_indices, type=pa.int64()))
            bucket_table = bucket_table.drop_columns(
                [c for c in plan.partition_columns if c in bucket_table.column_names]
            )
            in_memory_bucket, spill_path = _repartition_spill_bucket_object_store(
                filesystem,
                bucket_table,
                partition_index,
                run_id,
                spill_prefix,
                plan.repartition_memory_budget_mb,
                plan.selected_codec,
            )
            if in_memory_bucket is not None:
                chunks = _split_table_by_rows(in_memory_bucket, plan.max_rows_per_file)
            else:
                chunks = _stream_spilled_chunks_object_store(
                    filesystem, spill_path, plan.max_rows_per_file
                )
                spill_keys.append(spill_path)
            for chunk_index, chunk in enumerate(chunks):
                if chunk.num_rows == 0:
                    continue
                filename = (
                    f"repartitioned-{run_id}-{partition_index:04d}-"
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
    # and collectively contain exactly the source rows (every source row
    # appears exactly once in the output).
    # ------------------------------------------------------------------ #
    total_staged_rows = 0
    validation_error: str | None = None
    expected_file_schema = _repartition_file_schema(plan.schema, plan.partition_columns)
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
                if (
                    expected_file_schema is not None
                    and not parquet_file.schema_arrow.equals(expected_file_schema)
                ):
                    validation_error = f"Schema mismatch in staged file {staged_path}"
                    break
                expected_partition_path = _hive_partition_path(
                    plan.partition_columns, staged_partition_values[staged_path]
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
                duplicated_columns = [
                    column
                    for column in plan.partition_columns
                    if column in parquet_file.schema_arrow.names
                ]
                if duplicated_columns:
                    validation_error = (
                        f"Staged file {staged_path} physically contains hive "
                        f"partition columns {duplicated_columns!r}"
                    )
                    break
        if validation_error is None and total_staged_rows != source_row_count:
            validation_error = (
                f"Staged row count mismatch: got {total_staged_rows}, "
                f"expected {source_row_count}"
            )
    except Exception as exc:
        validation_error = f"Staged validation error: {exc}"
    validation = ValidationOutcome(
        succeeded=validation_error is None,
        staged_row_count=total_staged_rows,
        expected_row_count=source_row_count,
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
                if (
                    expected_file_schema is not None
                    and not parquet_file.schema_arrow.equals(expected_file_schema)
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
            row_count=source_row_count,
            file_count=len(copied_live_keys),
            total_bytes=total_bytes,
        ),
        copied_live_keys=tuple(copied_live_keys),
        untouched_source_keys=(),
    )


def _execute_best_effort_ordered_compaction(
    plan: OrderedCompactionPlan,
    filesystem: AbstractFileSystem,
) -> BestEffortOrderedCompactionResult:
    """Execute partition-ordered compaction through best-effort publication (#61).

    Mirrors :func:`_execute_best_effort_compaction` with two additions required
    by the ordered-compaction contract: each partition's output is a globally
    sorted run split into contiguous ``max_rows_per_file`` chunks, and the
    validate phase checks per-file and adjacent-file sort order within each
    partition in addition to readability, schema, row count, and placement.

    When ``plan.sort_memory_budget_mb`` is set and a partition's materialized
    size exceeds the budget, sorted runs are written under a caller-managed
    spill prefix (``plan.sort_spill_directory``) and merged through a streaming
    k-way merge. The spill prefix is caller-managed and is not part of
    recovery artifacts.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    if plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN:
        raise ValueError(
            "FULL_DISTINCT_KEY_SCAN is not applicable to ordered compaction: "
            "the operation has no key semantics."
        )

    run_id = uuid.uuid4().hex[:16]
    dataset_root = plan.source_snapshot.dataset_path
    staging_prefix = posixpath.join(dataset_root, "_maintenance_staging", run_id)
    source_keys = tuple(fi.absolute_path for fi in plan.source_snapshot.files)
    phase_outcomes: list[PhaseOutcome] = []
    staged_keys: list[str] = []
    staged_to_live: dict[str, str] = {}
    staged_rows: dict[str, int] = {}
    staged_partition_dirs: dict[str, str] = {}
    expected_rows = 0

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
    ) -> BestEffortOrderedCompactionResult:
        return BestEffortOrderedCompactionResult(
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
        )

    # ------------------------------------------------------------------ #
    # Phase: stage — sort each partition and write chunks under staging.
    # ------------------------------------------------------------------ #
    try:
        for group_index, group in enumerate(plan.ordered_groups):
            partition_dir = _group_partition_dir(group, dataset_root)
            sorted_files = sorted(
                group.files,
                key=lambda fi: _relative_file_path(fi.path, dataset_root),
            )
            source_tables: list[pa.Table] = []
            for fi in sorted_files:
                with filesystem.open(fi.path, "rb") as handle:
                    source_tables.append(_read_input_table(handle, plan.schema))
            expected_rows += sum(t.num_rows for t in source_tables)
            combined = (
                pa.concat_tables(source_tables)
                if len(source_tables) > 1
                else source_tables[0]
            )
            if plan.schema is not None:
                combined = combined.cast(plan.schema)

            spill_required = _ordered_compaction_spill_required(
                combined.nbytes, plan.sort_memory_budget_mb
            )
            if not spill_required:
                chunks = _ordered_compaction_in_memory_chunks(
                    combined, plan.sort_keys, plan.max_rows_per_file
                )
            else:
                spill_prefix = plan.sort_spill_directory or posixpath.join(
                    staging_prefix, "_sort_spill"
                )
                chunks = _ordered_compaction_object_store_spill_chunks(
                    filesystem,
                    source_tables,
                    plan.sort_keys,
                    plan.max_rows_per_file,
                    spill_prefix,
                    run_id,
                    group_index,
                    plan.selected_codec,
                )
            for chunk_index, chunk in enumerate(chunks):
                if chunk.num_rows == 0:
                    continue
                filename = (
                    f"ordered-{run_id}-{group_index:04d}-{chunk_index:04d}.parquet"
                )
                staged_path = posixpath.join(staging_prefix, partition_dir, filename)
                live_path = posixpath.join(dataset_root, partition_dir, filename)
                buffer = BytesIO()
                pq.write_table(chunk, buffer, compression=plan.selected_codec)
                filesystem.pipe(staged_path, buffer.getvalue())
                staged_keys.append(staged_path)
                staged_to_live[staged_path] = live_path
                staged_rows[staged_path] = chunk.num_rows
                staged_partition_dirs[staged_path] = partition_dir
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
    # Phase: validate — readability, schema, row count, partition placement,
    # and per-file + adjacent-file sort order within each partition.
    # ------------------------------------------------------------------ #
    total_staged_rows = 0
    validation_error: str | None = None
    staged_in_order = list(staged_keys)
    staged_partition_dir_list = [staged_partition_dirs[p] for p in staged_in_order]
    try:
        for staged_path in staged_keys:
            with filesystem.open(staged_path, "rb") as handle:
                parquet_file = pq.ParquetFile(handle)
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
        if validation_error is None and total_staged_rows != expected_rows:
            validation_error = (
                f"Staged row count mismatch: got {total_staged_rows}, "
                f"expected {expected_rows}"
            )
        if validation_error is None:
            validation_error = _validate_staged_partition_placement(
                staged_in_order,
                staged_partition_dir_list,
                staging_prefix,
            )
        if validation_error is None:
            validation_error = _validate_ordered_compaction_order(
                staged_in_order,
                staged_partition_dir_list,
                plan.sort_keys,
                lambda path: pq.ParquetFile(filesystem.open(path, "rb")).read(),
            )
    except Exception as exc:
        validation_error = f"Staged validation error: {exc}"
    validation = ValidationOutcome(
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
        return result(
            succeeded=False,
            validation=validation,
            recovery=RecoveryArtifacts(workspace_path=staging_prefix),
            error=f"Staged validation failed: {validation_error}",
        )

    # ------------------------------------------------------------------ #
    # Phase: publish — copy and validate each planned live key.
    # ------------------------------------------------------------------ #
    copied_live_keys: list[str] = []
    failed_copies: list[str] = []
    for staged_path, live_path in staged_to_live.items():
        try:
            content = filesystem.cat(staged_path)
            filesystem.pipe(live_path, content)
            if filesystem.cat(live_path) != content:
                raise ValueError(f"Live key content mismatch for {live_path}")
            with filesystem.open(live_path, "rb") as handle:
                parquet_file = pq.ParquetFile(handle)
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
        PhaseOutcome(phase="publish", succeeded=not failed_copies, error=copy_error)
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
    # Phase: cleanup — remove sources only after the preceding gates pass.
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
            row_count=expected_rows,
            file_count=len(copied_live_keys),
            total_bytes=total_bytes,
        ),
        copied_live_keys=tuple(copied_live_keys),
        untouched_source_keys=(),
    )


def _repartition_spill_bucket_object_store(
    filesystem: AbstractFileSystem,
    bucket_table: pa.Table,
    bucket_index: int,
    run_id: str,
    spill_prefix: str,
    memory_budget_mb: int | None,
    codec: str,
) -> tuple[pa.Table | None, str | None]:
    """Spill a destination-partition bucket to the object store when over budget."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    if memory_budget_mb is None:
        return bucket_table, None
    budget_bytes = memory_budget_mb * 1024 * 1024
    if bucket_table.nbytes <= budget_bytes:
        return bucket_table, None
    spill_path = posixpath.join(
        spill_prefix, f"repartition-bucket-{run_id}-{bucket_index:04d}.parquet"
    )
    buffer = BytesIO()
    pq.write_table(bucket_table, buffer, compression=codec)
    filesystem.pipe(spill_path, buffer.getvalue())
    return None, spill_path


def _stream_spilled_chunks_object_store(
    filesystem: AbstractFileSystem,
    spill_path: str,
    max_rows: int | None,
) -> Iterator[pa.Table]:
    """Yield row-bounded table chunks from a spilled object-store bucket."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    batch_size = max_rows if (max_rows is not None and max_rows > 0) else 1 << 15
    with filesystem.open(spill_path, "rb") as fh:
        parquet_file = pq.ParquetFile(fh)
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            if batch.num_rows == 0:
                continue
            yield pa.Table.from_batches([batch])


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
                        tables.append(_read_input_table(fh, plan.schema))
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
                    tables.append(_read_input_table(fh, plan.schema))
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


def _execute_best_effort_schema_rewrite(
    plan: SchemaRewritePlan,
    filesystem: AbstractFileSystem,
) -> BestEffortSchemaRewriteResult:
    """Execute a schema rewrite through best-effort publication (#62).

    Source objects are read in ``RecordBatch``-sized chunks; each chunk is
    cast in place under the configured :class:`CastPolicy` and appended to a
    staged output writer. No source object is removed until all staged
    output has been validated, every planned live key has been copied and
    validated, and every source object has been revalidated.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    run_id = uuid.uuid4().hex[:16]
    dataset_root = plan.source_snapshot.dataset_path
    staging_prefix = posixpath.join(dataset_root, "_maintenance_staging", run_id)
    source_keys = tuple(source.absolute_path for source in plan.source_snapshot.files)
    phase_outcomes: list[PhaseOutcome] = []
    staged_keys: list[str] = []
    staged_to_live: dict[str, str] = {}
    staged_rows: dict[str, int] = {}
    staged_partition_dirs: dict[str, str] = {}
    source_row_count = 0

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
    ) -> BestEffortSchemaRewriteResult:
        return BestEffortSchemaRewriteResult(
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
        )

    batch_size = _schema_rewrite_batch_size(plan.schema_rewrite_memory_budget_mb)

    # ------------------------------------------------------------------ #
    # Phase: stage — batch-stream each source object, cast, stage output.
    # ------------------------------------------------------------------ #
    try:
        for group in plan.schema_rewrite_groups:
            group_partition_dir = _group_partition_dir(group, dataset_root)
            writer: pq.ParquetWriter | None = None
            buffer: BytesIO | None = None
            current_staged_path = ""
            rows_in_file = 0
            file_index = 0

            for fi in group.files:
                with filesystem.open(fi.path, "rb") as fh:
                    pf = pq.ParquetFile(fh)
                    for record_batch in pf.iter_batches(batch_size=batch_size):
                        batch_table = pa.Table.from_batches([record_batch])
                        casted = _cast_schema_rewrite_batch(
                            batch_table,
                            plan.target_schema,
                            plan.source_schema,
                            plan.cast_policy,
                            plan.changed_fields,
                        )
                        source_row_count += casted.num_rows
                        # Write the casted batch, splitting it to honor
                        # max_rows_per_file as a hard per-file bound.
                        offset = 0
                        while offset < casted.num_rows:
                            if plan.max_rows_per_file is not None:
                                capacity = plan.max_rows_per_file - rows_in_file
                            else:
                                capacity = casted.num_rows - offset
                            if capacity <= 0:
                                assert writer is not None
                                assert buffer is not None
                                writer.close()
                                filesystem.pipe(current_staged_path, buffer.getvalue())
                                writer = None
                                buffer = None
                                rows_in_file = 0
                                capacity = plan.max_rows_per_file or (
                                    casted.num_rows - offset
                                )
                            chunk_rows = min(capacity, casted.num_rows - offset)
                            chunk = casted.slice(offset, chunk_rows)
                            if writer is None:
                                buffer = BytesIO()
                                filename = (
                                    f"schema-rewrite-{run_id}-{file_index:04d}.parquet"
                                )
                                current_staged_path = posixpath.join(
                                    staging_prefix,
                                    group_partition_dir,
                                    filename,
                                )
                                current_live_path = posixpath.join(
                                    dataset_root, group_partition_dir, filename
                                )
                                writer = pq.ParquetWriter(
                                    buffer,
                                    plan.target_schema,
                                    compression=plan.selected_codec,
                                )
                                staged_keys.append(current_staged_path)
                                staged_to_live[current_staged_path] = current_live_path
                                staged_rows[current_staged_path] = 0
                                staged_partition_dirs[current_staged_path] = (
                                    group_partition_dir
                                )
                                rows_in_file = 0
                                file_index += 1
                            assert writer is not None
                            writer.write_table(chunk)
                            rows_in_file += chunk_rows
                            staged_rows[current_staged_path] = rows_in_file
                            offset += chunk_rows
            if writer is not None:
                writer.close()
                assert buffer is not None
                filesystem.pipe(current_staged_path, buffer.getvalue())
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
    # Phase: validate — schema exactly matches; row-count invariant.
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
                if not parquet_file.schema_arrow.equals(
                    plan.target_schema, check_metadata=False
                ):
                    validation_error = f"Schema mismatch in staged file {staged_path}"
                    break
        if validation_error is None and total_staged_rows != source_row_count:
            validation_error = (
                f"Staged row count mismatch: got {total_staged_rows}, "
                f"expected {source_row_count}"
            )
    except Exception as exc:
        validation_error = f"Staged validation error: {exc}"
    validation = ValidationOutcome(
        succeeded=validation_error is None,
        staged_row_count=total_staged_rows,
        expected_row_count=source_row_count,
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
    # Phase: publish — copy and validate each planned live key.
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
                if not parquet_file.schema_arrow.equals(
                    plan.target_schema, check_metadata=False
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
    # Phase: cleanup — remove all sources after preceding gates pass.
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
            row_count=source_row_count,
            file_count=len(copied_live_keys),
            total_bytes=total_bytes,
        ),
        copied_live_keys=tuple(copied_live_keys),
        untouched_source_keys=(),
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
        # Compaction is partition-local for every guarantee level. Cross-
        # partition/global compaction must be an explicit future operation.
        guarantee_level = _classify_guarantee(fs)
        force_rewrite_paths: set[str] = set()
        if codec is not None:
            target_codec = selected_codec.lower()
            if target_codec == "none":
                target_codec = "uncompressed"
            force_rewrite_paths = {
                file_stat["path"]
                for file_stat in file_stats
                if _parquet_codecs(
                    fs, file_stat["path"], file_stat.get("codecs")
                )
                != {target_codec}
            }
        compaction_groups, skipped_files = _plan_partition_local_compaction(
            file_stats,
            snapshot.dataset_path,
            target_mb_per_file,
            target_rows_per_file,
            force_rewrite_paths,
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
            skipped_files=skipped_files,
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
        derived_partition_columns: dict[str, tuple[str, ...]] | None = None,
        partition_timezone: str = "UTC",
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
        if schema is None:
            raise ValueError("Global repartitioning requires a readable source schema")
        derived_partition_keys = _normalize_derived_partition_keys(
            schema, derived_partition_columns, partition_timezone
        )
        derived_names = {key.name for key in derived_partition_keys}
        missing_columns = [
            column
            for column in partition_columns
            if column not in schema.names and column not in derived_names
        ]
        if missing_columns:
            raise ValueError(
                "partition_columns must name source or derived columns; "
                f"missing: {missing_columns}"
            )
        unused_derived = derived_names - set(partition_columns)
        if unused_derived:
            raise ValueError(
                "Derived partition definitions must appear in partition_columns; "
                f"unused: {sorted(unused_derived)}"
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
            derived_partition_keys=derived_partition_keys,
            dedup_groups=groups,
        )

    def plan_repartition(
        self,
        dataset_path: str,
        partition_columns: list[str],
        filesystem: AbstractFileSystem | None = None,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        validation_level: ValidationLevel | str | None = None,
        codec: str | None = None,
        derived_partition_columns: dict[str, tuple[str, ...]] | None = None,
        partition_timezone: str = "UTC",
        memory_budget_mb: int | None = None,
    ) -> RepartitionPlan:
        """Create an immutable pure full-dataset repartition plan (#60).

        Pure full-dataset repartitioning is a physical rewrite that preserves
        every source row, including exact duplicates. It performs no winner
        selection and carries no deduplication fields. ``partition_filter`` is not accepted:
        every source file is in scope, and unrelated partitions and source
        files are replaced exactly as the full-dataset plan specifies.

        Destination partition columns may be source columns or validated
        :class:`DerivedPartitionKey` values; partition columns are path
        metadata only and are not stored in physical file schemas.
        ``max_rows_per_file`` remains a hard per-destination-partition bound.

        ``memory_budget_mb`` selects the bounded-memory strategy recorded on
        the plan as ``repartition_memory_budget_mb``: when ``None`` the
        operation is group-bounded like
        :class:`GlobalRepartitionDeduplicationPlan`; when set, destination
        partitions whose materialized bucket exceeds the budget spill to a
        per-bucket temporary file under the maintenance workspace and are
        re-read through a row-batch reader during output writing.

        ``FULL_DISTINCT_KEY_SCAN`` is rejected because pure full-dataset
        repartitioning has no key semantics.
        """
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
        # Reject FULL_DISTINCT_KEY_SCAN before reading any file: pure
        # repartition has no key semantics and the validation level is a
        # frozen plan field, so failing fast keeps the contract explicit.
        coerced_validation = _coerce_validation_level(validation_level)
        if coerced_validation == ValidationLevel.FULL_DISTINCT_KEY_SCAN:
            raise ValueError(
                "FULL_DISTINCT_KEY_SCAN is not applicable to pure repartition: "
                "the operation has no key semantics."
            )
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
            MaintenanceOperation.REPARTITION,
            dataset_path,
            filesystem,
            None,
            partition_columns,
            target_mb_per_file,
            validation_level,
            codec,
        )
        if schema is None:
            raise ValueError("Pure repartition requires a readable source schema")
        derived_partition_keys = _normalize_derived_partition_keys(
            schema, derived_partition_columns, partition_timezone
        )
        derived_names = {key.name for key in derived_partition_keys}
        missing_columns = [
            column
            for column in partition_columns
            if column not in schema.names and column not in derived_names
        ]
        if missing_columns:
            raise ValueError(
                "partition_columns must name source or derived columns; "
                f"missing: {missing_columns}"
            )
        unused_derived = derived_names - set(partition_columns)
        if unused_derived:
            raise ValueError(
                "Derived partition definitions must appear in partition_columns; "
                f"unused: {sorted(unused_derived)}"
            )
        # Pure repartition is a full-dataset rewrite; every source file is
        # in exactly one global group so the result contract is auditable.
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
        groups = (global_group,) if file_stats else ()
        return RepartitionPlan(
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
            derived_partition_keys=derived_partition_keys,
            repartition_memory_budget_mb=memory_budget_mb,
            repartition_groups=groups,
        )

    def plan_ordered_compaction(
        self,
        dataset_path: str,
        sort_keys: list[SortKey | str],
        filesystem: AbstractFileSystem | None = None,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        validation_level: ValidationLevel | str | None = None,
        codec: str | None = None,
        memory_budget_mb: int | None = None,
        spill_directory: str | None = None,
    ) -> OrderedCompactionPlan:
        """Create an immutable partition-ordered compaction plan (#61).

        Ordered compaction produces one globally ordered output sequence per
        affected physical partition, split into contiguous
        ``max_rows_per_file``-bounded chunks. Adjacent output files form a
        single sorted run; the result does not claim global business ordering
        across partitions. Ordinary :class:`CompactionPlan` remains unordered;
        no sort flag is added to :meth:`plan_compaction` or
        :meth:`plan_coordinated_optimization`.

        *sort_keys* accepts typed :class:`SortKey` values or strings using the
        existing ``+col`` / ``-col`` convention. String keys receive the
        SQL-standard null placement (``nulls_last`` for ascending,
        ``nulls_first`` for descending); typed ``SortKey`` values supply
        direction and null placement verbatim.

        *partition_filter* restricts which physical partitions are in scope;
        ordering stays partition-complete within each selected partition.

        Sorting is bounded by *memory_budget_mb* (external merge sort per
        physical partition). When ``memory_budget_mb`` is ``None`` the
        operation sorts each partition in memory and is partition-bounded; the
        planner rejects a partition whose estimated size exceeds the default
        budget unless a *spill_directory* is supplied. For ``atomic_local``
        the spill directory must be on the same filesystem as the dataset
        root.

        ``FULL_DISTINCT_KEY_SCAN`` is rejected: ordered compaction has no key
        semantics.
        """
        if target_rows_per_file is not None and target_rows_per_file <= 0:
            raise ValueError("target_rows_per_file must be > 0")
        coerced_validation = _coerce_validation_level(validation_level)
        if coerced_validation == ValidationLevel.FULL_DISTINCT_KEY_SCAN:
            raise ValueError(
                "FULL_DISTINCT_KEY_SCAN is not applicable to ordered "
                "compaction: the operation has no key semantics."
            )
        normalized_sort_keys = _normalize_sort_keys(sort_keys)
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
            MaintenanceOperation.ORDERED_COMPACTION,
            dataset_path,
            filesystem,
            partition_filter,
            target_mb_per_file,
            validation_level,
            codec,
        )
        if schema is None:
            raise ValueError("Ordered compaction requires a readable source schema")
        missing_sort_columns = [
            key.column for key in normalized_sort_keys if key.column not in schema.names
        ]
        if missing_sort_columns:
            raise ValueError(
                f"sort_keys must name source columns; missing: {missing_sort_columns}"
            )
        # One group per physical partition: ordering is partition-complete.
        groups = _plan_partition_local_deduplication_groups(
            file_stats, snapshot.dataset_path
        )
        guarantee_level = _classify_guarantee(fs)
        _validate_ordered_compaction_spill_contract(
            groups,
            memory_budget_mb,
            spill_directory,
            guarantee_level,
            snapshot.dataset_path,
        )
        return OrderedCompactionPlan(
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
            sort_keys=normalized_sort_keys,
            ordered_groups=groups,
            sort_memory_budget_mb=memory_budget_mb,
            sort_spill_directory=spill_directory,
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

    def plan_schema_rewrite(
        self,
        dataset_path: str,
        target_schema: pa.Schema,
        cast_policy: CastPolicy | str = CastPolicy.SAFE,
        filesystem: AbstractFileSystem | None = None,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        validation_level: ValidationLevel | str | None = None,
        codec: str | None = None,
        memory_budget_mb: int | None = None,
    ) -> SchemaRewritePlan:
        """Create an immutable caller-directed schema rewrite plan (#62).

        The caller supplies the target schema and cast policy; dtype inference
        is not invoked. The plan exposes the source schema, target schema,
        and the tuple of fields whose type changes.

        Planning validates target fields against the full rewrite scope —
        every source file's schema is checked by the shared reconciliation
        step. ``STRICT`` allows only value-preserving promotions; ``SAFE``
        adds lossless narrowing (``int64 → int32`` when every value fits);
        ``LOOSE`` allows narrowing that may truncate but validates every
        value across the full scope before publication.

        Partition columns remain path metadata; their types are not part of
        the physical file schema and are not cast. ``memory_budget_mb`` sets
        the batch size for both reading and casting; ``None`` uses the
        PyArrow scanner default (row-batch-bounded).
        """
        if not isinstance(target_schema, pa.Schema):
            raise ValueError("target_schema must be a pyarrow.Schema")
        if target_rows_per_file is not None and target_rows_per_file <= 0:
            raise ValueError("target_rows_per_file must be > 0")
        coerced_policy = _coerce_cast_policy(cast_policy)
        coerced_validation = _coerce_validation_level(validation_level)
        # FULL_DISTINCT_KEY_SCAN (full-scope cast validation) is mandatory for
        # STRICT and the default for LOOSE per the PRD validation table. SAFE
        # is opt-in only when the caller explicitly requests it.
        if coerced_validation == ValidationLevel.DEFAULT and coerced_policy in (
            CastPolicy.STRICT,
            CastPolicy.LOOSE,
        ):
            coerced_validation = ValidationLevel.FULL_DISTINCT_KEY_SCAN
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
            MaintenanceOperation.SCHEMA_REWRITE,
            dataset_path,
            filesystem,
            partition_filter,
            target_mb_per_file,
            validation_level,
            codec,
        )
        if schema is None:
            raise ValueError(
                "Schema rewrite requires a readable, losslessly reconciled "
                "source schema."
            )
        source_schema = schema
        # The target schema must name exactly the same physical columns; it
        # may reorder them but may not add or drop fields.
        source_names = set(source_schema.names)
        target_names = set(target_schema.names)
        if source_names != target_names:
            missing = source_names - target_names
            extra = target_names - source_names
            raise ValueError(
                "target_schema must name exactly the same fields as the "
                f"source schema; missing: {sorted(missing)}, "
                f"extra: {sorted(extra)}"
            )
        changed_fields = _schema_rewrite_changed_fields(source_schema, target_schema)
        if coerced_policy == CastPolicy.STRICT:
            _validate_strict_promotion(source_schema, target_schema, changed_fields)
        groups = _plan_partition_local_deduplication_groups(
            file_stats, snapshot.dataset_path
        )
        return SchemaRewritePlan(
            source_snapshot=snapshot,
            selected_backend=self.backend,
            guarantee_level=_classify_guarantee(fs),
            partition_scope=scope,
            schema_outcome=schema_outcome,
            selected_codec=selected_codec,
            max_rows_per_file=target_rows_per_file,
            target_byte_size=target_byte_size,
            validation_level=coerced_validation,
            schema=source_schema,
            target_schema=target_schema,
            source_schema=source_schema,
            cast_policy=coerced_policy,
            changed_fields=changed_fields,
            schema_rewrite_groups=groups,
            schema_rewrite_memory_budget_mb=memory_budget_mb,
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
        - ``atomic_local`` :class:`RepartitionPlan` — pure full-dataset
          staged-rename repartition with rollback; preserves every source
          row, including exact duplicates (#60).
        - ``atomic_local`` :class:`OrderedCompactionPlan` — partition-ordered
          staged-rename compaction with rollback; output is one globally
          sorted run per physical partition split into contiguous
          ``max_rows_per_file`` chunks (#61).
        - ``best_effort_object_store`` :class:`CompactionPlan` — staged-copy
          lifecycle with per-key validation, source-drift revalidation, and
          recovery artifact reporting.  *filesystem* is **required** for this
          path.
        - ``best_effort_object_store`` :class:`PartitionLocalDeduplicationPlan`
          — staged-copy deduplication with per-key validation and source-drift
          revalidation.
        - ``best_effort_object_store`` :class:`GlobalRepartitionDeduplicationPlan`
          — staged-copy global repartitioning deduplication.
        - ``best_effort_object_store`` :class:`RepartitionPlan` — staged-copy
          pure full-dataset repartition; preserves every source row, including
          exact duplicates (#60).
        - ``atomic_local`` :class:`SchemaRewritePlan` — caller-directed
          schema rewrite with batch-streamed casting and staged-rename
          publication; validates casts across the full scope before any
          live mutation (#62).
        - ``best_effort_object_store`` :class:`SchemaRewritePlan` — staged-copy
          schema rewrite with per-key validation and source-drift
          revalidation.  *filesystem* is **required** for this path.
        - ``best_effort_object_store`` :class:`OrderedCompactionPlan` —
          staged-copy partition-ordered compaction; per-file and adjacent-file
          sort order validated on staged output (#61).
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
            plan, RepartitionPlan
        ):
            return _execute_atomic_local_repartition(
                plan,
                lock_timeout_s=lock_timeout_s,
                lock_retry_interval_s=lock_retry_interval_s,
            )

        if plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL and isinstance(
            plan, OrderedCompactionPlan
        ):
            return _execute_atomic_local_ordered_compaction(
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

        if plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL and isinstance(
            plan, SchemaRewritePlan
        ):
            return _execute_atomic_local_schema_rewrite(
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

        if (
            plan.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
            and isinstance(plan, RepartitionPlan)
        ):
            if filesystem is None:
                raise ValueError(
                    "best_effort_object_store repartition requires the filesystem "
                    "used to create the plan."
                )
            return _execute_best_effort_repartition(plan, filesystem)

        if (
            plan.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
            and isinstance(plan, OrderedCompactionPlan)
        ):
            if filesystem is None:
                raise ValueError(
                    "best_effort_object_store ordered compaction requires the "
                    "filesystem used to create the plan."
                )
            return _execute_best_effort_ordered_compaction(plan, filesystem)

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
        # best_effort_object_store schema rewrite (#62)
        # ---------------------------------------------------------- #
        if (
            plan.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
            and isinstance(plan, SchemaRewritePlan)
        ):
            if filesystem is None:
                raise ValueError(
                    "best_effort_object_store schema rewrite requires the "
                    "filesystem used to create the plan."
                )
            return _execute_best_effort_schema_rewrite(plan, filesystem)

        # ---------------------------------------------------------- #
        # Seams for downstream issues — raise descriptive errors
        # ---------------------------------------------------------- #

        raise NotImplementedError(
            f"execute() is not implemented for plan type {type(plan).__name__!r} "
            f"with guarantee_level={plan.guarantee_level!r}."
        )
