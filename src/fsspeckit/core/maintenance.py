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

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem

from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)


@dataclass
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
        result = {
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


@dataclass
class CompactionGroup:
    """A group of files to be compacted or optimized together.

    This dataclass represents a logical grouping of files that will be processed
    together during maintenance operations. It enables streaming execution by
    bounding the amount of data processed at once.

    Attributes:
        files: List of FileInfo objects in this group.
        total_size_bytes: Total size of all files in this group (computed).
        total_rows: Total rows across all files in this group (computed).

    Note:
        Must contain at least one file. The total_size_bytes and total_rows
        are computed during initialization and used for planning decisions.
        This structure enables per-group streaming processing without
        materializing entire datasets.
    """

    files: list[FileInfo]
    total_size_bytes: int = field(init=False)
    total_rows: int = field(init=False)

    def __post_init__(self) -> None:
        if not self.files:
            raise ValueError("CompactionGroup must contain at least one file")
        self.total_size_bytes = sum(f.size_bytes for f in self.files)
        self.total_rows = sum(f.num_rows for f in self.files)

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
        files = [
            FileInfo(fi["path"], fi["size_bytes"], fi["num_rows"])  # type: ignore[union-attr]
            for fi in file_infos  # type: ignore[union-attr]
        ]
    else:
        files = file_infos  # type: ignore

    size_threshold_bytes = (
        target_mb_per_file * 1024 * 1024 if target_mb_per_file is not None else None
    )

    # Separate candidate files (eligible for compaction) from large files.
    candidates: list[FileInfo] = []
    large_files: list[FileInfo] = []
    for file_info in files:  # type: ignore[union-attr]
        size_bytes = file_info.size_bytes  # type: ignore[union-attr]
        if size_threshold_bytes is None or size_bytes < size_threshold_bytes:
            candidates.append(file_info)  # type: ignore[union-attr]
        else:
            large_files.append(file_info)  # type: ignore[union-attr]

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

    # Only compact groups that contain more than one file; singleton groups
    # would just rewrite an existing file.
    finalized_groups: list[CompactionGroup] = [
        CompactionGroup(files=group) for group in groups if len(group) > 1
    ]

    # Calculate statistics
    before_file_count = len(files)
    before_total_bytes = sum(f.size_bytes for f in files)  # type: ignore[union-attr]

    compacted_file_count = sum(len(group.files) for group in finalized_groups)
    untouched_files = large_files + [  # type: ignore[operator]
        file_info
        for file_info in candidates
        if not any(file_info in group.files for group in finalized_groups)
    ]

    after_file_count = len(untouched_files) + len(finalized_groups)

    # Estimate after_total_bytes (assume minimal compression change for planning)
    compacted_bytes = sum(group.total_size_bytes for group in finalized_groups)
    untouched_bytes = sum(f.size_bytes for f in untouched_files)  # type: ignore[union-attr]
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
        files = [
            FileInfo(fi["path"], fi["size_bytes"], fi["num_rows"])  # type: ignore[union-attr]
            for fi in file_infos  # type: ignore[union-attr]
        ]
    else:
        files = file_infos  # type: ignore

    # For optimization, we typically want to process all files unless they're
    # already large enough to be left alone
    size_threshold_bytes = (
        target_mb_per_file * 1024 * 1024 if target_mb_per_file is not None else None
    )

    # Separate candidate files from large files
    candidates: list[FileInfo] = []
    large_files: list[FileInfo] = []
    for file_info in files:  # type: ignore[union-attr]
        size_bytes = file_info.size_bytes  # type: ignore[union-attr]
        if size_threshold_bytes is None or size_bytes < size_threshold_bytes:
            candidates.append(file_info)  # type: ignore[union-attr]
        else:
            large_files.append(file_info)  # type: ignore[union-attr]

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
            finalized_groups.append(CompactionGroup(files=group))

    # Calculate statistics
    before_file_count = len(files)
    before_total_bytes = sum(f.size_bytes for f in files)  # type: ignore[union-attr]

    optimized_file_count = sum(len(group.files) for group in finalized_groups)
    untouched_files = large_files  # Only large files are left untouched in optimization

    after_file_count = len(untouched_files) + len(finalized_groups)

    # Estimate after_total_bytes (optimization may improve compression)
    optimized_bytes = sum(group.total_size_bytes for group in finalized_groups)
    untouched_bytes = sum(f.size_bytes for f in untouched_files)  # type: ignore[union-attr]
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
        files = [
            FileInfo(fi["path"], fi["size_bytes"], fi["num_rows"])  # type: ignore[union-attr]
            for fi in file_infos  # type: ignore[union-attr]
        ]
    else:
        files = file_infos  # type: ignore

    size_threshold_bytes = (
        target_mb_per_file * 1024 * 1024 if target_mb_per_file is not None else None
    )

    # For deduplication, we typically want to process all files since the goal
    # is to remove duplicates across the entire dataset
    # Only exclude files that are already large enough to be left alone
    candidates: list[FileInfo] = []
    large_files: list[FileInfo] = []
    for file_info in files:  # type: ignore[union-attr]
        size_bytes = file_info.size_bytes  # type: ignore[union-attr]
        if size_threshold_bytes is None or size_bytes < size_threshold_bytes:
            candidates.append(file_info)  # type: ignore[union-attr]
        else:
            large_files.append(file_info)  # type: ignore[union-attr]

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
            finalized_groups.append(CompactionGroup(files=group))

    # Calculate statistics
    before_file_count = len(files)
    before_total_bytes = sum(f.size_bytes for f in files)  # type: ignore[union-attr]

    deduplicated_file_count = sum(len(group.files) for group in finalized_groups)
    untouched_files = large_files  # Only large files are left untouched

    after_file_count = len(untouched_files) + len(finalized_groups)

    # Estimate after_total_bytes (deduplication may reduce data size)
    deduplicated_bytes = sum(group.total_size_bytes for group in finalized_groups)
    untouched_bytes = sum(f.size_bytes for f in untouched_files)  # type: ignore[union-attr]
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
