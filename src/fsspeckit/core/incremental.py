"""
Shared utilities for incremental parquet dataset rewrite operations.

This module provides backend-neutral functionality for selective rewriting
of parquet datasets based on metadata analysis and partition pruning.

Key responsibilities:
1. Parquet metadata extraction and analysis
2. Conservative file membership determination
3. Partition pruning logic
4. File management for incremental operations
"""

from __future__ import annotations

import os
import tempfile as temp_module
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Literal, Sequence

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.dataset as ds


@dataclass
class ParquetFileMetadata:
    """Metadata for a single parquet file."""

    path: str
    row_group_count: int
    total_rows: int
    column_stats: dict[
        str, dict[str, Any]
    ]  # column_name -> {min, max, null_count, etc}
    partition_values: dict[str, Any] | None = None  # For partitioned datasets


@dataclass
class IncrementalRewritePlan:
    """Plan for executing an incremental rewrite operation."""

    # Files that need to be rewritten
    affected_files: list[str]
    # Files that can be preserved unchanged
    unaffected_files: list[str]
    # New files to be created for inserts (UPSERT only)
    new_files: list[str]
    # Total rows in affected files
    affected_rows: int

    def __post_init__(self) -> None:
        # Ensure all file paths are unique
        all_files = self.affected_files + self.unaffected_files + self.new_files
        if len(all_files) != len(set(all_files)):
            raise ValueError("File paths in incremental rewrite plan must be unique")


class ParquetMetadataAnalyzer:
    """Extract and analyze parquet file metadata for incremental rewrite planning."""

    def __init__(self) -> None:
        self._file_metadata_cache: dict[str, ParquetFileMetadata] = {}

    def analyze_dataset_files(
        self,
        dataset_path: str,
        filesystem: Any = None,
    ) -> list[ParquetFileMetadata]:
        """
        Analyze all parquet files in a dataset directory.

        Args:
            dataset_path: Path to dataset directory
            filesystem: Optional filesystem object

        Returns:
            List of ParquetFileMetadata for all parquet files
        """
        import glob
        import pyarrow.parquet as pq

        # Find all parquet files
        if filesystem is not None:
            # Use filesystem to list files
            pattern = f"{dataset_path}/**/*.parquet"
            files = []
            for file_info in filesystem.walk(dataset_path):
                if file_info.path.endswith(".parquet"):
                    files.append(file_info.path)
        else:
            # Use glob pattern
            pattern = f"{dataset_path}/**/*.parquet"
            files = glob.glob(pattern, recursive=True)

        metadata_list = []
        for file_path in files:
            if file_path not in self._file_metadata_cache:
                try:
                    metadata = self._analyze_single_file(file_path, filesystem)
                    self._file_metadata_cache[file_path] = metadata
                except Exception:
                    # If metadata extraction fails, treat file as affected for safety
                    metadata = ParquetFileMetadata(
                        path=file_path, row_group_count=0, total_rows=0, column_stats={}
                    )
                    self._file_metadata_cache[file_path] = metadata

            metadata_list.append(self._file_metadata_cache[file_path])

        return metadata_list

    def _analyze_single_file(
        self,
        file_path: str,
        filesystem: Any = None,
    ) -> ParquetFileMetadata:
        """Analyze a single parquet file."""
        import pyarrow.parquet as pq

        # Open parquet file
        if filesystem is not None:
            parquet_file = pq.ParquetFile(file_path, filesystem=filesystem)
        else:
            parquet_file = pq.ParquetFile(file_path)

        # Extract basic metadata
        metadata = parquet_file.metadata
        row_group_count = metadata.num_row_groups
        total_rows = sum(metadata.row_group(i).num_rows for i in range(row_group_count))

        # Extract column statistics
        column_stats = {}
        for col_idx in range(metadata.num_columns):
            col_name = metadata.schema_column(col_idx)
            col_stats = {}

            # Aggregate statistics across row groups
            min_values = []
            max_values = []
            null_counts = []

            for rg_idx in range(row_group_count):
                rg = metadata.row_group(rg_idx)
                col_metadata = rg.column(col_idx)

                if col_metadata.num_values > 0:
                    if col_metadata.min is not None:
                        min_values.append(col_metadata.min)
                    if col_metadata.max is not None:
                        max_values.append(col_metadata.max)
                    null_counts.append(col_metadata.num_nulls)

            if min_values:
                col_stats["min"] = min(min_values)
                col_stats["max"] = max(max_values)
            if null_counts:
                col_stats["null_count"] = sum(null_counts)

            column_stats[col_name] = col_stats

        return ParquetFileMetadata(
            path=file_path,
            row_group_count=row_group_count,
            total_rows=total_rows,
            column_stats=column_stats,
        )


class PartitionPruner:
    """Identify candidate files based on partition values."""

    def __init__(self) -> None:
        pass

    def identify_candidate_files(
        self,
        file_metadata: list[ParquetFileMetadata],
        key_columns: Sequence[str],
        source_keys: Sequence[Any],
        partition_schema: pa.Schema | None = None,
    ) -> list[str]:
        """
        Identify files that might contain the specified keys based on partition pruning.

        Args:
            file_metadata: List of file metadata
            key_columns: Key columns to search for
            source_keys: Keys to search for (as tuples for multi-column keys)
            partition_schema: Schema for partitioned datasets

        Returns:
            List of file paths that might contain the keys
        """
        if not file_metadata:
            return []

        # If no partition schema, all files are candidates
        if partition_schema is None:
            return [meta.path for meta in file_metadata]

        # Extract partition values from source keys
        partition_keys = [col for col in key_columns if col in partition_schema.names]
        if not partition_keys:
            return [meta.path for meta in file_metadata]

        # Get unique partition values from source
        unique_partitions = set()
        for key_tuple in source_keys:
            if isinstance(key_tuple, (list, tuple)):
                partition_value = tuple(
                    key_tuple[key_columns.index(col)] for col in partition_keys
                )
            else:
                partition_value = (key_tuple,)
            unique_partitions.add(partition_value)

        # Filter files by partition values
        candidate_files = []
        for meta in file_metadata:
            if meta.partition_values is None:
                # If we don't have partition values, include file for safety
                candidate_files.append(meta.path)
            else:
                # Check if this file's partition matches any source partition
                file_partition = tuple(
                    meta.partition_values.get(col) for col in partition_keys
                )
                if file_partition in unique_partitions:
                    candidate_files.append(meta.path)

        return candidate_files


class ConservativeMembershipChecker:
    """Implement conservative pruning logic for file membership determination."""

    def __init__(self) -> None:
        pass

    def file_might_contain_keys(
        self,
        file_metadata: ParquetFileMetadata,
        key_columns: Sequence[str],
        source_keys: Sequence[Any],
    ) -> bool:
        """
        Conservative check if a file might contain any of the source keys.

        This is conservative: if we can't prove the file doesn't contain the keys,
        we assume it does.

        Args:
            file_metadata: Metadata for the file to check
            key_columns: Key columns being searched
            source_keys: Keys to search for

        Returns:
            True if file might contain keys (conservative), False if definitely doesn't
        """
        # Get key ranges from source data
        if isinstance(source_keys[0], (list, tuple)):
            # Multi-column keys
            key_ranges = self._get_multi_column_ranges(source_keys, key_columns)
        else:
            # Single column keys
            key_ranges = self._get_single_column_ranges(source_keys, key_columns)

        # Check each key column
        for col_name in key_columns:
            if col_name not in file_metadata.column_stats:
                # Column not found in file metadata - assume file might contain keys
                return True

            col_stats = file_metadata.column_stats[col_name]

            # Check if we have enough metadata to make a decision
            if "min" not in col_stats or "max" not in col_stats:
                # No min/max stats - assume file might contain keys
                return True

            file_min = col_stats["min"]
            file_max = col_stats["max"]

            # Check if any key range overlaps with file range
            col_ranges = key_ranges.get(col_name, [])
            for key_min, key_max in col_ranges:
                if self._ranges_overlap(file_min, file_max, key_min, key_max):
                    return True

        # If we get here, no overlap found - file definitely doesn't contain keys
        return False

    def _get_single_column_ranges(
        self,
        source_keys: Sequence[Any],
        key_columns: Sequence[str],
    ) -> dict[str, list[tuple[Any, Any]]]:
        """Get value ranges for single-column keys."""
        if len(key_columns) != 1:
            return {}

        col_name = key_columns[0]
        key_values = [key for key in source_keys if key is not None]

        if not key_values:
            return {col_name: [(None, None)]}

        return {col_name: [(min(key_values), max(key_values))]}

    def _get_multi_column_ranges(
        self,
        source_keys: Sequence[Any],
        key_columns: Sequence[str],
    ) -> dict[str, list[tuple[Any, Any]]]:
        """Get value ranges for multi-column keys."""
        ranges = {}

        for col_idx, col_name in enumerate(key_columns):
            col_values = []
            for key_tuple in source_keys:
                if key_tuple and len(key_tuple) > col_idx:
                    col_values.append(key_tuple[col_idx])

            if col_values:
                ranges[col_name] = [(min(col_values), max(col_values))]
            else:
                ranges[col_name] = [(None, None)]

        return ranges

    def _ranges_overlap(
        self,
        range1_min: Any,
        range1_max: Any,
        range2_min: Any,
        range2_max: Any,
    ) -> bool:
        """Check if two ranges overlap."""
        # Handle None values conservatively
        if (
            range1_min is None
            or range1_max is None
            or range2_min is None
            or range2_max is None
        ):
            return True  # Conservative: assume overlap

        # Check for overlap
        return not (range1_max < range2_min or range2_max < range1_min)


class IncrementalFileManager:
    """Manage file operations for incremental rewrite."""

    def __init__(self) -> None:
        self._temp_files: list[str] = []

    def generate_unique_filename(
        self,
        base_path: str,
        prefix: str = "incremental_",
        extension: str = ".parquet",
    ) -> str:
        """Generate a unique filename for incremental operations."""
        unique_id = str(uuid.uuid4())[:8]
        filename = f"{prefix}{unique_id}{extension}"
        return os.path.join(base_path, filename)

    def create_staging_directory(self, base_path: str) -> str:
        """Create a staging directory for incremental operations."""
        staging_dir = os.path.join(base_path, f".staging_{uuid.uuid4().hex[:8]}")
        os.makedirs(staging_dir, exist_ok=True)
        self._temp_files.append(staging_dir)
        return staging_dir

    def cleanup_staging_files(self) -> None:
        """Clean up temporary staging files."""
        import shutil

        for temp_path in self._temp_files:
            try:
                if os.path.isdir(temp_path):
                    shutil.rmtree(temp_path)
                elif os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                # Ignore cleanup errors
                pass

        self._temp_files.clear()


def plan_incremental_rewrite(
    dataset_path: str,
    source_keys: Sequence[Any],
    key_columns: Sequence[str],
    filesystem: Any = None,
    partition_schema: pa.Schema | None = None,
) -> IncrementalRewritePlan:
    """
    Plan an incremental rewrite operation based on metadata analysis.

    Args:
        dataset_path: Path to target dataset
        source_keys: Keys that will be updated/inserted
        key_columns: Key column names
        filesystem: Optional filesystem object
        partition_schema: Schema for partitioned datasets

    Returns:
        IncrementalRewritePlan with affected and unaffected files
    """
    # Analyze all files in the dataset
    analyzer = ParquetMetadataAnalyzer()
    file_metadata = analyzer.analyze_dataset_files(dataset_path, filesystem)

    # Perform partition pruning first
    partition_pruner = PartitionPruner()
    candidate_files = partition_pruner.identify_candidate_files(
        file_metadata, key_columns, source_keys, partition_schema
    )

    # Apply conservative metadata pruning
    membership_checker = ConservativeMembershipChecker()
    affected_files = []
    unaffected_files = []
    affected_rows = 0

    for meta in file_metadata:
        if meta.path not in candidate_files:
            # File was eliminated by partition pruning
            unaffected_files.append(meta.path)
        elif membership_checker.file_might_contain_keys(meta, key_columns, source_keys):
            # File might contain keys - include in affected files
            affected_files.append(meta.path)
            affected_rows += meta.total_rows
        else:
            # File definitely doesn't contain keys
            unaffected_files.append(meta.path)

    return IncrementalRewritePlan(
        affected_files=affected_files,
        unaffected_files=unaffected_files,
        new_files=[],  # Will be populated by caller for UPSERT operations
        affected_rows=affected_rows,
    )
