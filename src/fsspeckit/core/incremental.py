"""
Shared utilities for incremental parquet dataset rewrite operations.

This module provides backend-neutral functionality for selective rewriting
of parquet datasets based on metadata analysis and partition pruning.

Key responsibilities:
1. Parquet metadata extraction and analysis
2. Conservative file membership determination
3. Partition pruning logic
4. File management for incremental operations
5. Hive partition parsing
6. Key-based file scanning and validation
"""

from __future__ import annotations

import os
import re
import tempfile as temp_module
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Literal, Sequence

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.dataset as ds


def validate_no_null_keys(table: pa.Table, key_columns: Sequence[str]) -> None:
    """Reject NULL keys in source data.

    Merge semantics assume keys uniquely identify rows. NULLs would break that
    invariant and produce surprising behavior (e.g. many-to-many matches).
    """
    if not key_columns or table.num_rows == 0:
        return

    import pyarrow.compute as pc

    for col in key_columns:
        if col not in table.column_names:
            continue
        if pc.any(pc.is_null(table.column(col))).as_py():
            raise ValueError(f"NULL values are not allowed in key column '{col}'")


def validate_partition_column_immutability(
    source_table: pa.Table,
    target_table: pa.Table,
    key_columns: Sequence[str],
    partition_columns: Sequence[str],
) -> None:
    """Reject merges that would change partition columns for existing keys."""
    if not key_columns or not partition_columns:
        return
    if source_table.num_rows == 0 or target_table.num_rows == 0:
        return

    import pyarrow.compute as pc

    for col in key_columns:
        if col not in source_table.column_names or col not in target_table.column_names:
            raise ValueError(f"Key column '{col}' must exist in source and target")

    for col in partition_columns:
        if col not in source_table.column_names or col not in target_table.column_names:
            raise ValueError(
                f"Partition column '{col}' must exist in source and target for immutability validation"
            )

    joined = target_table.join(
        source_table,
        keys=list(key_columns),
        join_type="inner",
        right_suffix="__src",
        coalesce_keys=True,
    )

    if joined.num_rows == 0:
        return

    for col in partition_columns:
        if col in key_columns:
            continue
        src_name = f"{col}__src"
        if src_name not in joined.column_names:
            continue

        eq = pc.equal(joined.column(col), joined.column(src_name))
        violations = pc.fill_null(pc.invert(eq), True)
        if pc.any(violations).as_py():
            raise ValueError(
                "Cannot merge: partition column values cannot change for existing keys"
            )


def list_dataset_files(
    dataset_path: str,
    filesystem: Any = None,
) -> list[str]:
    """List parquet data files under a dataset path.

    Args:
        dataset_path: Dataset root path.
        filesystem: Optional fsspec filesystem instance.

    Returns:
        Sorted list of parquet file paths.
    """
    import glob

    if filesystem is None:
        return sorted(glob.glob(f"{dataset_path}/**/*.parquet", recursive=True))

    # fsspec: prefer find(), fall back to glob().
    try:
        files = filesystem.find(dataset_path, withdirs=False)  # type: ignore[call-arg]
        return sorted([p for p in files if p.endswith(".parquet")])
    except Exception:
        try:
            files = filesystem.find(dataset_path)  # type: ignore[call-arg]
            return sorted([p for p in files if p.endswith(".parquet")])
        except Exception:
            try:
                return sorted(filesystem.glob(f"{dataset_path}/**/*.parquet"))
            except Exception:
                # Last resort: no files discovered
                return []


def parse_hive_partition_path(
    file_path: str,
    partition_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Parse hive-style partition values from a file path.

    Hive partitioning uses directory structure like: key1=value1/key2=value2/file.parquet

    Args:
        file_path: Path to a parquet file inside a hive-partitioned dataset.
        partition_columns: Optional list of expected partition column names. If provided,
            only these keys are returned.

    Returns:
        Mapping of partition column name -> parsed value.
    """
    partition_values: dict[str, Any] = {}

    # Use Path.parts for robustness, but keep '=' matching simple.
    parts = Path(file_path).parts
    pattern = re.compile(r"^([^=]+)=(.+)$")

    for part in parts:
        match = pattern.match(part)
        if not match:
            continue

        key, value = match.groups()
        if partition_columns is not None and key not in set(partition_columns):
            continue

        partition_values[key] = _convert_partition_value(value)

    return partition_values


def _convert_partition_value(value: str) -> Any:
    """Convert a partition value string to a reasonable Python type."""
    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        pass

    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    return value


def extract_source_partition_values(
    source_table: pa.Table,
    partition_columns: Sequence[str],
) -> set[tuple[Any, ...]]:
    """Extract unique partition tuples from a source table.

    Args:
        source_table: Source data.
        partition_columns: Partition column names (in order).

    Returns:
        Set of tuples with partition column values in the given order.
    """
    if not partition_columns:
        return set()

    for col in partition_columns:
        if col not in source_table.column_names:
            raise ValueError(
                f"Partition column '{col}' not found in source table. "
                f"Available columns: {', '.join(source_table.column_names)}"
            )

    if len(partition_columns) == 1:
        col = partition_columns[0]
        # Use PyArrow's unique function for better performance
        import pyarrow.compute as pc

        unique_vals = pc.unique(source_table.column(col))
        return {(v.as_py(),) for v in unique_vals}

    # For multiple columns, use group_by to get unique combinations
    import pyarrow as pa

    subset = source_table.select(partition_columns)
    grouped = subset.group_by(partition_columns).aggregate([])
    result_dict = grouped.to_pydict()
    return {
        tuple(result_dict[col][i] for col in partition_columns)
        for i in range(len(next(iter(result_dict.values()))))
    }


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
class MergeFileMetadata:
    """Metadata for a file affected by a merge operation."""

    path: str
    row_count: int
    operation: Literal["rewritten", "inserted", "preserved"]
    size_bytes: int | None = None


@dataclass
class MergeResult:
    """Result of an incremental merge operation."""

    strategy: Literal["insert", "update", "upsert"]
    source_count: int
    target_count_before: int
    target_count_after: int
    inserted: int
    updated: int
    deleted: int
    files: list[MergeFileMetadata]

    rewritten_files: list[str]
    inserted_files: list[str]
    preserved_files: list[str]
    metrics: dict[str, Any] | None = None


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
        partition_columns: Sequence[str] | None = None,
    ) -> list[ParquetFileMetadata]:
        """
        Analyze all parquet files in a dataset directory.

        Args:
            dataset_path: Path to dataset directory
            filesystem: Optional filesystem object
            partition_columns: Optional partition columns (hive-style path parsing)

        Returns:
            List of ParquetFileMetadata for all parquet files
        """
        files = list_dataset_files(dataset_path, filesystem)

        metadata_list = []
        for file_path in files:
            if file_path not in self._file_metadata_cache:
                try:
                    metadata = self._analyze_single_file(file_path, filesystem)
                    if partition_columns is not None:
                        metadata.partition_values = parse_hive_partition_path(
                            file_path, partition_columns=partition_columns
                        )
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

        metadata = pq.read_metadata(file_path, filesystem=filesystem)

        row_group_count = metadata.num_row_groups
        total_rows = sum(metadata.row_group(i).num_rows for i in range(row_group_count))

        schema_names = list(metadata.schema.names)
        column_stats: dict[str, dict[str, Any]] = {}

        for col_idx, col_name in enumerate(schema_names):
            min_values: list[Any] = []
            max_values: list[Any] = []
            null_count_total = 0
            any_has_min_max = False

            for rg_idx in range(row_group_count):
                rg = metadata.row_group(rg_idx)
                col_chunk = rg.column(col_idx)
                stats = col_chunk.statistics
                if stats is None:
                    continue

                if stats.null_count is not None:
                    null_count_total += stats.null_count

                if getattr(stats, "has_min_max", False):
                    any_has_min_max = True
                    min_values.append(stats.min)
                    max_values.append(stats.max)

            col_stat: dict[str, Any] = {}
            if any_has_min_max and min_values and max_values:
                try:
                    col_stat["min"] = min(min_values)
                    col_stat["max"] = max(max_values)
                except TypeError:
                    # Non-comparable stats (e.g. mixed types) -> omit for safety
                    pass

            if null_count_total:
                col_stat["null_count"] = null_count_total

            if col_stat:
                column_stats[col_name] = col_stat

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

    def identify_candidate_files_by_partition_values(
        self,
        file_metadata: list[ParquetFileMetadata],
        *,
        partition_columns: Sequence[str],
        source_partition_values: set[tuple[Any, ...]],
    ) -> list[str]:
        """Prune candidate files by hive partition values.

        This is conservative: files with unknown partition_values are kept.
        """
        if not file_metadata:
            return []
        if not partition_columns or not source_partition_values:
            return [meta.path for meta in file_metadata]

        candidates: list[str] = []
        for meta in file_metadata:
            if not meta.partition_values:
                candidates.append(meta.path)
                continue

            file_tuple = tuple(
                meta.partition_values.get(col) for col in partition_columns
            )
            if file_tuple in source_partition_values:
                candidates.append(meta.path)

        return candidates

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

        # Backward-compatible behavior: partition pruning is only possible when
        # partition columns are present in the key tuple (i.e. partition columns ⊆ key_columns).
        partition_columns = [c for c in key_columns if c in partition_schema.names]
        if not partition_columns or not source_keys:
            return [meta.path for meta in file_metadata]

        source_partition_values: set[tuple[Any, ...]] = set()
        for key in source_keys:
            if isinstance(key, (list, tuple)):
                source_partition_values.add(
                    tuple(key[key_columns.index(col)] for col in partition_columns)
                )
            else:
                source_partition_values.add((key,))

        return self.identify_candidate_files_by_partition_values(
            file_metadata,
            partition_columns=partition_columns,
            source_partition_values=source_partition_values,
        )


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
        if not source_keys:
            return False

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

    def _mkdirs(self, path: str, filesystem: Any = None) -> None:
        if filesystem is None:
            os.makedirs(path, exist_ok=True)
            return

        for method_name, kwargs in (
            ("mkdirs", {"exist_ok": True}),
            ("makedirs", {"exist_ok": True}),
            ("mkdir", {"create_parents": True}),
        ):
            method = getattr(filesystem, method_name, None)
            if method is None:
                continue
            try:
                method(path, **kwargs)
                return
            except TypeError:
                try:
                    method(path)
                    return
                except Exception:
                    pass

        # Last resort: best-effort no-op (some fsspec FS create dirs on write).

    def _move(self, src: str, dst: str, filesystem: Any = None) -> None:
        if filesystem is None:
            os.replace(src, dst)
            return

        for method_name in ("move", "mv", "rename"):
            method = getattr(filesystem, method_name, None)
            if method is None:
                continue
            method(src, dst)
            return

        raise AttributeError("filesystem does not provide a move/mv/rename method")

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

    def create_staging_directory(self, base_path: str, filesystem: Any = None) -> str:
        """Create a staging directory for incremental operations."""
        staging_dir = os.path.join(base_path, f".staging_{uuid.uuid4().hex[:8]}")
        self._mkdirs(staging_dir, filesystem=filesystem)
        self._temp_files.append(staging_dir)
        return staging_dir

    def atomic_replace_files(
        self,
        source_files: list[str],
        target_files: list[str],
        filesystem: Any = None,
    ) -> None:
        """Replace target files with the provided source files (best-effort atomic).

        Callers are expected to write `source_files` into a staging area first.
        """
        if len(source_files) != len(target_files):
            raise ValueError("source_files and target_files must have the same length")

        for src, dst in zip(source_files, target_files):
            if filesystem is None:
                os.replace(src, dst)
                continue

            try:
                if filesystem.exists(dst):
                    filesystem.rm(dst)
            except Exception:
                pass

            self._move(src, dst, filesystem=filesystem)

    def cleanup_staging_files(self, filesystem: Any = None) -> None:
        """Clean up temporary staging files (best-effort)."""
        import shutil

        for temp_path in self._temp_files:
            try:
                if filesystem is None:
                    if os.path.isdir(temp_path):
                        shutil.rmtree(temp_path)
                    elif os.path.exists(temp_path):
                        os.remove(temp_path)
                else:
                    if filesystem.exists(temp_path):
                        filesystem.rm(temp_path, recursive=True)
            except Exception:
                pass

        self._temp_files.clear()


def plan_incremental_rewrite(
    dataset_path: str,
    source_keys: Sequence[Any],
    key_columns: Sequence[str],
    filesystem: Any = None,
    partition_schema: pa.Schema | None = None,
    partition_columns: Sequence[str] | None = None,
    source_partition_values: set[tuple[Any, ...]] | None = None,
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
    file_metadata = analyzer.analyze_dataset_files(
        dataset_path, filesystem, partition_columns=partition_columns
    )

    # Perform partition pruning first (when caller provides partition values)
    if partition_columns and source_partition_values:
        partition_pruner = PartitionPruner()
        candidate_files = partition_pruner.identify_candidate_files_by_partition_values(
            file_metadata,
            partition_columns=partition_columns,
            source_partition_values=source_partition_values,
        )
    else:
        candidate_files = [meta.path for meta in file_metadata]

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


def confirm_affected_files(
    candidate_files: list[str],
    key_columns: Sequence[str],
    source_keys: Sequence[Any],
    filesystem: Any = None,
) -> list[str]:
    """Confirm affected files by scanning only key columns.

    This is intended as a second-stage filter after conservative pruning:
    it checks whether any source key actually appears in a candidate file.

    Args:
        candidate_files: Candidate parquet files to scan.
        key_columns: Key column names to read.
        source_keys: Source keys to search for (single values or tuples).
        filesystem: Optional filesystem object.

    Returns:
        List of file paths that contain at least one of the source keys.
    """
    import pyarrow.parquet as pq

    if not candidate_files or not source_keys:
        return []

    multi_key = isinstance(source_keys[0], (list, tuple))
    if multi_key:
        source_set = {tuple(k) for k in source_keys}
    else:
        source_set = set(source_keys)

    affected: list[str] = []
    for file_path in candidate_files:
        try:
            table = pq.read_table(
                file_path, columns=list(key_columns), filesystem=filesystem
            )

            import pyarrow as pa
            import pyarrow.compute as pc

            if len(key_columns) == 1:
                # Use vectorized is_in for single column
                mask = pc.is_in(
                    table.column(key_columns[0]), value_set=pa.array(list(source_set))
                )
                if pc.any(mask).as_py():
                    affected.append(file_path)
            else:
                # For multi-column: convert source_set to table and use join
                source_list = list(source_set)
                source_table = pa.table(
                    {
                        col: [k[i] for k in source_list]
                        for i, col in enumerate(key_columns)
                    }
                )
                joined = table.join(source_table, keys=key_columns, join_type="inner")
                if joined.num_rows > 0:
                    affected.append(file_path)
        except Exception:
            # Conservative: if we can't confirm, treat as affected.
            affected.append(file_path)

    return affected
