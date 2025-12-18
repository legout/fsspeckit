"""Base class for dataset handlers with shared implementations.

This module provides the abstract base class that both DuckDB and PyArrow backends
inherit from. It contains common logic for validation, key normalization, and
shared helper methods to eliminate code duplication.
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Literal

if TYPE_CHECKING:
    import pyarrow as pa
    from fsspec import AbstractFileSystem

    from fsspeckit.core.incremental import MergeResult
    from fsspeckit.datasets.write_result import WriteDatasetResult

from fsspeckit.common.logging import get_logger
from fsspeckit.core.merge import normalize_key_columns

logger = get_logger(__name__)


class BaseDatasetHandler(ABC):
    """Abstract base class for dataset handlers.

    This class provides shared implementations for common operations across
    DuckDB and PyArrow backends. Backend-specific operations are delegated
    to abstract methods that subclasses must implement.

    Key shared functionality:
    - Input validation and normalization
    - Key column handling
    - Statistics calculation
    - Error handling patterns
    - File management utilities

    Subclasses must implement:
    - _get_filesystem(): Return the filesystem instance
    - _read_table_from_path(): Read a table from a path
    - _write_table_to_path(): Write a table to a path
    - write_dataset(): Write dataset with mode support
    - merge(): Merge data into existing dataset
    """

    @property
    @abstractmethod
    def filesystem(self) -> AbstractFileSystem:
        """Return the filesystem instance used by this handler."""
        ...

    @abstractmethod
    def read_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
        filters: Any = None,
        use_threads: bool = True,
    ) -> pa.Table:
        """Read parquet file(s) and return as PyArrow table.

        Args:
            path: Path to parquet file or directory
            columns: Optional list of columns to read
            filters: Optional row filter expression
            use_threads: Whether to use parallel reading

        Returns:
            PyArrow table containing the data
        """
        ...

    @abstractmethod
    def write_parquet(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        compression: str | None = "snappy",
        row_group_size: int | None = None,
    ) -> None:
        """Write data to a single parquet file.

        Args:
            data: PyArrow table or list of tables to write
            path: Output file path
            compression: Compression codec to use
            row_group_size: Rows per row group
        """
        ...

    @abstractmethod
    def write_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        *,
        mode: Literal["append", "overwrite"] = "append",
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
    ) -> WriteDatasetResult:
        """Write a parquet dataset with specified mode.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            mode: Write mode ('append' or 'overwrite')
            compression: Compression codec
            max_rows_per_file: Maximum rows per file
            row_group_size: Rows per row group

        Returns:
            WriteDatasetResult with metadata about written files
        """
        ...

    @abstractmethod
    def merge(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        strategy: Literal["insert", "update", "upsert"],
        key_columns: list[str] | str,
        *,
        partition_columns: list[str] | str | None = None,
        schema: pa.Schema | None = None,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
    ) -> MergeResult:
        """Merge data into an existing parquet dataset incrementally.

        Args:
            data: Source data to merge
            path: Target dataset path
            strategy: Merge strategy ('insert', 'update', 'upsert')
            key_columns: Key columns for matching records
            partition_columns: Optional partition columns
            schema: Optional schema to enforce
            compression: Compression codec
            max_rows_per_file: Maximum rows per file
            row_group_size: Rows per row group

        Returns:
            MergeResult with merge operation statistics
        """
        ...

    @abstractmethod
    def compact_parquet_dataset(
        self,
        path: str,
        *,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Compact a parquet dataset by combining small files.

        Args:
            path: Dataset path
            target_mb_per_file: Target size per file in MB
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec for output
            dry_run: Whether to perform a dry run
            verbose: Print progress information

        Returns:
            Dictionary containing compaction statistics
        """
        ...

    @abstractmethod
    def optimize_parquet_dataset(
        self,
        path: str,
        *,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Optimize a parquet dataset through compaction and maintenance.

        Args:
            path: Dataset path
            target_mb_per_file: Target size per file in MB
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec for output
            verbose: Print progress information

        Returns:
            Dictionary containing optimization statistics
        """
        ...

    def _validate_key_columns(
        self,
        key_columns: list[str] | str,
        available_columns: list[str],
        context: str = "source",
    ) -> list[str]:
        """Validate and normalize key columns.

        Args:
            key_columns: Key column(s) as string or list
            available_columns: Available column names to validate against
            context: Context for error messages (e.g., 'source', 'target')

        Returns:
            Normalized list of key column names

        Raises:
            ValueError: If key columns are invalid or missing
        """
        normalized = normalize_key_columns(key_columns)
        available_set = set(available_columns)

        missing = [col for col in normalized if col not in available_set]
        if missing:
            raise ValueError(
                f"Key column(s) missing from {context}: {', '.join(missing)}. "
                f"Available columns: {', '.join(sorted(available_set))}"
            )

        return normalized

    def _validate_partition_columns(
        self,
        partition_columns: list[str] | str | None,
        available_columns: list[str],
    ) -> list[str]:
        """Validate and normalize partition columns.

        Args:
            partition_columns: Partition column(s) as string or list
            available_columns: Available column names to validate against

        Returns:
            Normalized list of partition column names (empty if None)

        Raises:
            ValueError: If partition columns are invalid or missing
        """
        if partition_columns is None:
            return []

        if isinstance(partition_columns, str):
            partition_columns = [partition_columns]

        available_set = set(available_columns)
        missing = [col for col in partition_columns if col not in available_set]
        if missing:
            raise ValueError(
                f"Partition column(s) not found in source: {', '.join(missing)}. "
                f"Available columns: {', '.join(sorted(available_set))}"
            )

        return list(partition_columns)

    def _validate_merge_strategy(
        self,
        strategy: str,
        target_exists: bool,
    ) -> None:
        """Validate merge strategy against target state.

        Args:
            strategy: Merge strategy name
            target_exists: Whether target dataset exists

        Raises:
            ValueError: If strategy is invalid for current state
        """
        valid_strategies = ("insert", "update", "upsert")
        if strategy not in valid_strategies:
            raise ValueError(
                f"strategy must be one of: {', '.join(valid_strategies)}, got: {strategy}"
            )

        if strategy == "update" and not target_exists:
            raise ValueError(
                "UPDATE strategy requires an existing target dataset (non-existent target)"
            )

    def _validate_write_mode(self, mode: str) -> None:
        """Validate write mode.

        Args:
            mode: Write mode string

        Raises:
            ValueError: If mode is invalid
        """
        if mode not in ("append", "overwrite"):
            raise ValueError(f"mode must be 'append' or 'overwrite', got: {mode}")

    def _validate_write_parameters(
        self,
        max_rows_per_file: int | None,
        row_group_size: int | None,
    ) -> int | None:
        """Validate write parameters and return adjusted row_group_size.

        Args:
            max_rows_per_file: Maximum rows per file
            row_group_size: Rows per row group

        Returns:
            Adjusted row_group_size (capped to max_rows_per_file if needed)

        Raises:
            ValueError: If parameters are invalid
        """
        if max_rows_per_file is not None and max_rows_per_file <= 0:
            raise ValueError("max_rows_per_file must be > 0")
        if row_group_size is not None and row_group_size <= 0:
            raise ValueError("row_group_size must be > 0")

        if (
            max_rows_per_file is not None
            and row_group_size is not None
            and row_group_size > max_rows_per_file
        ):
            return max_rows_per_file

        return row_group_size

    def _combine_tables(self, data: pa.Table | list[pa.Table]) -> pa.Table:
        """Combine list of tables into single table.

        Args:
            data: Single table or list of tables

        Returns:
            Combined PyArrow table
        """
        import pyarrow as pa

        if isinstance(data, list):
            return pa.concat_tables(data, promote_options="permissive")
        return data

    def _generate_unique_filename(
        self,
        prefix: str = "part",
        suffix: str = ".parquet",
    ) -> str:
        """Generate a unique filename.

        Args:
            prefix: Filename prefix
            suffix: Filename suffix

        Returns:
            Unique filename string
        """
        unique_id = uuid.uuid4().hex[:16]
        return f"{prefix}-{unique_id}{suffix}"

    def _clear_parquet_files(self, path: str) -> None:
        """Remove only parquet files from a directory.

        Args:
            path: Directory path
        """
        fs = self.filesystem
        if fs.exists(path) and fs.isdir(path):
            for file_info in fs.find(path, withdirs=False):
                if file_info.endswith(".parquet"):
                    fs.rm(file_info)

    def _list_parquet_files(self, path: str) -> list[str]:
        """List all parquet files in a directory.

        Args:
            path: Directory path

        Returns:
            List of parquet file paths
        """
        from fsspeckit.core.incremental import list_dataset_files

        return list_dataset_files(path, filesystem=self.filesystem)

    def _get_file_row_count(self, file_path: str) -> int:
        """Get row count from a parquet file's metadata.

        Args:
            file_path: Path to parquet file

        Returns:
            Number of rows in the file
        """
        import pyarrow.parquet as pq

        try:
            metadata = pq.read_metadata(file_path, filesystem=self.filesystem)
            return metadata.num_rows
        except Exception as e:
            logger.warning(
                "Failed to read parquet metadata",
                path=file_path,
                error=str(e),
            )
            return 0

    def _get_file_size(self, file_path: str) -> int | None:
        """Get file size in bytes.

        Args:
            file_path: Path to file

        Returns:
            File size in bytes, or None if unavailable
        """
        try:
            size = self.filesystem.size(file_path)
            return int(size) if size is not None else None
        except Exception as e:
            logger.warning(
                "Failed to get file size",
                path=file_path,
                error=str(e),
            )
            return None

    def _dedupe_source_last_wins(
        self,
        table: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        """Deduplicate source table keeping last occurrence per key.

        Args:
            table: Source table to deduplicate
            key_columns: Key columns for deduplication

        Returns:
            Deduplicated table
        """
        import pyarrow as pa

        if table.num_rows == 0:
            return table

        if len(key_columns) == 1:
            keys = table.column(key_columns[0]).to_pylist()
            last_index = {}
            for idx, key in enumerate(keys):
                last_index[key] = idx
            indices = sorted(last_index.values())
        else:
            key_arrays = [table.column(c).to_pylist() for c in key_columns]
            last_index = {}
            for idx, key in enumerate(zip(*key_arrays)):
                last_index[key] = idx
            indices = sorted(last_index.values())

        return table.take(pa.array(indices, type=pa.int64()))

    def _select_rows_by_keys(
        self,
        table: pa.Table,
        key_columns: list[str],
        key_set: set,
    ) -> pa.Table:
        """Select rows from table that match keys in key_set.

        Args:
            table: Source table
            key_columns: Key column names
            key_set: Set of keys to match

        Returns:
            Filtered table
        """
        import pyarrow as pa

        if not key_set:
            return table.slice(0, 0)

        if len(key_columns) == 1:
            key_col = key_columns[0]
            value_list = list(key_set)
            table_keys = table.column(key_col).to_pylist()
            mask = [key in value_list for key in table_keys]
            return table.filter(pa.array(mask, type=pa.bool_()))

        key_list = [tuple(k) if isinstance(k, (list, tuple)) else (k,) for k in key_set]
        arrays = [table.column(c).to_pylist() for c in key_columns]
        table_keys = list(zip(*arrays))

        mask = [key in key_list for key in table_keys]
        return table.filter(pa.array(mask, type=pa.bool_()))

    def _extract_keys_from_table(
        self,
        table: pa.Table,
        key_columns: list[str],
    ) -> set:
        """Extract keys from table as a set.

        Args:
            table: Source table
            key_columns: Key column names

        Returns:
            Set of key values (tuples for multi-column keys)
        """
        if len(key_columns) == 1:
            return set(table.column(key_columns[0]).to_pylist())

        arrays = [table.column(c).to_pylist() for c in key_columns]
        return set(zip(*arrays))

    def _collect_dataset_stats(
        self,
        path: str,
        partition_filter: list[str] | None = None,
    ) -> dict[str, Any]:
        """Collect dataset statistics using shared core logic.

        Args:
            path: Dataset path
            partition_filter: Optional partition filters

        Returns:
            Dictionary with files, total_bytes, total_rows
        """
        from fsspeckit.core.maintenance import collect_dataset_stats

        return collect_dataset_stats(
            path=path,
            filesystem=self.filesystem,
            partition_filter=partition_filter,
        )
