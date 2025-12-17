"""Shared interfaces and protocols for dataset handlers.

This module defines the common surface that dataset handlers should implement
to provide a consistent API across different backends (e.g., DuckDB, PyArrow).

This project intentionally favors explicit, minimal write/merge APIs:
- `write_dataset(..., mode="append"|"overwrite")`
- `merge(..., strategy="insert"|"update"|"upsert")`
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

    from fsspeckit.core.incremental import MergeResult
    from fsspeckit.datasets.write_result import WriteDatasetResult

WriteMode = Literal["append", "overwrite"]
MergeStrategy = Literal["insert", "update", "upsert"]


class DatasetHandler(Protocol):
    """Protocol defining the shared dataset handler interface.

    This protocol describes the common surface for dataset handlers across
    different backends. It provides consistent method names and parameters
    while allowing backend-specific extensions.

    Note:
        This is a structural protocol - implementations don't need to explicitly
        inherit from it. They just need to implement the methods with compatible
        signatures.
    """

    def write_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        *,
        mode: WriteMode = "append",
        compression: str | None = "snappy",
        max_rows_per_file: int | None = None,
        row_group_size: int | None = None,
        **kwargs: Any,
    ) -> WriteDatasetResult:
        """Write a parquet dataset and return per-file metadata.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            mode: Write mode ('append' or 'overwrite')
            compression: Compression codec
            max_rows_per_file: Maximum rows per file
            row_group_size: Rows per row group
            **kwargs: Additional backend-specific arguments

        Returns:
            WriteDatasetResult
        """
        ...

    def merge(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        strategy: MergeStrategy,
        key_columns: list[str] | str,
        *,
        partition_columns: list[str] | str | None = None,
        schema: pa.Schema | None = None,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = None,
        row_group_size: int | None = None,
        **kwargs: Any,
    ) -> MergeResult:
        """Merge data into an existing parquet dataset incrementally.

        Args:
            data: Source data to merge.
            path: Dataset directory.
            strategy: Merge strategy ('insert', 'update', 'upsert').
            key_columns: Key columns for matching.
            partition_columns: Columns that must not change for existing keys.
            schema: Optional schema to enforce for newly written files.
            compression: Output compression codec.
            max_rows_per_file: Max rows per newly written file.
            row_group_size: Parquet row group size for newly written files.
            **kwargs: Additional backend-specific arguments

        Returns:
            MergeResult
        """
        ...

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
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Compact a parquet dataset by combining small files.

        Args:
            path: Dataset path
            target_mb_per_file: Target size per file in MB
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec for output
            dry_run: Whether to perform a dry run (return plan without executing)
            verbose: Print progress information
            **kwargs: Additional backend-specific arguments

        Returns:
            Dictionary containing compaction statistics and metadata
        """
        ...

    def optimize_parquet_dataset(
        self,
        path: str,
        *,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Optimize a parquet dataset through compaction and maintenance.

        Args:
            path: Dataset path
            target_mb_per_file: Target size per file in MB
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec for output
            verbose: Print progress information
            **kwargs: Additional backend-specific arguments

        Returns:
            Dictionary containing optimization statistics
        """
        ...
