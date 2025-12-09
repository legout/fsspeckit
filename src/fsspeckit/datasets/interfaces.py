"""Shared interfaces and protocols for dataset handlers.

This module defines the common surface that dataset handlers should implement
to provide a consistent API across different backends (e.g., DuckDB, PyArrow).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeVar

if TYPE_CHECKING:
    import pyarrow as pa

# Type variable for merge strategies
MergeStrategy = Literal["upsert", "insert", "update", "full_merge", "deduplicate"]


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

    def write_parquet_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        *,
        basename_template: str | None = None,
        schema: pa.Schema | None = None,
        partition_by: str | list[str] | None = None,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = None,
        row_group_size: int | None = None,
        strategy: MergeStrategy | None = None,
        key_columns: list[str] | str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Write a parquet dataset with optional merge strategies.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            basename_template: Template for file names
            schema: Optional schema to enforce
            partition_by: Column(s) to partition by
            compression: Compression codec
            max_rows_per_file: Maximum rows per file
            row_group_size: Rows per row group
            strategy: Optional merge strategy:
                - 'insert': Only insert new records
                - 'upsert': Insert or update existing records
                - 'update': Only update existing records
                - 'full_merge': Full replacement with source
                - 'deduplicate': Remove duplicates
            key_columns: Key columns for merge operations (required for relational strategies)
            **kwargs: Additional backend-specific arguments

        Returns:
            Backend-specific result (e.g., MergeStats for merge operations)
        """
        ...

    def merge_parquet_dataset(
        self,
        sources: list[str],
        output_path: str,
        *,
        target: str | None = None,
        strategy: MergeStrategy = "deduplicate",
        key_columns: list[str] | str | None = None,
        compression: str | None = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Merge multiple parquet datasets.

        Args:
            sources: List of source dataset paths
            output_path: Path for merged output
            target: Target dataset path (for upsert/update strategies)
            strategy: Merge strategy to use
            key_columns: Key columns for merging
            compression: Output compression codec
            verbose: Print progress information
            **kwargs: Additional backend-specific arguments

        Returns:
            Backend-specific result containing merge statistics
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
            Dictionary containing optimization statistics and metadata
        """
        ...
