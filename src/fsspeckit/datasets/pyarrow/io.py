"""PyArrow dataset I/O and maintenance operations.

This module contains the PyarrowDatasetIO class for reading, writing, and
maintaining parquet datasets using PyArrow's high-performance engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pyarrow as pa
    from fsspec import AbstractFileSystem

    from fsspeckit.core.incremental import MergeResult
    from fsspeckit.core.merge import MergeStats

from fsspec import filesystem as fsspec_filesystem

from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _import_pyarrow
from fsspeckit.core.merge import MergeStrategy

logger = get_logger(__name__)


class PyarrowDatasetIO:
    """PyArrow-based dataset I/O operations.

    This class provides methods for reading and writing parquet files and datasets
    using PyArrow's high-performance parquet engine.

    The class delegates to existing PyArrow functions while providing an interface
    symmetric with DuckDBDatasetIO for easy backend switching.

    Args:
        filesystem: Optional fsspec filesystem instance. If None, uses local filesystem.

    Example:
        ```python
        from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

        io = PyarrowDatasetIO()

        # Read parquet
        table = io.read_parquet("/path/to/data.parquet")

        # Write dataset
        io.write_dataset(table, "/path/to/dataset/", mode="append")

        # Merge into dataset
        io.merge(table, "/path/to/dataset/", strategy="upsert", key_columns=["id"])
        ```
    """

    def __init__(
        self,
        filesystem: AbstractFileSystem | None = None,
    ) -> None:
        """Initialize PyArrow dataset I/O.

        Args:
            filesystem: Optional fsspec filesystem. If None, uses local filesystem.
        """
        from fsspeckit.common import optional as optional_module

        if not optional_module._PYARROW_AVAILABLE:
            raise ImportError(
                "pyarrow is required for PyarrowDatasetIO. "
                "Install with: pip install fsspeckit[datasets]"
            )

        if filesystem is None:
            filesystem = fsspec_filesystem("file")

        self._filesystem = filesystem

    def _normalize_path(self, path: str) -> str:
        """Normalize path to absolute path to avoid filesystem working directory issues."""
        import os

        return os.path.abspath(path)

    @property
    def filesystem(self) -> AbstractFileSystem:
        """Return the filesystem instance."""
        return self._filesystem

    def read_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
        filters: Any | None = None,
        use_threads: bool = True,
    ) -> pa.Table:
        """Read parquet file(s) using PyArrow.

        Args:
            path: Path to parquet file or directory
            columns: Optional list of columns to read
            filters: Optional row filter expression
            use_threads: Whether to use parallel reading (default: True)

        Returns:
            PyArrow table containing the data

        Example:
            ```python
            io = PyarrowDatasetIO()
            table = io.read_parquet("/path/to/file.parquet")

            # With column selection
            table = io.read_parquet(
                "/path/to/data/",
                columns=["id", "name", "value"]
            )
            ```
        """
        pa = _import_pyarrow()
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq

        from fsspeckit.common.security import validate_path

        path = self._normalize_path(path)
        validate_path(path)

        # Check if path is a single file or directory
        if self._filesystem.isfile(path):
            return pq.read_table(
                path,
                filesystem=self._filesystem,
                columns=columns,
                filters=filters,
                use_threads=use_threads,
            )
        else:
            # Dataset directory
            dataset = ds.dataset(
                path,
                filesystem=self._filesystem,
                format="parquet",
            )
            return dataset.to_table(
                columns=columns,
                filter=filters,
            )

    def write_parquet(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        compression: str | None = "snappy",
        row_group_size: int | None = None,
    ) -> None:
        """Write parquet file using PyArrow.

        Args:
            data: PyArrow table or list of tables to write
            path: Output file path
            compression: Compression codec to use (default: snappy)
            row_group_size: Rows per row group

        Example:
            ```python
            import pyarrow as pa

            table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
            io = PyarrowDatasetIO()
            io.write_parquet(table, "/tmp/data.parquet")
            ```
        """
        pa_mod = _import_pyarrow()
        import pyarrow.parquet as pq

        from fsspeckit.common.security import validate_compression_codec, validate_path

        path = self._normalize_path(path)
        validate_path(path)
        validate_compression_codec(compression)

        # Handle list of tables
        if isinstance(data, list):
            data = pa_mod.concat_tables(data, promote_options="permissive")

        pq.write_table(
            data,
            path,
            filesystem=self._filesystem,
            compression=compression,
            row_group_size=row_group_size,
        )

    def write_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        *,
        mode: Literal["append", "overwrite"] = "append",
        basename_template: str | None = None,
        schema: pa.Schema | None = None,
        partition_by: str | list[str] | None = None,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
    ) -> "WriteDatasetResult":
        """Write a parquet dataset and return per-file metadata."""
        import uuid

        import pyarrow.dataset as pds
        import pyarrow.parquet as pq

        from fsspeckit.common.security import validate_compression_codec, validate_path
        from fsspeckit.datasets.write_result import (
            FileWriteMetadata,
            WriteDatasetResult,
        )

        pa_mod = _import_pyarrow()

        path = self._normalize_path(path)
        validate_path(path)
        validate_compression_codec(compression)

        if mode not in ("append", "overwrite"):
            raise ValueError(f"mode must be 'append' or 'overwrite', got: {mode}")
        if max_rows_per_file is not None and max_rows_per_file <= 0:
            raise ValueError("max_rows_per_file must be > 0")
        if row_group_size is not None and row_group_size <= 0:
            raise ValueError("row_group_size must be > 0")
        if (
            max_rows_per_file is not None
            and row_group_size is not None
            and row_group_size > max_rows_per_file
        ):
            row_group_size = max_rows_per_file

        # Combine list input.
        if isinstance(data, list):
            table = pa_mod.concat_tables(data, promote_options="permissive")
        else:
            table = data

        if schema is not None:
            from fsspeckit.common.schema import cast_schema

            table = cast_schema(table, schema)

        # Ensure dataset directory exists.
        self._filesystem.mkdirs(path, exist_ok=True)

        if mode == "overwrite":
            self._clear_dataset_parquet_only(path)

        if mode == "append" and (
            basename_template is None or basename_template == "part-{i}.parquet"
        ):
            unique_id = uuid.uuid4().hex[:16]
            basename_template = f"part-{unique_id}-{{i}}.parquet"

        if basename_template is None:
            basename_template = "part-{i}.parquet"

        written: list[pds.WrittenFile] = []
        file_options = pds.ParquetFileFormat().make_write_options(
            compression=compression
        )

        write_options: dict[str, object] = {
            "basename_template": basename_template,
            "max_rows_per_file": max_rows_per_file,
            "max_rows_per_group": row_group_size,
            "existing_data_behavior": "overwrite_or_ignore",
        }
        if partition_by is not None:
            write_options["partitioning"] = partition_by

        pds.write_dataset(
            table,
            base_dir=path,
            filesystem=self._filesystem,
            format="parquet",
            file_options=file_options,
            file_visitor=written.append,
            **write_options,
        )

        files: list[FileWriteMetadata] = []
        for wf in written:
            row_count = 0
            if wf.metadata is not None:
                row_count = int(wf.metadata.num_rows)
            else:
                try:
                    row_count = int(
                        pq.read_metadata(wf.path, filesystem=self._filesystem).num_rows
                    )
                except Exception:
                    row_count = 0

            size_bytes = None
            if wf.size is not None:
                size_bytes = int(wf.size)
            else:
                try:
                    size_bytes = int(self._filesystem.size(wf.path))
                except Exception:
                    size_bytes = None

            files.append(
                FileWriteMetadata(
                    path=wf.path,
                    row_count=row_count,
                    size_bytes=size_bytes,
                )
            )

        return WriteDatasetResult(
            files=files,
            total_rows=sum(f.row_count for f in files),
            mode=mode,
            backend="pyarrow",
        )

    def write_parquet_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        basename_template: str | None = None,
        schema: pa.Schema | None = None,
        partition_by: str | list[str] | None = None,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
        strategy: str | None = None,
        key_columns: list[str] | str | None = None,
        mode: Literal["append", "overwrite"] | None = "append",
        rewrite_mode: Literal["full", "incremental"] | None = "full",
    ) -> MergeStats | None:
        """Write a parquet dataset using PyArrow with optional merge strategies.

        When strategy is provided, the function performs an in-memory merge
        and writes the result back to the dataset.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            basename_template: Template for file names (default: part-{i}.parquet)
            schema: Optional schema to enforce
            partition_by: Column(s) to partition by
            compression: Compression codec (default: snappy)
            max_rows_per_file: Maximum rows per file (default: 5,000,000)
            row_group_size: Rows per row group (default: 500,000)
            strategy: Optional merge strategy:
                - 'insert': Only insert new records
                - 'upsert': Insert or update existing records
                - 'update': Only update existing records
                - 'full_merge': Full replacement with source
                - 'deduplicate': Remove duplicates
            key_columns: Key columns for merge operations (required for relational strategies)
            mode: Write mode:
                - 'append': Add new files without deleting existing ones (default, safer)
                - 'overwrite': Replace existing parquet files with new ones
            rewrite_mode: Rewrite mode for merge strategies:
                - 'full': Rewrite entire dataset (default, backward compatible)
                - 'incremental': Only rewrite files affected by merge (requires strategy in {'upsert', 'update'})

        Returns:
            None (merge stats are not returned for PyArrow handler)

        Note:
            **Mode/Strategy Precedence**: When both mode and strategy are provided, strategy takes precedence
            and mode is ignored. A warning is emitted when mode is explicitly provided alongside strategy.
            For merge operations, the strategy semantics control the behavior regardless of the mode setting.
        """
        raise NotImplementedError(
            "write_parquet_dataset has been removed. Use write_dataset(...) for append/overwrite writes "
            "or merge(...) for insert/update/upsert."
        )
            target = self._normalize_path(target)

        return merge_parquet_dataset_pyarrow(
            sources=sources,
            output_path=output_path,
            target=target,
            strategy=strategy,
            key_columns=key_columns,
            filesystem=self._filesystem,
            compression=compression,
            verbose=verbose,
            **kwargs,
        )

    def compact_parquet_dataset(
        self,
        path: str,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Compact a parquet dataset using PyArrow.

        Args:
            path: Dataset path
            target_mb_per_file: Target size per file in MB
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec
            dry_run: Whether to perform a dry run
            verbose: Print progress information

        Returns:
            Compaction statistics

        Example:
            ```python
            io = PyarrowDatasetIO()
            stats = io.compact_parquet_dataset(
                "/path/to/dataset/",
                target_mb_per_file=64,
                dry_run=True,
            )
            print(f"Files before: {stats['before_file_count']}")
            ```
        """
        from fsspeckit.datasets.pyarrow.dataset import compact_parquet_dataset_pyarrow

        path = self._normalize_path(path)

        return compact_parquet_dataset_pyarrow(
            path=path,
            target_mb_per_file=target_mb_per_file,
            target_rows_per_file=target_rows_per_file,
            partition_filter=partition_filter,
            compression=compression,
            dry_run=dry_run,
            filesystem=self._filesystem,
        )

    def optimize_parquet_dataset(
        self,
        path: str,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Optimize a parquet dataset.

        Args:
            path: Dataset path
            target_mb_per_file: Target size per file in MB
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec
            verbose: Print progress information

        Returns:
            Optimization statistics

        Example:
            ```python
            io = PyarrowDatasetIO()
            stats = io.optimize_parquet_dataset(
                "dataset/",
                target_mb_per_file=64,
                compression="zstd",
            )
            ```
        """
        from fsspeckit.datasets.pyarrow.dataset import optimize_parquet_dataset_pyarrow

        path = self._normalize_path(path)

        return optimize_parquet_dataset_pyarrow(
            path=path,
            target_mb_per_file=target_mb_per_file,
            target_rows_per_file=target_rows_per_file,
            partition_filter=partition_filter,
            compression=compression,
            filesystem=self._filesystem,
            verbose=verbose,
        )

    def insert_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        key_columns: list[str] | str,
        **kwargs: Any,
    ) -> None:
        """Insert-only dataset write.

        Convenience method that calls write_parquet_dataset with strategy='insert'.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            key_columns: Key columns for merge (required)
            **kwargs: Additional arguments passed to write_parquet_dataset

        Raises:
            ValueError: If key_columns is not provided
        """
        raise NotImplementedError(
            "insert_dataset has been removed. Use merge(..., strategy='insert') instead."
        )
        if not key_columns:
            raise ValueError("key_columns is required for insert_dataset")

        self.write_parquet_dataset(
            data=data,
            path=path,
            strategy="insert",
            key_columns=key_columns,
            **kwargs,
        )

    def upsert_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        key_columns: list[str] | str,
        **kwargs: Any,
    ) -> None:
        """Insert-or-update dataset write.

        Convenience method that calls write_parquet_dataset with strategy='upsert'.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            key_columns: Key columns for merge (required)
            **kwargs: Additional arguments passed to write_parquet_dataset

        Raises:
            ValueError: If key_columns is not provided
        """
        raise NotImplementedError(
            "upsert_dataset has been removed. Use merge(..., strategy='upsert') instead."
        )
        if not key_columns:
            raise ValueError("key_columns is required for upsert_dataset")

        self.write_parquet_dataset(
            data=data,
            path=path,
            strategy="upsert",
            key_columns=key_columns,
            **kwargs,
        )

    def update_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        key_columns: list[str] | str,
        **kwargs: Any,
    ) -> None:
        """Update-only dataset write.

        Convenience method that calls write_parquet_dataset with strategy='update'.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            key_columns: Key columns for merge (required)
            **kwargs: Additional arguments passed to write_parquet_dataset

        Raises:
            ValueError: If key_columns is not provided
        """
        raise NotImplementedError(
            "update_dataset has been removed. Use merge(..., strategy='update') instead."
        )
        if not key_columns:
            raise ValueError("key_columns is required for update_dataset")

        self.write_parquet_dataset(
            data=data,
            path=path,
            strategy="update",
            key_columns=key_columns,
            **kwargs,
        )

    def deduplicate_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        key_columns: list[str] | str | None = None,
        **kwargs: Any,
    ) -> None:
        """Deduplicate dataset write.

        Convenience method that calls write_parquet_dataset with strategy='deduplicate'.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            key_columns: Key columns for deduplication (optional)
            **kwargs: Additional arguments passed to write_parquet_dataset
        """
        raise NotImplementedError(
            "deduplicate_dataset has been removed. Use a dedicated dataset maintenance API instead."
        )
        self.write_parquet_dataset(
            data=data,
            path=path,
            strategy="deduplicate",
            key_columns=key_columns,
            **kwargs,
        )

    def _write_parquet_dataset_incremental(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        strategy: MergeStrategy,
        key_columns: list[str] | str,
        basename_template: str | None = None,
        schema: pa.Schema | None = None,
        partition_by: str | list[str] | None = None,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
    ) -> None:
        """Internal: Incremental rewrite for UPSERT/UPDATE strategies using PyArrow.

        Only rewrites files that might contain the keys being updated,
        preserving other files unchanged.

        Args:
            data: Source data to merge
            path: Target dataset path
            strategy: Merge strategy (UPSERT or UPDATE)
            key_columns: Key columns for matching
            basename_template: Template for file names
            schema: Optional schema to enforce
            partition_by: Column(s) to partition by
            compression: Compression codec
            max_rows_per_file: Maximum rows per file
            row_group_size: Rows per row group
        """
        import pyarrow.dataset as pds

        from fsspeckit.core.incremental import plan_incremental_rewrite

        # Convert data to single table if it's a list
        if isinstance(data, list):
            combined_data = pa.concat_tables(data, promote_options="permissive")
        else:
            combined_data = data

        # Extract source keys for planning
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        # For now, use a simplified approach that reads all data
        # In a full implementation, this would use metadata analysis
        logger.info("Using PyArrow incremental rewrite (simplified implementation)")

        # Read existing dataset
        try:
            existing_dataset = pds.dataset(
                path,
                filesystem=self._filesystem,
                format="parquet",
            )
            existing_table = existing_dataset.to_table()

            # Apply merge semantics
            if strategy == MergeStrategy.UPSERT:
                merged_table = self._merge_upsert_pyarrow(
                    existing_table, combined_data, key_columns
                )
            else:  # UPDATE
                merged_table = self._merge_update_pyarrow(
                    existing_table, combined_data, key_columns
                )

            # Write result using standard writer
            self._write_dataset_standard(
                data=merged_table,
                path=path,
                basename_template=basename_template,
                schema=schema,
                partition_by=partition_by,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
                mode="overwrite",
            )

        except Exception:
            # If incremental fails, fall back to full merge
            logger.warning("Incremental rewrite failed, falling back to full merge")
            self._perform_merge_in_memory(
                data=data,
                path=path,
                strategy=strategy,
                key_columns=key_columns,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
                partition_by=partition_by,
            )

    def _merge_upsert_pyarrow(
        self,
        existing: pa.Table,
        source: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        """Perform UPSERT merge using PyArrow operations."""
        pa = _import_pyarrow()

        # Create lookup dictionary from source
        source_lookup = {}
        for row in source.to_pylist():
            key = tuple(row[col] for col in key_columns)
            source_lookup[key] = row

        # Merge with existing data
        result_rows = []
        for row in existing.to_pylist():
            key = tuple(row[col] for col in key_columns)
            if key in source_lookup:
                # Update with source data
                result_rows.append(source_lookup[key])
            else:
                # Keep existing row
                result_rows.append(row)

        # Add new rows from source that don't exist in existing
        existing_keys = {
            tuple(row[col] for col in key_columns) for row in existing.to_pylist()
        }
        for row in source.to_pylist():
            key = tuple(row[col] for col in key_columns)
            if key not in existing_keys:
                result_rows.append(row)

        return pa.Table.from_pylist(result_rows, schema=existing.schema)

    def _merge_update_pyarrow(
        self,
        existing: pa.Table,
        source: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        """Perform UPDATE merge using PyArrow operations."""
        pa = _import_pyarrow()

        # Create lookup dictionary from source
        source_lookup = {}
        for row in source.to_pylist():
            key = tuple(row[col] for col in key_columns)
            source_lookup[key] = row

        # Update existing rows with source data
        result_rows = []
        for row in existing.to_pylist():
            key = tuple(row[col] for col in key_columns)
            if key in source_lookup:
                # Update with source data
                result_rows.append(source_lookup[key])
            else:
                # Keep existing row unchanged
                result_rows.append(row)

        return pa.Table.from_pylist(result_rows, schema=existing.schema)


class PyarrowDatasetHandler(PyarrowDatasetIO):
    """Convenience wrapper for PyArrow dataset operations.

    This class provides a familiar interface for users coming from DuckDBParquetHandler.
    It inherits all methods from PyarrowDatasetIO.

    Example:
        ```python
        from fsspeckit.datasets import PyarrowDatasetHandler

        handler = PyarrowDatasetHandler()

        # Read parquet
        table = handler.read_parquet("/path/to/data.parquet")

        # Merge into dataset
        handler.merge(table, "/path/to/dataset/", strategy="upsert", key_columns=["id"])
        ```
    """

    def __init__(
        self,
        filesystem: AbstractFileSystem | None = None,
    ) -> None:
        """Initialize PyArrow dataset handler.

        Args:
            filesystem: Optional fsspec filesystem instance
        """
        super().__init__(filesystem=filesystem)

    def __enter__(self) -> "PyarrowDatasetHandler":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager (no-op for PyArrow, kept for API symmetry)."""
        pass
