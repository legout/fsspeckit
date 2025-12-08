"""PyArrow dataset I/O and maintenance operations.

This module contains the PyarrowDatasetIO class for reading, writing, and
maintaining parquet datasets using PyArrow's high-performance engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa
    from fsspec import AbstractFileSystem
    from fsspeckit.core.merge import MergeStats

from fsspeckit.core.merge import MergeStrategy
from fsspec import filesystem as fsspec_filesystem

from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _PYARROW_AVAILABLE, _import_pyarrow

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

        # Write dataset with merge
        stats = io.write_parquet_dataset(
            table,
            "/path/to/dataset/",
            strategy="upsert",
            key_columns=["id"]
        )
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
        if not _PYARROW_AVAILABLE:
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
        import pyarrow.parquet as pq
        import pyarrow.dataset as ds

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

        from fsspeckit.common.security import validate_path, validate_compression_codec

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
    ) -> MergeStats | None:
        """Write a parquet dataset using PyArrow with optional merge strategies.

        When ``strategy`` is provided, the function performs an in-memory merge
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

        Returns:
            None (merge stats are not returned for PyArrow handler)

        Example:
            ```python
            import pyarrow as pa

            table = pa.table({'id': [1, 2, 3], 'value': ['x', 'y', 'z']})
            io = PyarrowDatasetIO()

            # Standard write
            io.write_parquet_dataset(table, "/tmp/dataset/")

            # Merge-aware write with upsert
            io.write_parquet_dataset(
                table,
                "/tmp/dataset/",
                strategy="upsert",
                key_columns=["id"]
            )
            ```
        """
        from fsspeckit.core.merge import MergeStrategy, validate_strategy_compatibility

        # Normalize path to absolute to avoid filesystem working directory issues
        path = self._normalize_path(path)

        # If no strategy, use standard write
        if strategy is None:
            # Use standard PyArrow dataset writer
            self._write_dataset_standard(
                data=data,
                path=path,
                basename_template=basename_template,
                schema=schema,
                partition_by=partition_by,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
            )
            return None

        # Validate and normalize strategy
        try:
            strategy_enum = MergeStrategy(strategy)
        except ValueError:
            valid_strategies = [s.value for s in MergeStrategy]
            raise ValueError(
                f"Invalid strategy '{strategy}'. Valid strategies: {', '.join(valid_strategies)}"
            )

        # Check if target exists
        target_exists = self._filesystem.exists(path) and any(
            self._filesystem.glob(f"{path}/**/*.parquet")
        )

        # Validate strategy compatibility
        from fsspeckit.common.optional import _import_pyarrow
        pa = _import_pyarrow()
        source_count = data.num_rows if hasattr(data, 'num_rows') else sum(t.num_rows for t in data)

        validate_strategy_compatibility(
            strategy=strategy_enum,
            source_count=source_count,
            target_exists=target_exists,
        )

        # For INSERT/UPSERT without existing target, do a simple write
        if strategy_enum in [MergeStrategy.INSERT, MergeStrategy.UPSERT] and not target_exists:
            logger.info("Target doesn't exist, using simple write for %s", strategy)
            self._write_dataset_standard(
                data=data,
                path=path,
                basename_template=basename_template,
                schema=schema,
                partition_by=partition_by,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
            )
            return None

        # For UPDATE without existing target, raise error
        if strategy_enum == MergeStrategy.UPDATE and not target_exists:
            raise ValueError(
                "UPDATE strategy requires an existing target dataset"
            )

        # For DEDUPLICATE without existing target and no key_columns, just write
        if strategy_enum == MergeStrategy.DEDUPLICATE and not target_exists and not key_columns:
            self._write_dataset_standard(
                data=data,
                path=path,
                basename_template=basename_template,
                schema=schema,
                partition_by=partition_by,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
            )
            return None

        # Perform in-memory merge
        self._perform_merge_in_memory(
            data=data,
            path=path,
            strategy=strategy_enum,
            key_columns=key_columns,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            row_group_size=row_group_size,
            partition_by=partition_by,
        )

        return None

    def _write_dataset_standard(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        basename_template: str | None = None,
        schema: pa.Schema | None = None,
        partition_by: str | list[str] | None = None,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
    ) -> None:
        """Internal: Write dataset using standard PyArrow dataset writer."""
        import pyarrow.dataset as pds

        # Set default basename if not provided
        if basename_template is None:
            basename_template = "part-{i}.parquet"

        # Prepare write options
        write_options = {
            "basename_template": basename_template,
            "max_rows_per_file": max_rows_per_file,
            "max_rows_per_group": row_group_size,
            "existing_data_behavior": "overwrite_or_ignore",
        }

        # Add partition_by if specified
        if partition_by is not None:
            write_options["partitioning"] = partition_by

        # Create file options for compression
        file_options = pds.ParquetFileFormat().make_write_options(
            compression=compression
        )

        # Write dataset
        pds.write_dataset(
            data,
            base_dir=path,
            filesystem=self._filesystem,
            format="parquet",
            file_options=file_options,
            **write_options,
        )

    def _perform_merge_in_memory(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        strategy: MergeStrategy,
        key_columns: list[str] | str | None = None,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
        partition_by: str | list[str] | None = None,
    ) -> None:
        """Internal: Perform merge operation in memory and write result."""
        import pyarrow.dataset as pds
        import tempfile

        pa = _import_pyarrow()

        # Read existing dataset
        try:
            existing_dataset = pds.dataset(
                path,
                filesystem=self._filesystem,
                format="parquet",
            )
            existing_table = existing_dataset.to_table()
        except Exception:
            # Dataset doesn't exist or can't be read
            existing_table = pa.table({})

        # Normalize key columns
        if key_columns is not None:
            if isinstance(key_columns, str):
                key_columns = [key_columns]
        else:
            key_columns = []

        # Combine source data
        if isinstance(data, list):
            source_table = pa.concat_tables(data, promote_options="permissive")
        else:
            source_table = data

        # Perform merge based on strategy
        if strategy == MergeStrategy.INSERT:
            # Only insert new records (not in existing)
            merged_table = self._merge_insert(existing_table, source_table, key_columns)
        elif strategy == MergeStrategy.UPSERT:
            # Insert or update existing
            merged_table = self._merge_upsert(existing_table, source_table, key_columns)
        elif strategy == MergeStrategy.UPDATE:
            # Only update existing (skip new)
            merged_table = self._merge_update(existing_table, source_table, key_columns)
        elif strategy == MergeStrategy.FULL_MERGE:
            # Full replacement with source
            merged_table = source_table
        elif strategy == MergeStrategy.DEDUPLICATE:
            # Remove duplicates
            merged_table = self._merge_deduplicate(existing_table, source_table, key_columns)
        else:
            merged_table = source_table

        # Write result back
        self._write_dataset_standard(
            data=merged_table,
            path=path,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            row_group_size=row_group_size,
            partition_by=partition_by,
        )

    def _merge_insert(
        self,
        existing: pa.Table,
        source: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        """Merge with INSERT strategy: only add new records."""
        pa = _import_pyarrow()

        if not key_columns or existing.num_rows == 0:
            # No keys or no existing data, concatenate
            return pa.concat_tables([existing, source], promote_options="permissive")

        # Find keys that don't exist in existing
        existing_keys = set(tuple(row[col] for col in key_columns) for row in existing.to_pylist())
        new_rows = [row for row in source.to_pylist() if tuple(row[col] for col in key_columns) not in existing_keys]

        if new_rows:
            new_table = pa.Table.from_pylist(new_rows, schema=source.schema)
            return pa.concat_tables([existing, new_table], promote_options="permissive")
        else:
            return existing

    def _merge_upsert(
        self,
        existing: pa.Table,
        source: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        """Merge with UPSERT strategy: insert or update existing records."""
        pa = _import_pyarrow()

        if not key_columns:
            # No keys, just concatenate
            return pa.concat_tables([existing, source], promote_options="permissive")

        # Create lookup dict for existing data
        existing_dict = {}
        for row in existing.to_pylist():
            key = tuple(row[col] for col in key_columns)
            existing_dict[key] = row

        # Build result
        result_rows = []
        source_keys_seen = set()

        # Add existing rows, updating with source rows that match
        for row in existing.to_pylist():
            key = tuple(row[col] for col in key_columns)
            source_keys_seen.add(key)

            # Check if there's a matching source row
            source_row = None
            for s_row in source.to_pylist():
                s_key = tuple(s_row[col] for col in key_columns)
                if s_key == key:
                    source_row = s_row
                    break

            if source_row:
                result_rows.append(source_row)
            else:
                result_rows.append(row)

        # Add source rows with new keys
        for row in source.to_pylist():
            key = tuple(row[col] for col in key_columns)
            if key not in source_keys_seen:
                result_rows.append(row)

        return pa.Table.from_pylist(result_rows, schema=source.schema)

    def _merge_update(
        self,
        existing: pa.Table,
        source: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        """Merge with UPDATE strategy: only update existing records."""
        pa = _import_pyarrow()

        if not key_columns or existing.num_rows == 0:
            # No keys or no existing data, return existing
            return existing

        # Create lookup dict for source data
        source_dict = {}
        for row in source.to_pylist():
            key = tuple(row[col] for col in key_columns)
            source_dict[key] = row

        # Update existing rows with matching source rows
        result_rows = []
        for row in existing.to_pylist():
            key = tuple(row[col] for col in key_columns)
            if key in source_dict:
                # Update with source row
                result_rows.append(source_dict[key])
            else:
                # Keep existing row
                result_rows.append(row)

        return pa.Table.from_pylist(result_rows, schema=existing.schema)

    def _merge_deduplicate(
        self,
        existing: pa.Table,
        source: pa.Table,
        key_columns: list[str] | None,
    ) -> pa.Table:
        """Merge with DEDUPLICATE strategy: remove duplicate records."""
        pa = _import_pyarrow()

        # Combine tables
        combined = pa.concat_tables([existing, source], promote_options="permissive")

        if not key_columns or combined.num_rows == 0:
            return combined

        # Remove duplicates based on key columns
        seen = set()
        unique_rows = []
        for row in combined.to_pylist():
            key = tuple(row[col] for col in key_columns)
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)

        return pa.Table.from_pylist(unique_rows, schema=combined.schema)

    def merge_parquet_dataset(
        self,
        sources: list[str],
        output_path: str,
        target: str | None = None,
        strategy: str | MergeStrategy = "deduplicate",
        key_columns: list[str] | str | None = None,
        compression: str | None = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> MergeStats:
        """Merge multiple parquet datasets using PyArrow.

        Args:
            sources: List of source dataset paths
            output_path: Path for merged output
            target: Target dataset path (for upsert/update strategies)
            strategy: Merge strategy to use (default: deduplicate)
            key_columns: Key columns for merging
            compression: Output compression codec
            verbose: Print progress information
            **kwargs: Additional arguments

        Returns:
            MergeStats with merge statistics

        Example:
            ```python
            io = PyarrowDatasetIO()
            stats = io.merge_parquet_dataset(
                sources=["dataset1/", "dataset2/"],
                output_path="merged/",
                strategy="deduplicate",
                key_columns=["id"],
            )
            ```
        """
        from fsspeckit.datasets.pyarrow.dataset import merge_parquet_dataset_pyarrow

        # Normalize paths to absolute
        sources = [self._normalize_path(s) for s in sources]
        output_path = self._normalize_path(output_path)
        if target is not None:
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
        self.write_parquet_dataset(
            data=data,
            path=path,
            strategy="deduplicate",
            key_columns=key_columns,
            **kwargs,
        )


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

        # Write with merge
        handler.upsert_dataset(
            table,
            "/path/to/dataset/",
            key_columns=["id"]
        )
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
