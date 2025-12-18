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
    from fsspeckit.datasets.write_result import WriteDatasetResult

from fsspec import filesystem as fsspec_filesystem

from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _import_pyarrow
from fsspeckit.core.merge import MergeStrategy
from fsspeckit.datasets.exceptions import (
    DatasetFileError,
    DatasetOperationError,
    DatasetPathError,
)
from fsspeckit.datasets.path_utils import normalize_path, validate_dataset_path

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

    def _normalize_path(self, path: str, operation: str = "other") -> str:
        """Normalize path based on filesystem type and validate it."""
        normalized = normalize_path(path, self._filesystem)
        validate_dataset_path(normalized, self._filesystem, operation)
        return normalized

    @property
    def filesystem(self) -> AbstractFileSystem:
        """Return the filesystem instance."""
        return self._filesystem

    def _clear_dataset_parquet_only(self, path: str) -> None:
        if self._filesystem.exists(path) and self._filesystem.isdir(path):
            for file_info in self._filesystem.find(path, withdirs=False):
                if file_info.endswith(".parquet"):
                    self._filesystem.rm(file_info)

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
        _import_pyarrow()
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq

        path = self._normalize_path(path, operation="read")

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

        from fsspeckit.common.security import validate_compression_codec

        path = self._normalize_path(path, operation="write")
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

        from fsspeckit.common.security import validate_compression_codec
        from fsspeckit.datasets.write_result import (
            FileWriteMetadata,
            WriteDatasetResult,
        )

        pa_mod = _import_pyarrow()

        path = self._normalize_path(path, operation="write")
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
                except (IOError, RuntimeError, ValueError) as e:
                    logger.warning(
                        "failed_to_read_metadata",
                        path=wf.path,
                        error=str(e),
                        operation="write_dataset",
                    )
                    row_count = 0

            size_bytes = None
            if wf.size is not None:
                size_bytes = int(wf.size)
            else:
                try:
                    size_bytes = int(self._filesystem.size(wf.path))
                except (IOError, RuntimeError) as e:
                    logger.warning(
                        "failed_to_get_file_size",
                        path=wf.path,
                        error=str(e),
                        operation="write_dataset",
                    )
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
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        from fsspeckit.core.incremental import (
            IncrementalFileManager,
            MergeFileMetadata,
            MergeResult,
            confirm_affected_files,
            extract_source_partition_values,
            list_dataset_files,
            plan_incremental_rewrite,
            validate_no_null_keys,
        )
        from fsspeckit.core.merge import normalize_key_columns
        from fsspeckit.common.security import validate_compression_codec, validate_path

        if isinstance(key_columns, str):
            key_columns = [key_columns]

        if isinstance(partition_columns, str):
            partition_columns = [partition_columns]

        if partition_columns is None:
            partition_columns = []

        key_cols = key_columns

        for col in partition_cols:
            if col not in source_table.column_names:
                raise ValueError(
                    f"Partition column '{col}' not found in source. "
                    f"Available columns: {', '.join(source_table.column_names)}"
                )

        def _dedupe_source_last_wins(table: pa.Table) -> pa.Table:
            if table.num_rows == 0:
                return table

            # Add row index to track original positions
            row_indices = pa.array(range(table.num_rows))
            table_with_index = table.append_column("__row_idx__", row_indices)

            # Group by key columns and get maximum row index (last occurrence)
            grouped = table_with_index.group_by(key_cols).aggregate(
                [("__row_idx__", "max")]
            )

            # Get the indices to keep and sort them
            indices = grouped.column("__row_idx___max")

            # Sort indices to preserve relative order
            sorted_indices = pc.sort_indices(indices)
            result_indices = pc.take(indices, sorted_indices)

            # Take the selected rows and remove the temporary index column
            result = table.take(result_indices)

            return result

        source_table = _dedupe_source_last_wins(source_table)

        # Keep source keys as PyArrow arrays for vectorized operations
        if len(key_cols) == 1:
            # For single column, keep as PyArrow array for vectorized operations
            source_key_array = source_table.column(key_cols[0])
            # Convert to set only when needed for Python set operations
            source_key_set: set[object] = set(source_key_array.to_pylist())
        else:
            # For multi-column keys, keep as list of tuples for now
            # This will be optimized when we process file matching
            arrays = [source_table.column(c).to_pylist() for c in key_cols]
            source_keys = list(zip(*arrays))
            source_key_set = set(source_keys)

        target_files = list_dataset_files(path, filesystem=self._filesystem)
        target_exists = bool(target_files)

        target_count_before = sum(
            pq.read_metadata(f, filesystem=self._filesystem).num_rows
            for f in target_files
        )

        if source_table.num_rows == 0:
            return MergeResult(
                strategy=strategy,
                source_count=0,
                target_count_before=target_count_before,
                target_count_after=target_count_before,
                inserted=0,
                updated=0,
                deleted=0,
                files=[
                    MergeFileMetadata(path=f, row_count=0, operation="preserved")
                    for f in target_files
                ],
                rewritten_files=[],
                inserted_files=[],
                preserved_files=list(target_files),
            )

        if not target_exists:
            if strategy == "update":
                raise ValueError(
                    "UPDATE strategy requires an existing target dataset (non-existent target)"
                )

            self._filesystem.mkdirs(path, exist_ok=True)
            write_res = self.write_dataset(
                source_table,
                path,
                mode="append",
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
            )

            inserted_files = [m.path for m in write_res.files]
            files_meta = [
                MergeFileMetadata(
                    path=m.path,
                    row_count=m.row_count,
                    operation="inserted",
                    size_bytes=m.size_bytes,
                )
                for m in write_res.files
            ]

            return MergeResult(
                strategy=strategy,
                source_count=source_table.num_rows,
                target_count_before=0,
                target_count_after=write_res.total_rows,
                inserted=write_res.total_rows,
                updated=0,
                deleted=0,
                files=files_meta,
                rewritten_files=[],
                inserted_files=inserted_files,
                preserved_files=[],
            )

        def _select_rows_by_keys(table: pa.Table, key_set: set[object]) -> pa.Table:
            if not key_set:
                return table.slice(0, 0)
            if len(key_cols) == 1:
                key_col = key_cols[0]
                value_set = pa_mod.array(
                    list(key_set), type=table.schema.field(key_col).type
                )
                mask = pc.is_in(table.column(key_col), value_set=value_set)
                return table.filter(mask)
            key_list = [tuple(k) for k in key_set]
            key_columns_values = list(zip(*key_list))
            value_arrays = [
                pa_mod.array(list(values), type=table.schema.field(col).type)
                for col, values in zip(key_cols, key_columns_values)
            ]
            keys_table = pa_mod.table(value_arrays, names=key_cols)
            return table.join(
                keys_table,
                keys=key_cols,
                join_type="inner",
                coalesce_keys=True,
            )

        source_partition_values: set[tuple[object, ...]] | None = None
        if partition_cols:
            source_partition_values = extract_source_partition_values(
                source_table, partition_cols
            )

        rewrite_plan = plan_incremental_rewrite(
            dataset_path=path,
            source_keys=source_keys,
            key_columns=key_cols,
            filesystem=self._filesystem,
            partition_columns=partition_cols or None,
            source_partition_values=source_partition_values,
        )

        affected_files = confirm_affected_files(
            candidate_files=rewrite_plan.affected_files,
            key_columns=key_cols,
            source_keys=source_keys,
            filesystem=self._filesystem,
        )

        matched_keys: set[object] = set()
        matched_keys_by_file: dict[str, set[object]] = {}
        for file_path in affected_files:
            try:
                key_table = pq.read_table(
                    file_path, columns=key_cols, filesystem=self._filesystem
                )

                if len(key_cols) == 1:
                    # Use vectorized is_in for single column matching
                    mask = pc.is_in(
                        key_table.column(key_cols[0]), value_set=source_key_array
                    )
                    matched_rows = key_table.filter(mask)
                    if matched_rows.num_rows > 0:
                        # Get the actual matched keys
                        file_matched = set(matched_rows.column(key_cols[0]).to_pylist())
                        matched_keys_by_file[file_path] = set(file_matched)
                        matched_keys |= set(file_matched)
                else:
                    # For multi-column: convert source keys to table and use join
                    source_list = list(source_key_set)
                    source_keys_table = pa.table(
                        {
                            col: [k[i] for k in source_list]
                            for i, col in enumerate(key_cols)
                        }
                    )
                    joined = key_table.join(
                        source_keys_table, keys=key_cols, join_type="inner"
                    )
                    if joined.num_rows > 0:
                        # Extract matched keys as tuples
                        file_matched = set()
                        for i in range(joined.num_rows):
                            key_tuple = tuple(
                                joined.column(col)[i].as_py() for col in key_cols
                            )
                            file_matched.add(key_tuple)
                        matched_keys_by_file[file_path] = set(file_matched)
                        matched_keys |= set(file_matched)
            except (IOError, RuntimeError, ValueError) as e:
                logger.error(
                    "failed_to_check_file_for_matching_keys",
                    path=file_path,
                    error=str(e),
                    operation="merge",
                    exc_info=True,
                )
                # Conservative: if we can't confirm, treat all source keys as matched
                matched_keys_by_file[file_path] = set(source_key_set)
                matched_keys |= set(source_key_set)

        inserted_key_set = source_key_set - matched_keys

        if strategy == "insert":
            preserved_files = list(target_files)
            if not inserted_key_set:
                return MergeResult(
                    strategy="insert",
                    source_count=source_table.num_rows,
                    target_count_before=target_count_before,
                    target_count_after=target_count_before,
                    inserted=0,
                    updated=0,
                    deleted=0,
                    files=[
                        MergeFileMetadata(path=f, row_count=0, operation="preserved")
                        for f in preserved_files
                    ],
                    rewritten_files=[],
                    inserted_files=[],
                    preserved_files=preserved_files,
                )
            insert_table = _select_rows_by_keys(source_table, set(inserted_key_set))
            write_res = self.write_dataset(
                insert_table,
                path,
                mode="append",
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
            )
            inserted_files = [m.path for m in write_res.files]
            inserted_meta = [
                MergeFileMetadata(
                    path=m.path,
                    row_count=m.row_count,
                    operation="inserted",
                    size_bytes=m.size_bytes,
                )
                for m in write_res.files
            ]
            files_meta = [
                MergeFileMetadata(path=f, row_count=0, operation="preserved")
                for f in preserved_files
            ] + inserted_meta
            return MergeResult(
                strategy="insert",
                source_count=source_table.num_rows,
                target_count_before=target_count_before,
                target_count_after=target_count_before + insert_table.num_rows,
                inserted=insert_table.num_rows,
                updated=0,
                deleted=0,
                files=files_meta,
                rewritten_files=[],
                inserted_files=inserted_files,
                preserved_files=preserved_files,
            )

        file_manager = IncrementalFileManager()
        import uuid

        staging_dir = file_manager.create_staging_directory(
            path, filesystem=self._filesystem
        )
        rewritten_files: list[str] = []
        rewritten_meta: list[MergeFileMetadata] = []
        preserved_files = [f for f in target_files if f not in affected_files]

        try:
            for file_path in affected_files:
                file_matched = matched_keys_by_file.get(file_path, set())
                if not file_matched:
                    preserved_files.append(file_path)
                    continue
                target_table = pq.read_table(file_path, filesystem=self._filesystem)
                source_for_file = _select_rows_by_keys(source_table, set(file_matched))

                if strategy == "upsert":
                    updated_table = self._merge_upsert_pyarrow(
                        target_table, source_for_file, key_cols
                    )
                else:
                    updated_table = self._merge_update_pyarrow(
                        target_table, source_for_file, key_cols
                    )

                staging_file = f"{staging_dir}/{uuid.uuid4().hex[:16]}.parquet"
                pq.write_table(
                    updated_table,
                    staging_file,
                    filesystem=self._filesystem,
                    compression=compression,
                    row_group_size=row_group_size,
                )
                size_bytes = None
                try:
                    size_bytes = int(self._filesystem.size(staging_file))
                except Exception:
                    size_bytes = None
                file_manager.atomic_replace_files(
                    [staging_file], [file_path], filesystem=self._filesystem
                )
                rewritten_files.append(file_path)
                rewritten_meta.append(
                    MergeFileMetadata(
                        path=file_path,
                        row_count=updated_table.num_rows,
                        operation="rewritten",
                        size_bytes=size_bytes,
                    )
                )
        finally:
            file_manager.cleanup_staging_files(filesystem=self._filesystem)

        inserted_files: list[str] = []
        inserted_meta: list[MergeFileMetadata] = []
        inserted_rows = 0

        if strategy == "upsert" and inserted_key_set:
            insert_table = _select_rows_by_keys(source_table, set(inserted_key_set))
            inserted_rows = insert_table.num_rows
            write_res = self.write_dataset(
                insert_table,
                path,
                mode="append",
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
            )
            inserted_files = [m.path for m in write_res.files]
            inserted_meta = [
                MergeFileMetadata(
                    path=m.path,
                    row_count=m.row_count,
                    operation="inserted",
                    size_bytes=m.size_bytes,
                )
                for m in write_res.files
            ]

        updated_rows = len(matched_keys)
        files_meta = (
            rewritten_meta
            + inserted_meta
            + [
                MergeFileMetadata(path=f, row_count=0, operation="preserved")
                for f in preserved_files
            ]
        )

        return MergeResult(
            strategy=strategy,
            source_count=source_table.num_rows,
            target_count_before=target_count_before,
            target_count_after=target_count_before + inserted_rows,
            inserted=inserted_rows,
            updated=updated_rows if strategy != "insert" else 0,
            deleted=0,
            files=files_meta,
            rewritten_files=rewritten_files,
            inserted_files=inserted_files,
            preserved_files=preserved_files,
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
        raise NotImplementedError(
            "merge_parquet_dataset has been removed. Use merge(...) for incremental merges."
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

        path = self._normalize_path(path, operation="compact")

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

        path = self._normalize_path(path, operation="optimize")

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

        except Exception as e:
            # If incremental fails, fall back to full merge
            logger.warning(
                "incremental_rewrite_failed_falling_back_to_full_merge",
                error=str(e),
                path=path,
                operation="merge",
            )
            # NOTE: _perform_merge_in_memory is not defined in PyarrowDatasetIO.
            # This is a legacy call that should be replaced with a proper full merge
            # implementation if needed. For now, we preserve the call but add logging.
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
        import pyarrow.compute as pc

        for field in existing.schema:
            if field.name not in source.column_names:
                source = source.append_column(
                    field.name, pa.nulls(len(source), type=field.type)
                )
        source = source.select(existing.schema.names).cast(existing.schema)

        if len(key_columns) == 1:
            key_col = key_columns[0]
            source_keys = source.column(key_col)
            existing_keys = existing.column(key_col)
            mask = pc.invert(pc.is_in(existing_keys, source_keys))
        else:
            source_key_strs = pc.binary_join_element_wise(
                *[pc.cast(source.column(c), pa.string()) for c in key_columns],
                "||",
            )
            existing_key_strs = pc.binary_join_element_wise(
                *[pc.cast(existing.column(c), pa.string()) for c in key_columns],
                "||",
            )
            mask = pc.invert(pc.is_in(existing_key_strs, source_key_strs))

        existing_not_in_source = existing.filter(mask)
        return pa.concat_tables([existing_not_in_source, source])

    def _merge_update_pyarrow(
        self,
        existing: pa.Table,
        source: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        """Perform UPDATE merge using PyArrow operations."""
        pa = _import_pyarrow()
        import pyarrow.compute as pc

        for field in existing.schema:
            if field.name not in source.column_names:
                source = source.append_column(
                    field.name, pa.nulls(len(source), type=field.type)
                )
        source = source.select(existing.schema.names).cast(existing.schema)

        if len(key_columns) == 1:
            key_col = key_columns[0]
            source_keys = source.column(key_col)
            existing_keys = existing.column(key_col)
            existing_not_in_source_mask = pc.invert(
                pc.is_in(existing_keys, source_keys)
            )
            source_in_existing_mask = pc.is_in(source_keys, existing_keys)
        else:
            source_key_strs = pc.binary_join_element_wise(
                *[pc.cast(source.column(c), pa.string()) for c in key_columns],
                "||",
            )
            existing_key_strs = pc.binary_join_element_wise(
                *[pc.cast(existing.column(c), pa.string()) for c in key_columns],
                "||",
            )
            existing_not_in_source_mask = pc.invert(
                pc.is_in(existing_key_strs, source_key_strs)
            )
            source_in_existing_mask = pc.is_in(source_key_strs, existing_key_strs)

        existing_not_in_source = existing.filter(existing_not_in_source_mask)
        source_in_existing = source.filter(source_in_existing_mask)
        return pa.concat_tables([existing_not_in_source, source_in_existing])


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
