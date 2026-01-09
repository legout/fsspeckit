"""PyArrow dataset I/O and maintenance operations.

This module contains the PyarrowDatasetIO class for reading, writing, and
maintaining parquet datasets using PyArrow's high-performance engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Callable, Iterable

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.dataset as ds
    from fsspec import AbstractFileSystem

    from fsspeckit.core.incremental import MergeResult
    from fsspeckit.core.merge import MergeStats
    from fsspeckit.datasets.pyarrow.memory import MemoryMonitor
    from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker
    from fsspeckit.datasets.write_result import WriteDatasetResult

from fsspec import filesystem as fsspec_filesystem

from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _import_pyarrow
from fsspeckit.core.merge import MergeStats, MergeStrategy
from fsspeckit.datasets.base import BaseDatasetHandler
from fsspeckit.datasets.exceptions import (
    DatasetFileError,
    DatasetOperationError,
    DatasetPathError,
)
from fsspeckit.datasets.path_utils import normalize_path, validate_dataset_path

logger = get_logger(__name__)


class PyarrowDatasetIO(BaseDatasetHandler):
    """PyArrow-based dataset I/O operations.

    This class provides methods for reading and writing parquet files and datasets
    using PyArrow's high-performance parquet engine.

    The class inherits from BaseDatasetHandler to leverage shared implementations
    while providing PyArrow-specific optimizations.

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

    @property
    def filesystem(self) -> AbstractFileSystem:
        """Return the filesystem instance."""
        return self._filesystem

    def _normalize_path(self, path: str, operation: str = "other") -> str:
        """Normalize path based on filesystem type and validate it."""
        normalized = normalize_path(path, self._filesystem)
        validate_dataset_path(normalized, self._filesystem, operation)
        return normalized

    def _clear_dataset_parquet_only(self, path: str) -> None:
        if self._filesystem.exists(path) and self._filesystem.isdir(path):
            for file_info in self._filesystem.find(path, withdirs=False):
                if file_info.endswith(".parquet"):
                    self._filesystem.rm(file_info)

    def _normalize_filters(
        self,
        filters: Any,
        path: str,
    ) -> Any:
        """Normalize filters parameter to PyArrow-compatible format.

        Converts SQL-like string filters to PyArrow compute expressions
        for API alignment with DuckDB backend. Passes through native
        PyArrow expressions and DNF tuples unchanged.

        Args:
            filters: Filter specification (SQL string, PyArrow expression, or DNF tuples)
            path: Path to dataset/file (used for schema resolution)

        Returns:
            Normalized filter suitable for PyArrow API

        Raises:
            ValueError: If SQL string parsing fails
        """
        if filters is None:
            return None
        if not isinstance(filters, str):
            return filters

        import pyarrow.parquet as pq
        import pyarrow.dataset as ds

        if self._filesystem.isfile(path):
            schema = pq.read_schema(path, filesystem=self._filesystem)
        else:
            dataset = ds.dataset(
                path,
                filesystem=self._filesystem,
                format="parquet",
            )
            schema = dataset.schema

        from fsspeckit.sql.filters import sql2pyarrow_filter

        return sql2pyarrow_filter(filters, schema)

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
            filters: Optional row filter expression. Accepts PyArrow compute expressions,
                DNF tuples, or SQL-like strings (converted to PyArrow expressions).
                Note: DuckDB backend only accepts SQL WHERE clause strings.. Accepts PyArrow compute expressions,
                DNF tuples, or SQL-like strings (converted to PyArrow expressions).
                Note: DuckDB backend only accepts SQL WHERE clause strings.
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
        merge_chunk_size_rows: int = 100_000,
        enable_streaming_merge: bool = True,
        merge_max_memory_mb: int = 1024,
        merge_max_process_memory_mb: int | None = None,
        merge_min_system_available_mb: int = 512,
        merge_progress_callback: Callable[[int, int], None] | None = None,
    ) -> MergeResult:
        """Merge data into an existing parquet dataset.

        This method performs an incremental merge (insert, update, or upsert)
        of the provided data into the target dataset. It uses PyArrow's
        high-performance operations and supports both in-memory and
        streaming merge strategies.

        Args:
            data: PyArrow table or list of tables to merge
            path: Target dataset path
            strategy: Merge strategy ('insert', 'update', or 'upsert')
            key_columns: Column(s) used as unique identifiers
            partition_columns: Optional column(s) to partition by
            schema: Optional schema to enforce on the source data
            compression: Compression codec (default: snappy)
            max_rows_per_file: Max rows per file in output (default: 5,000,000)
            row_group_size: Rows per row group (default: 500_000)
            merge_chunk_size_rows: Rows per processing chunk (default: 100_000)
            enable_streaming_merge: Whether to use streaming merge (default: True)
            merge_max_memory_mb: Max PyArrow memory in MB (default: 1024)
            merge_max_process_memory_mb: Optional max process RSS in MB
            merge_min_system_available_mb: Min system available memory in MB (default: 512)
            merge_progress_callback: Optional callback for progress updates

        Returns:
            MergeResult with detailed statistics
        """
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
        import pyarrow.dataset as ds

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
        from fsspeckit.datasets.pyarrow.dataset import (
            PerformanceMonitor,
            _create_composite_key_array,
            _create_fallback_key_array,
            _filter_by_key_membership,
            _make_struct_safe,
            _table_drop_duplicates,
            process_in_chunks,
        )
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        monitor = PerformanceMonitor(
            max_pyarrow_mb=merge_max_memory_mb,
            max_process_memory_mb=merge_max_process_memory_mb,
            min_system_available_mb=merge_min_system_available_mb,
        )
        monitor.start_op("initialization")

        pa_mod = _import_pyarrow()

        if isinstance(key_columns, str):
            key_columns = [key_columns]

        if isinstance(partition_columns, str):
            partition_columns = [partition_columns]

        partition_cols = partition_columns or []
        key_cols = key_columns

        # Convert data to source_table
        if isinstance(data, list):
            source_table = pa_mod.concat_tables(data, promote_options="permissive")
        else:
            source_table = data

        if schema is not None:
            from fsspeckit.common.schema import cast_schema

            source_table = cast_schema(source_table, schema)

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

        monitor.start_op("source_deduplication")
        source_table = _dedupe_source_last_wins(source_table)
        monitor.end_op()

        # Validate no null keys in source
        validate_no_null_keys(source_table, key_cols)

        # Keep source keys as PyArrow Table/Array for vectorized operations
        source_key_table = source_table.select(key_cols)
        # Deduplicate keys
        source_key_table = _table_drop_duplicates(source_key_table, subset=key_cols)

        source_key_tracker = AdaptiveKeyTracker()
        if len(key_cols) == 1:
            # For single column, keep as PyArrow array for vectorized operations
            source_key_array = source_key_table.column(0)
            for key in source_key_array.to_pylist():
                source_key_tracker.add(key)
        else:
            # For multi-column keys, use vectorized conversion
            source_key_array = None
            for d in _make_struct_safe(source_key_table, key_cols).to_pylist():
                source_key_tracker.add(tuple(d.values()))

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

        def _select_rows_by_keys(
            table: pa.Table, keys: set[object] | AdaptiveKeyTracker
        ) -> pa.Table:
            is_tracker = isinstance(keys, AdaptiveKeyTracker)
            if is_tracker:
                if keys.get_metrics()["unique_keys_estimate"] == 0:
                    return table.slice(0, 0)
            elif not keys:
                return table.slice(0, 0)

            if is_tracker:
                # If tracker has exact keys, use them for efficiency
                if (
                    hasattr(keys, "_tier")
                    and keys._tier == "EXACT"
                    and keys._exact_keys is not None
                ):
                    key_set = keys._exact_keys
                else:
                    # Probabilistic or LRU: must filter manually
                    mask = []
                    if len(key_cols) == 1:
                        col_values = table.column(key_cols[0]).to_pylist()
                        for val in col_values:
                            mask.append(val in keys)
                    else:
                        struct_keys = _make_struct_safe(table, key_cols).to_pylist()
                        for d in struct_keys:
                            mask.append(tuple(d.values()) in keys)
                    return table.filter(pa.array(mask))
            else:
                key_set = keys

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

        # Prepare keys for planning (using lists for now as the planning utilities expect sequences)
        # TODO: Update planning utilities to support trackers or PyArrow tables directly
        if len(key_cols) == 1:
            source_keys_seq = source_key_table.column(0).to_pylist()
        else:
            source_keys_seq = [
                tuple(d.values())
                for d in _make_struct_safe(source_key_table, key_cols).to_pylist()
            ]

        rewrite_plan = plan_incremental_rewrite(
            dataset_path=path,
            source_keys=source_keys_seq,
            key_columns=key_cols,
            filesystem=self._filesystem,
            partition_columns=partition_cols or None,
            source_partition_values=source_partition_values,
        )

        affected_files = confirm_affected_files(
            candidate_files=rewrite_plan.affected_files,
            key_columns=key_cols,
            source_keys=source_keys_seq,
            filesystem=self._filesystem,
        )

        matched_keys = AdaptiveKeyTracker()
        matched_keys_by_file: dict[str, AdaptiveKeyTracker] = {}
        for file_path in affected_files:
            try:
                key_table = pq.read_table(
                    file_path, columns=key_cols, filesystem=self._filesystem
                )

                # Use vectorized matching helper
                matched_rows = _filter_by_key_membership(
                    key_table, key_cols, source_key_table, keep_matches=True
                )
                if matched_rows.num_rows > 0:
                    file_matched = AdaptiveKeyTracker()
                    if len(key_cols) == 1:
                        # Get the actual matched keys
                        for key in matched_rows.column(0).to_pylist():
                            file_matched.add(key)
                            matched_keys.add(key)
                    else:
                        # Use struct array for vectorized multi-key extraction
                        for d in _make_struct_safe(matched_rows, key_cols).to_pylist():
                            key = tuple(d.values())
                            file_matched.add(key)
                            matched_keys.add(key)
                    matched_keys_by_file[file_path] = file_matched
            except (IOError, RuntimeError, ValueError) as e:
                logger.error(
                    "failed_to_check_file_for_matching_keys",
                    path=file_path,
                    error=str(e),
                    operation="merge",
                    exc_info=True,
                )
                # Conservative: if we can't confirm, treat all source keys as matched
                matched_keys_by_file[file_path] = source_key_tracker
                if len(key_cols) == 1:
                    for key in source_key_array.to_pylist():
                        matched_keys.add(key)
                else:
                    for d in _make_struct_safe(source_key_table, key_cols).to_pylist():
                        matched_keys.add(tuple(d.values()))

        # Calculate inserted keys using trackers
        inserted_key_tracker = AdaptiveKeyTracker()
        if len(key_cols) == 1:
            for key in source_key_array.to_pylist():
                if key not in matched_keys:
                    inserted_key_tracker.add(key)
        else:
            for d in _make_struct_safe(source_key_table, key_cols).to_pylist():
                key = tuple(d.values())
                if key not in matched_keys:
                    inserted_key_tracker.add(key)

        if strategy == "insert":
            preserved_files = list(target_files)
            if inserted_key_tracker.get_metrics()["unique_keys_estimate"] == 0:
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
            insert_table = _select_rows_by_keys(source_table, inserted_key_tracker)
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
                monitor.start_op("file_processing")
                file_matched = matched_keys_by_file.get(file_path)
                if not file_matched:
                    preserved_files.append(file_path)
                    monitor.end_op()
                    continue

                # Load only source rows relevant to this file
                source_for_file = _select_rows_by_keys(source_table, file_matched)
                staging_file = f"{staging_dir}/{uuid.uuid4().hex[:16]}.parquet"

                if enable_streaming_merge:
                    # Use streaming merge for this file
                    monitor.start_op("streaming_merge")
                    existing_dataset = ds.dataset(
                        file_path, filesystem=self._filesystem
                    )

                    with pq.ParquetWriter(
                        staging_file,
                        existing_dataset.schema,
                        filesystem=self._filesystem,
                        compression=compression or "snappy",
                    ) as writer:
                        from fsspeckit.datasets.pyarrow.dataset import (
                            merge_upsert_pyarrow,
                            merge_update_pyarrow,
                        )

                        if strategy == "upsert":
                            merge_upsert_pyarrow(
                                existing_dataset,
                                source_for_file,
                                key_cols,
                                chunk_size=merge_chunk_size_rows,
                                max_memory_mb=merge_max_memory_mb,
                                memory_monitor=monitor._memory_monitor,
                                writer=writer,
                            )
                        else:  # strategy == "update"
                            merge_update_pyarrow(
                                existing_dataset,
                                source_for_file,
                                key_cols,
                                chunk_size=merge_chunk_size_rows,
                                max_memory_mb=merge_max_memory_mb,
                                memory_monitor=monitor._memory_monitor,
                                writer=writer,
                            )
                    monitor.end_op()
                    updated_row_count = pq.read_metadata(
                        staging_file, filesystem=self._filesystem
                    ).num_rows
                else:
                    # Classic in-memory merge
                    monitor.start_op("in_memory_merge")
                    target_table = pq.read_table(file_path, filesystem=self._filesystem)

                    from fsspeckit.datasets.pyarrow.dataset import (
                        merge_upsert_pyarrow,
                        merge_update_pyarrow,
                    )

                    if strategy == "upsert":
                        updated_table = merge_upsert_pyarrow(
                            target_table,
                            source_for_file,
                            key_cols,
                            max_memory_mb=merge_max_memory_mb,
                            memory_monitor=monitor._memory_monitor,
                        )
                    else:
                        updated_table = merge_update_pyarrow(
                            target_table,
                            source_for_file,
                            key_cols,
                            max_memory_mb=merge_max_memory_mb,
                            memory_monitor=monitor._memory_monitor,
                        )

                    pq.write_table(
                        updated_table,
                        staging_file,
                        filesystem=self._filesystem,
                        compression=compression,
                        row_group_size=row_group_size,
                    )
                    updated_row_count = updated_table.num_rows
                    monitor.end_op()

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
                        row_count=updated_row_count,
                        operation="rewritten",
                        size_bytes=size_bytes,
                    )
                )
                monitor.files_processed += 1
                monitor.end_op()
        finally:
            file_manager.cleanup_staging_files(filesystem=self._filesystem)
            monitor.end_op()

        inserted_files: list[str] = []
        inserted_meta: list[MergeFileMetadata] = []
        inserted_rows = 0

        if (
            strategy == "upsert"
            and inserted_key_tracker.get_metrics()["unique_keys_estimate"] > 0
        ):
            insert_table = _select_rows_by_keys(source_table, inserted_key_tracker)
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

        updated_rows = matched_keys.get_metrics()["unique_keys_estimate"]
        files_meta = (
            rewritten_meta
            + inserted_meta
            + [
                MergeFileMetadata(path=f, row_count=0, operation="preserved")
                for f in preserved_files
            ]
        )

        monitor.track_memory()
        metrics = monitor.get_metrics(
            total_rows_before=target_count_before,
            total_rows_after=target_count_before + inserted_rows,
            total_bytes=sum(
                f.size_bytes for f in files_meta if f.size_bytes is not None
            ),
        )
        # Add tracker metrics
        metrics["key_tracker"] = matched_keys.get_metrics()
        logger.info("Merge operation completed", strategy=strategy, metrics=metrics)

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
            metrics=metrics,
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
