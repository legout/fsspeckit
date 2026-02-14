"""DuckDB dataset I/O and maintenance operations.

This module contains functions for reading, writing, and maintaining
parquet datasets using DuckDB.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa
    from fsspec import AbstractFileSystem

    from fsspeckit.core.incremental import MergeResult
    from fsspeckit.datasets.write_result import WriteDatasetResult


from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _DUCKDB_AVAILABLE
from fsspeckit.common.security import (
    PathValidator,
    safe_format_error,
    validate_compression_codec,
    validate_path,
)
from fsspeckit.core.merge import normalize_key_columns
from fsspeckit.datasets.duckdb.connection import (
    DuckDBConnection,
    create_duckdb_connection,
)
from fsspeckit.datasets.duckdb.helpers import _unregister_duckdb_table_safely
from fsspeckit.datasets.base import BaseDatasetHandler

logger = get_logger(__name__)


def collect_dataset_stats_duckdb(
    path: str,
    filesystem: AbstractFileSystem | None = None,
    partition_filter: list[str] | None = None,
) -> dict[str, Any]:
    """Collect file-level statistics for a parquet dataset using shared core logic.

    This function delegates to the shared ``fsspeckit.core.maintenance.collect_dataset_stats``
    function, ensuring consistent dataset discovery and statistics across both DuckDB
    and PyArrow backends.

    The helper walks the given dataset directory on the provided filesystem,
    discovers parquet files (recursively), and returns basic statistics:

    - Per-file path, size in bytes, and number of rows
    - Aggregated total bytes and total rows

    The function is intentionally streaming/metadata-driven and never
    materializes the full dataset as a single table.

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

    Note:
        This is a thin wrapper around the shared core function. See
        :func:`fsspeckit.core.maintenance.collect_dataset_stats` for the
        authoritative implementation.
    """
    from fsspeckit.core.maintenance import collect_dataset_stats

    return collect_dataset_stats(
        path=path,
        filesystem=filesystem,
        partition_filter=partition_filter,
    )


def compact_parquet_dataset_duckdb(
    path: str,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    dry_run: bool = False,
    filesystem: AbstractFileSystem | None = None,
) -> dict[str, Any]:
    """Compact a parquet dataset directory into fewer larger files using DuckDB and shared planning.

    Groups small files based on size (MB) and/or row thresholds, rewrites grouped
    files into new parquet files, and optionally changes compression. Supports a
    dry-run mode that returns the compaction plan without modifying files.

    The implementation uses the shared core planning algorithm for consistent
    behavior across backends. It processes data in a group-based, streaming fashion
    using DuckDB's native SQL COPY operations, which avoids loading all files into
    PyArrow memory before writing.

    Args:
        path: Dataset root directory (local path or fsspec URL).
        target_mb_per_file: Optional max output size per file; must be > 0.
        target_rows_per_file: Optional max rows per output file; must be > 0.
        partition_filter: Optional list of partition prefixes (e.g. ``["date=2025-11-15"]``)
            used to limit both stats collection and rewrites to matching paths.
        compression: Optional parquet compression codec; defaults to ``"snappy"``.
        dry_run: When ``True`` the function returns a plan + before/after stats
            without reading or writing any parquet data.
        filesystem: Optional ``fsspec.AbstractFileSystem`` to reuse existing FS clients.

    Returns:
        A stats dictionary describing before/after file counts, total bytes,
        rewritten bytes, and optional ``planned_groups`` when ``dry_run`` is enabled.
        The structure follows the canonical ``MaintenanceStats`` format from the shared core.

    Raises:
        ValueError: If thresholds are invalid or no files match partition filter.
        FileNotFoundError: If the path does not exist.

    Example:
        ```python
        result = compact_parquet_dataset_duckdb(
            "/path/to/dataset",
            target_mb_per_file=64,
            dry_run=True,
        )
        print(f"Files before: {result['before_file_count']}")
        print(f"Files after: {result['after_file_count']}")
        ```

    Note:
        This function delegates dataset discovery and compaction planning to the
        shared ``fsspeckit.core.maintenance`` module, ensuring consistent behavior
        across DuckDB and PyArrow backends.
    """
    from fsspec import filesystem as fsspec_filesystem

    from fsspeckit.core.maintenance import plan_compaction_groups

    fs = filesystem or fsspec_filesystem("file")

    # Get dataset stats using shared logic
    stats = collect_dataset_stats_duckdb(
        path=path, filesystem=fs, partition_filter=partition_filter
    )
    files = stats["files"]

    # Use shared compaction planning
    plan_result = plan_compaction_groups(
        file_infos=files,
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
    )

    groups = plan_result["groups"]
    planned_stats = plan_result["planned_stats"]

    # Update planned stats with compression info
    planned_stats.compression_codec = compression
    planned_stats.dry_run = dry_run

    # If dry run, return the plan
    if dry_run:
        result = planned_stats.to_dict()
        result["planned_groups"] = plan_result["planned_groups"]
        return result

    # Execute compaction
    if not groups:
        return planned_stats.to_dict()

    # Create DuckDB connection using context manager
    with create_duckdb_connection(filesystem=fs) as duckdb_conn:
        conn = duckdb_conn.connection

        # Execute the compaction using DuckDB SQL COPY
        for group in groups:
            # Build parquet_scan union query for this group
            file_paths = [file_info.path for file_info in group.files]

            # Validate and escape file paths for SQL
            escaped_paths = []
            for fp in file_paths:
                PathValidator.validate_path_for_sql(fp)
                escaped_paths.append(PathValidator.escape_for_sql(fp))

            # Build union query
            if len(escaped_paths) == 1:
                source_query = f"SELECT * FROM parquet_scan('{escaped_paths[0]}')"
            else:
                union_queries = " UNION ALL ".join(
                    [f"SELECT * FROM parquet_scan('{ep}')" for ep in escaped_paths]
                )
                source_query = f"({union_queries})"

            # Generate output path
            output_path = (
                f"{path.rstrip('/')}/compacted-{uuid.uuid4().hex[:16]}.parquet"
            )
            PathValidator.validate_path_for_sql(output_path)
            escaped_output = PathValidator.escape_for_sql(output_path)

            # Build COPY command with compression
            copy_command = f"COPY {source_query} TO '{escaped_output}'"
            options = []
            if compression:
                options.append(f"COMPRESSION {compression}")
            if options:
                copy_command += f" ({', '.join(options)})"

            # Execute COPY command
            conn.execute(copy_command)

        # Remove original files
        for group in groups:
            for file_info in group.files:
                fs.rm(file_info.path)

    return planned_stats.to_dict()


# DuckDB exception types for specific error handling
_DUCKDB_EXCEPTIONS = {}
if _DUCKDB_AVAILABLE:
    import duckdb

    _DUCKDB_EXCEPTIONS = {
        "InvalidInputException": duckdb.InvalidInputException,
        "OperationalException": duckdb.OperationalError,
        "CatalogException": duckdb.CatalogException,
        "IOException": duckdb.IOException,
        "OutOfMemoryException": duckdb.OutOfMemoryException,
        "ParserException": duckdb.ParserException,
        "ConnectionException": duckdb.ConnectionException,
        "SyntaxException": duckdb.SyntaxException,
    }

# Type alias for merge strategies
MergeStrategy = Literal["upsert", "insert", "update", "full_merge", "deduplicate"]


class DuckDBDatasetIO(BaseDatasetHandler):
    """DuckDB-based dataset I/O operations.

    This class provides methods for reading and writing parquet files and datasets
    using DuckDB's high-performance parquet engine.

    Implements the DatasetHandler protocol to provide a consistent interface
    across different backend implementations.

    Args:
        connection: DuckDB connection manager
    """

    def __init__(self, connection: DuckDBConnection) -> None:
        """Initialize DuckDB dataset I/O.

        Args:
            connection: DuckDB connection manager
        """
        self._connection = connection

    @property
    def filesystem(self) -> "AbstractFileSystem":
        """Return the filesystem instance used by this handler."""
        return self._connection.filesystem

    def read_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
        filters: Any | None = None,
        use_threads: bool = True,
    ) -> pa.Table:
        """Read parquet file(s) using DuckDB.

        Args:
            path: Path to parquet file or directory
            columns: Optional list of columns to read
            filters: Optional SQL WHERE clause string for DuckDB (e.g., "column > 5 AND other = 'value'")
            use_threads: Whether to use parallel reading (DuckDB ignores this)

        Returns:
            PyArrow table containing the data

        Raises:
            TypeError: If filters is not None and not a string

        Example:
            ```python
            from fsspeckit.datasets.duckdb.connection import create_duckdb_connection
            from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            table = io.read_parquet("/path/to/file.parquet", filters="id > 100")
            ```
        """
        validate_path(path)

        if filters is not None and not isinstance(filters, str):
            raise TypeError(
                "DuckDB filters must be a SQL WHERE clause string. "
                "Received type: {type(filters).__name__}. "
                "Example: filters='column > 5 AND other = \"value\"'"
            )

        conn = self._connection.connection

        # Build the query
        query = "SELECT * FROM parquet_scan(?)"

        params = [path]

        if columns:
            # Escape column names and build select list
            quoted_cols = [f'"{col}"' for col in columns]
            select_list = ", ".join(quoted_cols)
            query = f"SELECT {select_list} FROM parquet_scan(?)"

        if filters:
            query += f" WHERE {filters}"

        # DuckDB ignores use_threads parameter, but we accept it for interface compatibility
        _ = use_threads

        try:
            # Execute query
            result = conn.execute(query, params).fetch_arrow_table()

            return result

        except (
            _DUCKDB_EXCEPTIONS.get("IOException"),
            _DUCKDB_EXCEPTIONS.get("InvalidInputException"),
            _DUCKDB_EXCEPTIONS.get("ParserException"),
        ) as e:
            raise RuntimeError(
                f"Failed to read parquet from {path}: {safe_format_error(e)}"
            ) from e

    def write_parquet(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        compression: str | None = "snappy",
        row_group_size: int | None = None,
        use_threads: bool = False,
    ) -> None:
        """Write parquet file using DuckDB.

        Args:
            data: PyArrow table or list of tables to write
            path: Output file path
            compression: Compression codec to use
            row_group_size: Rows per row group
            use_threads: Whether to use parallel writing

        Example:
            ```python
            import pyarrow as pa
            from fsspeckit.datasets.duckdb.connection import create_duckdb_connection
            from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

            table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            io.write_parquet(table, "/tmp/data.parquet")
            ```
        """
        validate_path(path)
        compression_final = compression or "snappy"
        validate_compression_codec(compression_final)

        fs = self._connection.filesystem
        parent = str(Path(path).parent)
        if parent and parent not in (".", "/"):
            fs.mkdirs(parent, exist_ok=True)

        conn = self._connection.connection
        table = self._combine_tables(data)

        # Register the data as a temporary table
        f"temp_{uuid.uuid4().hex[:16]}"
        conn.register("data_table", table)

        try:
            # Build the COPY command
            copy_query = "COPY data_table TO ?"

            params = [path]

            options: list[str] = []
            if compression_final:
                options.append(f"COMPRESSION {compression_final}")
            if row_group_size:
                options.append(f"ROW_GROUP_SIZE {row_group_size}")
            if options:
                copy_query += " (" + ", ".join(options) + ")"

            # Execute the copy
            if use_threads:
                conn.execute(copy_query, params)
            else:
                conn.execute(copy_query, params)

        finally:
            # Clean up temporary table
            _unregister_duckdb_table_safely(conn, "data_table")

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

        from fsspeckit.common.security import validate_compression_codec, validate_path
        from fsspeckit.core.incremental import IncrementalFileManager
        from fsspeckit.datasets.write_result import (
            FileWriteMetadata,
            WriteDatasetResult,
        )

        validate_path(path)
        validate_compression_codec(compression)

        self._validate_write_mode(mode)
        row_group_size = self._validate_write_parameters(
            max_rows_per_file,
            row_group_size,
        )

        table = self._combine_tables(data)
        if schema is not None:
            from fsspeckit.common.schema import cast_schema

            table = cast_schema(table, schema)

        partition_cols = self._validate_partition_columns(
            partition_by,
            table.column_names,
        )

        if basename_template is None:
            basename_template = "part-{i}.parquet"

        if mode == "append" and basename_template == "part-{i}.parquet":
            unique_id = uuid.uuid4().hex[:16]
            basename_template = f"part-{unique_id}-{{i}}.parquet"

        def _format_filename(index: int) -> str:
            if "{i}" in basename_template:
                return basename_template.format(i=index)
            if basename_template.endswith(".parquet"):
                stem = basename_template[:-8]
                return f"{stem}-{uuid.uuid4().hex[:16]}.parquet"
            return f"{basename_template}-{uuid.uuid4().hex[:16]}"

        fs = self._connection.filesystem
        fs.mkdirs(path, exist_ok=True)

        if mode == "overwrite":
            self._clear_dataset_parquet_only(path)

        file_manager = IncrementalFileManager()
        staging_dir = file_manager.create_staging_directory(path, filesystem=fs)

        moved_files: list[str] = []
        try:
            self._write_to_path(
                data=table,
                path=staging_dir,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
                mode="overwrite",
                partition_by=partition_cols or None,
            )

            staging_files = [
                f
                for f in fs.find(staging_dir, withdirs=False)
                if f.endswith(".parquet")
            ]
            staging_prefix = staging_dir.rstrip("/") + "/"

            for index, staging_file in enumerate(staging_files):
                staging_file_path = fs._strip_protocol(staging_file)
                staging_prefix_path = fs._strip_protocol(staging_prefix)

                if os.path.isabs(staging_file_path) and not os.path.isabs(
                    staging_prefix_path
                ):
                    staging_prefix_path = os.path.abspath(staging_prefix_path)

                if staging_file_path.startswith(staging_prefix_path):
                    relative = staging_file_path[len(staging_prefix_path) :].lstrip("/")
                else:
                    relative = os.path.relpath(staging_file_path, staging_prefix_path)

                relative_path = Path(relative)
                partition_dir = relative_path.parent.as_posix()

                if partition_dir not in ("", "."):
                    partition_parts = relative_path.parent.parts
                    if (
                        partition_cols
                        and len(partition_parts) == len(partition_cols)
                        and not any("=" in part for part in partition_parts)
                    ):
                        # Normalize value-only directories to Hive-style col=value
                        partition_dir = "/".join(
                            f"{col}={val}"
                            for col, val in zip(partition_cols, partition_parts)
                        )

                target_dir = (
                    path if partition_dir in ("", ".") else f"{path}/{partition_dir}"
                )
                fs.mkdirs(target_dir, exist_ok=True)
                filename = _format_filename(index)
                target_file = f"{target_dir}/{filename}"
                fs.move(staging_file, target_file)
                moved_files.append(target_file)
        finally:
            file_manager.cleanup_staging_files(filesystem=fs)

        files: list[FileWriteMetadata] = []
        for f in moved_files:
            row_count = int(self._get_file_row_count(f))
            size_bytes = None
            try:
                size_bytes = int(fs.size(f))
            except (OSError, IOError, PermissionError) as e:
                logger.warning(
                    "Failed to retrieve file size",
                    path=f,
                    error=str(e),
                    operation="write_dataset",
                )
                size_bytes = None
            except (TypeError, ValueError) as e:
                logger.warning(
                    "Invalid file size value",
                    path=f,
                    error=str(e),
                    operation="write_dataset",
                )
                size_bytes = None

            files.append(
                FileWriteMetadata(path=f, row_count=row_count, size_bytes=size_bytes)
            )

        return WriteDatasetResult(
            files=files,
            total_rows=sum(f.row_count for f in files),
            mode=mode,
            backend="duckdb",
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
        use_merge: bool | None = None,
    ) -> "MergeResult":
        """Merge data into an existing parquet dataset incrementally (DuckDB backend).

        Semantics:
        - `insert`: append only new keys as new file(s); never rewrites existing files.
        - `update`: rewrite only files that actually contain keys being updated; never inserts.
        - `upsert`: rewrite only affected files and append inserted keys as new file(s).

        Args:
            use_merge: Ignored (reserved for backward compatibility).
            merge_chunk_size_rows: Streaming merge chunk size (ignored by DuckDB).
            enable_streaming_merge: Streaming merge toggle (ignored by DuckDB).
            merge_max_memory_mb: Max PyArrow memory in MB (ignored by DuckDB).
            merge_max_process_memory_mb: Max process RSS in MB (ignored by DuckDB).
            merge_min_system_available_mb: Min system available memory in MB (ignored by DuckDB).
            merge_progress_callback: Progress callback (ignored by DuckDB).
        """
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        from fsspeckit.core.incremental import (
            IncrementalFileManager,
            MergeFileMetadata,
            MergeResult,
            confirm_affected_files,
            extract_source_partition_values,
            list_dataset_files,
            parse_hive_partition_path,
            plan_incremental_rewrite,
            validate_no_null_keys,
        )

        validate_path(path)
        validate_compression_codec(compression)
        row_group_size = self._validate_write_parameters(
            max_rows_per_file,
            row_group_size,
        )

        if use_merge is not None:
            logger.debug("duckdb_merge_use_merge_ignored", use_merge=use_merge)

        # Combine source input to a single table.
        source_table = self._combine_tables(data)

        if schema is not None:
            from fsspeckit.common.schema import cast_schema

            source_table = cast_schema(source_table, schema)

        key_cols = self._validate_key_columns(
            key_columns,
            source_table.column_names,
            context="source",
        )
        partition_cols = self._validate_partition_columns(
            partition_columns,
            source_table.column_names,
        )

        validate_no_null_keys(source_table, key_cols)

        # De-duplicate source by key (last-write-wins).
        source_table = self._dedupe_source_last_wins(source_table, key_cols)

        # Extract source keys for planning.
        if len(key_cols) == 1:
            source_keys = source_table.column(key_cols[0]).to_pylist()
            source_key_set: set[object] = set(source_keys)
        else:
            arrays = [source_table.column(c).to_pylist() for c in key_cols]
            source_keys = list(zip(*arrays))
            source_key_set = set(source_keys)

        fs = self._connection.filesystem

        # List existing parquet files in the dataset.
        target_files = list_dataset_files(path, filesystem=fs)
        target_exists = bool(target_files)
        self._validate_merge_strategy(strategy, target_exists)

        target_count_before = sum(
            pq.read_metadata(f, filesystem=fs).num_rows for f in target_files
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
            # INSERT/UPSERT into a non-existent dataset: write all rows as inserts.
            fs.mkdirs(path, exist_ok=True)
            write_res = self.write_dataset(
                source_table,
                path,
                mode="append",
                partition_by=partition_cols or None,
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

        # Existing dataset: plan incremental rewrite candidates using metadata.
        source_partition_values: set[tuple[object, ...]] | None = None
        if partition_cols:
            source_partition_values = extract_source_partition_values(
                source_table, partition_cols
            )

        rewrite_plan = plan_incremental_rewrite(
            dataset_path=path,
            source_keys=source_keys,
            key_columns=key_cols,
            filesystem=fs,
            partition_columns=partition_cols or None,
            source_partition_values=source_partition_values,
        )

        # Confirm actual affected files by scanning key columns.
        affected_files = confirm_affected_files(
            candidate_files=rewrite_plan.affected_files,
            key_columns=key_cols,
            source_keys=source_keys,
            filesystem=fs,
        )

        # Compute per-file matched keys for accurate updates and insert determination.
        matched_keys: set[object] = set()
        matched_keys_by_file: dict[str, set[object]] = {}
        for file_path in affected_files:
            try:
                key_table = pq.read_table(
                    file_path,
                    columns=key_cols,
                    filesystem=fs,
                    partitioning=None,
                )
                if len(key_cols) == 1:
                    file_keys = set(key_table.column(key_cols[0]).to_pylist())
                else:
                    file_keys = set(
                        zip(*[key_table.column(c).to_pylist() for c in key_cols])
                    )
                file_matched = source_key_set & file_keys
                if file_matched:
                    matched_keys_by_file[file_path] = set(file_matched)
                    matched_keys |= set(file_matched)
            except (OSError, IOError, Exception):
                # Conservative: assume all source keys might be present.
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

            insert_table = self._select_rows_by_keys(
                source_table,
                key_cols,
                set(inserted_key_set),
            )
            write_res = self.write_dataset(
                insert_table,
                path,
                mode="append",
                partition_by=partition_cols or None,
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

        # UPDATE / UPSERT: rewrite only actually affected files.
        file_manager = IncrementalFileManager()
        staging_dir = file_manager.create_staging_directory(path, filesystem=fs)

        rewritten_files: list[str] = []
        rewritten_meta: list[MergeFileMetadata] = []

        preserved_files = [f for f in target_files if f not in affected_files]

        # Prepare a match marker for join-driven full-row replacement.
        match_col_name = "__fsspeckit_match"
        if match_col_name in source_table.column_names:
            raise ValueError(f"Source contains reserved column: {match_col_name}")

        import pyarrow as pa_mod

        source_with_match = source_table.append_column(
            match_col_name, pa_mod.array([True] * source_table.num_rows)
        )

        try:
            for file_path in affected_files:
                file_matched = matched_keys_by_file.get(file_path, set())
                if not file_matched:
                    preserved_files.append(file_path)
                    continue

                target_table = pq.read_table(
                    file_path,
                    filesystem=fs,
                    partitioning=None,
                )
                output_columns = target_table.column_names

                if partition_cols:
                    partition_values = parse_hive_partition_path(
                        file_path,
                        partition_columns=partition_cols,
                    )
                    for col, value in partition_values.items():
                        if col in target_table.column_names:
                            continue
                        target_table = target_table.append_column(
                            col,
                            pa_mod.array([value] * target_table.num_rows),
                        )
                source_for_file = self._select_rows_by_keys(
                    source_with_match,
                    key_cols,
                    set(file_matched),
                )

                joined = target_table.join(
                    source_for_file,
                    keys=key_cols,
                    join_type="left outer",
                    right_suffix="__src",
                    coalesce_keys=True,
                )

                match_mask = pc.is_valid(joined.column(match_col_name))

                if partition_cols:
                    for col in partition_cols:
                        if col in key_cols:
                            continue
                        src_name = f"{col}__src"
                        if src_name not in joined.column_names:
                            raise ValueError(
                                f"Partition column '{col}' must be present in source for merge"
                            )
                        eq = pc.equal(joined.column(col), joined.column(src_name))
                        neq = pc.invert(eq)
                        violations = pc.and_(match_mask, pc.fill_null(neq, True))
                        if pc.any(violations).as_py():
                            raise ValueError(
                                "Cannot merge: partition column values cannot change for existing keys"
                            )

                out_arrays = []
                out_names = []
                for col in output_columns:
                    if col in key_cols:
                        out_arrays.append(joined.column(col))
                        out_names.append(col)
                        continue

                    src_name = f"{col}__src"
                    if src_name in joined.column_names:
                        out_arrays.append(
                            pc.if_else(
                                match_mask, joined.column(src_name), joined.column(col)
                            )
                        )
                    else:
                        out_arrays.append(joined.column(col))
                    out_names.append(col)

                updated_table = pa_mod.table(out_arrays, names=out_names)

                staging_file = f"{staging_dir}/{uuid.uuid4().hex[:16]}.parquet"
                pq.write_table(
                    updated_table,
                    staging_file,
                    filesystem=fs,
                    compression=compression,
                    row_group_size=row_group_size,
                )

                size_bytes = None
                try:
                    size_bytes = int(fs.size(staging_file))
                except (OSError, IOError, PermissionError):
                    size_bytes = None

                file_manager.atomic_replace_files(
                    [staging_file], [file_path], filesystem=fs
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
            file_manager.cleanup_staging_files(filesystem=fs)

        inserted_files: list[str] = []
        inserted_meta: list[MergeFileMetadata] = []
        inserted_rows = 0

        if strategy == "upsert" and inserted_key_set:
            insert_table = self._select_rows_by_keys(
                source_table,
                key_cols,
                set(inserted_key_set),
            )
            inserted_rows = insert_table.num_rows
            write_res = self.write_dataset(
                insert_table,
                path,
                mode="append",
                partition_by=partition_cols or None,
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

    def _get_file_row_count(self, file_path: str) -> int:
        """Get row count of a parquet file."""
        import pyarrow.parquet as pq

        metadata = pq.read_metadata(file_path, filesystem=self._connection.filesystem)
        return metadata.num_rows

    def _write_to_path(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
        mode: Literal["append", "overwrite"] | None = "append",
        partition_by: list[str] | None = None,
    ) -> None:
        """Internal helper to write data to a path."""
        conn = self._connection.connection

        table = self._combine_tables(data)

        # Register the data as a temporary table
        f"temp_{uuid.uuid4().hex[:16]}"
        conn.register("data_table", table)

        try:
            # Build the COPY command for dataset
            # DuckDB cannot combine PER_THREAD_OUTPUT with PARTITION_BY
            use_per_thread_output = partition_by is None
            copy_query = f"COPY data_table TO '{path}' (FORMAT PARQUET"
            if use_per_thread_output:
                copy_query += ", PER_THREAD_OUTPUT TRUE"

            # Note: We don't use OVERWRITE option here because we already manually
            # cleared parquet files with _clear_dataset_parquet_only in overwrite mode

            if compression:
                copy_query += f", COMPRESSION {compression}"

            if row_group_size:
                copy_query += f", ROW_GROUP_SIZE {row_group_size}"

            if partition_by:
                for col in partition_by:
                    PathValidator.validate_sql_identifier(col)
                partition_expr = ", ".join([f'"{col}"' for col in partition_by])
                copy_query += f", PARTITION_BY ({partition_expr})"

            copy_query += ")"

            # Execute
            conn.execute(copy_query)

        finally:
            # Clean up temporary table
            _unregister_duckdb_table_safely(conn, "data_table")

    def _clear_dataset_parquet_only(self, path: str) -> None:
        """Remove only parquet files in a dataset directory, preserving other files.

        Args:
            path: Dataset directory path
        """
        self._clear_parquet_files(path)

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
        """Compact a parquet dataset using DuckDB.

        Args:
            path: Dataset path
            target_mb_per_file: Target size per file
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec
            dry_run: Whether to perform a dry run
            verbose: Print progress information

        Returns:
            Compaction statistics
        """
        import uuid

        import pyarrow as pa
        import pyarrow.parquet as pq

        from fsspeckit.core.maintenance import plan_compaction_groups

        # Collect stats
        stats = self._collect_dataset_stats(path, partition_filter)
        files = stats["files"]

        # Plan compaction
        plan_result = plan_compaction_groups(
            file_infos=files,
            target_mb_per_file=target_mb_per_file,
            target_rows_per_file=target_rows_per_file,
        )

        groups = plan_result["groups"]
        planned_stats = plan_result["planned_stats"]

        planned_stats.compression_codec = compression
        planned_stats.dry_run = dry_run

        if dry_run:
            result = planned_stats.to_dict()
            result["planned_groups"] = plan_result["planned_groups"]
            return result

        # Execute compaction
        if not groups:
            return planned_stats.to_dict()

        fs = self._connection.filesystem

        for group in groups:
            tables: list[pa.Table] = []
            for file_info in group.files:
                tables.append(
                    pq.read_table(
                        file_info.path,
                        filesystem=fs,
                        partitioning=None,
                    )
                )

            # Concatenate tables
            if len(tables) > 1:
                combined = pa.concat_tables(tables, promote_options="permissive")
            else:
                combined = tables[0]

            output_path = (
                f"{path.rstrip('/')}/compacted-{uuid.uuid4().hex[:16]}.parquet"
            )
            self.write_parquet(combined, output_path, compression=compression)

        # Remove original files
        for group in groups:
            for file_info in group.files:
                fs.rm(file_info.path)

        return planned_stats.to_dict()

    def optimize_parquet_dataset(
        self,
        path: str,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        deduplicate_key_columns: list[str] | str | None = None,
        dedup_order_by: list[str] | str | None = None,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Optimize a parquet dataset with optional deduplication.

        Args:
            path: Dataset path
            target_mb_per_file: Target size per file
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec
            deduplicate_key_columns: Optional key columns for deduplication before optimization
            dedup_order_by: Columns to order by for deduplication
            verbose: Print progress information

        Returns:
            Optimization statistics
        """
        # Perform deduplication first if requested
        if deduplicate_key_columns is not None:
            dedup_stats = self.deduplicate_parquet_dataset(
                path=path,
                key_columns=deduplicate_key_columns,
                dedup_order_by=dedup_order_by,
                partition_filter=partition_filter,
                compression=compression,
                verbose=verbose,
            )

            if verbose:
                logger.info("Deduplication completed: %s", dedup_stats)

        # Use compaction for optimization
        result = self.compact_parquet_dataset(
            path=path,
            target_mb_per_file=target_mb_per_file,
            target_rows_per_file=target_rows_per_file,
            partition_filter=partition_filter,
            compression=compression,
            dry_run=False,
            verbose=verbose,
        )

        if verbose:
            logger.info("Optimization complete: %s", result)

        return result

    def deduplicate_parquet_dataset(
        self,
        path: str,
        *,
        key_columns: list[str] | str | None = None,
        dedup_order_by: list[str] | str | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Deduplicate an existing parquet dataset using DuckDB.

        This method removes duplicate rows from an existing parquet dataset,
        supporting both key-based deduplication and exact duplicate removal.
        Can be run independently of ingestion workflows.

        Args:
            path: Dataset path
            key_columns: Optional key columns for deduplication.
                If provided, keeps one row per key combination.
                If None, removes exact duplicate rows across all columns.
            dedup_order_by: Columns to order by for selecting which
                record to keep when duplicates are found. Defaults to key_columns.
            partition_filter: Optional partition filters to limit scope
            compression: Output compression codec
            dry_run: Whether to perform a dry run (return plan without execution)
            verbose: Print progress information

        Returns:
            Dictionary containing deduplication statistics

        Raises:
            ValueError: If key_columns is empty when provided
            FileNotFoundError: If dataset path doesn't exist

        Example:
            ```python
            from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)

            # Key-based deduplication
            stats = io.deduplicate_parquet_dataset(
                "/tmp/dataset/",
                key_columns=["id", "timestamp"],
                dedup_order_by=["-timestamp"],  # Keep most recent
                verbose=True
            )

            # Exact duplicate removal
            stats = io.deduplicate_parquet_dataset("/tmp/dataset/")
            ```
        """
        from fsspeckit.core.maintenance import plan_deduplication_groups

        # Validate inputs
        if key_columns is not None and not key_columns:
            raise ValueError("key_columns cannot be empty when provided")

        # Normalize parameters
        if key_columns is not None:
            key_columns = normalize_key_columns(key_columns)

        if dedup_order_by is not None:
            dedup_order_by = normalize_key_columns(dedup_order_by)
        elif key_columns is not None:
            dedup_order_by = key_columns

        # Collect dataset stats and plan deduplication
        stats = self._collect_dataset_stats(path, partition_filter)
        files = stats["files"]

        # Plan deduplication groups
        plan_result = plan_deduplication_groups(
            file_infos=files,
            key_columns=key_columns,
            dedup_order_by=dedup_order_by,
        )

        groups = plan_result["groups"]
        planned_stats = plan_result["planned_stats"]

        # Update planned stats with compression info
        planned_stats.compression_codec = compression
        planned_stats.dry_run = dry_run

        # If dry run, return the plan
        if dry_run:
            result = planned_stats.to_dict()
            result["planned_groups"] = [group.file_paths() for group in groups]
            return result

        # Execute deduplication
        if not groups:
            return planned_stats.to_dict()

        conn = self._connection.connection

        # Process each group
        total_deduplicated_rows = 0

        for group in groups:
            # Build query to deduplicate this group
            group_files = group.file_paths()

            # Create a temporary table for this group's data
            temp_table_name = f"temp_group_{uuid.uuid4().hex[:16]}"

            try:
                # Read all files in this group into a temporary table
                if len(group_files) == 1:
                    # Single file
                    query = f"CREATE TABLE {temp_table_name} AS SELECT * FROM parquet_scan('{group_files[0]}')"
                else:
                    # Multiple files - union them
                    union_queries = " UNION ALL ".join(
                        [
                            f"SELECT * FROM parquet_scan('{file_path}')"
                            for file_path in group_files
                        ]
                    )
                    query = f"CREATE TABLE {temp_table_name} AS {union_queries}"

                conn.execute(query)

                # Get original row count
                original_count = conn.execute(
                    f"SELECT COUNT(*) FROM {temp_table_name}"
                ).fetchone()[0]

                # Build deduplication query
                if key_columns:
                    # Key-based deduplication
                    quoted_keys = [f'"{col}"' for col in key_columns]
                    key_list = ", ".join(quoted_keys)

                    if dedup_order_by and dedup_order_by != key_columns:
                        # Custom ordering - need to specify which row to keep
                        quoted_order_cols = [f'"{col}"' for col in dedup_order_by]
                        order_clause = f" ORDER BY {', '.join(quoted_order_cols)}"
                    else:
                        order_clause = ""

                    dedup_query = f"""
                    CREATE TABLE {temp_table_name}_deduped AS
                    SELECT DISTINCT ON ({key_list}) * FROM {temp_table_name}
                    {order_clause}
                    """
                else:
                    # Exact duplicate removal
                    dedup_query = f"""
                    CREATE TABLE {temp_table_name}_deduped AS
                    SELECT DISTINCT * FROM {temp_table_name}
                    """

                conn.execute(dedup_query)

                # Get deduplicated row count
                deduped_count = conn.execute(
                    f"SELECT COUNT(*) FROM {temp_table_name}_deduped"
                ).fetchone()[0]

                total_deduplicated_rows += original_count - deduped_count

                # Write deduplicated data back to the first file in the group
                output_file = group_files[0]

                # Use COPY to write the deduplicated data
                write_query = f"COPY {temp_table_name}_deduped TO '{output_file}'"
                if compression:
                    write_query += f" (COMPRESSION {compression})"

                conn.execute(write_query)

                # Remove remaining files in the group (if multiple files)
                for file_to_remove in group_files[1:]:
                    if self._connection.filesystem.exists(file_to_remove):
                        self._connection.filesystem.rm(file_to_remove)

            finally:
                # Clean up temporary tables
                for table_suffix in ["", "_deduped"]:
                    table_name = f"{temp_table_name}{table_suffix}"
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except (
                        _DUCKDB_EXCEPTIONS.get("CatalogException"),
                        _DUCKDB_EXCEPTIONS.get("OperationalException"),
                    ):
                        pass  # Table might not exist

        # Update final statistics
        final_stats = planned_stats.to_dict()
        final_stats["deduplicated_rows"] = total_deduplicated_rows

        if verbose:
            logger.info("Deduplication complete: %s", final_stats)

        return final_stats
