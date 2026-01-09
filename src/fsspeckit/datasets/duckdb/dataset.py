"""DuckDB dataset I/O and maintenance operations.

This module contains functions for reading, writing, and maintaining
parquet datasets using DuckDB.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

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
from fsspeckit.common.validation import validate_columns
from fsspeckit.core.merge import (
    MergeStats,
    calculate_merge_stats,
    normalize_key_columns,
)
from fsspeckit.core.merge import (
    MergeStrategy as CoreMergeStrategy,
)
from fsspeckit.datasets.duckdb.connection import (
    DuckDBConnection,
    create_duckdb_connection,
)
from fsspeckit.datasets.duckdb.helpers import _unregister_duckdb_table_safely
from fsspeckit.datasets.exceptions import (
    DatasetFileError,
    DatasetMergeError,
    DatasetOperationError,
)
from fsspeckit.datasets.base import BaseDatasetHandler
from fsspeckit.datasets.path_utils import normalize_path, validate_dataset_path

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

    from fsspeckit.core.maintenance import MaintenanceStats, plan_compaction_groups

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
        filters: str | None = None,
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

        # Register the data as a temporary table
        f"temp_{uuid.uuid4().hex[:16]}"
        conn.register("data_table", data)

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

        if mode not in ("append", "overwrite"):
            raise ValueError(f"mode must be 'append' or 'overwrite', got: {mode}")
        if max_rows_per_file is not None and max_rows_per_file <= 0:
            raise ValueError("max_rows_per_file must be > 0")
        if row_group_size is not None and row_group_size <= 0:
            raise ValueError("row_group_size must be > 0")

        fs = self._connection.filesystem
        fs.mkdirs(path, exist_ok=True)

        if mode == "overwrite":
            self._clear_dataset_parquet_only(path)

        file_manager = IncrementalFileManager()
        staging_dir = file_manager.create_staging_directory(path, filesystem=fs)

        moved_files: list[str] = []
        try:
            self._write_to_path(
                data=data,
                path=staging_dir,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
                mode="overwrite",
            )

            staging_files = [
                f
                for f in fs.find(staging_dir, withdirs=False)
                if f.endswith(".parquet")
            ]

            for staging_file in staging_files:
                filename = f"part-{uuid.uuid4().hex[:16]}.parquet"
                target_file = f"{path}/{filename}"
                fs.move(staging_file, target_file)
                moved_files.append(target_file)
        finally:
            file_manager.cleanup_staging_files(filesystem=fs)

        files: list[FileWriteMetadata] = []
        for f in moved_files:
            row_count = int(self._get_file_row_count(f, fs))
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
        use_merge: bool | None = None,
    ) -> "MergeResult":
        """Merge data into an existing parquet dataset incrementally (DuckDB backend).

        Semantics:
        - `insert`: append only new keys as new file(s); never rewrites existing files.
        - `update`: rewrite only files that actually contain keys being updated; never inserts.
        - `upsert`: rewrite only affected files and append inserted keys as new file(s).

        Args:
            use_merge: Control over MERGE statement usage.
                - True: Force MERGE (raises error if DuckDB < 1.4.0)
                - False: Force UNION ALL fallback (always)
                - None: Auto-detect based on DuckDB version (default)
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
            plan_incremental_rewrite,
            validate_no_null_keys,
            validate_partition_column_immutability,
        )
        from fsspeckit.core.merge import normalize_key_columns

        validate_path(path)
        validate_compression_codec(compression)

        if strategy not in ("insert", "update", "upsert"):
            raise ValueError("strategy must be one of: insert, update, upsert")

        key_cols = normalize_key_columns(key_columns)

        merge_impl = self._select_merge_implementation(use_merge)
        logger.info(
            "Merge strategy selected",
            strategy=strategy,
            implementation="MERGE"
            if merge_impl == self._merge_using_duckdb_merge
            else "UNION ALL",
            use_merge=use_merge,
        )

        partition_cols: list[str] = []
        if partition_columns is not None:
            partition_cols = normalize_key_columns(partition_columns)

        # Combine source input to a single table.
        if isinstance(data, list):
            import pyarrow as pa_mod

            source_table = pa_mod.concat_tables(data, promote_options="permissive")
        else:
            source_table = data

        if schema is not None:
            from fsspeckit.common.schema import cast_schema

            source_table = cast_schema(source_table, schema)

        validate_no_null_keys(source_table, key_cols)

        for col in key_cols:
            if col not in source_table.column_names:
                raise ValueError(
                    f"Key column '{col}' not found in source. "
                    f"Available columns: {', '.join(source_table.column_names)}"
                )

        for col in partition_cols:
            if col not in source_table.column_names:
                raise ValueError(
                    f"Partition column '{col}' not found in source. "
                    f"Available columns: {', '.join(source_table.column_names)}"
                )

        # De-duplicate source by key (last-write-wins).
        def _dedupe_source_last_wins(table: pa.Table) -> pa.Table:
            import pyarrow as pa_mod

            if table.num_rows == 0:
                return table

            if len(key_cols) == 1:
                keys = table.column(key_cols[0]).to_pylist()
                last_index: dict[object, int] = {}
                for idx, key in enumerate(keys):
                    last_index[key] = idx
                indices = sorted(last_index.values())
            else:
                key_arrays = [table.column(c).to_pylist() for c in key_cols]
                last_index = {}
                for idx, key in enumerate(zip(*key_arrays)):
                    last_index[key] = idx
                indices = sorted(last_index.values())

            return table.take(pa_mod.array(indices, type=pa_mod.int64()))

        source_table = _dedupe_source_last_wins(source_table)

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
            if strategy == "update":
                raise ValueError(
                    "UPDATE strategy requires an existing target dataset (non-existent target)"
                )

            # INSERT/UPSERT into a non-existent dataset: write all rows as inserts.
            fs.mkdirs(path, exist_ok=True)
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
            import pyarrow as pa_mod

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
                key_table = pq.read_table(file_path, columns=key_cols, filesystem=fs)
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

                target_table = pq.read_table(file_path, filesystem=fs)
                source_for_file = _select_rows_by_keys(
                    source_with_match, set(file_matched)
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
                for col in target_table.column_names:
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

    def _merge_with_sql(
        self,
        source_path: str,
        output_path: str,
        target_path: str | None,
        strategy: str,
        key_columns: list[str] | None,
        compression: str | None,
    ) -> MergeStats:
        """Perform merge operation using DuckDB SQL.

        Args:
            source_path: Path to source parquet dataset
            output_path: Path for output
            target_path: Path to target parquet dataset (or None)
            strategy: Merge strategy (upsert, insert, update, deduplicate, full_merge)
            key_columns: Key columns for merging
            compression: Output compression

        Returns:
            MergeStats with merge statistics
        """
        import shutil
        import tempfile as temp_module

        conn = self._connection.connection
        fs = self._connection.filesystem

        # Build source and target paths for parquet_scan
        source_glob = (
            f"{source_path}/**/*.parquet" if fs.isdir(source_path) else source_path
        )

        # SQL injection protection: validate file paths before SQL interpolation
        if "'" in source_glob or "--" in source_glob or ";" in source_glob:
            raise ValueError(f"Invalid path characters in source path: {source_glob}")

        # Get source row count
        source_count = conn.execute(
            f"SELECT COUNT(*) FROM parquet_scan('{source_glob}')"
        ).fetchone()[0]

        target_count = 0
        target_glob = None
        if target_path:
            target_glob = (
                f"{target_path}/**/*.parquet" if fs.isdir(target_path) else target_path
            )
            # SQL injection protection: validate file paths before SQL interpolation
            if "'" in target_glob or "--" in target_glob or ";" in target_glob:
                raise ValueError(
                    f"Invalid path characters in target path: {target_glob}"
                )
            target_count = conn.execute(
                f"SELECT COUNT(*) FROM parquet_scan('{target_glob}')"
            ).fetchone()[0]

        # Build the merge query based on strategy
        if strategy == "full_merge":
            # Simply use source data
            query = f"SELECT * FROM parquet_scan('{source_glob}')"
        elif strategy == "deduplicate":
            if key_columns:
                quoted_keys = [f'"{col}"' for col in key_columns]
                key_list = ", ".join(quoted_keys)
                # Deduplicate based on keys - keep first occurrence
                query = f"""
                SELECT DISTINCT ON ({key_list}) *
                FROM parquet_scan('{source_glob}')
                ORDER BY {key_list}
                """
            else:
                # No keys - remove exact duplicates
                query = f"SELECT DISTINCT * FROM parquet_scan('{source_glob}')"
        elif strategy in ["upsert", "insert", "update"] and target_glob:
            quoted_keys = [f'"{col}"' for col in key_columns]
            key_conditions = " AND ".join(
                [f's."{col}" = t."{col}"' for col in key_columns]
            )

            if strategy == "insert":
                # Only insert rows not in target
                query = f"""
                SELECT s.* FROM parquet_scan('{source_glob}') s
                WHERE NOT EXISTS (
                    SELECT 1 FROM parquet_scan('{target_glob}') t
                    WHERE {key_conditions}
                )
                """
            elif strategy == "update":
                # Only update rows that exist in target
                query = f"""
                SELECT s.* FROM parquet_scan('{source_glob}') s
                WHERE EXISTS (
                    SELECT 1 FROM parquet_scan('{target_glob}') t
                    WHERE {key_conditions}
                )
                """
            else:  # upsert
                # Combine: source + target rows not in source
                query = f"""
                SELECT * FROM parquet_scan('{source_glob}')
                UNION ALL
                SELECT t.* FROM parquet_scan('{target_glob}') t
                WHERE NOT EXISTS (
                    SELECT 1 FROM parquet_scan('{source_glob}') s
                    WHERE {key_conditions}
                )
                """
        else:
            raise ValueError(f"Unsupported strategy: {strategy}")

        # Get output row count
        output_count = conn.execute(
            f"SELECT COUNT(*) FROM ({query}) AS result"
        ).fetchone()[0]

        # Write to a temp location first to avoid read/write conflicts
        with temp_module.TemporaryDirectory() as temp_output_dir:
            temp_output = f"{temp_output_dir}/merged"
            fs.mkdirs(temp_output, exist_ok=True)

            # Write result to temp
            write_query = f"COPY ({query}) TO '{temp_output}' (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE"
            if compression:
                write_query += f", COMPRESSION {compression}"
            write_query += ")"

            conn.execute(write_query)

            # Clear output directory and move temp files
            if fs.exists(output_path):
                # Remove existing files
                for f in fs.glob(f"{output_path}/**/*.parquet"):
                    fs.rm(f)
            else:
                fs.mkdirs(output_path, exist_ok=True)

            # Move temp files to output
            for f in fs.glob(f"{temp_output}/**/*.parquet"):
                dest = f.replace(temp_output, output_path)
                shutil.move(f, dest)

        # Calculate stats
        stats = calculate_merge_stats(
            strategy=CoreMergeStrategy(strategy),
            source_count=source_count,
            target_count_before=target_count,
            target_count_after=output_count,
        )

        return stats

    def _write_parquet_dataset_standard(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
        mode: Literal["append", "overwrite"] | None = "append",
    ) -> None:
        """Internal: Standard dataset write without merge logic.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            compression: Compression codec
            max_rows_per_file: Maximum rows per file (not used by DuckDB, kept for API compat)
            row_group_size: Rows per row group
            mode: Write mode - 'append' or 'overwrite'
        """
        import tempfile

        fs = self._connection.filesystem

        # Ensure output directory exists
        fs.mkdirs(path, exist_ok=True)

        # Handle mode-specific behavior using temp directory approach for both modes
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f"{temp_dir}/temp_dataset"

            # Write to temp directory with overwrite
            self._write_to_path(
                data=data,
                path=temp_path,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
                mode="overwrite",  # Always use overwrite for temp directory
            )

            if mode == "append":
                # For append mode, move files with unique names to avoid collisions
                import uuid

                for temp_file in fs.find(temp_path, withdirs=False):
                    if temp_file.endswith(".parquet"):
                        # Generate unique filename to avoid collisions
                        # Use 'part-' prefix to match expected test behavior
                        unique_id = uuid.uuid4().hex[:16]
                        filename = f"part-{unique_id}.parquet"
                        target_file = f"{path}/{filename}"
                        fs.move(temp_file, target_file)
            elif mode == "overwrite":
                # For overwrite mode, first backup non-parquet files
                non_parquet_files = {}
                if fs.exists(path) and fs.isdir(path):
                    for file_info in fs.find(path, withdirs=False):
                        if not file_info.endswith(".parquet"):
                            # Read file content
                            with fs.open(file_info, "rb") as f:
                                content = f.read()
                            filename = file_info.split("/")[-1]
                            non_parquet_files[filename] = content

                # Clear target directory completely
                if fs.exists(path):
                    if fs.isfile(path):
                        fs.rm(path)
                    else:
                        for file_info in fs.find(path, withdirs=False):
                            fs.rm(file_info)

                # Move all files from temp to target
                for temp_file in fs.find(temp_path, withdirs=False):
                    if temp_file.endswith(".parquet"):
                        filename = temp_file.split("/")[-1]
                        target_file = f"{path}/{filename}"
                        fs.move(temp_file, target_file)

                # Restore non-parquet files
                for filename, content in non_parquet_files.items():
                    target_file = f"{path}/{filename}"
                    with fs.open(target_file, "wb") as f:
                        f.write(content)

            return  # Early return for both modes

        # For overwrite mode, write directly
        self._write_to_path(
            data=data,
            path=path,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            row_group_size=row_group_size,
            mode=mode,
        )

    def _write_parquet_dataset_incremental(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        strategy: CoreMergeStrategy,
        key_columns: list[str],
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
    ) -> MergeStats:
        """Internal: Incremental rewrite for UPSERT/UPDATE strategies.

        Only rewrites files that might contain the keys being updated,
        preserving other files unchanged.

        Args:
            data: Source data to merge
            path: Target dataset path
            strategy: Merge strategy (UPSERT or UPDATE)
            key_columns: Key columns for matching
            compression: Compression codec
            max_rows_per_file: Maximum rows per file
            row_group_size: Rows per row group

        Returns:
            MergeStats with operation results
        """

        from fsspeckit.core.incremental import plan_incremental_rewrite

        fs = self._connection.filesystem

        # Extract source keys for planning
        if isinstance(data, list):
            combined_data = pa.concat_tables(data)
        else:
            combined_data = data

        source_keys = self._extract_key_values(combined_data, key_columns)

        # Plan incremental rewrite using metadata analysis
        rewrite_plan = plan_incremental_rewrite(
            dataset_path=path,
            source_keys=source_keys,
            key_columns=key_columns,
            filesystem=fs,
        )

        logger.info(
            "Incremental rewrite: %d files affected, %d files preserved",
            len(rewrite_plan.affected_files),
            len(rewrite_plan.unaffected_files),
        )

        # If no files are affected, just write new data for UPSERT
        if not rewrite_plan.affected_files:
            if strategy == CoreMergeStrategy.UPSERT:
                # Write source data as new files
                return self._write_new_files_incremental(
                    data=combined_data,
                    path=path,
                    compression=compression,
                    max_rows_per_file=max_rows_per_file,
                    row_group_size=row_group_size,
                )
            else:
                # UPDATE with no affected files - nothing to do
                return MergeStats(
                    strategy=strategy,
                    source_count=combined_data.num_rows,
                    target_count_before=0,
                    target_count_after=0,
                    inserted=0,
                    updated=0,
                    deleted=0,
                )

        # Perform incremental rewrite
        return self._perform_incremental_rewrite(
            data=combined_data,
            path=path,
            strategy=strategy,
            key_columns=key_columns,
            rewrite_plan=rewrite_plan,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            row_group_size=row_group_size,
        )

    def _extract_key_values(
        self,
        data: pa.Table,
        key_columns: list[str],
    ) -> list[tuple]:
        """Extract key values from data as tuples for multi-column keys."""
        if len(key_columns) == 1:
            # Single column - return as single values
            return data.column(key_columns[0]).to_pylist()
        else:
            # Multi-column - return as tuples
            key_arrays = [data.column(col) for col in key_columns]
            return list(zip(*[arr.to_pylist() for arr in key_arrays]))

    def _get_duckdb_version(self) -> tuple[int, int, int]:
        conn = self._connection.connection
        try:
            result = conn.execute(
                "SELECT library_version FROM pragma_version()"
            ).fetchone()
            version_str = result[0]

            if not version_str or not isinstance(version_str, str):
                raise ValueError(f"Invalid version string: {version_str}")

            import re

            version_pattern = re.compile(r"^\d+\.\d+\.\d+$")
            if not version_pattern.match(version_str):
                raise ValueError(f"Invalid version format: {version_str}")

            parts = version_str.split(".")
            if len(parts) != 3:
                raise ValueError(f"Invalid version format: {version_str}")

            return tuple(map(int, parts))
        except Exception as e:
            logger.warning(
                "Could not determine DuckDB version, assuming old version",
                error=safe_format_error(e),
            )
            return (0, 0, 0)

    def _supports_merge(self) -> bool:
        version = self._get_duckdb_version()
        return version >= (1, 4, 0)

    def _select_merge_implementation(self, use_merge: bool | None) -> Callable:
        """Select appropriate merge implementation based on version and user preference.

        Args:
            use_merge: User preference or None for auto-detect

        Returns:
            Merge function to use

        Raises:
            DatasetMergeError: If MERGE requested but not available
        """
        if use_merge is False:
            return self._merge_using_union_all

        if use_merge is True or use_merge is None:
            supports_merge = self._supports_merge()

            if use_merge is True and not supports_merge:
                version = self._get_duckdb_version()
                raise DatasetMergeError(
                    f"DuckDB MERGE requested but not available. "
                    f"Required version: >=1.4.0, Current: {'.'.join(map(str, version))}"
                )

            return (
                self._merge_using_duckdb_merge
                if supports_merge
                else self._merge_using_union_all
            )

        raise ValueError(f"Invalid use_merge value: {use_merge}")

    def _write_new_files_incremental(
        self,
        data: pa.Table,
        path: str,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
    ) -> MergeStats:
        """Write source data as new files for UPSERT when no existing files are affected."""
        import tempfile

        fs = self._connection.filesystem

        # Create temporary directory for new files
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f"{temp_dir}/new_files"
            fs.mkdirs(temp_path, exist_ok=True)

            # Write source data to temp directory
            self._write_to_path(
                data=data,
                path=temp_path,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
                mode="overwrite",
            )

            # Move files to target with unique names
            inserted_rows = 0
            for temp_file in fs.find(temp_path, withdirs=False):
                if temp_file.endswith(".parquet"):
                    import uuid

                    unique_id = uuid.uuid4().hex[:16]
                    filename = f"part-{unique_id}.parquet"
                    target_file = f"{path}/{filename}"
                    fs.move(temp_file, target_file)

                    # Count rows in file
                    import pyarrow.parquet as pq

                    with fs.open(target_file, "rb") as f:
                        parquet_file = pq.ParquetFile(f)
                        inserted_rows += parquet_file.metadata.num_rows

            return MergeStats(
                strategy=CoreMergeStrategy.UPSERT,
                source_count=data.num_rows,
                target_count_before=0,
                target_count_after=inserted_rows,
                inserted=inserted_rows,
                updated=0,
                deleted=0,
            )

    def _perform_incremental_rewrite(
        self,
        data: pa.Table,
        path: str,
        strategy: CoreMergeStrategy,
        key_columns: list[str],
        rewrite_plan: Any,  # IncrementalRewritePlan
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
    ) -> MergeStats:
        """Perform the actual incremental rewrite operation."""

        from fsspeckit.core.incremental import IncrementalFileManager

        fs = self._connection.filesystem
        file_manager = IncrementalFileManager()

        try:
            # Create staging directory
            staging_dir = file_manager.create_staging_directory(path, filesystem=fs)

            # Process each affected file
            updated_rows = 0
            inserted_rows = 0

            for affected_file in rewrite_plan.affected_files:
                existing_data = self._read_parquet_file(affected_file, fs)

                if merge_impl == self._merge_using_duckdb_merge:
                    merged_data, file_updated, file_inserted = merge_impl(
                        existing_data, data, key_columns, strategy
                    )
                    updated_rows += file_updated
                    inserted_rows += file_inserted
                else:
                    if strategy == CoreMergeStrategy.UPSERT:
                        merged_data = self._merge_upsert(
                            existing_data, data, key_columns
                        )
                        existing_count = len(existing_data)
                        final_count = len(merged_data)
                        inserted_rows += max(0, final_count - existing_count)
                        updated_rows += min(existing_count, final_count)
                    else:
                        merged_data = self._merge_update(
                            existing_data, data, key_columns
                        )
                        updated_rows += len(merged_data)

            # For UPSERT, write new files for inserted rows
            if strategy == CoreMergeStrategy.UPSERT and inserted_rows > 0:
                import pyarrow as pa

                all_existing = []
                for f in rewrite_plan.affected_files:
                    all_existing.append(self._read_parquet_file(f, fs))
                existing_table = (
                    pa.concat_tables(all_existing)
                    if all_existing
                    else pa.Table.from_batches([], schema=data.schema)
                )
                new_data = self._extract_inserted_rows(
                    existing_table, data, key_columns
                )
                if len(new_data) > 0:
                    inserted_stats = self._write_new_files_incremental(
                        new_data, path, compression, max_rows_per_file, row_group_size
                    )
                    inserted_rows = inserted_stats.inserted

            # Atomically replace affected files
            for i, affected_file in enumerate(rewrite_plan.affected_files):
                staging_files = fs.find(staging_dir, withdirs=False)
                if staging_files and i < len(staging_files):
                    staging_file = staging_files[i]
                    # Replace affected file with staging file
                    fs.move(staging_file, affected_file)

            # Remove staging directory
            file_manager.cleanup_staging_files(filesystem=fs)

            # Calculate final statistics
            target_count_before = sum(
                self._get_file_row_count(f, fs) for f in rewrite_plan.affected_files
            )
            target_count_after = target_count_before + inserted_rows

            return MergeStats(
                strategy=strategy,
                source_count=data.num_rows,
                target_count_before=target_count_before,
                target_count_after=target_count_after,
                inserted=inserted_rows,
                updated=updated_rows,
                deleted=0,
            )

        except Exception as e:
            # Clean up on error
            logger.error(
                "Error during incremental merge operation, cleaning up staging files",
                error=str(e),
                operation="merge_incremental",
            )
            file_manager.cleanup_staging_files(filesystem=fs)
            raise

    def _read_parquet_file(self, file_path: str, filesystem: Any) -> pa.Table:
        """Read a single parquet file."""
        import pyarrow.parquet as pq

        if filesystem is not None:
            with filesystem.open(file_path, "rb") as f:
                return pq.read_table(f)
        else:
            return pq.read_table(file_path)

    def _write_single_file(
        self,
        data: pa.Table,
        file_path: str,
        compression: str | None,
        filesystem: Any,
    ) -> None:
        """Write a single parquet file."""
        import pyarrow.parquet as pq

        if filesystem is not None:
            with filesystem.open(file_path, "wb") as f:
                pq.write_table(data, f, compression=compression)
        else:
            pq.write_table(data, file_path, compression=compression)

    def _get_file_row_count(self, file_path: str, filesystem: Any) -> int:
        """Get row count of a parquet file."""
        import pyarrow.parquet as pq

        if filesystem is not None:
            with filesystem.open(file_path, "rb") as f:
                return pq.read_metadata(f).num_rows
        else:
            return pq.read_metadata(file_path).num_rows

    def _merge_using_union_all(
        self,
        existing_data: pa.Table,
        source_data: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        """Merge using UNION ALL + NOT EXISTS (fallback for DuckDB < 1.4.0)."""
        for col in key_columns:
            PathValidator.validate_sql_identifier(col)

        conn = self._connection.connection
        conn.register("existing_union", existing_data)
        conn.register("source_union", source_data)
        try:
            key_conditions = " AND ".join([f'e."{c}" = s."{c}"' for c in key_columns])
            query = f"""
                SELECT s.*
                FROM source_union s
                UNION ALL
                SELECT e.*
                FROM existing_union e
                WHERE NOT EXISTS (
                    SELECT 1 FROM source_union s
                    WHERE {key_conditions}
                )
            """
            return conn.execute(query).fetch_arrow_table()
        except Exception as e:
            logger.error(
                "UNION ALL merge failed",
                error=safe_format_error(e),
                operation="merge_union_all",
            )
            raise DatasetMergeError(
                f"UNION ALL merge failed: {safe_format_error(e)}"
            ) from e
        finally:
            _unregister_duckdb_table_safely(conn, "existing_union")
            _unregister_duckdb_table_safely(conn, "source_union")

    def _merge_using_duckdb_merge(
        self,
        existing_data: pa.Table,
        source_data: pa.Table,
        key_columns: list[str],
        strategy: CoreMergeStrategy,
    ) -> tuple[pa.Table, int, int]:
        """Merge using DuckDB's native MERGE statement (DuckDB 1.4.0+).

        Args:
            existing_data: Existing target data as PyArrow table
            source_data: Source data as PyArrow table
            key_columns: Columns to match on
            strategy: Merge strategy (INSERT, UPDATE, UPSERT)

        Returns:
            Tuple of (merged_table, updated_count, inserted_count)
        """
        for col in key_columns:
            PathValidator.validate_sql_identifier(col)

        conn = self._connection.connection
        conn.register("existing_merge", existing_data)
        conn.register("source_merge", source_data)

        try:
            key_conditions = " AND ".join([f'e."{c}" = s."{c}"' for c in key_columns])

            if strategy == CoreMergeStrategy.INSERT:
                merge_sql = f"""
                    MERGE INTO existing_merge AS e
                        USING source_merge AS s
                        ON ({key_conditions})
                        WHEN NOT MATCHED BY TARGET THEN INSERT BY NAME
                    """

                result_table = conn.execute(merge_sql).fetch_arrow_table()
                updated_count = 0
                inserted_count = len(result_table)

            elif strategy == CoreMergeStrategy.UPDATE:
                merge_sql = f"""
                    MERGE INTO existing_merge AS e
                        USING source_merge AS s
                        ON ({key_conditions})
                        WHEN MATCHED THEN UPDATE SET *
                    """

                result_table = conn.execute(merge_sql).fetch_arrow_table()
                updated_count = len(result_table)
                inserted_count = 0

            else:  # UPSERT
                merge_sql = f"""
                    MERGE INTO existing_merge AS e
                        USING source_merge AS s
                        ON ({key_conditions})
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED BY TARGET THEN INSERT BY NAME
                    """

                result_table = conn.execute(merge_sql).fetch_arrow_table()

                existing_count = len(existing_data)
                final_count = len(result_table)

                updated_count = 0
                inserted_count = 0

                if final_count > existing_count:
                    inserted_count = final_count - existing_count
                    updated_count = self._count_updated_rows(
                        existing_data, result_table, key_columns
                    )
                elif final_count < existing_count:
                    updated_count = self._count_updated_rows(
                        existing_data, result_table, key_columns
                    )
                else:
                    updated_count = self._count_updated_rows(
                        existing_data, result_table, key_columns
                    )

            return (result_table, updated_count, inserted_count)

        except (
            _DUCKDB_EXCEPTIONS.get("ParserException"),
            _DUCKDB_EXCEPTIONS.get("InvalidInputException"),
        ) as e:
            logger.error(
                "MERGE statement execution failed",
                error=safe_format_error(e),
                operation="merge_duckdb_merge",
                merge_strategy=strategy.value
                if hasattr(strategy, "value")
                else str(strategy),
            )
            raise DatasetMergeError(
                f"MERGE operation failed: {safe_format_error(e)}"
            ) from e
        finally:
            _unregister_duckdb_table_safely(conn, "existing_merge")
            _unregister_duckdb_table_safely(conn, "source_merge")

    def _count_updated_rows(
        self,
        existing_data: pa.Table,
        merged_data: pa.Table,
        key_columns: list[str],
    ) -> int:
        import pyarrow as pa_mod
        import pyarrow.compute as pc

        if len(existing_data) == 0 or len(merged_data) == 0:
            return 0

        non_key_cols = [c for c in merged_data.column_names if c not in key_columns]

        if not non_key_cols:
            return min(len(existing_data), len(merged_data))

        table = pa_mod.table({"source": existing_data, "target": merged_data})
        joined = table.join(
            "source",
            "target",
            keys=key_columns,
            join_type="inner",
            right_suffix="_r",
        )

        if joined.num_rows == 0:
            return 0

        changed_mask = pc.zeros(joined.num_rows, type=pc.bool_())
        for col in non_key_cols:
            source_col = joined.column(f"source_{col}")
            target_col = joined.column(f"target_{col}")

            if pa_mod.types.is_null(source_col.type):
                source_has_null = pc.is_null(source_col).to_pylist()
                target_has_null = pc.is_null(target_col).to_pylist()
                col_changed = [
                    sn != tn for sn, tn in zip(source_has_null, target_has_null)
                ]
            else:
                col_changed = (source_col != target_col).to_pylist()

            changed_mask = pc.or_(changed_mask, pc.array(col_changed, type=pc.bool_()))

        return int(pc.sum(changed_mask).as_py())

    def _merge_upsert(
        self,
        existing_data: pa.Table,
        source_data: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        conn = self._connection.connection
        conn.register("existing_data", existing_data)
        conn.register("source_data", source_data)
        try:
            key_conditions = " AND ".join([f'e."{c}" = s."{c}"' for c in key_columns])
            query = f"""
                SELECT s.*
                FROM source_data s
                UNION ALL
                SELECT e.*
                FROM existing_data e
                WHERE NOT EXISTS (
                    SELECT 1 FROM source_data s
                    WHERE {key_conditions}
                )
            """
            return conn.execute(query).fetch_arrow_table()
        finally:
            _unregister_duckdb_table_safely(conn, "existing_data")
            _unregister_duckdb_table_safely(conn, "source_data")

    def _merge_update(
        self,
        existing_data: pa.Table,
        source_data: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        for col in key_columns:
            PathValidator.validate_sql_identifier(col)

        conn = self._connection.connection
        conn.register("existing_data", existing_data)
        conn.register("source_data", source_data)
        try:
            key_conditions = " AND ".join([f'e."{c}" = s."{c}"' for c in key_columns])
            query = f"""
                SELECT s.*
                FROM source_data s
                JOIN existing_data e ON {key_conditions}
                UNION ALL
                SELECT e.*
                FROM existing_data e
                WHERE NOT EXISTS (
                    SELECT 1 FROM source_data s
                    WHERE {key_conditions}
                )
            """
            return conn.execute(query).fetch_arrow_table()
        except Exception as e:
            logger.error(
                "UNION ALL update merge failed",
                error=safe_format_error(e),
                operation="merge_update",
            )
            raise DatasetMergeError(
                f"UNION ALL update merge failed: {safe_format_error(e)}"
            ) from e
        finally:
            _unregister_duckdb_table_safely(conn, "existing_data")
            _unregister_duckdb_table_safely(conn, "source_data")

    def _extract_inserted_rows(
        self,
        existing_data: pa.Table,
        source_data: pa.Table,
        key_columns: list[str],
    ) -> pa.Table:
        for col in key_columns:
            PathValidator.validate_sql_identifier(col)

        conn = self._connection.connection
        conn.register("existing_data", existing_data)
        conn.register("source_data", source_data)
        try:
            key_conditions = " AND ".join([f'e."{c}" = s."{c}"' for c in key_columns])
            query = f"""
                SELECT s.*
                FROM source_data s
                WHERE NOT EXISTS (
                    SELECT 1 FROM existing_data e
                    WHERE {key_conditions}
                )
            """
            return conn.execute(query).fetch_arrow_table()
        except Exception as e:
            logger.error(
                "UNION ALL extract inserted rows failed",
                error=safe_format_error(e),
                operation="extract_inserted_rows",
            )
            raise DatasetMergeError(
                f"UNION ALL extract inserted rows failed: {safe_format_error(e)}"
            ) from e
        finally:
            _unregister_duckdb_table_safely(conn, "existing_data")
            _unregister_duckdb_table_safely(conn, "source_data")

    def _write_to_path(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        compression: str | None = "snappy",
        max_rows_per_file: int | None = 5_000_000,
        row_group_size: int | None = 500_000,
        mode: Literal["append", "overwrite"] | None = "append",
    ) -> None:
        """Internal helper to write data to a path."""
        conn = self._connection.connection

        # Register the data as a temporary table
        f"temp_{uuid.uuid4().hex[:16]}"
        conn.register("data_table", data)

        try:
            # Build the COPY command for dataset
            # DuckDB writes to directory with PER_THREAD_OUTPUT
            copy_query = (
                f"COPY data_table TO '{path}' (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE"
            )

            # Note: We don't use OVERWRITE option here because we already manually
            # cleared parquet files with _clear_dataset_parquet_only in overwrite mode

            if compression:
                copy_query += f", COMPRESSION {compression}"

            if row_group_size:
                copy_query += f", ROW_GROUP_SIZE {row_group_size}"

            copy_query += ")"

            # Execute
            conn.execute(copy_query)

        finally:
            # Clean up temporary table
            _unregister_duckdb_table_safely(conn, "data_table")

    def _generate_unique_filename(self, template: str = "data-{i}.parquet") -> str:
        """Generate a unique filename template.

        Args:
            template: Filename template with {i} placeholder

        Returns:
            Unique filename template
        """
        unique_id = uuid.uuid4().hex[:16]
        if "{i}" in template:
            return template.replace("{i}", unique_id)
        else:
            # If no placeholder, insert unique ID before extension
            if template.endswith(".parquet"):
                base = template[:-8]  # Remove '.parquet'
                return f"{base}-{unique_id}.parquet"
            else:
                return f"{template}-{unique_id}"

    def _clear_dataset(self, path: str) -> None:
        """Remove all files in a dataset directory.

        Args:
            path: Dataset directory path
        """
        fs = self._connection.filesystem

        if fs.exists(path):
            if fs.isfile(path):
                fs.rm(path)
            else:
                # Directory - remove all files
                for file_info in fs.find(path, withdirs=False):
                    fs.rm(file_info)

    def _clear_dataset_parquet_only(self, path: str) -> None:
        """Remove only parquet files in a dataset directory, preserving other files.

        Args:
            path: Dataset directory path
        """
        fs = self._connection.filesystem

        if fs.exists(path) and fs.isdir(path):
            # Find and remove only parquet files
            for file_info in fs.find(path, withdirs=False):
                if file_info.endswith(".parquet"):
                    fs.rm(file_info)
        # If path doesn't exist or isn't a directory, nothing to clear.

    def merge_parquet_dataset(
        self,
        sources: list[str],
        output_path: str,
        target: str | None = None,
        strategy: str | CoreMergeStrategy = "deduplicate",
        key_columns: list[str] | str | None = None,
        compression: str | None = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> MergeStats:
        """Merge multiple parquet datasets using DuckDB.

        Args:
            sources: List of source dataset paths
            output_path: Path for merged output
            target: Target dataset path (for upsert/update strategies)
            strategy: Merge strategy to use
            key_columns: Key columns for merging
            compression: Output compression codec
            verbose: Print progress information
            **kwargs: Additional arguments

        Returns:
            MergeStats with merge statistics

        Example:
            ```python
            from fsspeckit.datasets.duckdb.connection import create_duckdb_connection
            from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            stats = io.merge_parquet_dataset(
                sources=["dataset1/", "dataset2/"],
                output_path="merged/",
                strategy="deduplicate",
                key_columns=["id"],
            )
            ```
        """
        raise NotImplementedError(
            "DuckDBDatasetIO.merge_parquet_dataset() legacy API has been removed; "
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
                tables.append(pq.read_table(file_info.path, filesystem=fs))

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

    def insert_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        key_columns: list[str] | str,
        **kwargs: Any,
    ) -> MergeStats | None:
        """Insert-only dataset write.

        Convenience method that calls write_parquet_dataset with strategy='insert'.
        Only inserts records whose keys don't already exist in the target dataset.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            key_columns: Key columns for merge (required)
            **kwargs: Additional arguments passed to write_parquet_dataset

        Returns:
            MergeStats with merge statistics

        Raises:
            ValueError: If key_columns is not provided

        Example:
            ```python
            import pyarrow as pa
            from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            new_records = pa.table({'id': [4, 5], 'value': ['d', 'e']})
            stats = io.insert_dataset(new_records, "/tmp/dataset/", key_columns=["id"])
            ```
        """
        raise NotImplementedError(
            "DuckDBDatasetIO.insert_dataset() legacy API has been removed; "
        )

    def upsert_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        key_columns: list[str] | str,
        **kwargs: Any,
    ) -> MergeStats | None:
        """Insert-or-update dataset write.

        Convenience method that calls write_parquet_dataset with strategy='upsert'.
        Inserts new records and updates existing ones based on key columns.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            key_columns: Key columns for merge (required)
            **kwargs: Additional arguments passed to write_parquet_dataset

        Returns:
            MergeStats with merge statistics

        Raises:
            ValueError: If key_columns is not provided

        Example:
            ```python
            import pyarrow as pa
            from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            updates = pa.table({'id': [1, 4], 'value': ['updated', 'new']})
            stats = io.upsert_dataset(updates, "/tmp/dataset/", key_columns=["id"])
            ```
        """
        raise NotImplementedError(
            "DuckDBDatasetIO.upsert_dataset() legacy API has been removed; "
        )

    def update_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        key_columns: list[str] | str,
        **kwargs: Any,
    ) -> MergeStats | None:
        """Update-only dataset write.

        Convenience method that calls write_parquet_dataset with strategy='update'.
        Only updates records that already exist in the target dataset.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            key_columns: Key columns for merge (required)
            **kwargs: Additional arguments passed to write_parquet_dataset

        Returns:
            MergeStats with merge statistics

        Raises:
            ValueError: If key_columns is not provided

        Example:
            ```python
            import pyarrow as pa
            from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            updates = pa.table({'id': [1, 2], 'value': ['updated1', 'updated2']})
            stats = io.update_dataset(updates, "/tmp/dataset/", key_columns=["id"])
            ```
        """
        raise NotImplementedError(
            "DuckDBDatasetIO.update_dataset() legacy API has been removed; "
        )

    def deduplicate_dataset(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        key_columns: list[str] | str | None = None,
        **kwargs: Any,
    ) -> MergeStats | None:
        """Deduplicate dataset write.

        Convenience method that calls write_parquet_dataset with strategy='deduplicate'.
        Removes duplicate records based on key columns (or exact duplicates if no keys provided).

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            key_columns: Key columns for deduplication (optional; if None, removes exact duplicates)
            **kwargs: Additional arguments passed to write_parquet_dataset

        Returns:
            MergeStats with merge statistics

        Example:
            ```python
            import pyarrow as pa
            from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            data = pa.table({'id': [1, 1, 2], 'value': ['a', 'b', 'c']})
            stats = io.deduplicate_dataset(data, "/tmp/dataset/", key_columns=["id"])
            ```
        """
        raise NotImplementedError(
            "DuckDBDatasetIO.deduplicate_dataset() legacy API has been removed; "
        )

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
