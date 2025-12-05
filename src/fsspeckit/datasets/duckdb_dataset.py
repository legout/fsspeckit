"""DuckDB dataset I/O and maintenance operations.

This module contains functions for reading, writing, and maintaining
parquet datasets using DuckDB.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa
    from fsspeckit.storage_options.base import BaseStorageOptions

from fsspec import AbstractFileSystem

from fsspeckit.core.merge import (
    MergeStrategy as CoreMergeStrategy,
    MergeStats,
    calculate_merge_stats,
    check_null_keys,
    normalize_key_columns,
    validate_merge_inputs,
    validate_strategy_compatibility,
)
from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _DUCKDB_AVAILABLE
from fsspeckit.common.security import (
    validate_path,
    validate_compression_codec,
    scrub_credentials,
    safe_format_error,
)
from fsspeckit.datasets.duckdb_connection import DuckDBConnection

logger = get_logger(__name__)

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


class DuckDBDatasetIO:
    """DuckDB-based dataset I/O operations.

    This class provides methods for reading and writing parquet files and datasets
    using DuckDB's high-performance parquet engine.

    Args:
        connection: DuckDB connection manager
    """

    def __init__(self, connection: DuckDBConnection) -> None:
        """Initialize DuckDB dataset I/O.

        Args:
            connection: DuckDB connection manager
        """
        self._connection = connection

    def read_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
        filter: str | None = None,
        use_threads: bool = True,
    ) -> pa.Table:
        """Read parquet file(s) using DuckDB.

        Args:
            path: Path to parquet file or directory
            columns: Optional list of columns to read
            filter: Optional SQL WHERE clause
            use_threads: Whether to use parallel reading

        Returns:
            PyArrow table containing the data

        Example:
            ```python
            from fsspeckit.datasets.duckdb_connection import create_duckdb_connection
            from fsspeckit.datasets.duckdb_dataset import DuckDBDatasetIO

            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            table = io.read_parquet("/path/to/file.parquet")
            ```
        """
        validate_path(path)

        conn = self._connection.connection
        fs = self._connection.filesystem

        # Build the query
        query = "SELECT * FROM parquet_scan(?)"

        params = [path]

        if columns:
            # Escape column names and build select list
            quoted_cols = [f'"{col}"' for col in columns]
            select_list = ", ".join(quoted_cols)
            query = f"SELECT {select_list} FROM parquet_scan(?)"

        if filter:
            query += f" WHERE {filter}"

        try:
            # Execute query
            if use_threads:
                result = conn.execute(query, params).fetch_arrow_table()
            else:
                result = conn.execute(query, params).fetch_arrow_table()

            return result

        except (_DUCKDB_EXCEPTIONS.get("IOException"), _DUCKDB_EXCEPTIONS.get("InvalidInputException"), _DUCKDB_EXCEPTIONS.get("ParserException")) as e:
            raise RuntimeError(
                f"Failed to read parquet from {path}: {safe_format_error(e)}"
            ) from e

    def write_parquet(
        self,
        data: pa.Table | list[pa.Table],
        path: str,
        compression: str | None = "snappy",
        row_group_size: int | None = None,
        use_threads: bool = True,
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
            from fsspeckit.datasets.duckdb_connection import create_duckdb_connection
            from fsspeckit.datasets.duckdb_dataset import DuckDBDatasetIO

            table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            io.write_parquet(table, "/tmp/data.parquet")
            ```
        """
        validate_path(path)
        validate_compression_codec(compression)

        conn = self._connection.connection

        # Register the data as a temporary table
        table_name = f"temp_{uuid.uuid4().hex[:16]}"
        conn.register("data_table", data)

        try:
            # Build the COPY command
            copy_query = f"COPY data_table TO ?"

            params = [path]

            if compression:
                copy_query += f" (COMPRESSION {compression})"

            if row_group_size:
                copy_query += f" (ROW_GROUP_SIZE {row_group_size})"

            # Execute the copy
            if use_threads:
                conn.execute(copy_query, params)
            else:
                conn.execute(copy_query, params)

        finally:
            # Clean up temporary table
            try:
                conn.unregister("data_table")
            except (_DUCKDB_EXCEPTIONS.get("CatalogException"), _DUCKDB_EXCEPTIONS.get("ConnectionException")) as e:
                logger.warning("Failed to unregister temporary table: %s", e)

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
        use_threads: bool = True,
    ) -> None:
        """Write a parquet dataset using DuckDB.

        Args:
            data: PyArrow table or list of tables to write
            path: Output directory path
            basename_template: Template for file names
            schema: Optional schema to enforce
            partition_by: Column(s) to partition by
            compression: Compression codec
            max_rows_per_file: Maximum rows per file
            row_group_size: Rows per row group
            use_threads: Whether to use parallel writing

        Example:
            ```python
            import pyarrow as pa
            from fsspeckit.datasets.duckdb_connection import create_duckdb_connection
            from fsspeckit.datasets.duckdb_dataset import DuckDBDatasetIO

            table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            io.write_parquet_dataset(table, "/tmp/dataset/")
            ```
        """
        from fsspeckit.common.optional import _import_pyarrow

        validate_path(path)
        validate_compression_codec(compression)

        pa_mod = _import_pyarrow()

        conn = self._connection.connection

        # Register the data as a temporary table
        table_name = f"temp_{uuid.uuid4().hex[:16]}"
        conn.register("data_table", data)

        try:
            # Build the COPY command for dataset
            copy_query = "COPY data_table TO ? (FORMAT PARQUET"

            params = [path + "/{i}.parquet"]

            if compression:
                copy_query += f", COMPRESSION {compression}"

            if max_rows_per_file:
                copy_query += f", MAX_ROWS_PER_FILE {max_rows_per_file}"

            if row_group_size:
                copy_query += f", ROW_GROUP_SIZE {row_group_size}"

            copy_query += ")"

            # Execute with file numbering
            conn.execute(copy_query, params)

        finally:
            # Clean up temporary table
            try:
                conn.unregister("data_table")
            except (_DUCKDB_EXCEPTIONS.get("CatalogException"), _DUCKDB_EXCEPTIONS.get("ConnectionException")) as e:
                logger.warning("Failed to unregister temporary table: %s", e)

    def _generate_unique_filename(self, template: str = "data-{i}.parquet") -> str:
        """Generate a unique filename template.

        Args:
            template: Filename template with {i} placeholder

        Returns:
            Unique filename template
        """
        unique_id = uuid.uuid4().hex[:16]
        return template.replace("{i}", unique_id)

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
            from fsspeckit.datasets.duckdb_connection import create_duckdb_connection
            from fsspeckit.datasets.duckdb_dataset import DuckDBDatasetIO

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
        # Validate inputs using shared core logic
        validate_merge_inputs(
            sources=sources,
            strategy=strategy,
            key_columns=key_columns,
            target=target,
        )

        validate_strategy_compatibility(strategy, key_columns, target)

        # Normalize parameters
        if key_columns is not None:
            key_columns = normalize_key_columns(key_columns)

        # Process merge using DuckDB
        result = self._execute_merge_strategy(
            sources=sources,
            output_path=output_path,
            target=target,
            strategy=strategy,
            key_columns=key_columns,
            compression=compression,
            verbose=verbose,
        )

        return result

    def _execute_merge_strategy(
        self,
        sources: list[str],
        output_path: str,
        target: str | None,
        strategy: str | CoreMergeStrategy,
        key_columns: list[str] | None,
        compression: str | None,
        verbose: bool,
    ) -> MergeStats:
        """Execute merge strategy using DuckDB.

        Args:
            sources: Source paths
            output_path: Output path
            target: Target path
            strategy: Merge strategy
            key_columns: Key columns
            compression: Compression codec
            verbose: Verbose output

        Returns:
            Merge statistics
        """
        conn = self._connection.connection

        # Register all sources
        registered_tables = []
        for i, source in enumerate(sources):
            table_name = f"source_{i}"
            conn.register(table_name, f"SELECT * FROM parquet_scan('{source}')")
            registered_tables.append(table_name)

        # Register target if provided
        target_table_name = None
        if target:
            target_table_name = "target"
            conn.register(target_table_name, f"SELECT * FROM parquet_scan('{target}')")

        try:
            # Build the query based on strategy
            query, output_rows = self._build_merge_query(
                strategy=strategy,
                key_columns=key_columns,
                target_table_name=target_table_name,
                source_table_names=registered_tables,
            )

            # Execute the query to a table
            result_table_name = f"result_{uuid.uuid4().hex[:16]}"
            conn.execute(f"CREATE TABLE {result_table_name} AS {query}")

            # Write to output
            output_query = f"COPY {result_table_name} TO '{output_path}'"
            if compression:
                output_query += f" (COMPRESSION {compression})"

            conn.execute(output_query)

            # Calculate stats
            stats = calculate_merge_stats(
                sources=sources,
                target=output_path,
                strategy=strategy,
                total_rows=output_rows,
                output_rows=output_rows,
            )

            return stats

        finally:
            # Clean up registered tables
            for table_name in registered_tables:
                try:
                    conn.unregister(table_name)
                except (_DUCKDB_EXCEPTIONS.get("CatalogException"), _DUCKDB_EXCEPTIONS.get("ConnectionException")) as e:
                    logger.warning("Failed to unregister table %s: %s", table_name, e)

            if target_table_name:
                try:
                    conn.unregister(target_table_name)
                except (_DUCKDB_EXCEPTIONS.get("CatalogException"), _DUCKDB_EXCEPTIONS.get("ConnectionException")) as e:
                    logger.warning("Failed to unregister table %s: %s", target_table_name, e)

    def _build_merge_query(
        self,
        strategy: str | CoreMergeStrategy,
        key_columns: list[str] | None,
        target_table_name: str | None,
        source_table_names: list[str],
    ) -> tuple[str, int]:
        """Build SQL query for merge strategy.

        Args:
            strategy: Merge strategy
            key_columns: Key columns
            target_table_name: Target table name
            source_table_names: Source table names

        Returns:
            Tuple of (query, output_rows)
        """
        conn = self._connection.connection

        if strategy == "full_merge":
            # Simply union all sources
            if len(source_table_names) == 1:
                query = f"SELECT * FROM {source_table_names[0]}"
            else:
                unions = " UNION ALL ".join(
                    [f"SELECT * FROM {name}" for name in source_table_names]
                )
                query = f"SELECT * FROM ({unions}) AS merged"
            output_rows = conn.execute(f"SELECT COUNT(*) FROM ({query}) AS t").fetchone()[0]

        elif strategy == "deduplicate":
            if key_columns:
                # Deduplicate based on keys
                unions = " UNION ".join(
                    [f"SELECT * FROM {name}" for name in source_table_names]
                )
                quoted_keys = [f'"{col}"' for col in key_columns]
                key_list = ", ".join(quoted_keys)
                query = f"""
                SELECT DISTINCT * FROM (
                    SELECT DISTINCT ON ({key_list}) *
                    FROM ({unions}) AS merged
                    ORDER BY {key_list}
                ) AS deduped
                """
            else:
                # No keys, remove exact duplicates
                if len(source_table_names) == 1:
                    query = f"SELECT DISTINCT * FROM {source_table_names[0]}"
                else:
                    unions = " UNION ".join(
                        [f"SELECT * FROM {name}" for name in source_table_names]
                    )
                    query = f"SELECT DISTINCT * FROM ({unions}) AS merged"

            output_rows = conn.execute(f"SELECT COUNT(*) FROM ({query}) AS t").fetchone()[0]

        elif strategy in ["upsert", "insert", "update"] and target_table_name:
            # Key-based relational operations
            if strategy == "insert":
                # Only insert non-existing rows
                quoted_keys = [f'"{col}"' for col in key_columns]
                key_list = ", ".join(quoted_keys)

                # Get source data not in target
                unions = " UNION ".join(
                    [f"SELECT * FROM {name}" for name in source_table_names]
                )
                query = f"""
                SELECT s.* FROM ({unions}) AS s
                LEFT JOIN {target_table_name} AS t ON { ' AND '.join([ f's."{col}" = t."{col}"' for col in key_columns ]) }
                WHERE t.{quoted_keys[0]} IS NULL
                """
            else:
                # Update or upsert - full merge
                if len(source_table_names) == 1:
                    unions = f"SELECT * FROM {source_table_names[0]}"
                else:
                    unions = " UNION ".join(
                        [f"SELECT * FROM {name}" for name in source_table_names]
                    )
                query = f"SELECT * FROM ({unions}) AS merged"

            output_rows = conn.execute(f"SELECT COUNT(*) FROM ({query}) AS t").fetchone()[0]

        else:
            raise ValueError(f"Unsupported strategy: {strategy}")

        return query, output_rows

    def _collect_dataset_stats(
        self,
        path: str,
        partition_filter: list[str] | None = None,
    ) -> dict[str, Any]:
        """Collect statistics for a parquet dataset.

        Args:
            path: Dataset path
            partition_filter: Optional partition filters

        Returns:
            Dictionary of dataset statistics
        """
        from fsspeckit.core.maintenance import collect_dataset_stats

        return collect_dataset_stats(
            path=path,
            filesystem=self._connection.filesystem,
            partition_filter=partition_filter,
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
        from fsspeckit.core.maintenance import plan_compaction_groups, MaintenanceStats

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

        if dry_run:
            result = planned_stats.to_dict()
            result["planned_groups"] = groups
            return result

        # Execute compaction
        if not groups:
            return planned_stats.to_dict()

        conn = self._connection.connection

        for group in groups:
            # Read all files in this group into DuckDB
            tables = []
            for file_info in group["files"]:
                file_path = file_info["path"]
                table = conn.execute(
                    f"SELECT * FROM parquet_scan('{file_path}')"
                ).fetch_arrow_table()
                tables.append(table)

            # Concatenate tables
            if len(tables) > 1:
                combined = pa.concat_tables(tables, promote_options="permissive")
            else:
                combined = tables[0]

            # Write to output
            output_path = group["output_path"]
            self.write_parquet(combined, output_path, compression=compression)

        # Remove original files
        for group in groups:
            for file_info in group["files"]:
                file_path = file_info["path"]
                self._connection.filesystem.rm(file_path)

        return planned_stats.to_dict()

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
            target_mb_per_file: Target size per file
            target_rows_per_file: Target rows per file
            partition_filter: Optional partition filters
            compression: Compression codec
            verbose: Print progress information

        Returns:
            Optimization statistics
        """
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
