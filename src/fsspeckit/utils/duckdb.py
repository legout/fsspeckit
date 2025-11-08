"""DuckDB-based parquet dataset handler with fsspec integration.

This module provides a high-performance interface for reading and writing parquet
datasets using DuckDB with support for various filesystems through fsspec.
DuckDB provides excellent parquet support with SQL analytics capabilities.
"""

from pathlib import Path
from typing import Any, Literal, TYPE_CHECKING
import uuid

import duckdb
import pyarrow as pa
from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem

if TYPE_CHECKING:
    from ..storage_options.base import BaseStorageOptions


# Type alias for merge strategies
MergeStrategy = Literal["upsert", "insert", "update", "full_merge", "deduplicate"]


class DuckDBParquetHandler:
    """Handler for parquet operations using DuckDB with fsspec integration.

    This class provides methods for reading and writing parquet files and datasets
    using DuckDB's high-performance parquet engine. It integrates with fsspec
    filesystems to support local and remote storage (S3, GCS, Azure, etc.).

    The handler can be initialized with either storage options or an existing
    filesystem instance. For remote filesystems, the fsspec filesystem is
    registered in DuckDB using `.register_filesystem(fs)` to enable direct
    access to remote paths.

    Args:
        storage_options: Storage configuration options (e.g., AwsStorageOptions).
            If provided, a filesystem is created from these options.
        filesystem: An existing fsspec filesystem instance. Takes precedence over
            storage_options if both are provided.

    Examples:
        Basic usage with local filesystem:
        >>> from fsspeckit.utils import DuckDBParquetHandler
        >>> import pyarrow as pa
        >>>
        >>> # Create sample data
        >>> table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
        >>>
        >>> # Write and read parquet file
        >>> with DuckDBParquetHandler() as handler:
        ...     handler.write_parquet(table, "/tmp/data.parquet")
        ...     result = handler.read_parquet("/tmp/data.parquet")
        ...     print(result)

        Using with AWS S3:
        >>> from fsspeckit.storage_options import AwsStorageOptions
        >>> from fsspeckit.utils import DuckDBParquetHandler
        >>>
        >>> options = AwsStorageOptions(
        ...     access_key_id="YOUR_KEY",
        ...     secret_access_key="YOUR_SECRET",
        ...     region="us-east-1"
        ... )
        >>>
        >>> with DuckDBParquetHandler(storage_options=options) as handler:
        ...     # Read from S3
        ...     table = handler.read_parquet("s3://bucket/data.parquet")
        ...     
        ...     # Execute SQL query on S3 data
        ...     result = handler.execute_sql(
        ...         "SELECT * FROM parquet_scan('s3://bucket/data.parquet') WHERE col > 10"
        ...     )

        Using with existing filesystem:
        >>> from fsspeckit import filesystem
        >>> from fsspeckit.utils import DuckDBParquetHandler
        >>>
        >>> fs = filesystem("file")
        >>> with DuckDBParquetHandler(filesystem=fs) as handler:
        ...     result = handler.read_parquet("/path/to/data.parquet")

        SQL query execution:
        >>> with DuckDBParquetHandler() as handler:
        ...     handler.write_parquet(table, "/tmp/data.parquet")
        ...     
        ...     # Simple query
        ...     result = handler.execute_sql(
        ...         "SELECT a, b FROM parquet_scan('/tmp/data.parquet') WHERE a > 1"
        ...     )
        ...     
        ...     # Parameterized query
        ...     result = handler.execute_sql(
        ...         "SELECT * FROM parquet_scan('/tmp/data.parquet') WHERE a BETWEEN ? AND ?",
        ...         parameters=[1, 3]
        ...     )
        ...     
        ...     # Aggregation query
        ...     result = handler.execute_sql(
        ...         '''
        ...         SELECT b, COUNT(*) as count, AVG(a) as avg_a
        ...         FROM parquet_scan('/tmp/data.parquet')
        ...         GROUP BY b
        ...         '''
        ...     )

        Reading specific columns:
        >>> with DuckDBParquetHandler() as handler:
        ...     # Only read columns 'a' and 'b'
        ...     result = handler.read_parquet("/tmp/data.parquet", columns=["a", "b"])

        Writing with compression:
        >>> with DuckDBParquetHandler() as handler:
        ...     handler.write_parquet(table, "/tmp/data.parquet", compression="gzip")
        ...     handler.write_parquet(table, "/tmp/data2.parquet", compression="zstd")
    """

    def __init__(
        self,
        storage_options: "BaseStorageOptions | None" = None,
        filesystem: AbstractFileSystem | None = None,
    ) -> None:
        """Initialize the DuckDB parquet handler.

        Args:
            storage_options: Storage configuration options. If provided, a filesystem
                is created from these options.
            filesystem: An existing fsspec filesystem instance. Takes precedence over
                storage_options if both are provided.
        """
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._filesystem: AbstractFileSystem | None = None
        self._storage_options = storage_options

        # Determine which filesystem to use
        if filesystem is not None:
            self._filesystem = filesystem
        elif storage_options is not None:
            self._filesystem = storage_options.to_filesystem()
        else:
            # Default to local filesystem
            self._filesystem = fsspec_filesystem("file")

    def _ensure_connection(self) -> duckdb.DuckDBPyConnection:
        """Ensure DuckDB connection is initialized and filesystem is registered.

        Returns:
            Active DuckDB connection.
        """
        if self._connection is None:
            self._connection = duckdb.connect(":memory:")
            self._register_filesystem()
        return self._connection

    def _require_filesystem(self) -> AbstractFileSystem:
        """Return initialized filesystem or raise.

        Raises:
            RuntimeError: If filesystem is not initialized.
        """
        if self._filesystem is None:
            raise RuntimeError("Filesystem is not initialized")
        return self._filesystem

    def _register_filesystem(self) -> None:
        """Register fsspec filesystem in DuckDB connection.

        This enables DuckDB to access files through the registered filesystem,
        supporting operations on remote storage systems like S3, GCS, Azure.
        """
        if self._connection is not None and self._filesystem is not None:
            self._connection.register_filesystem(self._filesystem)

    def read_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
    ) -> pa.Table:
        """Read parquet file or dataset directory.

        Reads a single parquet file or all parquet files in a directory and
        returns the data as a PyArrow table. Supports column projection for
        efficient reading of large datasets.

        Args:
            path: Path to parquet file or directory containing parquet files.
                Can be local path or remote URI (s3://, gs://, etc.).
            columns: Optional list of column names to read. If None, reads all columns.
                Specifying columns improves performance for large datasets.

        Returns:
            PyArrow table containing the parquet data.

        Raises:
            FileNotFoundError: If the specified path does not exist.
            Exception: If DuckDB encounters an error reading the parquet file.

        Examples:
            Read entire parquet file:
            >>> handler = DuckDBParquetHandler()
            >>> table = handler.read_parquet("/path/to/data.parquet")

            Read with column selection:
            >>> table = handler.read_parquet("/path/to/data.parquet", columns=["col1", "col2"])

            Read parquet dataset directory:
            >>> table = handler.read_parquet("/path/to/dataset/")

            Read from S3:
            >>> from fsspeckit.storage_options import AwsStorageOptions
            >>> handler = DuckDBParquetHandler(storage_options=AwsStorageOptions(...))
            >>> table = handler.read_parquet("s3://bucket/data.parquet")
        """
        conn = self._ensure_connection()

        # Build column selection clause
        columns_clause = "*" if columns is None else ", ".join(columns)

        # Build query to read parquet
        query = f"SELECT {columns_clause} FROM parquet_scan('{path}')"

        try:
            # Execute query and return as PyArrow table
            result = conn.execute(query).arrow()
            # Convert RecordBatchReader to Table
            if hasattr(result, 'read_all'):
                result = result.read_all()
            return result
        except Exception as e:
            # Re-raise with more context
            raise Exception(f"Failed to read parquet from '{path}': {e}") from e

    def write_parquet(
        self,
        table: pa.Table,
        path: str,
        compression: str = "snappy",
    ) -> None:
        """Write PyArrow table to parquet file.

        Writes a PyArrow table to a parquet file with configurable compression.
        Automatically creates parent directories if they don't exist.

        Args:
            table: PyArrow table to write.
            path: Output path for parquet file. Can be local path or remote URI.
            compression: Compression codec to use. Supported values: "snappy", "gzip",
                "lz4", "zstd", "brotli", "uncompressed". Default is "snappy".

        Raises:
            Exception: If DuckDB encounters an error writing the parquet file.

        Examples:
            Write with default compression:
            >>> import pyarrow as pa
            >>> table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
            >>> handler = DuckDBParquetHandler()
            >>> handler.write_parquet(table, "/tmp/output.parquet")

            Write with gzip compression:
            >>> handler.write_parquet(table, "/tmp/output.parquet", compression="gzip")

            Write to nested directory:
            >>> handler.write_parquet(table, "/tmp/2024/01/15/data.parquet")

            Write to S3:
            >>> from fsspeckit.storage_options import AwsStorageOptions
            >>> handler = DuckDBParquetHandler(storage_options=AwsStorageOptions(...))
            >>> handler.write_parquet(table, "s3://bucket/output.parquet")
        """
        conn = self._ensure_connection()

        # Ensure parent directory exists
        parent_path = str(Path(path).parent)
        if self._filesystem is not None:
            try:
                if not self._filesystem.exists(parent_path):
                    self._filesystem.makedirs(parent_path, exist_ok=True)
            except Exception:
                # Some filesystems may not support exists/makedirs on remote paths
                # DuckDB will handle the path directly
                pass

        try:
            # Register the table in DuckDB
            conn.register("temp_table", table)

            # Use COPY command to write parquet
            query = f"COPY temp_table TO '{path}' (FORMAT PARQUET, COMPRESSION '{compression}')"
            conn.execute(query)

            # Unregister the temporary table
            conn.unregister("temp_table")

        except Exception as e:
            # Clean up on error
            try:
                conn.unregister("temp_table")
            except Exception:
                pass
            raise Exception(f"Failed to write parquet to '{path}': {e}") from e

    def write_parquet_dataset(
        self,
        table: pa.Table,
        path: str,
        mode: Literal["overwrite", "append"] = "append",
        max_rows_per_file: int | None = None,
        compression: str = "snappy",
        basename_template: str = "part-{}.parquet",
    ) -> None:
        """Write PyArrow table to parquet dataset directory with unique filenames.

        Writes a PyArrow table to a directory as one or more parquet files with
        automatically generated unique filenames. Supports overwrite and append modes
        for managing existing datasets, and can split large tables across multiple files.

        Args:
            table: PyArrow table to write.
            path: Output directory path for the dataset. Can be local or remote URI.
            mode: Write mode. "append" (default) adds files without deleting existing ones.
                "overwrite" deletes existing parquet files before writing.
            max_rows_per_file: Optional maximum rows per file. If specified and table
                has more rows, splits into multiple files. If None, writes single file.
            compression: Compression codec. Supported: "snappy", "gzip", "lz4", "zstd",
                "brotli", "uncompressed". Default is "snappy".
            basename_template: Template for filenames with {} placeholder for unique ID.
                Default is "part-{}.parquet". The {} will be replaced with a short UUID.

        Raises:
            ValueError: If mode is invalid or max_rows_per_file <= 0.
            Exception: If filesystem operations or writing fails.

        Examples:
            Basic dataset write with unique filename:
            >>> import pyarrow as pa
            >>> table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
            >>> with DuckDBParquetHandler() as handler:
            ...     handler.write_parquet_dataset(table, "/tmp/dataset/")
            ...     # Creates: /tmp/dataset/part-a1b2c3d4.parquet

            Append mode (incremental updates):
            >>> # First write
            >>> handler.write_parquet_dataset(table1, "/data/sales/", mode="append")
            >>> # Second write (adds new file)
            >>> handler.write_parquet_dataset(table2, "/data/sales/", mode="append")
            >>> # Read combined dataset
            >>> result = handler.read_parquet("/data/sales/")

            Overwrite mode (replace dataset):
            >>> handler.write_parquet_dataset(
            ...     new_table,
            ...     "/data/output/",
            ...     mode="overwrite"  # Deletes existing parquet files
            ... )

            Split large table across multiple files:
            >>> large_table = pa.table({'id': range(10000), 'value': range(10000)})
            >>> handler.write_parquet_dataset(
            ...     large_table,
            ...     "/data/output/",
            ...     max_rows_per_file=2500  # Creates 4 files
            ... )

            Custom filename template:
            >>> handler.write_parquet_dataset(
            ...     table,
            ...     "/data/output/",
            ...     basename_template="data_{}.parquet"
            ... )
            ... # Creates: data_a1b2c3d4.parquet

            With compression:
            >>> handler.write_parquet_dataset(
            ...     table,
            ...     "/data/output/",
            ...     compression="gzip"
            ... )

            Remote storage (S3):
            >>> from fsspeckit.storage_options import AwsStorageOptions
            >>> handler = DuckDBParquetHandler(storage_options=AwsStorageOptions(...))
            >>> handler.write_parquet_dataset(table, "s3://bucket/dataset/")
        """
        # Validate inputs
        if mode not in ("overwrite", "append"):
            raise ValueError(
                f"Invalid mode: '{mode}'. Must be 'overwrite' or 'append'."
            )

        if max_rows_per_file is not None and max_rows_per_file <= 0:
            raise ValueError(
                f"max_rows_per_file must be > 0, got {max_rows_per_file}"
            )

        conn = self._ensure_connection()

        # Ensure directory exists
        if self._filesystem is not None:
            try:
                if not self._filesystem.exists(path):
                    self._filesystem.makedirs(path, exist_ok=True)
            except Exception as e:
                raise Exception(f"Failed to create dataset directory '{path}': {e}") from e

        # Handle overwrite mode - clear existing parquet files
        if mode == "overwrite":
            self._clear_dataset(path)

        # Determine how many files to write
        if max_rows_per_file is not None and table.num_rows > max_rows_per_file:
            # Split table into multiple files
            num_files = (table.num_rows + max_rows_per_file - 1) // max_rows_per_file

            for i in range(num_files):
                start_idx = i * max_rows_per_file
                end_idx = min((i + 1) * max_rows_per_file, table.num_rows)
                slice_table = table.slice(start_idx, end_idx - start_idx)

                # Generate unique filename
                filename = self._generate_unique_filename(basename_template)
                file_path = str(Path(path) / filename)

                # Write slice to file
                try:
                    conn.register("temp_table", slice_table)
                    query = f"COPY temp_table TO '{file_path}' (FORMAT PARQUET, COMPRESSION '{compression}')"
                    conn.execute(query)
                    conn.unregister("temp_table")
                except Exception as e:
                    try:
                        conn.unregister("temp_table")
                    except Exception:
                        pass
                    raise Exception(
                        f"Failed to write parquet file '{file_path}': {e}"
                    ) from e
        else:
            # Write single file
            filename = self._generate_unique_filename(basename_template)
            file_path = str(Path(path) / filename)

            try:
                conn.register("temp_table", table)
                query = f"COPY temp_table TO '{file_path}' (FORMAT PARQUET, COMPRESSION '{compression}')"
                conn.execute(query)
                conn.unregister("temp_table")
            except Exception as e:
                try:
                    conn.unregister("temp_table")
                except Exception:
                    pass
                raise Exception(
                    f"Failed to write parquet file '{file_path}': {e}"
                ) from e

    def _generate_unique_filename(self, template: str) -> str:
        """Generate unique filename using template with UUID.

        Args:
            template: Filename template with optional {} placeholder.
                If {} is present, replaced with short UUID.
                If no placeholder, UUID is inserted before extension.

        Returns:
            Unique filename string.

        Examples:
            >>> handler._generate_unique_filename("part-{}.parquet")
            'part-a1b2c3d4.parquet'
            >>> handler._generate_unique_filename("data_{}.parquet")
            'data-5e6f7890.parquet'
            >>> handler._generate_unique_filename("file.parquet")
            'file-1a2b3c4d.parquet'
        """
        # Generate short UUID (first 8 characters)
        unique_id = str(uuid.uuid4())[:8]

        if "{}" in template:
            # Template has placeholder
            return template.format(unique_id)
        else:
            # No placeholder - insert UUID before extension
            if "." in template:
                base, ext = template.rsplit(".", 1)
                return f"{base}-{unique_id}.{ext}"
            else:
                # No extension
                return f"{template}-{unique_id}"

    def _clear_dataset(self, path: str) -> None:
        """Clear parquet files from dataset directory.

        Deletes only files with .parquet extension, preserving other files
        like metadata or documentation.

        Args:
            path: Directory path to clear.

        Raises:
            Exception: If clearing fails.
        """
        if self._filesystem is None:
            return

        try:
            if self._filesystem.exists(path):
                # List all files in directory
                files = self._filesystem.ls(path, detail=False)

                # Filter for parquet files only
                parquet_files = [f for f in files if f.endswith('.parquet')]

                # Delete parquet files
                for file in parquet_files:
                    try:
                        self._filesystem.rm(file)
                    except Exception as e:
                        # Log but continue with other files
                        print(f"Warning: Failed to delete '{file}': {e}")

        except Exception as e:
            raise Exception(f"Failed to clear dataset at '{path}': {e}") from e

    def merge_parquet_dataset(
        self,
        source: pa.Table | str,
        target_path: str,
        key_columns: list[str] | str,
        strategy: MergeStrategy = "upsert",
        dedup_order_by: list[str] | None = None,
        compression: str = "snappy",
    ) -> dict[str, int]:
        """Merge source data into target parquet dataset using specified strategy.

        Performs intelligent merge operations on parquet datasets with support for
        UPSERT, INSERT-only, UPDATE-only, FULL_MERGE (sync), and DEDUPLICATE strategies.
        Uses DuckDB's SQL engine for efficient merging with QUALIFY for deduplication.

        Args:
            source: Source data as PyArrow table or path to parquet dataset.
            target_path: Path to target parquet dataset directory.
            key_columns: Column(s) to use for matching records. Can be single column
                name (string) or list of column names for composite keys.
            strategy: Merge strategy to use:
                - "upsert": Insert new records, update existing (default)
                - "insert": Insert only new records, ignore existing
                - "update": Update only existing records, ignore new
                - "full_merge": Insert, update, and delete (full sync with source)
                - "deduplicate": Remove duplicates from source, then upsert
            dedup_order_by: Columns to use for ordering when deduplicating (for
                "deduplicate" strategy). Keeps record with highest value. If None,
                uses first occurrence.
            compression: Compression codec for output. Default is "snappy".

        Returns:
            Dictionary with merge statistics:
                - "inserted": Number of records inserted
                - "updated": Number of records updated  
                - "deleted": Number of records deleted
                - "total": Total records in merged dataset

        Raises:
            ValueError: If strategy invalid, key columns missing, or NULL keys present.
            TypeError: If source/target schemas incompatible.
            Exception: If merge operation fails.

        Examples:
            UPSERT - insert new and update existing:
            >>> with DuckDBParquetHandler() as handler:
            ...     stats = handler.merge_parquet_dataset(
            ...         source=new_data_table,
            ...         target_path="/data/customers/",
            ...         key_columns=["customer_id"],
            ...         strategy="upsert"
            ...     )
            ...     print(f"Inserted: {stats['inserted']}, Updated: {stats['updated']}")

            INSERT - add only new records:
            >>> stats = handler.merge_parquet_dataset(
            ...     source="/staging/new_orders/",
            ...     target_path="/data/orders/",
            ...     key_columns=["order_id"],
            ...     strategy="insert"
            ... )
            ... print(f"Added {stats['inserted']} new orders")

            UPDATE - update existing only:
            >>> stats = handler.merge_parquet_dataset(
            ...     source=product_updates,
            ...     target_path="/data/products/",
            ...     key_columns=["product_id"],
            ...     strategy="update"
            ... )

            FULL_MERGE - complete synchronization:
            >>> stats = handler.merge_parquet_dataset(
            ...     source=authoritative_data,
            ...     target_path="/data/inventory/",
            ...     key_columns=["item_id"],
            ...     strategy="full_merge"
            ... )
            ... print(f"Synced: +{stats['inserted']} -{stats['deleted']}")

            DEDUPLICATE - remove duplicates first:
            >>> stats = handler.merge_parquet_dataset(
            ...     source=raw_data_with_dups,
            ...     target_path="/data/transactions/",
            ...     key_columns=["transaction_id"],
            ...     strategy="deduplicate",
            ...     dedup_order_by=["timestamp"]  # Keep latest
            ... )

            Composite key:
            >>> stats = handler.merge_parquet_dataset(
            ...     source=updates,
            ...     target_path="/data/sales/",
            ...     key_columns=["customer_id", "order_date"],
            ...     strategy="upsert"
            ... )
        """
        # Validate strategy
        valid_strategies = {"upsert", "insert", "update", "full_merge", "deduplicate"}
        if strategy not in valid_strategies:
            raise ValueError(
                f"Invalid strategy: '{strategy}'. Must be one of: {', '.join(sorted(valid_strategies))}"
            )

        # Normalize key_columns to list
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        conn = self._ensure_connection()

        # Load source data
        if isinstance(source, str):
            # Source is path to parquet dataset
            source_table = self.read_parquet(source)
        else:
            # Source is PyArrow table
            source_table = source

        # Load target data (create empty if doesn't exist)
        if self._filesystem is not None and self._filesystem.exists(target_path):
            target_table = self.read_parquet(target_path)
        else:
            # Target doesn't exist - treat as empty dataset with source schema
            target_table = pa.table({col: pa.array([], type=source_table.schema.field(col).type) 
                                    for col in source_table.schema.names})

        # Validate inputs
        self._validate_merge_inputs(source_table, target_table, key_columns)

        # Calculate pre-merge counts for statistics
        target_count_before = target_table.num_rows
        source_count = source_table.num_rows

        # Register tables in DuckDB
        conn.register("source_data", source_table)
        conn.register("target_dataset", target_table)

        # Execute merge based on strategy
        merged_table = self._execute_merge_strategy(
            conn, strategy, key_columns, dedup_order_by
        )

        # Calculate statistics
        stats = self._calculate_merge_stats(
            target_count_before, source_count, merged_table.num_rows, strategy
        )

        # Write merged result back to target (overwrite mode)
        self.write_parquet_dataset(
            merged_table,
            target_path,
            mode="overwrite",
            compression=compression
        )

        # Cleanup
        try:
            conn.unregister("source_data")
            conn.unregister("target_dataset")
            conn.unregister("merged_result")
        except Exception:
            pass

        return stats

    def _validate_merge_inputs(
        self,
        source: pa.Table,
        target: pa.Table,
        key_columns: list[str]
    ) -> None:
        """Validate merge inputs for correctness.

        Args:
            source: Source PyArrow table.
            target: Target PyArrow table.
            key_columns: List of key column names.

        Raises:
            ValueError: If validation fails.
        """
        # Check key columns exist in source
        source_cols = set(source.column_names)
        for key_col in key_columns:
            if key_col not in source_cols:
                raise ValueError(
                    f"Key column '{key_col}' not found in source. "
                    f"Available columns: {', '.join(sorted(source_cols))}"
                )

        # Check key columns exist in target (if target has data)
        if target.num_rows > 0:
            target_cols = set(target.column_names)
            for key_col in key_columns:
                if key_col not in target_cols:
                    raise ValueError(
                        f"Key column '{key_col}' not found in target. "
                        f"Available columns: {', '.join(sorted(target_cols))}"
                    )

            # Check schema compatibility
            for col in source_cols:
                if col in target_cols:
                    source_type = source.schema.field(col).type
                    target_type = target.schema.field(col).type
                    if source_type != target_type:
                        raise TypeError(
                            f"Column '{col}' type mismatch: "
                            f"source={source_type}, target={target_type}"
                        )

            # Check for extra columns
            source_only = source_cols - target_cols
            target_only = target_cols - source_cols
            if source_only or target_only:
                msg_parts = []
                if source_only:
                    msg_parts.append(f"source-only: {', '.join(sorted(source_only))}")
                if target_only:
                    msg_parts.append(f"target-only: {', '.join(sorted(target_only))}")
                raise ValueError(f"Schema mismatch - {'; '.join(msg_parts)}")

        # Check for NULL values in key columns
        for key_col in key_columns:
            source_col = source.column(key_col)
            if source_col.null_count > 0:
                raise ValueError(
                    f"Key column '{key_col}' contains {source_col.null_count} NULL values in source. "
                    f"Key columns must not have NULLs."
                )
            
            if target.num_rows > 0:
                target_col = target.column(key_col)
                if target_col.null_count > 0:
                    raise ValueError(
                        f"Key column '{key_col}' contains {target_col.null_count} NULL values in target. "
                        f"Key columns must not have NULLs."
                    )

    def _execute_merge_strategy(
        self,
        conn: duckdb.DuckDBPyConnection,
        strategy: MergeStrategy,
        key_columns: list[str],
        dedup_order_by: list[str] | None
    ) -> pa.Table:
        """Execute the specified merge strategy using DuckDB SQL.

        Args:
            conn: DuckDB connection.
            strategy: Merge strategy to execute.
            key_columns: List of key column names.
            dedup_order_by: Columns for deduplication ordering.

        Returns:
            Merged PyArrow table.
        """
        # Build JOIN condition
        join_condition = " AND ".join([f"s.{col} = t.{col}" for col in key_columns])

        if strategy == "upsert":
            # Remove target records that will be updated, then add all source
            query = f"""
            SELECT * FROM (
                SELECT t.* FROM target_dataset t
                LEFT JOIN source_data s ON {join_condition}
                WHERE s.{key_columns[0]} IS NULL
                UNION ALL
                SELECT * FROM source_data
            )
            """

        elif strategy == "insert":
            # Add only new records from source
            query = f"""
            SELECT * FROM (
                SELECT * FROM target_dataset
                UNION ALL
                SELECT s.* FROM source_data s
                LEFT JOIN target_dataset t ON {join_condition}
                WHERE t.{key_columns[0]} IS NULL
            )
            """

        elif strategy == "update":
            # Update only existing records
            query = f"""
            SELECT * FROM (
                SELECT t.* FROM target_dataset t
                LEFT JOIN source_data s ON {join_condition}
                WHERE s.{key_columns[0]} IS NULL
                UNION ALL
                SELECT s.* FROM source_data s
                INNER JOIN target_dataset t ON {join_condition}
            )
            """

        elif strategy == "full_merge":
            # Replace target with source (deletes records not in source)
            query = "SELECT * FROM source_data"

        elif strategy == "deduplicate":
            # Deduplicate source using QUALIFY, then UPSERT
            partition_cols = ", ".join(key_columns)
            
            if dedup_order_by:
                order_cols = ", ".join([f"{col} DESC" for col in dedup_order_by])
            else:
                # Default: order by key columns descending
                order_cols = ", ".join([f"{col} DESC" for col in key_columns])

            # First deduplicate source using QUALIFY
            dedup_query = f"""
            CREATE TEMP TABLE deduplicated_source AS
            SELECT * FROM source_data
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY {order_cols}) = 1
            """
            conn.execute(dedup_query)

            # Then perform UPSERT with deduplicated source
            join_condition_dedup = " AND ".join([f"s.{col} = t.{col}" for col in key_columns])
            query = f"""
            SELECT * FROM (
                SELECT t.* FROM target_dataset t
                LEFT JOIN deduplicated_source s ON {join_condition_dedup}
                WHERE s.{key_columns[0]} IS NULL
                UNION ALL
                SELECT * FROM deduplicated_source
            )
            """

        # Execute and return result
        result = conn.execute(query).arrow()
        if hasattr(result, 'read_all'):
            result = result.read_all()
        
        return result

    def _calculate_merge_stats(
        self,
        target_before: int,
        source_count: int,
        target_after: int,
        strategy: MergeStrategy
    ) -> dict[str, int]:
        """Calculate merge statistics.

        Args:
            target_before: Target row count before merge.
            source_count: Source row count.
            target_after: Target row count after merge.
            strategy: Merge strategy used.

        Returns:
            Dictionary with merge statistics.
        """
        stats: dict[str, int] = {
            "total": target_after
        }

        if strategy == "insert":
            # INSERT: only additions, no updates or deletes
            stats["inserted"] = target_after - target_before
            stats["updated"] = 0
            stats["deleted"] = 0

        elif strategy == "update":
            # UPDATE: no additions or deletes
            stats["inserted"] = 0
            stats["updated"] = target_before  # All existing potentially updated
            stats["deleted"] = 0

        elif strategy == "full_merge":
            # FULL_MERGE: source replaces target completely
            stats["inserted"] = source_count
            stats["updated"] = 0
            stats["deleted"] = target_before

        else:  # upsert or deduplicate
            # UPSERT/DEDUPLICATE: additions and updates
            net_change = target_after - target_before
            stats["inserted"] = max(0, net_change)
            stats["updated"] = source_count - stats["inserted"]
            stats["deleted"] = 0

        return stats

    def execute_sql(
        self,
        query: str,
        parameters: list[Any] | None = None,
    ) -> pa.Table:
        """Execute SQL query on parquet data and return results.

        Executes a SQL query using DuckDB and returns the results as a PyArrow table.
        The query can reference parquet files using the `parquet_scan()` function.
        Supports parameterized queries for safe value substitution.

        Args:
            query: SQL query string. Use `parquet_scan('path')` to reference parquet files.
            parameters: Optional list of parameter values for parameterized queries.
                Use `?` placeholders in the query string.

        Returns:
            PyArrow table containing the query results.

        Raises:
            Exception: If DuckDB encounters a SQL syntax error or query execution error.

        Examples:
            Simple query:
            >>> handler = DuckDBParquetHandler()
            >>> result = handler.execute_sql(
            ...     "SELECT * FROM parquet_scan('/tmp/data.parquet') WHERE age > 30"
            ... )

            Parameterized query:
            >>> result = handler.execute_sql(
            ...     "SELECT * FROM parquet_scan('/tmp/data.parquet') WHERE age BETWEEN ? AND ?",
            ...     parameters=[25, 40]
            ... )

            Aggregation query:
            >>> result = handler.execute_sql(
            ...     '''
            ...     SELECT category, COUNT(*) as count, AVG(price) as avg_price
            ...     FROM parquet_scan('/tmp/data.parquet')
            ...     GROUP BY category
            ...     ORDER BY count DESC
            ...     '''
            ... )

            Join multiple parquet files:
            >>> result = handler.execute_sql(
            ...     '''
            ...     SELECT a.*, b.name
            ...     FROM parquet_scan('/tmp/data1.parquet') a
            ...     JOIN parquet_scan('/tmp/data2.parquet') b
            ...     ON a.id = b.id
            ...     '''
            ... )

            Window functions:
            >>> result = handler.execute_sql(
            ...     '''
            ...     SELECT
            ...         date,
            ...         revenue,
            ...         AVG(revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg
            ...     FROM parquet_scan('/tmp/sales.parquet')
            ...     '''
            ... )
        """
        conn = self._ensure_connection()

        try:
            if parameters is not None:
                # Execute parameterized query
                result = conn.execute(query, parameters).arrow()
            else:
                # Execute regular query
                result = conn.execute(query).arrow()
            # Convert RecordBatchReader to Table
            if hasattr(result, 'read_all'):
                result = result.read_all()
            return result
        except Exception as e:
            raise Exception(f"Failed to execute SQL query: {e}\nQuery: {query}") from e

    def close(self) -> None:
        """Close the DuckDB connection.

        This method is called automatically when using the context manager.
        Manual calls are only needed when not using the context manager pattern.
        """
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> "DuckDBParquetHandler":
        """Enter context manager.

        Returns:
            Self for use in with statement.
        """
        self._ensure_connection()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close connection.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        self.close()

    def _collect_dataset_stats(
        self,
        path: str,
        partition_filter: list[str] | None = None,
    ) -> dict[str, Any]:
        """Collect file-level statistics for a parquet dataset.

        Args:
            path: Dataset directory path.
            partition_filter: Optional list of partition prefix filters (e.g. ["date=2025-11-04"]).

        Returns:
            Dict with keys:
                files: list of file info dicts {path, size_bytes, num_rows}
                total_bytes: sum of sizes
                total_rows: sum of rows
        Raises:
            FileNotFoundError: If path does not exist or has no parquet files.
        """
        if self._filesystem is None:
            raise FileNotFoundError(f"Filesystem not initialized for path '{path}'")
        if not self._filesystem.exists(path):
            raise FileNotFoundError(f"Dataset path '{path}' does not exist")
        # Recursive discovery of parquet files
        try:
            entries = self._filesystem.ls(path, detail=False)
        except Exception as e:
            raise Exception(f"Failed listing dataset path '{path}': {e}") from e
        parquet_files: list[str] = []
        stack: list[str] = [path]
        while stack:
            current_dir = stack.pop()
            try:
                cur_entries = self._filesystem.ls(current_dir, detail=False)
            except Exception:
                continue
            for e in cur_entries:
                if e.endswith('.parquet'):
                    parquet_files.append(e)
                else:
                    # Heuristic: treat as directory if it exists and is not a parquet
                    try:
                        if self._filesystem.isdir(e):
                            stack.append(e)
                    except Exception:
                        pass
        if partition_filter:
            parquet_files = [
                f for f in parquet_files
                if any(Path(f).relative_to(Path(path)).as_posix().startswith(pfx) for pfx in partition_filter)
            ]
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found under '{path}' matching filter")
        from pyarrow import parquet as pq  # local import to avoid top-time cost if unused
        file_infos: list[dict[str, Any]] = []
        total_bytes = 0
        total_rows = 0
        for f in parquet_files:
            size = 0
            try:
                info = self._filesystem.info(f)
                size = info.get('size', 0) if isinstance(info, dict) else 0
            except Exception:
                pass
            num_rows = 0
            try:
                fs = self._require_filesystem()
                with fs.open(f, 'rb') as fh:
                    pf = pq.ParquetFile(fh)
                    num_rows = pf.metadata.num_rows
            except Exception:
                try:
                    table = self.read_parquet(f)
                    num_rows = table.num_rows
                except Exception:
                    num_rows = 0
            total_bytes += size
            total_rows += num_rows
            file_infos.append({"path": f, "size_bytes": size, "num_rows": num_rows})
        return {
            "files": file_infos,
            "total_bytes": total_bytes,
            "total_rows": total_rows,
        }

    def compact_parquet_dataset(
        self,
        path: str,
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Compact a parquet dataset directory into fewer larger files.

        Groups small files based on size (MB) and/or row thresholds, rewrites grouped
        files into new parquet files, optionally changing compression. Supports
        dry-run mode returning planned groups without writing.

        Args:
            path: Dataset directory path.
            target_mb_per_file: Desired approximate size per output file (MB).
            target_rows_per_file: Desired maximum rows per output file.
            partition_filter: Optional list of partition prefixes to restrict scope.
            compression: Optional compression codec; defaults to existing or 'snappy'.
            dry_run: If True, plan only without modifying files.

        Returns:
            Statistics dict including before/after counts and optional plan.
        """
        if target_mb_per_file is None and target_rows_per_file is None:
            raise ValueError("Must provide at least one of target_mb_per_file or target_rows_per_file")
        if target_mb_per_file is not None and target_mb_per_file <= 0:
            raise ValueError("target_mb_per_file must be > 0")
        if target_rows_per_file is not None and target_rows_per_file <= 0:
            raise ValueError("target_rows_per_file must be > 0")
        stats_before = self._collect_dataset_stats(path, partition_filter)
        files = stats_before["files"]
        before_file_count = len(files)
        before_total_bytes = stats_before["total_bytes"]
        # Identify candidate files (those below size threshold if size given or all if only rows)
        size_threshold_bytes = target_mb_per_file * 1024 * 1024 if target_mb_per_file else None
        candidates = []
        large_files = []
        for fi in files:
            size_ok = size_threshold_bytes is not None and fi["size_bytes"] < size_threshold_bytes
            if size_threshold_bytes is None:
                size_ok = True  # row-only grouping; consider all
            if size_ok:
                candidates.append(fi)
            else:
                large_files.append(fi)
        # Grouping algorithm
        groups: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_size = 0
        current_rows = 0
        def flush_current():
            nonlocal current, current_size, current_rows
            if current:
                groups.append(current)
                current = []
                current_size = 0
                current_rows = 0
        for fi in sorted(candidates, key=lambda x: x["size_bytes"]):
            size_bytes = fi["size_bytes"]
            num_rows = fi["num_rows"]
            would_exceed_size = size_threshold_bytes is not None and current_size + size_bytes > size_threshold_bytes and current
            would_exceed_rows = target_rows_per_file is not None and current_rows + num_rows > target_rows_per_file and current
            if would_exceed_size or would_exceed_rows:
                flush_current()
            current.append(fi)
            current_size += size_bytes
            current_rows += num_rows
        flush_current()
        # Remove singleton groups that already exceed thresholds (no benefit)
        finalized_groups = [g for g in groups if len(g) > 1]
        compacted_file_count = sum(len(g) for g in finalized_groups)
        planned_groups_paths = [[f["path"] for f in g] for g in finalized_groups]
        if dry_run or not finalized_groups:
            after_file_count_est = len(large_files) + len(finalized_groups) + (before_file_count - compacted_file_count - len(large_files))
            # After_total_bytes unchanged in dry-run
            return {
                "before_file_count": before_file_count,
                "after_file_count": after_file_count_est,
                "before_total_bytes": before_total_bytes,
                "after_total_bytes": before_total_bytes,
                "compacted_file_count": compacted_file_count,
                "rewritten_bytes": sum(f["size_bytes"] for g in finalized_groups for f in g),
                "compression_codec": compression,
                "dry_run": dry_run,
                "planned_groups": planned_groups_paths,
            }
        # Execute compaction
        conn = self._ensure_connection()
        rewritten_bytes = 0
        for group in finalized_groups:
            # Read group into Arrow table
            paths = [fi["path"] for fi in group]
            try:
                # Use DuckDB parquet_scan for efficiency
                scan_list = ",".join([f"'{p}'" for p in paths])
                table = conn.execute(f"SELECT * FROM parquet_scan([{scan_list}])").arrow()
                if hasattr(table, 'read_all'):
                    table = table.read_all()
            except Exception:
                # Fallback to pyarrow
                import pyarrow.parquet as pq
                tables = []
                for p in paths:
                    with self._filesystem.open(p, 'rb') as fh:
                        tables.append(pq.read_table(fh))
                table = pa.concat_tables(tables)
            out_name = self._generate_unique_filename("compact-{}.parquet")
            out_path = str(Path(path) / out_name)
            self.write_parquet(table, out_path, compression=compression or "snappy")
            rewritten_bytes += sum(f["size_bytes"] for f in group)
            # Remove originals
            for f in paths:
                try:
                    self._filesystem.rm(f)
                except Exception as e:
                    print(f"Warning: failed to delete '{f}': {e}")
        # Recompute stats after write
        stats_after = self._collect_dataset_stats(path, partition_filter=None)
        return {
            "before_file_count": before_file_count,
            "after_file_count": len(stats_after["files"]),
            "before_total_bytes": before_total_bytes,
            "after_total_bytes": stats_after["total_bytes"],
            "compacted_file_count": compacted_file_count,
            "rewritten_bytes": rewritten_bytes,
            "compression_codec": compression or "snappy",
            "dry_run": False,
        }

    def optimize_parquet_dataset(
        self,
        path: str,
        zorder_columns: list[str],
        target_mb_per_file: int | None = None,
        target_rows_per_file: int | None = None,
        partition_filter: list[str] | None = None,
        compression: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Optimize a parquet dataset by clustering (approximate z-order) and optional compaction.

        Reads dataset, orders rows by given columns, optionally groups into sized chunks
        similar to compaction, rewrites dataset (overwrite semantics). Supports dry-run.

        Args:
            path: Dataset directory path.
            zorder_columns: Columns to cluster by (must exist).
            target_mb_per_file: Optional desired size per output file.
            target_rows_per_file: Optional desired row cap per output file.
            partition_filter: Optional list of partition prefixes.
            compression: Optional compression codec for output files.
            dry_run: If True, plan only.

        Returns:
            Statistics dict; may include planned grouping if dry-run.
        """
        if not zorder_columns:
            raise ValueError("zorder_columns must be a non-empty list")
        stats_before = self._collect_dataset_stats(path, partition_filter)
        files = stats_before["files"]
        before_file_count = len(files)
        before_total_bytes = stats_before["total_bytes"]
        # Validate columns exist
        # Load a sample table to inspect schema
        sample_table = self.read_parquet(files[0]["path"])  # first file
        schema_cols = set(sample_table.column_names)
        missing = [c for c in zorder_columns if c not in schema_cols]
        if missing:
            raise ValueError(
                f"Missing z-order columns: {', '.join(missing)}. Available: {', '.join(sorted(schema_cols))}"
            )
        # Dry-run grouping estimation: assume entire dataset loaded then split by thresholds
        if dry_run:
            size_threshold_bytes = target_mb_per_file * 1024 * 1024 if target_mb_per_file else None
            planned_groups: list[list[str]] = []
            current_group: list[str] = []
            current_size = 0
            current_rows = 0
            for fi in files:
                size_b = fi["size_bytes"]
                rows = fi["num_rows"]
                would_exceed_size = size_threshold_bytes is not None and current_size + size_b > size_threshold_bytes and current_group
                would_exceed_rows = target_rows_per_file is not None and current_rows + rows > target_rows_per_file and current_group
                if would_exceed_size or would_exceed_rows:
                    planned_groups.append(current_group)
                    current_group = []
                    current_size = 0
                    current_rows = 0
                current_group.append(fi["path"])
                current_size += size_b
                current_rows += rows
            if current_group:
                planned_groups.append(current_group)
            return {
                "before_file_count": before_file_count,
                "after_file_count": len(planned_groups),
                "before_total_bytes": before_total_bytes,
                "after_total_bytes": before_total_bytes,
                "compacted_file_count": before_file_count,  # all will be rewritten
                "rewritten_bytes": before_total_bytes,
                "compression_codec": compression,
                "dry_run": True,
                "zorder_columns": zorder_columns,
                "planned_groups": planned_groups,
            }
        # Full optimize execution
        conn = self._ensure_connection()
        all_paths_sql = ",".join([f"'{fi['path']}'" for fi in files])
        # ORDER BY columns with NULL handling
        order_clause_parts = []
        for col in zorder_columns:
            # Put NULLs last
            order_clause_parts.append(f"({col} IS NULL) ASC")
            order_clause_parts.append(f"{col} ASC")
        order_clause = ", ".join(order_clause_parts)
        query = f"SELECT * FROM parquet_scan([{all_paths_sql}]) ORDER BY {order_clause}"  # simple composite ordering
        ordered_table = conn.execute(query).arrow()
        if hasattr(ordered_table, 'read_all'):
            ordered_table = ordered_table.read_all()
        # Determine chunking post-order
        size_threshold_bytes = target_mb_per_file * 1024 * 1024 if target_mb_per_file else None
        chunks: list[pa.Table] = []
        if target_rows_per_file and target_rows_per_file > 0:
            # Row-based splitting
            num_rows = ordered_table.num_rows
            for start in range(0, num_rows, target_rows_per_file):
                end = min(start + target_rows_per_file, num_rows)
                chunks.append(ordered_table.slice(start, end - start))
        else:
            chunks = [ordered_table]
        # If size threshold provided without row threshold, we approximate by rows proportionally
        if size_threshold_bytes and target_rows_per_file is None and ordered_table.num_rows > 0:
            # Estimate average bytes per row
            avg_bytes_per_row = before_total_bytes / max(ordered_table.num_rows, 1)
            est_rows_per_chunk = int(size_threshold_bytes / max(avg_bytes_per_row, 1))
            if est_rows_per_chunk > 0 and est_rows_per_chunk < ordered_table.num_rows:
                chunks = []
                for start in range(0, ordered_table.num_rows, est_rows_per_chunk):
                    end = min(start + est_rows_per_chunk, ordered_table.num_rows)
                    chunks.append(ordered_table.slice(start, end - start))
        # Rewrite only filtered subset (do not clear entire dataset)
        compression_codec = compression or "snappy"
        written_paths: list[str] = []
        # Delete original filtered files
        for fi in files:
            try:
                self._filesystem.rm(fi["path"])
            except Exception:
                pass
        for chunk in chunks:
            filename = self._generate_unique_filename("optimized-{}.parquet")
            out_path = str(Path(path) / filename)
            self.write_parquet(chunk, out_path, compression=compression_codec)
            written_paths.append(out_path)
        # Collect stats again for filtered subset only
        stats_after = self._collect_dataset_stats(path, partition_filter=partition_filter)
        return {
            "before_file_count": before_file_count,
            "after_file_count": len(stats_after["files"]),
            "before_total_bytes": before_total_bytes,
            "after_total_bytes": stats_after["total_bytes"],
            "compacted_file_count": before_file_count,  # all filtered files were rewritten
            "rewritten_bytes": before_total_bytes,
            "compression_codec": compression_codec,
            "dry_run": False,
            "zorder_columns": zorder_columns,
        }

    def __del__(self) -> None:
        """Cleanup on deletion."""
        self.close()
