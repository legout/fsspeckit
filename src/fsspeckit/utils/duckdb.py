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

from fsspeckit.core.merge import (
    MergeStrategy as CoreMergeStrategy,
    MergeStats,
    calculate_merge_stats,
    check_null_keys,
    normalize_key_columns,
    validate_merge_inputs,
    validate_strategy_compatibility,
)

if TYPE_CHECKING:
    from ..storage_options.base import BaseStorageOptions


# Type alias for merge strategies (for backward compatibility)
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

        # Check if path exists before executing DuckDB query
        if self._filesystem is not None:
            if not self._filesystem.exists(path):
                raise FileNotFoundError(f"Parquet path '{path}' does not exist")

        # Build column selection clause
        columns_clause = "*" if columns is None else ", ".join(columns)

        # Build query to read parquet
        query = f"SELECT {columns_clause} FROM parquet_scan('{path}')"

        try:
            # Execute query and return as PyArrow table
            result = conn.execute(query).arrow()
            # Convert RecordBatchReader to Table
            if hasattr(result, "read_all"):
                result = result.read_all()
            return result
        except Exception as e:
            # Preserve original error type and message when possible
            error_msg = str(e)
            if (
                "does not exist" in error_msg.lower()
                or "not found" in error_msg.lower()
            ):
                raise FileNotFoundError(
                    f"Parquet path '{path}' does not exist: {error_msg}"
                ) from e
            else:
                # Re-raise with original exception type preserved
                raise type(e)(
                    f"Failed to read parquet from '{path}': {error_msg}"
                ) from e

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

        # Ensure parent directory exists and is not a file
        parent_path = str(Path(path).parent)
        if self._filesystem is not None:
            # Check if parent path exists and is a file (not directory)
            if self._filesystem.exists(parent_path):
                if not self._filesystem.isdir(parent_path):
                    raise NotADirectoryError(
                        f"Parent directory '{parent_path}' exists but is a file. Cannot create file '{path}'."
                    )

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
            raise ValueError(f"max_rows_per_file must be > 0, got {max_rows_per_file}")

        conn = self._ensure_connection()

        # Ensure directory exists
        if self._filesystem is not None:
            # Check if path exists and is a file (not directory)
            if self._filesystem.exists(path):
                if not self._filesystem.isdir(path):
                    raise NotADirectoryError(
                        f"Dataset path '{path}' exists but is a file. Dataset paths must be directories."
                    )

            try:
                if not self._filesystem.exists(path):
                    self._filesystem.makedirs(path, exist_ok=True)
            except Exception as e:
                raise Exception(
                    f"Failed to create dataset directory '{path}': {e}"
                ) from e

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
                parquet_files = [f for f in files if f.endswith(".parquet")]

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
        progress_callback: callable[[str, int, int], None] | None = None,
    ) -> dict[str, int]:
        """Merge source data into target parquet dataset using specified strategy.

        Performs intelligent merge operations on parquet datasets with support for
        UPSERT, INSERT-only, UPDATE-only, FULL_MERGE (sync), and DEDUPLICATE strategies.
        Uses DuckDB's SQL engine for efficient merging with QUALIFY for deduplication.

        This implementation uses shared merge validation and semantics from fsspeckit.core.merge
        to ensure consistent behavior across all backends. All merge strategies share the same
        semantics, validation rules, and statistical calculations as the PyArrow implementation.

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
            progress_callback: Optional callback function for progress tracking.
                Called with (stage, current, total) where stage is a string description.

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

        Note:
            This implementation uses shared merge validation and semantics from fsspeckit.core.merge
            to ensure consistent behavior across all backends. Atomic operations are performed using
            temporary directories with backup-and-restore for error recovery.
        """
        # Convert string strategy to core enum and validate
        try:
            core_strategy = CoreMergeStrategy(strategy)
        except ValueError:
            valid_strategies = {s.value for s in CoreMergeStrategy}
            raise ValueError(
                f"Invalid strategy: '{strategy}'. Must be one of: {', '.join(sorted(valid_strategies))}"
            )

        # Normalize key_columns using shared helper
        normalized_keys = normalize_key_columns(key_columns)

        conn = self._ensure_connection()

        # Report progress start
        if progress_callback:
            progress_callback("Loading source data", 0, 1)

        # Load source data
        if isinstance(source, str):
            # Source is path to parquet dataset
            source_table = self.read_parquet(source)
        else:
            # Source is PyArrow table
            source_table = source

        # Report progress for source loading
        if progress_callback:
            progress_callback("Loading target data", 1, 4)

        # Load target data (create empty if doesn't exist)
        target_schema = None
        target_table = None
        if self._filesystem is not None and self._filesystem.exists(target_path):
            target_table = self.read_parquet(target_path)
            target_schema = target_table.schema

        # Report progress for validation
        if progress_callback:
            progress_callback("Validating inputs", 2, 4)

        # Validate inputs using shared helpers
        merge_plan = validate_merge_inputs(
            source_table.schema, target_schema, normalized_keys, core_strategy
        )
        merge_plan.source_count = source_table.num_rows

        # Validate strategy compatibility
        validate_strategy_compatibility(
            core_strategy, source_table.num_rows, target_table is not None
        )

        # Check for NULL keys using shared helper
        check_null_keys(source_table, target_table, normalized_keys)

        # Calculate pre-merge counts for statistics
        target_count_before = target_table.num_rows if target_table else 0
        source_count = source_table.num_rows

        # Create empty target table if needed
        if target_table is None:
            target_table = pa.table(
                {
                    col: pa.array([], type=source_table.schema.field(col).type)
                    for col in source_table.schema.names
                }
            )

        # Register tables in DuckDB
        conn.register("source_data", source_table)
        conn.register("target_dataset", target_table)

        # Report progress for merge execution
        if progress_callback:
            progress_callback("Executing merge strategy", 3, 4)

        # Configure DuckDB for optimal merge performance
        conn.execute("SET memory_limit='1GB'")
        conn.execute("SET threads=4")

        # Enable DuckDB's parallel processing for large datasets
        if source_table.num_rows > 100000:
            conn.execute("SET enable_progress_bar=true")
            conn.execute("SET preserve_insertion_order=false")

        # Execute merge based on strategy
        merged_table = self._execute_merge_strategy(
            conn, core_strategy, normalized_keys, dedup_order_by
        )

        # Calculate statistics using shared helper
        merge_stats = calculate_merge_stats(
            core_strategy, source_count, target_count_before, merged_table.num_rows
        )
        stats = merge_stats.to_dict()

        # Report progress for writing results
        if progress_callback:
            progress_callback("Writing merged results", 4, 4)

        # Write merged result to temporary directory first, then move for atomicity
        import tempfile
        import shutil

        with tempfile.TemporaryDirectory(prefix=f"merge_{uuid.uuid4().hex[:8]}_") as temp_dir:
            temp_path = Path(temp_dir) / "merged_dataset"
            temp_path_str = str(temp_path)

            # Write merged result to temporary location
            self.write_parquet_dataset(
                merged_table, temp_path_str, mode="overwrite", compression=compression
            )

            # Atomic move: if target exists, remove it first, then move temp data
            if self._filesystem is not None and self._filesystem.exists(target_path):
                # Make a backup in case something goes wrong
                backup_path = f"{target_path}.backup.{uuid.uuid4().hex[:8]}"
                try:
                    # Rename existing target to backup
                    self._filesystem.mv(target_path, backup_path)
                    # Move temp data to final location
                    self._filesystem.mv(temp_path_str, target_path)
                    # Remove backup after successful move
                    self._filesystem.rm(backup_path, recursive=True)
                except Exception as e:
                    # Try to restore backup if move failed
                    if self._filesystem.exists(backup_path):
                        self._filesystem.mv(backup_path, target_path)
                    raise Exception(f"Atomic merge failed: {e}")
            else:
                # No existing target, just move temp data
                self._filesystem.mv(temp_path_str, target_path)

        # Cleanup
        try:
            conn.unregister("source_data")
            conn.unregister("target_dataset")
            conn.unregister("merged_result")
        except Exception:
            pass

        return stats

    
    def _execute_merge_strategy(
        self,
        conn: duckdb.DuckDBPyConnection,
        strategy: CoreMergeStrategy,
        key_columns: list[str],
        dedup_order_by: list[str] | None,
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

        if strategy == CoreMergeStrategy.UPSERT:
            # Optimized UPSERT using CTE and better query plan
            query = f"""
            WITH source_existing AS (
                SELECT s.* FROM source_data s
                INNER JOIN target_dataset t ON {join_condition}
            ),
            source_new AS (
                SELECT s.* FROM source_data s
                LEFT JOIN target_dataset t ON {join_condition}
                WHERE t.{key_columns[0]} IS NULL
            ),
            target_unchanged AS (
                SELECT t.* FROM target_dataset t
                LEFT JOIN source_data s ON {join_condition}
                WHERE s.{key_columns[0]} IS NULL
            )
            SELECT * FROM target_unchanged
            UNION ALL
            SELECT * FROM source_existing
            UNION ALL
            SELECT * FROM source_new
            """

        elif strategy == CoreMergeStrategy.INSERT:
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

        elif strategy == CoreMergeStrategy.UPDATE:
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

        elif strategy == CoreMergeStrategy.FULL_MERGE:
            # Replace target with source (deletes records not in source)
            query = "SELECT * FROM source_data"

        elif strategy == CoreMergeStrategy.DEDUPLICATE:
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
            join_condition_dedup = " AND ".join(
                [f"s.{col} = t.{col}" for col in key_columns]
            )
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
        if hasattr(result, "read_all"):
            result = result.read_all()

        return result

    
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
            if hasattr(result, "read_all"):
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
        """Collect file-level statistics for a parquet dataset using shared core logic.

        This method delegates to the shared ``fsspeckit.core.maintenance.collect_dataset_stats``
        function, ensuring consistent dataset discovery and statistics across both DuckDB
        and PyArrow backends.

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

        Note:
            This is a thin wrapper around the shared core function. See
            :func:`fsspeckit.core.maintenance.collect_dataset_stats` for the
            authoritative implementation.
        """
        from fsspeckit.core.maintenance import collect_dataset_stats

        fs = self._require_filesystem()
        return collect_dataset_stats(
            path=path,
            filesystem=fs,
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
    ) -> dict[str, Any]:
        """Compact a parquet dataset directory into fewer larger files using shared planning.

        This function delegates compaction planning to the shared core module,
        ensuring consistent behavior across DuckDB and PyArrow backends.

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
            Statistics dict following canonical MaintenanceStats format, including
            before/after counts and optional plan when dry_run=True.

        Note:
            This function delegates dataset discovery and compaction planning to the
            shared ``fsspeckit.core.maintenance`` module for consistent behavior
            with the PyArrow backend.
        """
        from fsspeckit.core.maintenance import plan_compaction_groups, MaintenanceStats

        if self._filesystem is None:
            raise FileNotFoundError("Filesystem not initialized for compaction")
        filesystem = self._filesystem

        # Get dataset stats using shared logic
        stats_before = self._collect_dataset_stats(path, partition_filter)
        files = stats_before["files"]

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

        if dry_run or not groups:
            return planned_stats.to_dict()

        # Execute compaction using DuckDB
        conn = self._ensure_connection()
        rewritten_bytes = 0

        for group in groups:
            # Read group into Arrow table using DuckDB for efficiency
            paths = [file_info.path for file_info in group.files]
            try:
                scan_list = ",".join([f"'{p}'" for p in paths])
                table = conn.execute(
                    f"SELECT * FROM parquet_scan([{scan_list}])"
                ).arrow()
                if hasattr(table, "read_all"):
                    table = table.read_all()
            except Exception:
                # Fallback to pyarrow
                import pyarrow.parquet as pq

                tables = []
                for p in paths:
                    with filesystem.open(p, "rb") as fh:
                        tables.append(pq.read_table(fh))
                table = pa.concat_tables(tables)

            # Write compacted file
            out_name = self._generate_unique_filename("compact-{}.parquet")
            out_path = str(Path(path) / out_name)
            self.write_parquet(table, out_path, compression=compression or "snappy")

            rewritten_bytes += group.total_size_bytes

            # Remove original files
            for file_info in group.files:
                try:
                    filesystem.rm(file_info.path)
                except Exception as e:
                    print(f"Warning: failed to delete '{file_info.path}': {e}")

        # Recompute stats after compaction
        stats_after = self._collect_dataset_stats(path, partition_filter=None)

        # Create final stats
        final_stats = MaintenanceStats(
            before_file_count=planned_stats.before_file_count,
            after_file_count=len(stats_after["files"]),
            before_total_bytes=planned_stats.before_total_bytes,
            after_total_bytes=stats_after["total_bytes"],
            compacted_file_count=planned_stats.compacted_file_count,
            rewritten_bytes=rewritten_bytes,
            compression_codec=compression or "snappy",
            dry_run=False,
        )

        return final_stats.to_dict()

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
        """Optimize a parquet dataset by clustering (approximate z-order) using shared planning.

        This function delegates optimization planning to the shared core module,
        ensuring consistent behavior across DuckDB and PyArrow backends.

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
            Statistics dict following canonical MaintenanceStats format; may include
            planned grouping if dry-run=True.

        Note:
            This function delegates optimization planning and validation to the
            shared ``fsspeckit.core.maintenance.plan_optimize_groups`` function,
            ensuring consistent behavior with the PyArrow backend.
        """
        from fsspeckit.core.maintenance import plan_optimize_groups, MaintenanceStats

        if not zorder_columns:
            raise ValueError("zorder_columns must be a non-empty list")
        if self._filesystem is None:
            raise FileNotFoundError("Filesystem not initialized for optimization")
        filesystem = self._filesystem

        # Get dataset stats using shared logic
        stats_before = self._collect_dataset_stats(path, partition_filter)
        files = stats_before["files"]

        # Load a sample table to inspect schema for z-order validation
        sample_table = self.read_parquet(files[0]["path"])  # first file

        # Use shared optimization planning with schema validation
        plan_result = plan_optimize_groups(
            file_infos=files,
            zorder_columns=zorder_columns,
            target_mb_per_file=target_mb_per_file,
            target_rows_per_file=target_rows_per_file,
            sample_schema=sample_table.schema,
        )

        groups = plan_result["groups"]
        planned_stats = plan_result["planned_stats"]

        # Update planned stats with compression info
        planned_stats.compression_codec = compression
        planned_stats.dry_run = dry_run

        if dry_run or not groups:
            return planned_stats.to_dict()

        # Execute optimization using DuckDB
        conn = self._ensure_connection()
        compression_codec = compression or "snappy"
        rewritten_bytes = 0

        def _quote_identifier(identifier: str) -> str:
            escaped = identifier.replace('"', '""')
            return f'"{escaped}"'

        # Helper function to build ORDER BY clause with NULL handling
        def _build_order_clause(columns: list[str]) -> str:
            order_parts = []
            for col in columns:
                quoted = _quote_identifier(col)
                # Put NULLs last
                order_parts.append(f"({quoted} IS NULL) ASC")
                order_parts.append(f"{quoted} ASC")
            return ", ".join(order_parts)

        order_clause = _build_order_clause(zorder_columns)

        # Process each group separately for more memory-efficient operation
        written_paths: list[str] = []
        for group_idx, group in enumerate(groups):
            # Read group files and sort by z-order columns
            paths = [file_info.path for file_info in group.files]
            all_paths_sql = ",".join([f"'{p}'" for p in paths])

            query = f"SELECT * FROM parquet_scan([{all_paths_sql}]) ORDER BY {order_clause}"
            ordered_table = conn.execute(query).arrow()
            if hasattr(ordered_table, "read_all"):
                ordered_table = ordered_table.read_all()

            # Apply chunking within the group if needed
            if target_rows_per_file and target_rows_per_file > 0:
                # Row-based splitting
                num_rows = ordered_table.num_rows
                chunks = []
                for start in range(0, num_rows, target_rows_per_file):
                    end = min(start + target_rows_per_file, num_rows)
                    chunk = ordered_table.slice(start, end - start)
                    if chunk.num_rows > 0:
                        chunks.append(chunk)
            else:
                chunks = [ordered_table]

            # Write optimized chunks
            for chunk_idx, chunk in enumerate(chunks):
                if len(groups) == 1:
                    # Single group case - use simple naming
                    filename = f"optimized-{chunk_idx:05d}.parquet"
                else:
                    # Multiple groups - include group index
                    filename = f"optimized-{group_idx:02d}-{chunk_idx:05d}.parquet"

                out_path = str(Path(path) / filename)
                self.write_parquet(chunk, out_path, compression=compression_codec)
                written_paths.append(out_path)

            rewritten_bytes += group.total_size_bytes

            # Remove original files in this group
            for file_info in group.files:
                try:
                    filesystem.rm(file_info.path)
                except Exception:
                    pass

        # Recompute stats after optimization
        stats_after = self._collect_dataset_stats(path, partition_filter=partition_filter)

        # Create final stats
        final_stats = MaintenanceStats(
            before_file_count=planned_stats.before_file_count,
            after_file_count=len(stats_after["files"]),
            before_total_bytes=planned_stats.before_total_bytes,
            after_total_bytes=stats_after["total_bytes"],
            compacted_file_count=planned_stats.compacted_file_count,
            rewritten_bytes=rewritten_bytes,
            compression_codec=compression_codec,
            dry_run=False,
            zorder_columns=zorder_columns,
        )

        return final_stats.to_dict()

    def __del__(self) -> None:
        """Cleanup on deletion."""
        self.close()
