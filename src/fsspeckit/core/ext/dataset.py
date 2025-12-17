"""Dataset creation helpers for fsspec filesystems.

This module contains functions for creating PyArrow datasets with support for:
- Schema enforcement
- Partitioning
- Format-specific optimizations
- Predicate pushdown
- Merge-aware writes

The functions in this module implement the DatasetHandler protocol to provide
a consistent interface across different backend implementations (DuckDB, PyArrow, etc.).
"""

from __future__ import annotations

import posixpath
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.dataset as pds

    from fsspeckit.datasets.interfaces import DatasetHandler

from fsspec import AbstractFileSystem

from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)


def pyarrow_dataset(
    self: AbstractFileSystem,
    path: str,
    format: str = "parquet",
    schema: pa.Schema | None = None,
    partitioning: str | list[str] | pds.Partitioning = None,
    **kwargs: Any,
) -> pds.Dataset:
    """Create a PyArrow dataset from files in any supported format.

    Creates a dataset that provides optimized reading and querying capabilities
    including:
    - Schema inference and enforcement
    - Partition discovery and pruning
    - Predicate pushdown
    - Column projection

    Args:
        path: Base path to dataset files
        format: File format. Currently supports:
            - "parquet" (default)
            - "csv"
            - "json" (experimental)
        schema: Optional schema to enforce. If None, inferred from data.
        partitioning: How the dataset is partitioned. Can be:
            - str: Single partition field
            - list[str]: Multiple partition fields
            - pds.Partitioning: Custom partitioning scheme
        **kwargs: Additional arguments for dataset creation

    Returns:
        pds.Dataset: PyArrow dataset instance

    Example:
        ```python
        fs = LocalFileSystem()

        # Simple Parquet dataset
        ds = fs.pyarrow_dataset("data/")
        print(ds.schema)

        # Partitioned dataset
        ds = fs.pyarrow_dataset(
            "events/",
            partitioning=["year", "month"],
        )
        # Query with partition pruning
        table = ds.to_table(filter=(ds.field("year") == 2024))

        # CSV with schema
        ds = fs.pyarrow_dataset(
            "logs/",
            format="csv",
            schema=pa.schema(
                [
                    ("timestamp", pa.timestamp("s")),
                    ("level", pa.string()),
                    ("message", pa.string()),
                ],
            ),
        )
        ```
    """
    return pds.dataset(
        path,
        filesystem=self,
        partitioning=partitioning,
        schema=schema,
        format=format,
        **kwargs,
    )


def pyarrow_parquet_dataset(
    self: AbstractFileSystem,
    path: str,
    schema: pa.Schema | None = None,
    partitioning: str | list[str] | pds.Partitioning = None,
    **kwargs: Any,
) -> pds.Dataset:
    """Create a PyArrow dataset optimized for Parquet files.

    Creates a dataset specifically for Parquet data, automatically handling
    _metadata files for optimized reading.

    This function is particularly useful for:
    - Datasets with existing _metadata files
    - Multi-file datasets that should be treated as one
    - Partitioned Parquet datasets

    Args:
        path: Path to dataset directory or _metadata file
        schema: Optional schema to enforce. If None, inferred from data.
        partitioning: How the dataset is partitioned. Can be:
            - str: Single partition field
            - list[str]: Multiple partition fields
            - pds.Partitioning: Custom partitioning scheme
        **kwargs: Additional dataset arguments

    Returns:
        pds.Dataset: PyArrow dataset instance

    Example:
        ```python
        fs = LocalFileSystem()

        # Dataset with _metadata
        ds = fs.pyarrow_parquet_dataset("data/_metadata")
        print(ds.files)  # Shows all data files

        # Partitioned dataset directory
        ds = fs.pyarrow_parquet_dataset(
            "sales/",
            partitioning=["year", "region"],
        )
        # Query with partition pruning
        table = ds.to_table(
            filter=(
                (ds.field("year") == 2024)
                & (ds.field("region") == "EMEA")
            ),
        )
        ```
    """
    if not self.isfile(path):
        path = posixpath.join(path, "_metadata")
    return pds.parquet_dataset(
        path,
        filesystem=self,
        partitioning=partitioning,
        schema=schema,
        **kwargs,
    )


def write_pyarrow_dataset(
    self: AbstractFileSystem,
    data: Any,
    path: str,
    basename: str | None = None,
    schema: pa.Schema | None = None,
    partition_by: str | list[str] | pds.Partitioning | None = None,
    partitioning_flavor: str = "hive",
    mode: str = "append",
    format: str | None = "parquet",
    compression: str = "zstd",
    max_rows_per_file: int | None = 2_500_000,
    row_group_size: int | None = 250_000,
    concat: bool = True,
    unique: bool | str | list[str] = False,
    strategy: str | None = None,
    key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    verbose: bool = False,
    **kwargs: Any,
) -> list[Any] | None:
    """Write a DataFrame/Table to a PyArrow dataset with optional merge strategies.

    This function provides dataset writing with support for:
    - Standard dataset writing (partitioning, compression, etc.)
    - Merge-aware writes with various strategies (insert, upsert, update, full_merge, deduplicate)
    - Batch processing for large datasets
    - Flexible data input formats

    When ``strategy`` is provided, the function delegates to merge logic to apply
    merge semantics directly on the incoming data. This allows for one-step merge
    operations without requiring separate staging and merge steps.

    Args:
        data: Input data in various formats:
            - Polars DataFrame/LazyFrame
            - PyArrow Table/RecordBatch/RecordBatchReader
            - Pandas DataFrame
            - List of DataFrames/Tables
        path: Path to write the dataset
        basename: Basename template for files (default: part-{i}.parquet)
        schema: Optional schema to enforce
        partition_by: Partition columns or partitioning scheme
        partitioning_flavor: Partitioning flavor (default: 'hive')
        mode: Write mode (default: 'append')
        format: Output format (default: 'parquet')
        compression: Compression algorithm (default: 'zstd')
        max_rows_per_file: Maximum rows per file (default: 2,500,000)
        row_group_size: Row group size (default: 250,000)
        concat: Concatenate multiple inputs (default: True)
        unique: Remove duplicates (default: False)
        strategy: Optional merge strategy:
            - 'insert': Only insert new records
            - 'upsert': Insert or update existing records
            - 'update': Only update existing records
            - 'full_merge': Full replacement with source
            - 'deduplicate': Remove duplicates then upsert
        key_columns: Key columns for merge operations (required for relational strategies)
        dedup_order_by: Columns to order by for deduplication (default: key_columns)
        verbose: Print progress information
        **kwargs: Additional arguments for pds.write_dataset()

    Returns:
        List of Parquet file metadata or None

    Raises:
        ValueError: If strategy is invalid or required parameters are missing
        FileNotFoundError: If target dataset doesn't exist for update/upsert

    Example:
        ```python
        fs = LocalFileSystem()

        # Standard dataset write
        fs.write_pyarrow_dataset(
            data=table,
            path="dataset/",
            partition_by=["year", "month"]
        )

        # Merge-aware write with upsert
        fs.write_pyarrow_dataset(
            data=new_data,
            path="dataset/",
            strategy="upsert",
            key_columns=["id"]
        )

        # Convenience helpers (also available)
        fs.upsert_dataset(
            data=new_data,
            path="dataset/",
            key_columns=["id"]
        )
        ```
    """
    raise NotImplementedError(
        "write_pyarrow_dataset has been removed. Use PyarrowDatasetIO.write_dataset "
        "or PyarrowDatasetIO.merge instead."
    )
    from fsspeckit.common.types import to_pyarrow_table

    # Import merge strategy validation
    from fsspeckit.core.merge import (
        MergeStrategy,
        validate_merge_inputs,
        validate_strategy_compatibility,
    )
    from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow

    # Convert data to PyArrow table
    table = to_pyarrow_table(data, concat=concat, unique=unique)

    # Ensure we have a single table
    if isinstance(table, list):
        # to_pyarrow_table with concat=True should already handle this
        # but let's be defensive
        if len(table) == 1:
            table = table[0]
        else:
            import pyarrow as pa

            table = pa.concat_tables(table, promote_options="permissive")

    # Apply schema if provided
    if schema is not None:
        from fsspeckit.common.schema import cast_schema

        table = cast_schema(table, schema)

    # If no strategy provided, use standard write
    if strategy is None:
        return _write_pyarrow_dataset_standard(
            self=self,
            table=table,
            path=path,
            basename=basename,
            partition_by=partition_by,
            partitioning_flavor=partitioning_flavor,
            mode=mode,
            format=format,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            row_group_size=row_group_size,
            **kwargs,
        )

    # Handle merge-aware write
    if verbose:
        logger.info("Using merge strategy: %s", strategy)

    # Validate and normalize strategy
    try:
        strategy_enum = MergeStrategy(strategy)
    except ValueError:
        valid_strategies = [s.value for s in MergeStrategy]
        raise ValueError(
            f"Invalid strategy '{strategy}'. Valid strategies: {', '.join(valid_strategies)}"
        )

    # Validate key columns for merge strategies
    from fsspeckit.core.merge import normalize_key_columns

    if key_columns is not None:
        key_columns = normalize_key_columns(key_columns)

    # Check if target exists
    target_exists = self.exists(path) and any(
        self.glob(posixpath.join(path, "**", "*.parquet"))
    )

    # Validate strategy compatibility
    validate_strategy_compatibility(
        strategy=strategy_enum,
        source_count=table.num_rows,
        target_exists=target_exists,
    )

    # For strategies that need a target dataset
    if strategy_enum in [
        MergeStrategy.INSERT,
        MergeStrategy.UPDATE,
        MergeStrategy.UPSERT,
    ]:
        if not target_exists:
            # If target doesn't exist, INSERT and UPSERT become simple writes
            # UPDATE would fail but we already validated above
            if verbose:
                logger.info("Target doesn't exist, using simple write for %s", strategy)
            return _write_pyarrow_dataset_standard(
                self=self,
                table=table,
                path=path,
                basename=basename,
                partition_by=partition_by,
                partitioning_flavor=partitioning_flavor,
                mode=mode,
                format=format,
                compression=compression,
                max_rows_per_file=max_rows_per_file,
                row_group_size=row_group_size,
                **kwargs,
            )

    # Use merge_parquet_dataset_pyarrow for merge operations
    # We need to write source to a temporary location first
    with tempfile.TemporaryDirectory() as temp_dir:
        # Write source data to temp directory
        temp_source = posixpath.join(temp_dir, "source.parquet")

        # Use standard write for the source
        _write_pyarrow_dataset_standard(
            self=self,
            table=table,
            path=temp_source,
            basename="source-{i}.parquet",
            partition_by=None,
            mode="overwrite",
            format=format,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            row_group_size=row_group_size,
            **kwargs,
        )

        # Determine target path for merge
        merge_target = path if target_exists else None

        # Normalize dedup_order_by
        merge_dedup_order_by = None
        if dedup_order_by is not None:
            merge_dedup_order_by = (
                [dedup_order_by] if isinstance(dedup_order_by, str) else dedup_order_by
            )
        elif strategy_enum == MergeStrategy.DEDUPLICATE:
            merge_dedup_order_by = key_columns

        # Call merge function
        stats = merge_parquet_dataset_pyarrow(
            sources=[temp_source],
            output_path=path,
            target=merge_target,
            strategy=strategy_enum.value,
            key_columns=key_columns,
            filesystem=self,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            row_group_size=row_group_size,
            dedup_order_by=merge_dedup_order_by,
            verbose=verbose,
        )

        if verbose:
            logger.info("Merge completed: %s", stats.to_dict())

        return None  # merge_parquet_dataset_pyarrow doesn't return metadata


def _write_pyarrow_dataset_standard(
    self: AbstractFileSystem,
    table: pa.Table,
    path: str,
    basename: str | None = None,
    partition_by: str | list[str] | pds.Partitioning | None = None,
    partitioning_flavor: str = "hive",
    mode: str = "append",
    format: str | None = "parquet",
    compression: str = "zstd",
    max_rows_per_file: int | None = 2_500_000,
    row_group_size: int | None = 250_000,
    **kwargs: Any,
) -> list[Any]:
    """Internal: Write table to dataset using standard PyArrow dataset writer.

    Args:
        table: PyArrow table to write
        path: Output path
        basename: File basename template
        partition_by: Partition columns
        partitioning_flavor: Partitioning flavor
        mode: Write mode
        format: File format
        compression: Compression codec
        max_rows_per_file: Max rows per file
        row_group_size: Row group size
        **kwargs: Additional arguments

    Returns:
        List of file metadata
    """
    import pyarrow.dataset as pds

    from fsspeckit.common.optional import _import_pyarrow_parquet

    pq = _import_pyarrow_parquet()

    # Set default basename if not provided
    if basename is None:
        basename = "part-{i}.parquet"

    # Prepare write options
    if mode == "overwrite":
        existing_behavior = "delete_matching"
    else:
        existing_behavior = "overwrite_or_ignore"

    write_options = {
        "basename_template": basename,
        "max_rows_per_file": max_rows_per_file,
        "max_rows_per_group": row_group_size,
        "existing_data_behavior": existing_behavior,
    }

    # Add partition_by if specified
    if partition_by is not None:
        write_options["partitioning"] = partition_by
        write_options["partitioning_flavor"] = partitioning_flavor

    # Add any additional kwargs
    write_options.update(kwargs)

    # Create file options for compression
    file_options = pds.ParquetFileFormat().make_write_options(compression=compression)

    # Collect metadata
    metadata_collector = []

    # Write dataset
    pds.write_dataset(
        table,
        base_dir=path,
        filesystem=self,
        format=format or "parquet",
        file_options=file_options,
        file_visitor=metadata_collector.append if metadata_collector else None,
        **write_options,
    )

    return metadata_collector


def insert_dataset(
    self: AbstractFileSystem,
    data: Any,
    path: str,
    key_columns: list[str] | str,
    **kwargs: Any,
) -> list[Any] | None:
    """Insert-only dataset write.

    Convenience method that calls write_pyarrow_dataset with strategy='insert'.

    Args:
        data: Input data in various formats
        path: Path to write the dataset
        key_columns: Key columns for merge (required)
        **kwargs: Additional arguments passed to write_pyarrow_dataset

    Returns:
        List of Parquet file metadata or None

    Raises:
        ValueError: If key_columns is not provided
    """
    raise NotImplementedError(
        "insert_dataset has been removed. Use PyarrowDatasetIO.merge(strategy='insert') instead."
    )
    from fsspeckit.core.merge import normalize_key_columns

    if not key_columns:
        raise ValueError("key_columns is required for insert_dataset")

    key_columns = normalize_key_columns(key_columns)

    return self.write_pyarrow_dataset(
        data=data,
        path=path,
        strategy="insert",
        key_columns=key_columns,
        **kwargs,
    )


def upsert_dataset(
    self: AbstractFileSystem,
    data: Any,
    path: str,
    key_columns: list[str] | str,
    **kwargs: Any,
) -> list[Any] | None:
    """Insert-or-update dataset write.

    Convenience method that calls write_pyarrow_dataset with strategy='upsert'.

    Args:
        data: Input data in various formats
        path: Path to write the dataset
        key_columns: Key columns for merge (required)
        **kwargs: Additional arguments passed to write_pyarrow_dataset

    Returns:
        List of Parquet file metadata or None

    Raises:
        ValueError: If key_columns is not provided
    """
    raise NotImplementedError(
        "upsert_dataset has been removed. Use PyarrowDatasetIO.merge(strategy='upsert') instead."
    )
    from fsspeckit.core.merge import normalize_key_columns

    if not key_columns:
        raise ValueError("key_columns is required for upsert_dataset")

    key_columns = normalize_key_columns(key_columns)

    return self.write_pyarrow_dataset(
        data=data,
        path=path,
        strategy="upsert",
        key_columns=key_columns,
        **kwargs,
    )


def update_dataset(
    self: AbstractFileSystem,
    data: Any,
    path: str,
    key_columns: list[str] | str,
    **kwargs: Any,
) -> list[Any] | None:
    """Update-only dataset write.

    Convenience method that calls write_pyarrow_dataset with strategy='update'.

    Args:
        data: Input data in various formats
        path: Path to write the dataset
        key_columns: Key columns for merge (required)
        **kwargs: Additional arguments passed to write_pyarrow_dataset

    Returns:
        List of Parquet file metadata or None

    Raises:
        ValueError: If key_columns is not provided
    """
    raise NotImplementedError(
        "update_dataset has been removed. Use PyarrowDatasetIO.merge(strategy='update') instead."
    )
    from fsspeckit.core.merge import normalize_key_columns

    if not key_columns:
        raise ValueError("key_columns is required for update_dataset")

    key_columns = normalize_key_columns(key_columns)

    return self.write_pyarrow_dataset(
        data=data,
        path=path,
        strategy="update",
        key_columns=key_columns,
        **kwargs,
    )


def deduplicate_dataset(
    self: AbstractFileSystem,
    data: Any,
    path: str,
    key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    **kwargs: Any,
) -> list[Any] | None:
    """Deduplicate dataset write.

    Convenience method that calls write_pyarrow_dataset with strategy='deduplicate'.

    Args:
        data: Input data in various formats
        path: Path to write the dataset
        key_columns: Optional key columns for deduplication
        dedup_order_by: Columns to order by for deduplication
        **kwargs: Additional arguments passed to write_pyarrow_dataset

    Returns:
        List of Parquet file metadata or None
    """
    raise NotImplementedError(
        "deduplicate_dataset has been removed. Use a dedicated dataset maintenance API instead."
    )
    return self.write_pyarrow_dataset(
        data=data,
        path=path,
        strategy="deduplicate",
        key_columns=key_columns,
        dedup_order_by=dedup_order_by,
        **kwargs,
    )


def deduplicate_parquet_dataset(
    self: AbstractFileSystem,
    path: str,
    *,
    key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    """Deduplicate an existing parquet dataset using the most appropriate backend.

    This method provides a unified interface for deduplicating existing parquet datasets
    across different backends. It will automatically select the best backend available
    (DuckDB if available, otherwise PyArrow).

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
        **kwargs: Additional backend-specific arguments

    Returns:
        Dictionary containing deduplication statistics

    Example:
        ```python
        from fsspec import LocalFileSystem

        fs = LocalFileSystem()

        # Key-based deduplication
        stats = fs.deduplicate_parquet_dataset(
            "/tmp/dataset/",
            key_columns=["id", "timestamp"],
            dedup_order_by=["-timestamp"],  # Keep most recent
            verbose=True
        )

        # Exact duplicate removal
        stats = fs.deduplicate_parquet_dataset("/tmp/dataset/")
        ```
    """
    # Try DuckDB first if available, fall back to PyArrow
    try:
        from fsspeckit.datasets.duckdb.connection import create_duckdb_connection
        from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

        # Use DuckDB backend
        conn = create_duckdb_connection()
        io = DuckDBDatasetIO(conn)

        return io.deduplicate_parquet_dataset(
            path=path,
            key_columns=key_columns,
            dedup_order_by=dedup_order_by,
            partition_filter=partition_filter,
            compression=compression,
            dry_run=dry_run,
            verbose=verbose,
        )

    except ImportError:
        # DuckDB not available, use PyArrow
        from fsspeckit.datasets.pyarrow.dataset import (
            deduplicate_parquet_dataset_pyarrow,
        )

        return deduplicate_parquet_dataset_pyarrow(
            path=path,
            key_columns=key_columns,
            dedup_order_by=dedup_order_by,
            partition_filter=partition_filter,
            compression=compression,
            dry_run=dry_run,
            filesystem=self,
            verbose=verbose,
        )
