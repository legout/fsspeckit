"""Dataset creation helpers for fsspec filesystems.

This module contains functions for creating PyArrow datasets with support for:
- Schema enforcement
- Partitioning
- Format-specific optimizations
- Predicate pushdown
"""

from __future__ import annotations

import posixpath
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.dataset as pds

from fsspec import AbstractFileSystem


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
