"""Dataset creation helpers for fsspec filesystems.

This module contains functions for creating PyArrow datasets with support for:
- Schema enforcement
- Partitioning
- Format-specific optimizations
- Predicate pushdown
"""

from __future__ import annotations

import posixpath
from typing import Any

import pyarrow as pa
import pyarrow.dataset as pds


from fsspec import AbstractFileSystem

from fsspeckit.core.maintenance import (
    CompactionPlan,
    CoordinatedOptimizationPlan,
    DatasetMaintenanceCoordinator,
    GlobalRepartitionDeduplicationPlan,
    MaintenancePlan,
    MaintenanceResult,
    PartitionLocalDeduplicationPlan,
)


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


def _automatic_maintenance_coordinator() -> DatasetMaintenanceCoordinator:
    """Create the façade's always-available maintenance coordinator.

    PyArrow is a required dependency, unlike DuckDB.  Selecting it here keeps
    filesystem maintenance usable in the base installation and records that
    stable choice in every returned plan.
    """

    return DatasetMaintenanceCoordinator("pyarrow")


def _normalize_optional_columns(
    columns: list[str] | str | None,
) -> list[str] | None:
    if columns is None:
        return None

    from fsspeckit.core.merge import normalize_key_columns

    return normalize_key_columns(columns)


def plan_parquet_compaction(
    self: AbstractFileSystem,
    path: str,
    *,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
) -> CompactionPlan:
    """Create an immutable, lock-free compaction plan for *path*."""
    return _automatic_maintenance_coordinator().plan_compaction(
        path,
        filesystem=self,
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
        partition_filter=partition_filter,
        codec=compression,
    )


def plan_parquet_partition_local_deduplication(
    self: AbstractFileSystem,
    path: str,
    *,
    key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
) -> PartitionLocalDeduplicationPlan:
    """Create an immutable plan for partition-local deduplication."""
    return _automatic_maintenance_coordinator().plan_partition_local_deduplication(
        path,
        filesystem=self,
        key_columns=_normalize_optional_columns(key_columns),
        dedup_order_by=_normalize_optional_columns(dedup_order_by),
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
        partition_filter=partition_filter,
        codec=compression,
    )


def plan_parquet_global_repartition_deduplication(
    self: AbstractFileSystem,
    path: str,
    *,
    partition_columns: list[str] | str,
    key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    compression: str | None = None,
    derived_partition_columns: dict[str, tuple[str, ...]] | None = None,
    partition_timezone: str = "UTC",
) -> GlobalRepartitionDeduplicationPlan:
    """Create an explicit whole-dataset repartitioning deduplication plan."""
    return _automatic_maintenance_coordinator().plan_global_repartition_deduplication(
        path,
        partition_columns=_normalize_optional_columns(partition_columns) or [],
        filesystem=self,
        key_columns=_normalize_optional_columns(key_columns),
        dedup_order_by=_normalize_optional_columns(dedup_order_by),
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
        codec=compression,
        derived_partition_columns=derived_partition_columns,
        partition_timezone=partition_timezone,
    )


def plan_parquet_optimization(
    self: AbstractFileSystem,
    path: str,
    *,
    deduplicate_key_columns: list[str] | str | None = None,
    dedup_order_by: list[str] | str | None = None,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
) -> CoordinatedOptimizationPlan:
    """Create an immutable plan for optional deduplication then compaction."""
    return _automatic_maintenance_coordinator().plan_coordinated_optimization(
        path,
        filesystem=self,
        dedup_key_columns=_normalize_optional_columns(deduplicate_key_columns),
        dedup_order_by=_normalize_optional_columns(dedup_order_by),
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
        partition_filter=partition_filter,
        codec=compression,
    )


def execute_maintenance_plan(
    self: AbstractFileSystem, plan: MaintenancePlan
) -> MaintenanceResult:
    """Execute an accepted façade plan and return its typed result."""
    if plan.selected_backend != "pyarrow":
        raise ValueError("filesystem façade plans must use the pyarrow backend")
    return DatasetMaintenanceCoordinator("pyarrow").execute(plan, filesystem=self)


def compact_parquet_dataset(self: AbstractFileSystem, path: str, **kwargs: Any):
    """Plan then execute compaction, returning ``MaintenanceResult``."""
    return execute_maintenance_plan(self, plan_parquet_compaction(self, path, **kwargs))


def deduplicate_parquet_dataset(self: AbstractFileSystem, path: str, **kwargs: Any):
    """Plan then execute partition-local deduplication."""
    return execute_maintenance_plan(
        self, plan_parquet_partition_local_deduplication(self, path, **kwargs)
    )


def deduplicate_and_repartition_parquet_dataset(
    self: AbstractFileSystem, path: str, **kwargs: Any
):
    """Plan then execute explicit global-repartitioning deduplication."""
    return execute_maintenance_plan(
        self, plan_parquet_global_repartition_deduplication(self, path, **kwargs)
    )


def optimize_parquet_dataset(self: AbstractFileSystem, path: str, **kwargs: Any):
    """Plan then execute optional deduplication followed by compaction."""
    return execute_maintenance_plan(
        self, plan_parquet_optimization(self, path, **kwargs)
    )
