"""Registration layer for extending AbstractFileSystem with format-specific methods.

This module provides the wiring layer that attaches format-specific helpers
(JSON, CSV, Parquet) to the AbstractFileSystem class through monkey-patching.
This ensures that all filesystem instances have access to the enhanced I/O methods.
"""

from typing import Any

from fsspec import AbstractFileSystem

from fsspeckit.core.ext.csv import (
    read_csv,
    read_csv_file,
    write_csv,
)
from fsspeckit.core.ext.dataset import (
    compact_parquet_dataset,
    deduplicate_and_repartition_parquet_dataset,
    deduplicate_parquet_dataset,
    execute_maintenance_plan,
    optimize_parquet_dataset,
    plan_parquet_compaction,
    plan_parquet_global_repartition_deduplication,
    plan_parquet_optimization,
    plan_parquet_partition_local_deduplication,
    plan_parquet_repartition,
    pyarrow_dataset,
    pyarrow_parquet_dataset,
    repartition_parquet_dataset,
)

# Import universal I/O helpers
from fsspeckit.core.ext.io import (
    read_files,
    write_file,
    write_files,
)

# Import all the format-specific helpers
from fsspeckit.core.ext.json import (
    read_json,
    read_json_file,
    write_json,
)
from fsspeckit.core.ext.parquet import (
    read_parquet,
    read_parquet_file,
    write_parquet,
)


def clear_cache(fs: AbstractFileSystem | None):
    """Clear filesystem cache.

    Args:
        fs: Filesystem instance or None
    """
    if fs is None or not hasattr(fs, "dircache"):
        return

    target: Any = getattr(fs, "fs", fs)
    target.invalidate_cache()
    target.clear_instance_cache()


# Register all methods with AbstractFileSystem
# This is the single place where monkey-patching happens
_FILESYSTEM_METHODS = {
    "clear_cache": clear_cache,
    "read_json_file": read_json_file,
    "read_json": read_json,
    "read_csv_file": read_csv_file,
    "read_csv": read_csv,
    "read_parquet_file": read_parquet_file,
    "read_parquet": read_parquet,
    "read_files": read_files,
    "pyarrow_dataset": pyarrow_dataset,
    "pyarrow_parquet_dataset": pyarrow_parquet_dataset,
    "plan_parquet_compaction": plan_parquet_compaction,
    "plan_parquet_partition_local_deduplication": (
        plan_parquet_partition_local_deduplication
    ),
    "plan_parquet_global_repartition_deduplication": (
        plan_parquet_global_repartition_deduplication
    ),
    "plan_parquet_repartition": plan_parquet_repartition,
    "plan_parquet_optimization": plan_parquet_optimization,
    "execute_maintenance_plan": execute_maintenance_plan,
    "compact_parquet_dataset": compact_parquet_dataset,
    "deduplicate_parquet_dataset": deduplicate_parquet_dataset,
    "deduplicate_and_repartition_parquet_dataset": (
        deduplicate_and_repartition_parquet_dataset
    ),
    "repartition_parquet_dataset": repartition_parquet_dataset,
    "optimize_parquet_dataset": optimize_parquet_dataset,
    "write_parquet": write_parquet,
    "write_json": write_json,
    "write_csv": write_csv,
    "write_file": write_file,
    "write_files": write_files,
}

for method_name, method in _FILESYSTEM_METHODS.items():
    setattr(AbstractFileSystem, method_name, method)
