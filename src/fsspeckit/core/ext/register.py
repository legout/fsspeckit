"""Registration layer for extending AbstractFileSystem with format-specific methods.

This module provides the wiring layer that attaches format-specific helpers
(JSON, CSV, Parquet) to the AbstractFileSystem class through monkey-patching.
This ensures that all filesystem instances have access to the enhanced I/O methods.
"""

from fsspec import AbstractFileSystem

# Import all the format-specific helpers
from fsspeckit.core.ext.json import (
    read_json_file,
    read_json,
)
from fsspeckit.core.ext.csv import (
    read_csv_file,
    read_csv,
)
from fsspeckit.core.ext.parquet import (
    read_parquet_file,
    read_parquet,
    write_parquet,
)
from fsspeckit.core.ext.json import write_json
from fsspeckit.core.ext.csv import write_csv
from fsspeckit.core.ext.dataset import (
    deduplicate_dataset,
    insert_dataset,
    pyarrow_dataset,
    pyarrow_parquet_dataset,
    update_dataset,
    upsert_dataset,
    write_pyarrow_dataset,
)

# Import universal I/O helpers
from fsspeckit.core.ext.io import (
    read_files,
    write_file,
    write_files,
)


def clear_cache(fs: AbstractFileSystem | None):
    """Clear filesystem cache.

    Args:
        fs: Filesystem instance or None
    """
    if hasattr(fs, "dircache"):
        if hasattr(fs, "fs"):
            fs.fs.invalidate_cache()
            fs.fs.clear_instance_cache()
        else:
            fs.invalidate_cache()
            fs.clear_instance_cache()


# Register all methods with AbstractFileSystem
# This is the single place where monkey-patching happens
AbstractFileSystem.clear_cache = clear_cache
AbstractFileSystem.read_json_file = read_json_file
AbstractFileSystem.read_json = read_json
AbstractFileSystem.read_csv_file = read_csv_file
AbstractFileSystem.read_csv = read_csv
AbstractFileSystem.read_parquet_file = read_parquet_file
AbstractFileSystem.read_parquet = read_parquet
AbstractFileSystem.read_files = read_files
AbstractFileSystem.pyarrow_dataset = pyarrow_dataset
AbstractFileSystem.pyarrow_parquet_dataset = pyarrow_parquet_dataset
AbstractFileSystem.write_parquet = write_parquet
AbstractFileSystem.write_json = write_json
AbstractFileSystem.write_csv = write_csv
AbstractFileSystem.write_file = write_file
AbstractFileSystem.write_files = write_files
AbstractFileSystem.write_pyarrow_dataset = write_pyarrow_dataset
AbstractFileSystem.insert_dataset = insert_dataset
AbstractFileSystem.upsert_dataset = upsert_dataset
AbstractFileSystem.update_dataset = update_dataset
AbstractFileSystem.deduplicate_dataset = deduplicate_dataset
