"""Core extension I/O helpers for fsspec filesystems.

This package contains focused submodules for different file formats and operations:
- csv: CSV file I/O helpers
- json: JSON/JSONL file I/O helpers
- parquet: Parquet file I/O helpers
- dataset: PyArrow dataset creation helpers
- io: Universal I/O interfaces
- register: Registration layer for AbstractFileSystem

All public APIs are re-exported here for convenient access.
"""

import warnings
from typing import Any

# Re-export all public APIs for backward compatibility and convenience

# CSV I/O
from .csv import (
    read_csv_file,
    read_csv,
    write_csv,
)

# JSON I/O
from .json import (
    read_json_file,
    read_json,
    write_json,
)

# Parquet I/O
from .parquet import (
    read_parquet_file,
    read_parquet,
    write_parquet,
)

# Dataset helpers
from .dataset import (
    deduplicate_dataset,
    insert_dataset,
    pyarrow_dataset,
    pyarrow_parquet_dataset,
    update_dataset,
    upsert_dataset,
    write_pyarrow_dataset,
)

# Universal I/O
from .io import (
    read_files,
    write_file,
    write_files,
)

# Import the registration layer to attach methods to AbstractFileSystem
# This must happen after all imports
from . import register  # noqa: F401

__all__ = [
    # CSV I/O
    "read_csv_file",
    "read_csv",
    "write_csv",
    # JSON I/O
    "read_json_file",
    "read_json",
    "write_json",
    # Parquet I/O
    "read_parquet_file",
    "read_parquet",
    "write_parquet",
    # Dataset helpers
    "pyarrow_dataset",
    "pyarrow_parquet_dataset",
    "write_pyarrow_dataset",
    "insert_dataset",
    "upsert_dataset",
    "update_dataset",
    "deduplicate_dataset",
    # Universal I/O
    "read_files",
    "write_file",
    "write_files",
]
