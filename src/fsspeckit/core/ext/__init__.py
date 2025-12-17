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

# Import the registration layer to attach methods to AbstractFileSystem
# This must happen after all imports
from . import register  # noqa: F401

# Re-export all public APIs for backward compatibility and convenience
# CSV I/O
from .csv import (
    read_csv,
    read_csv_file,
    write_csv,
)

# Dataset helpers
from .dataset import (
    pyarrow_dataset,
    pyarrow_parquet_dataset,
)

# Universal I/O
from .io import (
    read_files,
    write_file,
    write_files,
)

# JSON I/O
from .json import (
    read_json,
    read_json_file,
    write_json,
)

# Parquet I/O
from .parquet import (
    read_parquet,
    read_parquet_file,
    write_parquet,
)

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
    # Universal I/O
    "read_files",
    "write_file",
    "write_files",
]
