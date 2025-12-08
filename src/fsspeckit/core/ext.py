"""Re-export module for backward compatibility.

This module has been decomposed into focused submodules:
- ext_json: JSON/JSONL file I/O helpers
- ext_csv: CSV file I/O helpers
- ext_parquet: Parquet file I/O helpers
- ext_dataset: PyArrow dataset creation helpers
- ext_io: Universal I/O interfaces
- ext_register: Registration layer for AbstractFileSystem

All public APIs are re-exported here to maintain backward compatibility.
New code should import directly from the submodules for better organization.

DEPRECATED: This module is deprecated. Import from fsspeckit.core.ext instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.core.ext is deprecated. "
    "Import from fsspeckit.core.ext directly instead, e.g., "
    "from fsspeckit.core.ext import read_csv",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export all public APIs for backward compatibility
from fsspeckit.core.ext.csv import (
    read_csv_file,
    read_csv,
    write_csv,
)
from fsspeckit.core.ext.json import (
    read_json_file,
    read_json,
    write_json,
)
from fsspeckit.core.ext.parquet import (
    read_parquet_file,
    read_parquet,
    write_parquet,
)
from fsspeckit.core.ext.dataset import (
    pyarrow_dataset,
    pyarrow_parquet_dataset,
)
from fsspeckit.core.ext.io import (
    read_files,
    write_file,
    write_files,
)

# Import the registration layer to attach methods to AbstractFileSystem
# This must happen after all imports
from fsspeckit.core.ext import register  # noqa: F401

__all__ = [
    # JSON I/O
    "read_json_file",
    "read_json",
    "write_json",
    # CSV I/O
    "read_csv_file",
    "read_csv",
    "write_csv",
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
