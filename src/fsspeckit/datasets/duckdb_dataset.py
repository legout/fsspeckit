"""Dataset I/O and maintenance operations for DuckDB.

DEPRECATED: This module is deprecated. Import from fsspeckit.datasets.duckdb.dataset instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.datasets.duckdb_dataset is deprecated. "
    "Import from fsspeckit.datasets.duckdb.dataset instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.datasets.duckdb.dataset import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
