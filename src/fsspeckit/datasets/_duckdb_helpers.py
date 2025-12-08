"""Utility functions for DuckDB operations.

DEPRECATED: This module is deprecated. Import from fsspeckit.datasets.duckdb.helpers instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.datasets._duckdb_helpers is deprecated. "
    "Import from fsspeckit.datasets.duckdb.helpers instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.datasets.duckdb.helpers import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
