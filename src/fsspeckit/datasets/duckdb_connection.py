"""Connection management and filesystem registration for DuckDB.

DEPRECATED: This module is deprecated. Import from fsspeckit.datasets.duckdb.connection instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.datasets.duckdb_connection is deprecated. "
    "Import from fsspeckit.datasets.duckdb.connection instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.datasets.duckdb.connection import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
