"""Re-export module for backward compatibility.

This module has been decomposed into focused submodules:
- duckdb_connection: Connection management and filesystem registration
- duckdb_dataset: Dataset I/O and maintenance operations
- _duckdb_helpers: Utility functions for DuckDB operations

All public APIs are re-exported here to maintain backward compatibility.
New code should import directly from the submodules for better organization.

DEPRECATED: This module is deprecated. Import from fsspeckit.datasets.duckdb instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.datasets.duckdb is deprecated. "
    "Import from fsspeckit.datasets.duckdb directly instead, e.g., "
    "from fsspeckit.datasets.duckdb import DuckDBDatasetIO",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.datasets.duckdb import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
