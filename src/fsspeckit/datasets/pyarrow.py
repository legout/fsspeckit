"""Re-export module for backward compatibility.

This module has been decomposed into focused submodules:
- pyarrow_schema: Schema unification, type inference, and optimization
- pyarrow_dataset: Dataset merge and maintenance operations

All public APIs are re-exported here to maintain backward compatibility.
New code should import directly from the submodules for better organization.

DEPRECATED: This module is deprecated. Import from fsspeckit.datasets.pyarrow instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.datasets.pyarrow is deprecated. "
    "Import from fsspeckit.datasets.pyarrow directly instead, e.g., "
    "from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.datasets.pyarrow import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
