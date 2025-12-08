"""PyArrow schema utilities for type inference, unification, and optimization.

DEPRECATED: This module is deprecated. Import from fsspeckit.datasets.pyarrow.schema instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.datasets.pyarrow_schema is deprecated. "
    "Import from fsspeckit.datasets.pyarrow.schema instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.datasets.pyarrow.schema import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
