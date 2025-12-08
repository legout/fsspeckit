"""PyArrow dataset operations including merge and maintenance helpers.

DEPRECATED: This module is deprecated. Import from fsspeckit.datasets.pyarrow.dataset instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.datasets.pyarrow_dataset is deprecated. "
    "Import from fsspeckit.datasets.pyarrow.dataset instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.datasets.pyarrow.dataset import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
