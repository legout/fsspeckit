"""Dataset creation helpers for fsspec filesystems.

DEPRECATED: This module is deprecated. Import from fsspeckit.core.ext.dataset instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.core.ext_dataset is deprecated. "
    "Import from fsspeckit.core.ext.dataset instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.core.ext.dataset import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
