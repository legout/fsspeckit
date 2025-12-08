"""Universal I/O helpers for fsspec filesystems.

DEPRECATED: This module is deprecated. Import from fsspeckit.core.ext.io instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.core.ext_io is deprecated. "
    "Import from fsspeckit.core.ext.io instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.core.ext.io import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
