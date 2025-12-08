"""Path manipulation and protocol detection utilities.

DEPRECATED: This module is deprecated. Import from fsspeckit.core.filesystem.paths instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.core.filesystem_paths is deprecated. "
    "Import from fsspeckit.core.filesystem.paths instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.core.filesystem.paths import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
