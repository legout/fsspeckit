"""Cache filesystem utilities.

DEPRECATED: This module is deprecated. Import from fsspeckit.core.filesystem.cache instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.core.filesystem_cache is deprecated. "
    "Import from fsspeckit.core.filesystem.cache instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.core.filesystem.cache import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
