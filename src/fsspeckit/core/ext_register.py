"""Registration layer for extending AbstractFileSystem with format-specific methods.

DEPRECATED: This module is deprecated. Import from fsspeckit.core.ext.register instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.core.ext_register is deprecated. "
    "Import from fsspeckit.core.ext.register instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.core.ext.register import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
