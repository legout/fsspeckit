"""Logging configuration utilities for fsspeckit.

DEPRECATED: This module is deprecated. Import from fsspeckit.common.logging instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.common.logging is deprecated. "
    "Import from fsspeckit.common.logging directly instead, e.g., "
    "from fsspeckit.common.logging import get_logger",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.common.logging import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
