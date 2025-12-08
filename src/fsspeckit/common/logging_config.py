"""Logging configuration utilities for fsspeckit using Python's standard logging module.

DEPRECATED: This module is deprecated. The logging functionality has been moved to
fsspeckit.common.logging. Consider using loguru-based logging instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.common.logging_config is deprecated. "
    "Import from fsspeckit.common.logging instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location (using the loguru-based implementation)
from fsspeckit.common.logging import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
