"""Re-export module for backward compatibility.

This module has been decomposed into focused submodules:
- filesystem_paths: Path manipulation and protocol detection utilities
- filesystem_cache: Cache filesystem classes and utilities

All public APIs are re-exported here to maintain backward compatibility.
New code should import directly from the submodules for better organization.

DEPRECATED: This module is deprecated. Import from fsspeckit.core.filesystem instead.
"""

import warnings

# Emit deprecation warning
warnings.warn(
    "Importing from fsspeckit.core.filesystem is deprecated. "
    "Import from fsspeckit.core.filesystem directly instead, e.g., "
    "from fsspeckit.core.filesystem import filesystem",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location
from fsspeckit.core.filesystem import *  # noqa: F401,F403

__all__ = []  # All exports come from the imported module
