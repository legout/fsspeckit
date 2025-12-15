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

# Re-export from new location.
#
# Note: `from ... import *` does not export names starting with "_" unless the
# target module defines `__all__`. This module exists as a compatibility façade
# and MUST continue to expose the underscore-prefixed helpers that older callers
# import directly.
from fsspeckit.core.filesystem import paths as _paths  # noqa: F401

for _name in dir(_paths):
    if _name.startswith("_") and not _name.startswith("__"):
        globals()[_name] = getattr(_paths, _name)

__all__ = [
    _name for _name in globals() if _name.startswith("_") and not _name.startswith("__")
]
