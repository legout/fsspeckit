"""Core base functionality for fsspeckit.

This module imports filesystem functionality from the dedicated filesystem module
to provide a clean separation of concerns.
"""

# Import filesystem functionality from the dedicated filesystem module
from .filesystem import (
    # Filesystem classes
    FileNameCacheMapper,
    MonitoredSimpleCacheFileSystem,
    GitLabFileSystem,

    # Filesystem functions
    filesystem,
    get_filesystem,
    setup_filesystem_logging,

    # Re-export DirFileSystem for convenience
    DirFileSystem,
)

__all__ = [
    "FileNameCacheMapper",
    "MonitoredSimpleCacheFileSystem",
    "GitLabFileSystem",
    "filesystem",
    "get_filesystem",
    "setup_filesystem_logging",
    "DirFileSystem",
]
