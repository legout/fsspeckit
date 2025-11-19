"""Core filesystem functionality for fsspeckit."""

from .base import (
    GitLabFileSystem,
    MonitoredSimpleCacheFileSystem,
    filesystem,
    get_filesystem,
    DirFileSystem,
)

# Conditional imports for extended functionality
try:
    from .ext import AbstractFileSystem
except ImportError:
    from fsspec import AbstractFileSystem

__all__ = [
    "GitLabFileSystem",
    "MonitoredSimpleCacheFileSystem",
    "DirFileSystem",
    "AbstractFileSystem",
    "filesystem",
    "get_filesystem",
]
