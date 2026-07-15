"""fsspeckit: Enhanced utilities and extensions for fsspec filesystems.

This package provides enhanced filesystem utilities built on top of fsspec,
including:
- Multi-format data I/O (JSON, CSV, Parquet)
- Cloud storage configuration utilities
- Enhanced caching and monitoring
- Batch processing and parallel operations
"""

import importlib.metadata
from contextlib import suppress

__version__ = "0.5.0-dev"
with suppress(importlib.metadata.PackageNotFoundError):
    __version__ = importlib.metadata.version("fsspeckit")
from fsspec import AbstractFileSystem

from .common.logging import setup_logging
from .core import DirFileSystem, filesystem, get_filesystem
from .storage_options import (
    AwsStorageOptions,
    AzureStorageOptions,
    BaseStorageOptions,
    GcsStorageOptions,
    GitHubStorageOptions,
    GitLabStorageOptions,
    LocalStorageOptions,
    StorageOptions,
)

# Configure logging when package is imported
# setup_logging()

__all__ = [
    "filesystem",
    "get_filesystem",
    "AbstractFileSystem",
    "DirFileSystem",
    "AwsStorageOptions",
    "AzureStorageOptions",
    "BaseStorageOptions",
    "GcsStorageOptions",
    "GitHubStorageOptions",
    "GitLabStorageOptions",
    "LocalStorageOptions",
    "StorageOptions",
    "setup_logging",
]
