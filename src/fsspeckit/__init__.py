"""fsspeckit: Enhanced utilities and extensions for fsspec filesystems.

This package provides enhanced filesystem utilities built on top of fsspec,
including:
- Multi-format data I/O (JSON, CSV, Parquet)
- Cloud storage configuration utilities
- Enhanced caching and monitoring
- Batch processing and parallel operations
"""

import importlib.metadata

__version__ = importlib.metadata.version("fsspeckit")


from .core import AbstractFileSystem, DirFileSystem, filesystem, get_filesystem
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
]
