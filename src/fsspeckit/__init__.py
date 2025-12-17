"""fsspeckit: Enhanced utilities and extensions for fsspec filesystems.

This package provides enhanced filesystem utilities built on top of fsspec,
including:
- Multi-format data I/O (JSON, CSV, Parquet)
- Cloud storage configuration utilities
- Enhanced caching and monitoring
- Batch processing and parallel operations
"""

import importlib.metadata

try:
    __version__ = importlib.metadata.version("fsspeckit")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.5.0-dev"
except Exception:
    # Fallback for any other import issues during development
    __version__ = "0.5.0-dev"


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
from .common.logging_config import setup_logging

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
]
