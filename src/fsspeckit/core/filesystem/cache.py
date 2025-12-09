"""Cache filesystem utilities.

This module contains classes and functions for managing cache filesystems:
- FileNameCacheMapper: Maps remote file paths to local cache paths
- MonitoredSimpleCacheFileSystem: Cache filesystem with monitoring
"""

import inspect
import os
import posixpath

import fsspec
from fsspec import AbstractFileSystem
from fsspec.implementations.cache_mapper import AbstractCacheMapper
from fsspec.implementations.cached import SimpleCacheFileSystem

from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)


class FileNameCacheMapper(AbstractCacheMapper):
    """Maps remote file paths to local cache paths while preserving directory structure.

    This cache mapper maintains the original file path structure in the cache directory,
    creating necessary subdirectories as needed.

    Attributes:
        directory (str): Base directory for cached files

    Example:
        ```python
        # Create cache mapper for S3 files
        mapper = FileNameCacheMapper("/tmp/cache")

        # Map remote path to cache path
        cache_path = mapper("bucket/data/file.csv")
        print(cache_path)  # Preserves structure
        # 'bucket/data/file.csv'
        ```
    """

    def __init__(self, directory: str):
        """Initialize cache mapper with base directory.

        Args:
            directory: Base directory where cached files will be stored
        """
        self.directory = directory

    def __call__(self, path: str) -> str:
        """Map remote file path to cache file path.

        Creates necessary subdirectories in the cache directory to maintain
        the original path structure.

        Args:
            path: Original file path from remote filesystem

        Returns:
            str: Cache file path that preserves original structure

        Example:
            ```python
            mapper = FileNameCacheMapper("/tmp/cache")
            # Maps maintain directory structure
            print(mapper("data/nested/file.txt"))
            # 'data/nested/file.txt'
            ```
        """
        os.makedirs(
            posixpath.dirname(posixpath.join(self.directory, path)), exist_ok=True
        )

        return path


class MonitoredSimpleCacheFileSystem(SimpleCacheFileSystem):
    """Simple cache filesystem with monitoring and logging.

    This filesystem wraps another filesystem and caches files locally.
    It provides monitoring capabilities and verbose logging of cache operations.

    Args:
        fs: Underlying filesystem to cache. If None, creates a local filesystem.
        cache_storage: Cache storage location(s). Can be string path or list of paths.
        verbose: Whether to enable verbose logging of cache operations.
        **kwargs: Additional arguments passed to SimpleCacheFileSystem.

    Example:
        ```python
        # Cache S3 filesystem
        s3_fs = filesystem("s3")
        cached = MonitoredSimpleCacheFileSystem(
            fs=s3_fs,
            cache_storage="/tmp/s3_cache",
            verbose=True,
        )
        ```
    """

    def __init__(
        self,
        fs: fsspec.AbstractFileSystem | None = None,
        cache_storage: str = "~/.cache/fsspec",
        verbose: bool = False,
        **kwargs,
    ):
        """Initialize monitored cache filesystem.

        Args:
            fs: Underlying filesystem to cache. If None, creates a local filesystem.
            cache_storage: Cache storage location(s). Can be string path or list of paths.
            verbose: Whether to enable verbose logging of cache operations.
            **kwargs: Additional arguments passed to SimpleCacheFileSystem.
        """
        self._verbose = verbose

        super().__init__(fs=fs, cache_storage=cache_storage, **kwargs)
        self._mapper = FileNameCacheMapper(cache_storage)

        if self._verbose:
            logger.info(f"Initialized cache filesystem with storage: {cache_storage}")

    def open(self, path, mode="rb", **kwargs):
        """Open a file from cache or remote filesystem.

        Args:
            path: File path
            mode: File mode
            **kwargs: Additional arguments

        Returns:
            File-like object
        """
        return super().open(path, mode=mode, **kwargs)

    def _check_file(self, path: str) -> str | None:
        """Check if file exists in cache.

        Args:
            path: File path to check

        Returns:
            Cached file path if found, None otherwise
        """
        if self._verbose:
            logger.info(f"Checking file: {path}")

        for storage in self.storage:
            fn = os.path.join(storage, path)
            if os.path.exists(fn):
                return fn

        return None

    def size(self, path: str) -> int:
        """Get size of file in bytes.

        Checks cache first, falls back to remote filesystem.

        Args:
            path: Path to file

        Returns:
            Size of file in bytes

        Example:
            ```python
            fs = MonitoredSimpleCacheFileSystem(
                fs=remote_fs,
                cache_storage="/tmp/cache",
            )
            size = fs.size("large_file.dat")
            print(f"File size: {size} bytes")
            ```
        """
        cached_file = self._check_file(self._strip_protocol(path))
        if cached_file is None:
            return self.fs.size(path)
        else:
            return posixpath.getsize(cached_file)

    def sync_cache(self, reload: bool = False) -> None:
        """Synchronize cache with remote filesystem.

        Downloads all files in remote path to cache if not present.

        Args:
            reload: Whether to force reload all files, ignoring existing cache

        Example:
            ```python
            fs = MonitoredSimpleCacheFileSystem(
                fs=remote_fs,
                cache_storage="/tmp/cache",
            )
            # Initial sync
            fs.sync_cache()

            # Force reload all files
            fs.sync_cache(reload=True)
            ```
        """
        if reload:
            if hasattr(self, "clear_cache"):
                self.clear_cache()

        files = self.glob("**/*")
        [self.open(f, mode="rb").close() for f in files if self.isfile(f)]

    def __getattribute__(self, item):
        """Custom attribute access to delegate to underlying filesystem.

        This method ensures that attributes not found in this class
        are looked up in the underlying filesystem.
        """
        if item in {
            # new items
            "size",
            "glob",
            # previous
            "load_cache",
            "_open",
            "save_cache",
            "close_and_update",
            "sync_cache",
            "__init__",
            "__getattribute__",
            "__reduce__",
            "_make_local_details",
            "open",
            "cat",
            "cat_file",
            "cat_ranges",
            "get",
            "read_block",
            "tail",
            "head",
            "info",
            "ls",
            "exists",
            "isfile",
            "isdir",
            "_check_file",
            "_check_cache",
            "_mkcache",
            "clear_cache",
            "clear_expired_cache",
            "pop_from_cache",
            "local_file",
            "_paths_from_path",
            "get_mapper",
            "open_many",
            "commit_many",
            "hash_name",
            "__hash__",
            "__eq__",
            "to_json",
            "to_dict",
            "cache_size",
            "pipe_file",
            "pipe",
            "start_transaction",
            "end_transaction",
            "sync_cache",
        }:
            # all the methods defined in this class. Note `open` here, since
            # it calls `_open`, but is actually in superclass
            return lambda *args, **kw: getattr(type(self), item).__get__(self)(
                *args, **kw
            )
        if item in ["__reduce_ex__"]:
            raise AttributeError
        if item in ["transaction"]:
            # property
            return type(self).transaction.__get__(self)
        if item in ["_cache", "transaction_type"]:
            # class attributes
            return getattr(type(self), item)
        if item == "__class__":
            return type(self)
        d = object.__getattribute__(self, "__dict__")
        fs = d.get("fs", None)  # fs is not immediately defined
        if item in d:
            return d[item]
        elif fs is not None:
            if item in fs.__dict__:
                # attribute of instance
                return fs.__dict__[item]
            # attributed belonging to the target filesystem
            cls = type(fs)
            m = getattr(cls, item)
            if (inspect.isfunction(m) or inspect.ismethod(m)) and (
                not hasattr(m, "__self__") or m.__self__ is None
            ):
                # instance method
                return m.__get__(fs, cls)
            return m  # class method or attribute
        else:
            # attributes of the superclass, while target is being set up
            return super().__getattribute__(item)
