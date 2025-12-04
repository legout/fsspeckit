"""Core filesystem functionality and utilities."""

import inspect
import base64
import os
import posixpath
import urllib
import warnings
from pathlib import Path
from typing import Optional, Union

import fsspec
import requests
from fsspec import filesystem as fsspec_filesystem
from fsspec.core import split_protocol
from fsspec.implementations.cache_mapper import AbstractCacheMapper
from fsspec.implementations.cached import SimpleCacheFileSystem
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.memory import MemoryFile
from fsspec.registry import known_implementations

from ..storage_options.base import BaseStorageOptions
from ..storage_options.core import from_dict as storage_options_from_dict
from ..common.logging import get_logger

# Import fsspec's AbstractFileSystem directly
from fsspec import AbstractFileSystem

# Import ext module for side effects (method registration)
from . import ext  # noqa: F401

logger = get_logger(__name__)


class FileNameCacheMapper(AbstractCacheMapper):
    """Maps remote file paths to local cache paths while preserving directory structure.

    This cache mapper maintains the original file path structure in the cache directory,
    creating necessary subdirectories as needed.

    Attributes:
        directory (str): Base directory for cached files

    Example:
        >>> # Create cache mapper for S3 files
        >>> mapper = FileNameCacheMapper("/tmp/cache")
        >>>
        >>> # Map remote path to cache path
        >>> cache_path = mapper("bucket/data/file.csv")
        >>> print(cache_path)  # Preserves structure
        'bucket/data/file.csv'
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
            >>> mapper = FileNameCacheMapper("/tmp/cache")
            >>> # Maps maintain directory structure
            >>> print(mapper("data/nested/file.txt"))
            'data/nested/file.txt'
        """
        # os.makedirs(
        #     posixpath.dirname(posixpath.join(self.directory, path)), exist_ok=True
        # )
        os.makedirs(
            posixpath.dirname(posixpath.join(self.directory, path)), exist_ok=True
        )

        return path


class MonitoredSimpleCacheFileSystem(SimpleCacheFileSystem):
    def __init__(
        self,
        fs: Optional[fsspec.AbstractFileSystem] = None,
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

        Example:
            >>> # Cache S3 filesystem
            >>> s3_fs = filesystem("s3")
            >>> cached = MonitoredSimpleCacheFileSystem(
            ...     fs=s3_fs,
            ...     cache_storage="/tmp/s3_cache",
            ...     verbose=True
            ... )
        """
        self._verbose = verbose

        # # Initialize with expanded cache storage paths
        # expanded_storage = os.path.expanduser(cache_storage)
        # super().__init__(
        #     fs=fs,
        #     cache_storage=expanded_storage,
        #     cache_mapper=FileNameCacheMapper(expanded_storage),
        #     **kwargs,
        # )
        # kwargs["cache_storage"] = os.path.join(kwargs.get("cache_storage"), "123")
        super().__init__(fs=fs, cache_storage=cache_storage, **kwargs)
        self._mapper = FileNameCacheMapper(cache_storage)

        if self._verbose:
            logger.info(f"Initialized cache filesystem with storage: {cache_storage}")

    def open(self, path, mode="rb", **kwargs):
        """
        Open a file. If the file's path does not match the cache regex, bypass the
        caching and read directly from the underlying filesystem.
        """
        # if not ICEBERG_FILE_REGEX.search(path):
        #     # bypass caching.
        #     return self.fs.open(path, mode=mode, **kwargs)

        return super().open(path, mode=mode, **kwargs)

    def _check_file(self, path):
        # self._check_cache()
        if self._verbose:
            logger.info(f"Checking file: {path}")
        # cache_path = self._mapper(path)

        for storage in self.storage:
            fn = os.path.join(storage, path)
            if os.path.exists(fn):
                return fn
            # else:
            #    self.open(path, mode="rb").close()
            #    logger.info(f"Downloading {self.protocol[0]}://{path}")

    def size(self, path: str) -> int:
        """Get size of file in bytes.

        Checks cache first, falls back to remote filesystem.

        Args:
            path: Path to file

        Returns:
            int: Size of file in bytes

        Example:
            >>> fs = MonitoredSimpleCacheFileSystem(
            ...     fs=remote_fs,
            ...     cache_storage="/tmp/cache"
            ... )
            >>> size = fs.size("large_file.dat")
            >>> print(f"File size: {size} bytes")
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
            >>> fs = MonitoredSimpleCacheFileSystem(
            ...     fs=remote_fs,
            ...     cache_storage="/tmp/cache"
            ... )
            >>> # Initial sync
            >>> fs.sync_cache()
            >>>
            >>> # Force reload all files
            >>> fs.sync_cache(reload=True)
        """
        if reload:
            if hasattr(self, "clear_cache"):
                self.clear_cache()

        files = self.glob("**/*")
        [self.open(f, mode="rb").close() for f in files if self.isfile(f)]

    def __getattribute__(self, item):
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
            if (inspect.isfunction(m) or inspect.isdatadescriptor(m)) and (
                not hasattr(m, "__self__") or m.__self__ is None
            ):
                # instance method
                return m.__get__(fs, cls)
            return m  # class method or attribute
        else:
            # attributes of the superclass, while target is being set up
            return super().__getattribute__(item)


class GitLabFileSystem(AbstractFileSystem):
    """Filesystem interface for GitLab repositories.

    Provides read-only access to files in GitLab repositories, including:
    - Public and private repositories
    - Self-hosted GitLab instances
    - Branch/tag/commit selection
    - Token-based authentication

    Attributes:
        protocol (str): Always "gitlab"
        base_url (str): GitLab instance URL
        project_id (str): Project ID
        project_name (str): Project name/path
        ref (str): Git reference (branch, tag, commit)
        token (str): Access token
        api_version (str): API version

    Example:
        >>> # Public repository
        >>> fs = GitLabFileSystem(
        ...     project_name="group/project",
        ...     ref="main"
        ... )
        >>> files = fs.ls("/")
        >>>
        >>> # Private repository with token
        >>> fs = GitLabFileSystem(
        ...     project_id="12345",
        ...     token="glpat_xxxx",
        ...     ref="develop"
        ... )
        >>> content = fs.cat("README.md")
    """

    protocol = "gitlab"

    def __init__(
        self,
        base_url: str = "https://gitlab.com",
        project_id: Optional[Union[str, int]] = None,
        project_name: Optional[str] = None,
        ref: str = "main",
        token: Optional[str] = None,
        api_version: str = "v4",
        **kwargs,
    ):
        """Initialize GitLab filesystem.

        Args:
            base_url: GitLab instance URL
            project_id: Project ID number
            project_name: Project name/path (alternative to project_id)
            ref: Git reference (branch, tag, or commit SHA)
            token: GitLab personal access token
            api_version: API version to use
            **kwargs: Additional filesystem arguments

        Raises:
            ValueError: If neither project_id nor project_name is provided
        """
        super().__init__(**kwargs)

        if project_id is None and project_name is None:
            raise ValueError("Either project_id or project_name must be provided")

        self.base_url = base_url.rstrip("/")
        self.project_id = str(project_id) if project_id else None
        self.project_name = project_name
        self.ref = ref
        self.token = token
        self.api_version = api_version

        # Build API URL
        self.api_url = f"{self.base_url}/api/{self.api_version}"

        # Determine project identifier for API calls
        if self.project_id:
            self.project_identifier = self.project_id
        else:
            # URL encode project name
            self.project_identifier = urllib.parse.quote(self.project_name, safe="")

        # Setup session with authentication
        self.session = requests.Session()
        if self.token:
            self.session.headers["Private-Token"] = self.token

    def _get_file_content(self, path: str) -> bytes:
        """Get file content from GitLab API.

        Args:
            path: File path in repository

        Returns:
            File content as bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            requests.HTTPError: For other HTTP errors
        """
        # Remove leading slash for API consistency
        path = path.lstrip("/")

        url = f"{self.api_url}/projects/{self.project_identifier}/repository/files/{urllib.parse.quote(path, safe='')}"
        params = {"ref": self.ref}

        response = self.session.get(url, params=params)

        if response.status_code == 404:
            raise FileNotFoundError(f"File not found: {path}")

        response.raise_for_status()
        data = response.json()

        # Decode content (GitLab returns base64-encoded content)
        content = base64.b64decode(data["content"])
        return content

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        cache_options: Optional[dict] = None,
        **kwargs,
    ):
        """Open file for reading.

        Args:
            path: File path to open
            mode: File mode (only 'rb' and 'r' supported)
            block_size: Block size for reading (unused)
            cache_options: Cache options (unused)
            **kwargs: Additional options

        Returns:
            File-like object for reading

        Raises:
            ValueError: If mode is not supported
        """
        if mode not in ["rb", "r"]:
            raise ValueError(
                f"Mode '{mode}' not supported. Only 'rb' and 'r' are supported."
            )

        content = self._get_file_content(path)

        if mode == "r":
            content = content.decode("utf-8")

        return MemoryFile(None, None, content)

    def cat(self, path: str, **kwargs) -> bytes:
        """Get file contents as bytes.

        Args:
            path: File path
            **kwargs: Additional options

        Returns:
            File content as bytes
        """
        return self._get_file_content(path)

    def ls(self, path: str = "", detail: bool = True, **kwargs) -> list:
        """List directory contents.

        Args:
            path: Directory path to list
            detail: Whether to return detailed information
            **kwargs: Additional options

        Returns:
            List of files/directories or their details
        """
        path = path.lstrip("/")

        url = f"{self.api_url}/projects/{self.project_identifier}/repository/tree"
        params = {"ref": self.ref, "path": path, "recursive": False}

        response = self.session.get(url, params=params)
        response.raise_for_status()

        items = response.json()

        if detail:
            return [
                {
                    "name": posixpath.join(path, item["name"])
                    if path
                    else item["name"],
                    "size": None,  # GitLab API doesn't provide size in tree endpoint
                    "type": "directory" if item["type"] == "tree" else "file",
                    "id": item["id"],
                }
                for item in items
            ]
        else:
            return [
                posixpath.join(path, item["name"]) if path else item["name"]
                for item in items
            ]

    def exists(self, path: str, **kwargs) -> bool:
        """Check if file or directory exists.

        Args:
            path: Path to check
            **kwargs: Additional options

        Returns:
            True if path exists, False otherwise
        """
        try:
            self._get_file_content(path)
            return True
        except FileNotFoundError:
            return False

    def info(self, path: str, **kwargs) -> dict:
        """Get file information.

        Args:
            path: File path
            **kwargs: Additional options

        Returns:
            Dictionary with file information
        """
        # For simplicity, we'll use the file content request
        # In a production implementation, you might want to use a more efficient endpoint
        try:
            content = self._get_file_content(path)
            return {
                "name": path,
                "size": len(content),
                "type": "file",
            }
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {path}")


fsspec.register_implementation("gitlab", GitLabFileSystem)

# Original ls Methode speichern
dirfs_ls_o = DirFileSystem.ls
mscf_ls_o = MonitoredSimpleCacheFileSystem.ls


# Neue ls Methode definieren
def dir_ls_p(self, path, detail=False, **kwargs):
    return dirfs_ls_o(self, path, detail=detail, **kwargs)


def mscf_ls_p(self, path, detail=False, **kwargs):
    return mscf_ls_o(self, path, detail=detail, **kwargs)


# patchen
DirFileSystem.ls = dir_ls_p
MonitoredSimpleCacheFileSystem.ls = mscf_ls_p


def _ensure_string(path: str | Path | None) -> str:
    if isinstance(path, Path):
        return path.as_posix()
    return str(path) if path is not None else ""


def _normalize_path(path: str, sep: str = "/") -> str:
    if not path:
        return ""
    if sep == "/":
        path = path.replace("\\", "/")
    normalized = posixpath.normpath(path)
    if normalized == ".":
        return ""
    return normalized


def _join_paths(base: str, rel: str, sep: str = "/") -> str:
    base_norm = _normalize_path(base, sep)
    rel = rel or ""
    if not rel:
        return base_norm
    if rel.startswith(sep):
        return _normalize_path(rel, sep)
    if not base_norm:
        return _normalize_path(rel, sep)
    return _normalize_path(f"{base_norm.rstrip(sep)}{sep}{rel}", sep)


def _is_within(base: str, target: str, sep: str = "/") -> bool:
    base_norm = _normalize_path(base, sep)
    target_norm = _normalize_path(target, sep)
    if not base_norm:
        return True
    if base_norm == target_norm:
        return True
    prefix = f"{base_norm.rstrip(sep)}{sep}"
    return target_norm.startswith(prefix)


def _smart_join(base: str, rel: str, sep: str = "/") -> str:
    """Join while avoiding duplicate overlapping segments.

    Example:
        base = 'ewn/mms2/stage1', rel = 'mms2/stage1/SC' -> 'ewn/mms2/stage1/SC'
        base = 'ewn/mms2/stage1', rel = 'stage1/SC' -> 'ewn/mms2/stage1/SC'
        base = 'ewn/mms2/stage1', rel = 'ewn/mms2/stage1/SC' -> 'ewn/mms2/stage1/SC'
    """
    base_norm = _normalize_path(base, sep)
    rel_norm = _normalize_path(rel, sep)

    if not rel_norm:
        return base_norm
    if not base_norm:
        return rel_norm

    # If rel already has the base prefix, accept as absolute
    if _is_within(base_norm, rel_norm, sep):
        return rel_norm

    base_parts = [p for p in base_norm.split(sep) if p]
    rel_parts = [p for p in rel_norm.split(sep) if p]

    # Longest suffix of base matching a prefix of rel
    max_overlap = 0
    lim = min(len(base_parts), len(rel_parts))
    for k in range(lim, 0, -1):
        if base_parts[-k:] == rel_parts[:k]:
            max_overlap = k
            break

    merged_parts = base_parts + rel_parts[max_overlap:]
    # Preserve absolute root marker if base was absolute
    leading = sep if base_norm.startswith(sep) else ""
    return f"{leading}{sep.join(merged_parts)}"


def _protocol_set(protocol: str | tuple[str, ...] | list[str]) -> set[str]:
    if isinstance(protocol, (list, tuple, set)):
        values = protocol
    else:
        values = [protocol]
    normalized = set()
    for value in values:
        if value is None:
            continue
        value = value.lower()
        if value in {"file", "local"}:
            normalized.update({"file", "local"})
        else:
            normalized.add(value)
    return normalized


def _protocol_matches(requested: str, candidates: set[str]) -> bool:
    if requested is None:
        return True
    requested = requested.lower()
    if requested in {"file", "local"}:
        return bool({"file", "local"} & candidates)
    return requested in candidates


def _strip_for_fs(fs: AbstractFileSystem, url: str) -> str:
    if not url:
        return ""
    try:
        return fs._strip_protocol(url)
    except AttributeError:
        _, path = split_protocol(url)
        return path or ""


def _detect_local_file_path(path: str) -> tuple[str, bool]:
    """Return parent directory for local files that exist."""
    if not path:
        return "", False
    try:
        candidate = Path(path)
    except (TypeError, ValueError):
        return path, False
    try:
        if candidate.exists() and candidate.is_file():
            parent = candidate.parent
            parent_str = parent.as_posix()
            return parent_str if parent_str != "." else "", True
    except OSError:
        # Non-existent or inaccessible paths fall back to original input.
        pass
    return path, False


def _default_cache_storage(cache_path_hint: str | None) -> str:
    base_cache_dir = Path.home() / ".fsspec_cache"
    if cache_path_hint:
        base_cache_dir = base_cache_dir / cache_path_hint
    return base_cache_dir.as_posix()


def filesystem(
    protocol_or_path: str | None = "",
    storage_options: Optional[Union[BaseStorageOptions, dict]] = None,
    cached: bool = False,
    cache_storage: Optional[str] = None,
    verbose: bool = False,
    dirfs: bool = True,
    base_fs: AbstractFileSystem = None,
    use_listings_cache=True,  # ← disable directory-listing cache
    skip_instance_cache=False,
    **kwargs,
) -> AbstractFileSystem:
    """Get filesystem instance with enhanced configuration options.

    Creates filesystem instances with support for storage options classes,
    intelligent caching, and protocol inference from paths.

    Args:
        protocol_or_path: Filesystem protocol (e.g., "s3", "file") or path with protocol prefix
        storage_options: Storage configuration as BaseStorageOptions instance or dict
        cached: Whether to wrap filesystem in caching layer
        cache_storage: Cache directory path (if cached=True)
        verbose: Enable verbose logging for cache operations
        dirfs: Whether to wrap filesystem in DirFileSystem
        base_fs: Base filesystem instance to use
        use_listings_cache: Whether to enable directory-listing cache
        skip_instance_cache: Whether to skip fsspec instance caching
        **kwargs: Additional filesystem arguments

    Returns:
        AbstractFileSystem: Configured filesystem instance

    Example:
        >>> # Basic local filesystem
        >>> fs = filesystem("file")
        >>>
        >>> # S3 with storage options
        >>> from fsspeckit.storage import AwsStorageOptions
        >>> opts = AwsStorageOptions(region="us-west-2")
        >>> fs = filesystem("s3", storage_options=opts, cached=True)
        >>>
        >>> # Infer protocol from path
        >>> fs = filesystem("s3://my-bucket/", cached=True)
        >>>
        >>> # GitLab filesystem
        >>> fs = filesystem("gitlab", storage_options={
        ...     "project_name": "group/project",
        ...     "token": "glpat_xxxx"
        ... })
    """
    if isinstance(protocol_or_path, Path):
        protocol_or_path = protocol_or_path.as_posix()

    raw_input = _ensure_string(protocol_or_path)
    protocol_from_kwargs = kwargs.pop("protocol", None)

    provided_protocol: str | None = None
    base_path_input: str = ""

    if raw_input:
        provided_protocol, remainder = split_protocol(raw_input)
        if provided_protocol:
            base_path_input = remainder or ""
        else:
            base_path_input = remainder or raw_input
            if base_fs is None and base_path_input in known_implementations:
                provided_protocol = base_path_input
                base_path_input = ""
    else:
        base_path_input = ""

    base_path_input = base_path_input.replace("\\", "/")

    if (
        base_fs is None
        and base_path_input
        and (provided_protocol or protocol_from_kwargs) in {None, "file", "local"}
    ):
        detected_parent, is_file = _detect_local_file_path(base_path_input)
        if is_file:
            base_path_input = detected_parent

    base_path = _normalize_path(base_path_input)
    cache_path_hint = base_path

    if base_fs is not None:
        if not dirfs:
            raise ValueError("dirfs must be True when providing base_fs")

        base_is_dir = isinstance(base_fs, DirFileSystem)
        underlying_fs = base_fs.fs if base_is_dir else base_fs
        underlying_protocols = _protocol_set(underlying_fs.protocol)
        requested_protocol = provided_protocol or protocol_from_kwargs

        if requested_protocol and not _protocol_matches(
            requested_protocol, underlying_protocols
        ):
            raise ValueError(
                f"Protocol '{requested_protocol}' does not match base filesystem protocol "
                f"{sorted(underlying_protocols)}"
            )

        sep = getattr(underlying_fs, "sep", "/") or "/"
        base_root = base_fs.path if base_is_dir else ""
        base_root_norm = _normalize_path(base_root, sep)
        cache_path_hint = base_root_norm

        fs: AbstractFileSystem
        path_for_cache = base_root_norm

        if requested_protocol:
            absolute_target = _strip_for_fs(underlying_fs, raw_input)
            absolute_target = _normalize_path(absolute_target, sep)

            if (
                base_is_dir
                and base_root_norm
                and not _is_within(base_root_norm, absolute_target, sep)
            ):
                raise ValueError(
                    f"Requested path '{absolute_target}' is outside the base directory "
                    f"'{base_root_norm}'"
                )

            if base_is_dir and absolute_target == base_root_norm:
                fs = base_fs
            else:
                fs = DirFileSystem(path=absolute_target, fs=underlying_fs)

            path_for_cache = absolute_target
        else:
            rel_input = base_path
            if rel_input:
                segments = [segment for segment in rel_input.split(sep) if segment]
                if any(segment == ".." for segment in segments):
                    raise ValueError(
                        "Relative paths must not escape the base filesystem root"
                    )

                candidate = _normalize_path(rel_input, sep)
                absolute_target = _smart_join(base_root_norm, candidate, sep)

                if (
                    base_is_dir
                    and base_root_norm
                    and not _is_within(base_root_norm, absolute_target, sep)
                ):
                    raise ValueError(
                        f"Resolved path '{absolute_target}' is outside the base "
                        f"directory '{base_root_norm}'"
                    )

                if base_is_dir and absolute_target == base_root_norm:
                    fs = base_fs
                else:
                    fs = DirFileSystem(path=absolute_target, fs=underlying_fs)

                path_for_cache = absolute_target
            else:
                fs = base_fs
                path_for_cache = base_root_norm

        cache_path_hint = path_for_cache

        if cached:
            if getattr(fs, "is_cache_fs", False):
                return fs
            storage = cache_storage
            if storage is None:
                storage = _default_cache_storage(cache_path_hint or None)
            cached_fs = MonitoredSimpleCacheFileSystem(
                fs=fs, cache_storage=storage, verbose=verbose
            )
            cached_fs.is_cache_fs = True
            return cached_fs

        if not hasattr(fs, "is_cache_fs"):
            fs.is_cache_fs = False
        return fs

    protocol = provided_protocol or protocol_from_kwargs
    if protocol is None:
        if isinstance(storage_options, dict):
            protocol = storage_options.get("protocol")
        else:
            protocol = getattr(storage_options, "protocol", None)

    protocol = protocol or "file"
    protocol = protocol.lower()

    if protocol in {"file", "local"}:
        fs = fsspec_filesystem(
            protocol,
            use_listings_cache=use_listings_cache,
            skip_instance_cache=skip_instance_cache,
        )
        if dirfs:
            dir_path: str | Path = base_path or Path.cwd()
            fs = DirFileSystem(path=dir_path, fs=fs)
            cache_path_hint = _ensure_string(dir_path)
        if not hasattr(fs, "is_cache_fs"):
            fs.is_cache_fs = False
    else:
        storage_opts = storage_options
        if isinstance(storage_opts, dict):
            storage_opts = storage_options_from_dict(protocol, storage_opts)
        if storage_opts is None:
            storage_opts = storage_options_from_dict(protocol, kwargs)
        fs = storage_opts.to_filesystem(
            use_listings_cache=use_listings_cache,
            skip_instance_cache=skip_instance_cache,
        )
        if dirfs and base_path:
            fs = DirFileSystem(path=base_path, fs=fs)
            cache_path_hint = base_path
        if not hasattr(fs, "is_cache_fs"):
            fs.is_cache_fs = False

    if cached:
        if getattr(fs, "is_cache_fs", False):
            return fs
        storage = cache_storage
        if storage is None:
            storage = _default_cache_storage(cache_path_hint or None)
        cached_fs = MonitoredSimpleCacheFileSystem(
            fs=fs, cache_storage=storage, verbose=verbose
        )
        cached_fs.is_cache_fs = True
        return cached_fs

    if not hasattr(fs, "is_cache_fs"):
        fs.is_cache_fs = False

    return fs


def get_filesystem(
    protocol_or_path: str | None = None,
    storage_options: Optional[Union[BaseStorageOptions, dict]] = None,
    cached: bool = False,
    cache_storage: Optional[str] = None,
    verbose: bool = False,
    **kwargs,
) -> fsspec.AbstractFileSystem:
    """Get filesystem instance with enhanced configuration options.

    .. deprecated:: 0.1.0
        Use :func:`filesystem` instead. This function will be removed in a future version.

    Creates filesystem instances with support for storage options classes,
    intelligent caching, and protocol inference from paths.

    Args:
        protocol_or_path: Filesystem protocol (e.g., "s3", "file") or path with protocol prefix
        storage_options: Storage configuration as BaseStorageOptions instance or dict
        cached: Whether to wrap filesystem in caching layer
        cache_storage: Cache directory path (if cached=True)
        verbose: Enable verbose logging for cache operations
        **kwargs: Additional filesystem arguments

    Returns:
        AbstractFileSystem: Configured filesystem instance

    Example:
        >>> # Basic local filesystem
        >>> fs = get_filesystem("file")
        >>>
        >>> # S3 with storage options
        >>> from fsspeckit.storage import AwsStorageOptions
        >>> opts = AwsStorageOptions(region="us-west-2")
        >>> fs = get_filesystem("s3", storage_options=opts, cached=True)
        >>>
        >>> # Infer protocol from path
        >>> fs = get_filesystem("s3://my-bucket/", cached=True)
        >>>
        >>> # GitLab filesystem
        >>> fs = get_filesystem("gitlab", storage_options={
        ...     "project_name": "group/project",
        ...     "token": "glpat_xxxx"
        ... })
    """
    warnings.warn(
        "get_filesystem() is deprecated and will be removed in a future version. "
        "Use filesystem() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return filesystem(
        protocol_or_path=protocol_or_path,
        storage_options=storage_options,
        cached=cached,
        cache_storage=cache_storage,
        verbose=verbose,
        **kwargs,
    )


def setup_filesystem_logging():
    """Setup logging configuration for filesystem operations.

    This function configures logging for filesystem-related operations,
    providing appropriate log levels and formatters for debugging
    and monitoring filesystem activity.
    """
    # Implementation would go here - this is a placeholder
    # to match what was referenced in the plan
    pass