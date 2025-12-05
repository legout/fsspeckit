"""Core filesystem functionality - focused on factory functions and high-level APIs.

This module has been refactored to be more focused:
- Main factory functions (filesystem, get_filesystem)
- High-level filesystem types (GitLabFileSystem)
- Path helpers imported from filesystem_paths
- Cache classes imported from filesystem_cache

Internal implementation details have been moved to:
- filesystem_paths: Path manipulation and protocol detection
- filesystem_cache: Cache mapper and monitored cache filesystem
"""

import inspect
import os
import posixpath
from pathlib import Path
from typing import Any, Optional, Union

import fsspec
import requests
from fsspec import filesystem as fsspec_filesystem
from fsspec.core import split_protocol
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.memory import MemoryFile
from fsspec.registry import known_implementations

from fsspec import AbstractFileSystem

from ..storage_options.base import BaseStorageOptions
from ..storage_options.core import from_dict as storage_options_from_dict
from ..common.logging import get_logger

# Import ext module for side effects (method registration)
from . import ext  # noqa: F401

# Import path helpers from submodule
from .filesystem_paths import (
    _ensure_string,
    _normalize_path,
    _join_paths,
    _is_within,
    _smart_join,
    _protocol_set,
    _protocol_matches,
    _strip_for_fs,
    _detect_local_file_path,
    _default_cache_storage,
)

# Import cache classes from submodule
from .filesystem_cache import (
    FileNameCacheMapper,
    MonitoredSimpleCacheFileSystem,
)

logger = get_logger(__name__)


# Custom DirFileSystem methods
def dir_ls_p(self, path: str, detail: bool = False, **kwargs: Any) -> Union[List[Any], Any]:
    """List directory contents with path handling.

    Args:
        path: Directory path
        detail: Whether to return detailed information
        **kwargs: Additional arguments

    Returns:
        Directory listing
    """
    path = self._strip_protocol(path)
    return self.fs.ls(path, detail=detail, **kwargs)


def mscf_ls_p(self, path: str, detail: bool = False, **kwargs: Any) -> Union[List[Any], Any]:
    """List directory for monitored cache filesystem.

    Args:
        path: Directory path
        detail: Whether to return detailed information
        **kwargs: Additional arguments

    Returns:
        Directory listing
    """
    return self.fs.ls(path, detail=detail, **kwargs)


# Attach methods to DirFileSystem
DirFileSystem.ls_p = dir_ls_p


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
        ```python
        # Public repository
        fs = GitLabFileSystem(
            project_name="group/project",
            ref="main",
        )
        files = fs.ls("/")

        # Private repository with token
        fs = GitLabFileSystem(
            project_id="12345",
            token="glpat_xxxx",
            ref="develop",
        )
        content = fs.cat("README.md")
        ```
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
        **kwargs: Any,
    ):
        """Initialize GitLab filesystem.

        Args:
            base_url: GitLab instance URL
            project_id: Project ID number
            project_name: Project name/path (alternative to project_id)
            ref: Git reference (branch, tag, or commit SHA)
            token: GitLab personal access token
            api_version: API version to use
            **kwargs: Additional arguments
        """
        super().__init__(**kwargs)

        self.base_url = base_url.rstrip("/")
        self.project_id = project_id
        self.project_name = project_name
        self.ref = ref
        self.token = token
        self.api_version = api_version

        if not project_id and not project_name:
            raise ValueError("Either project_id or project_name must be provided")

    def _get_project_identifier(self) -> str:
        """Get project identifier for API calls.

        Returns:
            Project identifier (ID or path)
        """
        if self.project_id:
            return str(self.project_id)
        return self.project_name

    def _make_request(self, endpoint: str, params: dict = None) -> requests.Response:
        """Make API request to GitLab.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Response object
        """
        if params is None:
            params = {}

        url = f"{self.base_url}/api/{self.api_version}/projects/{self._get_project_identifier()}/{endpoint}"

        headers = {}
        if self.token:
            headers["PRIVATE-TOKEN"] = self.token

        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response

    def _get_file_path(self, path: str) -> str:
        """Get full file path in repository.

        Args:
            path: File path

        Returns:
            Full file path
        """
        # Remove leading slash if present
        path = path.lstrip("/")
        return f"/{path}"

    def ls(self, path: str = "", detail: bool = False, **kwargs: Any) -> Union[List[Any], Any]:
        """List files in repository.

        Args:
            path: Directory path
            detail: Whether to return detailed information
            **kwargs: Additional arguments

        Returns:
            List of files
        """
        params = {"ref": self.ref, "per_page": 100}

        if path:
            params["path"] = path.lstrip("/")

        response = self._make_request("repository/tree", params)
        files = response.json()

        if detail:
            return files
        else:
            return [item["name"] for item in files]

    def cat_file(self, path: str, **kwargs: Any) -> bytes:
        """Get file content.

        Args:
            path: File path
            **kwargs: Additional arguments

        Returns:
            File content
        """
        params = {"ref": self.ref}

        response = self._make_request(f"repository/files/{path}", params)
        data = response.json()

        import base64

        return base64.b64decode(data["content"])

    def info(self, path: str, **kwargs: Any) -> dict:
        """Get file information.

        Args:
            path: File path
            **kwargs: Additional arguments

        Returns:
            File information
        """
        params = {"ref": self.ref}

        response = self._make_request(f"repository/files/{path}", params)
        return response.json()

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if file exists.

        Args:
            path: File path
            **kwargs: Additional arguments

        Returns:
            True if file exists
        """
        try:
            self.info(path, **kwargs)
            return True
        except requests.HTTPError:
            return False


# Main factory function
def filesystem(
    protocol_or_path: str | None = "",
    storage_options: Optional[Union[BaseStorageOptions, dict]] = None,
    cached: bool = False,
    cache_storage: Optional[str] = None,
    verbose: bool = False,
    dirfs: bool = True,
    base_fs: AbstractFileSystem = None,
    use_listings_cache: bool = True,  # ← disable directory-listing cache
    skip_instance_cache: bool = False,
    **kwargs: Any,
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
        ```python
        # Basic local filesystem
        fs = filesystem("file")

        # S3 with storage options
        from fsspeckit.storage_options import AwsStorageOptions
        opts = AwsStorageOptions(region="us-west-2")
        fs = filesystem("s3", storage_options=opts, cached=True)

        # Infer protocol from path
        fs = filesystem("s3://my-bucket/", cached=True)

        # GitLab filesystem
        fs = filesystem(
            "gitlab",
            storage_options={
                "project_name": "group/project",
                "token": "glpat_xxxx",
            },
        )
        ```
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

    protocol_for_instance_cache = protocol
    kwargs["protocol"] = protocol

    fs = fsspec_filesystem(
        protocol,
        **kwargs,
        use_listings_cache=use_listings_cache,
        skip_instance_cache=skip_instance_cache,
    )

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
    protocol_or_path: str | None = "",
    storage_options: Optional[Union[BaseStorageOptions, dict]] = None,
    **kwargs: Any,
) -> AbstractFileSystem:
    """Get filesystem instance (simple version).

    This is a simplified version of filesystem() for backward compatibility.
    See filesystem() for full documentation.

    Args:
        protocol_or_path: Filesystem protocol or path
        storage_options: Storage configuration
        **kwargs: Additional arguments

    Returns:
        AbstractFileSystem: Filesystem instance
    """
    return filesystem(
        protocol_or_path=protocol_or_path,
        storage_options=storage_options,
        **kwargs,
    )


def setup_filesystem_logging() -> None:
    """Setup filesystem logging configuration."""
    # This is a placeholder for any filesystem-specific logging setup
    # Currently, logging is handled by the common logging module
    pass
