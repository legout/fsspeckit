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
import urllib.parse
from pathlib import Path
from typing import Any, Optional, Union, List

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
    _detect_local_vs_remote_path,
    _detect_file_vs_directory_path,
    _detect_local_file_path,
    _default_cache_storage,
)

# Import cache classes from submodule
from .filesystem_cache import (
    FileNameCacheMapper,
    MonitoredSimpleCacheFileSystem,
)

logger = get_logger(__name__)


def _resolve_base_and_cache_paths(
    protocol: Optional[str],
    base_path_input: str,
    base_fs: Optional[AbstractFileSystem],
    dirfs: bool,
    raw_input: str,
) -> tuple[str, Optional[str], str]:
    """Resolve base path and cache path hint from inputs.

    Args:
        protocol: Detected or provided protocol
        base_path_input: Base path from input parsing
        base_fs: Optional base filesystem instance
        dirfs: Whether DirFileSystem wrapping is enabled
        raw_input: Original input string

    Returns:
        Tuple of (resolved_base_path, cache_path_hint, target_path)
    """
    if base_fs is not None:
        # When base_fs is provided, use its structure
        base_is_dir = isinstance(base_fs, DirFileSystem)
        underlying_fs = base_fs.fs if base_is_dir else base_fs
        sep = getattr(underlying_fs, "sep", "/") or "/"
        base_root = base_fs.path if base_is_dir else ""
        base_root_norm = _normalize_path(base_root, sep)

        # For base_fs case, cache path is based on the base root
        cache_path_hint = base_root_norm

        if protocol:
            # When protocol is specified, target is derived from raw_input
            target_path = _strip_for_fs(underlying_fs, raw_input)
            target_path = _normalize_path(target_path, sep)

            # Validate that target is within base directory
            if (
                base_is_dir
                and base_root_norm
                and not _is_within(base_root_norm, target_path, sep)
            ):
                raise ValueError(
                    f"Requested path '{target_path}' is outside the base directory "
                    f"'{base_root_norm}'"
                )
        else:
            # When no protocol, target is based on base_path_input relative to base
            if base_path_input:
                segments = [
                    segment for segment in base_path_input.split(sep) if segment
                ]
                if any(segment == ".." for segment in segments):
                    raise ValueError(
                        "Relative paths must not escape the base filesystem root"
                    )

                candidate = _normalize_path(base_path_input, sep)
                target_path = _smart_join(base_root_norm, candidate, sep)

                # Validate that target is within base directory
                if (
                    base_is_dir
                    and base_root_norm
                    and not _is_within(base_root_norm, target_path, sep)
                ):
                    raise ValueError(
                        f"Resolved path '{target_path}' is outside the base "
                        f"directory '{base_root_norm}'"
                    )
            else:
                target_path = base_root_norm

        cache_path_hint = target_path
        return base_root_norm, cache_path_hint, target_path
    else:
        # When no base_fs, handle local vs remote path resolution
        resolved_base_path = base_path_input

        # For local filesystems, detect and normalize local paths
        if protocol in {None, "file", "local"}:
            detected_parent, is_local_fs = _detect_local_vs_remote_path(base_path_input)
            if is_local_fs:
                resolved_base_path = detected_parent

        resolved_base_path = _normalize_path(resolved_base_path)
        cache_path_hint = resolved_base_path

        return resolved_base_path, cache_path_hint, resolved_base_path


def _build_filesystem_with_caching(
    fs: AbstractFileSystem,
    cache_path_hint: Optional[str],
    cached: bool,
    cache_storage: Optional[str],
    verbose: bool,
) -> AbstractFileSystem:
    """Wrap filesystem with caching if requested.

    Args:
        fs: Base filesystem instance
        cache_path_hint: Hint for cache storage location
        cached: Whether to enable caching
        cache_storage: Explicit cache storage path
        verbose: Whether to enable verbose cache logging

    Returns:
        Filesystem instance (possibly wrapped with cache)
    """
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


# Custom DirFileSystem methods
def dir_ls_p(
    self, path: str, detail: bool = False, **kwargs: Any
) -> Union[List[Any], Any]:
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


def mscf_ls_p(
    self, path: str, detail: bool = False, **kwargs: Any
) -> Union[List[Any], Any]:
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
        timeout: float = 30.0,
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
            timeout: Request timeout in seconds
            **kwargs: Additional arguments
        """
        super().__init__(**kwargs)

        self.base_url = base_url.rstrip("/")
        self.project_id = project_id
        self.project_name = project_name
        self.ref = ref
        self.token = token
        self.api_version = api_version
        self.timeout = timeout

        if not project_id and not project_name:
            raise ValueError("Either project_id or project_name must be provided")

        # Create a shared requests session with timeout
        self._session = requests.Session()
        if self.token:
            self._session.headers["PRIVATE-TOKEN"] = self.token

    def _get_project_identifier(self) -> str:
        """Get URL-encoded project identifier for API calls.

        Returns:
            URL-encoded project identifier (ID or path)
        """
        if self.project_id:
            identifier = str(self.project_id)
        else:
            identifier = self.project_name

        # URL-encode the project identifier to handle special characters
        return urllib.parse.quote(identifier, safe="")

    def _make_request(self, endpoint: str, params: dict = None) -> requests.Response:
        """Make API request to GitLab with proper error handling.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Response object

        Raises:
            requests.RequestException: For HTTP errors
        """
        if params is None:
            params = {}

        # URL-encode the endpoint path
        encoded_endpoint = urllib.parse.quote(endpoint, safe="")
        project_identifier = self._get_project_identifier()

        url = f"{self.base_url}/api/{self.api_version}/projects/{project_identifier}/{encoded_endpoint}"

        try:
            response = self._session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(
                "GitLab API request failed: %s %s - %s",
                e.response.status_code if e.response else "N/A",
                e.response.reason if e.response else str(e),
                url,
            )
            if e.response is not None:
                logger.error("Response content: %s", e.response.text[:500])
            raise

    def _get_file_path(self, path: str) -> str:
        """Get URL-encoded full file path in repository.

        Args:
            path: File path

        Returns:
            URL-encoded full file path
        """
        # Remove leading slash if present
        path = path.lstrip("/")
        # URL-encode the path to handle special characters
        encoded_path = urllib.parse.quote(path, safe="")
        return f"/{encoded_path}"

    def ls(
        self, path: str = "", detail: bool = False, **kwargs: Any
    ) -> Union[List[Any], Any]:
        """List files in repository with pagination support.

        Args:
            path: Directory path
            detail: Whether to return detailed information
            **kwargs: Additional arguments

        Returns:
            List of files
        """
        all_files = []
        page = 1
        per_page = 100

        while True:
            params = {"ref": self.ref, "per_page": per_page, "page": page}

            if path:
                params["path"] = path.lstrip("/")

            try:
                response = self._make_request("repository/tree", params)
                files = response.json()

                if not files:
                    # No more pages
                    break

                all_files.extend(files)

                # Check for pagination headers
                next_page = response.headers.get("X-Next-Page")
                if not next_page:
                    # No more pages
                    break

                page = int(next_page)

            except requests.RequestException:
                # If we have some files already, return what we have
                if all_files:
                    logger.warning(
                        "GitLab API request failed for page %d, returning %d files from previous pages",
                        page,
                        len(all_files),
                    )
                    break
                else:
                    # Re-raise if no files collected yet
                    raise

        if detail:
            return all_files
        else:
            return [item["name"] for item in all_files]

    def cat_file(self, path: str, **kwargs: Any) -> bytes:
        """Get file content.

        Args:
            path: File path
            **kwargs: Additional arguments

        Returns:
            File content

        Raises:
            requests.HTTPError: If file not found or other HTTP error
        """
        params = {"ref": self.ref}

        # URL-encode the file path
        encoded_path = urllib.parse.quote(path.lstrip("/"), safe="")

        response = self._make_request(f"repository/files/{encoded_path}", params)
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

        Raises:
            requests.HTTPError: If file not found or other HTTP error
        """
        params = {"ref": self.ref}

        # URL-encode the file path
        encoded_path = urllib.parse.quote(path.lstrip("/"), safe="")

        response = self._make_request(f"repository/files/{encoded_path}", params)
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
        except requests.HTTPError as e:
            if e.response and e.response.status_code == 404:
                return False
            # Re-raise for other HTTP errors
            raise
        except requests.RequestException:
            # Re-raise for other request errors
            raise


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

    # Resolve base path and cache path using helpers
    resolved_base_path, cache_path_hint, target_path = _resolve_base_and_cache_paths(
        provided_protocol, base_path_input, base_fs, dirfs, raw_input
    )

    if base_fs is not None:
        # Handle base filesystem case
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

        # Build the appropriate filesystem
        if target_path == (base_fs.path if base_is_dir else ""):
            fs = base_fs
        else:
            fs = DirFileSystem(path=target_path, fs=underlying_fs)

        return _build_filesystem_with_caching(
            fs, cache_path_hint, cached, cache_storage, verbose
        )

    # Handle non-base filesystem case
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
            dir_path: str | Path = resolved_base_path or Path.cwd()
            fs = DirFileSystem(path=dir_path, fs=fs)
            cache_path_hint = _ensure_string(dir_path)

        return _build_filesystem_with_caching(
            fs, cache_path_hint, cached, cache_storage, verbose
        )

    # Handle other protocols
    protocol_for_instance_cache = protocol
    kwargs["protocol"] = protocol

    fs = fsspec_filesystem(
        protocol,
        **kwargs,
        use_listings_cache=use_listings_cache,
        skip_instance_cache=skip_instance_cache,
    )

    return _build_filesystem_with_caching(
        fs, cache_path_hint, cached, cache_storage, verbose
    )


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
