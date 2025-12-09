"""Core filesystem functionality with factory functions and high-level APIs.

This package contains focused submodules for different filesystem concerns:
- paths: Path manipulation and protocol detection utilities
- cache: Cache filesystem classes and utilities
- gitlab: GitLab repository filesystem implementation

Main factory functions and high-level APIs are exposed here for convenience.
"""

import warnings
from typing import Any

import fsspec
from fsspec import AbstractFileSystem
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.registry import known_implementations

from fsspeckit.storage_options.base import BaseStorageOptions
from fsspeckit.storage_options.core import from_dict as storage_options_from_dict

# Import from submodules
from .paths import (
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
from .cache import (
    FileNameCacheMapper,
    MonitoredSimpleCacheFileSystem,
)
from .gitlab import GitLabFileSystem

# Import ext module for side effects (method registration)
from .. import ext  # noqa: F401


# Custom DirFileSystem methods
def dir_ls_p(
    self, path: str, detail: bool = False, **kwargs: Any
) -> list[Any] | Any:
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
) -> list[Any] | Any:
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


def _resolve_base_and_cache_paths(
    protocol: str | None,
    base_path_input: str,
    base_fs: AbstractFileSystem | None,
    dirfs: bool,
    raw_input: str,
) -> tuple[str, str | None, str]:
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
    cache_path_hint: str | None,
    cached: bool,
    cache_storage: str | None,
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


# Main factory function
def filesystem(
    protocol_or_path: str | None = "",
    storage_options: BaseStorageOptions | dict | None = None,
    cached: bool = False,
    cache_storage: str | None = None,
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
    from pathlib import Path
    from fsspec.core import split_protocol

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
        fs = fsspec.filesystem(
            protocol,
            use_listings_cache=use_listings_cache,
            skip_instance_cache=skip_instance_cache,
        )

        if dirfs:
            from pathlib import Path

            dir_path: str | Path = resolved_base_path or Path.cwd()
            fs = DirFileSystem(path=dir_path, fs=fs)
            cache_path_hint = _ensure_string(dir_path)

        return _build_filesystem_with_caching(
            fs, cache_path_hint, cached, cache_storage, verbose
        )

    # Handle other protocols
    protocol_for_instance_cache = protocol
    kwargs["protocol"] = protocol

    fs = fsspec.filesystem(
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
    storage_options: BaseStorageOptions | dict | None = None,
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


__all__ = [
    # Main factory functions
    "filesystem",
    "get_filesystem",
    # GitLab filesystem
    "GitLabFileSystem",
    # Cache utilities
    "FileNameCacheMapper",
    "MonitoredSimpleCacheFileSystem",
    # Path utilities (for advanced usage)
    "_ensure_string",
    "_normalize_path",
    "_join_paths",
    "_is_within",
    "_smart_join",
    "_protocol_set",
    "_protocol_matches",
    "_strip_for_fs",
    "_detect_local_vs_remote_path",
    "_detect_file_vs_directory_path",
    "_detect_local_file_path",
    "_default_cache_storage",
    # Setup function
    "setup_filesystem_logging",
]
