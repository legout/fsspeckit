"""Core filesystem functionality with factory functions and high-level APIs.

This package contains focused submodules for different filesystem concerns:
- paths: Path manipulation and protocol detection utilities
- cache: Cache filesystem classes and utilities
- gitlab: GitLab repository filesystem implementation

Main factory functions and high-level APIs are exposed here for convenience.
"""

import warnings
from typing import Any, Union

import fsspec
from fsspec import AbstractFileSystem
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.registry import known_implementations

from fsspeckit.storage_options.base import BaseStorageOptions
from fsspeckit.storage_options.core import from_dict as storage_options_from_dict

# Import ext module for side effects (method registration)
from .. import ext  # noqa: F401
from .cache import (
    FileNameCacheMapper,
    MonitoredSimpleCacheFileSystem,
)
from .gitlab import GitLabFileSystem

# Import from submodules
from .paths import (
    _default_cache_storage,
    _detect_file_vs_directory_path,
    _detect_local_file_path,
    _detect_local_vs_remote_path,
    _ensure_string,
    _is_within,
    _join_paths,
    _normalize_path,
    _protocol_matches,
    _protocol_set,
    _smart_join,
    _strip_for_fs,
)


# Custom DirFileSystem methods
def dir_ls_p(
    self, path: str, detail: bool = False, **kwargs: Any
) -> Union[list[Any], Any]:
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
) -> Union[list[Any], Any]:
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


_orig_dirfs_ls = DirFileSystem.ls
_orig_dirfs__ls = DirFileSystem._ls


async def _dirfs_ls_default_detail_false(
    self, path: str, detail: bool = False, **kwargs: Any
) -> Any:
    return await _orig_dirfs__ls(self, path, detail=detail, **kwargs)


def dirfs_ls_default_detail_false(
    self, path: str, detail: bool = False, **kwargs: Any
) -> Any:
    return _orig_dirfs_ls(self, path, detail=detail, **kwargs)


DirFileSystem._ls = _dirfs_ls_default_detail_false
DirFileSystem.ls = dirfs_ls_default_detail_false


def _resolve_base_and_cache_paths(
    protocol: Union[str, None],
    base_path_input: str,
    base_fs: Union[AbstractFileSystem, None],
    dirfs: bool,
    raw_input: str,
) -> tuple[str, Union[str, None], str]:
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
                if base_root_norm and candidate and not candidate.startswith(sep):
                    base_parts = [part for part in base_root_norm.split(sep) if part]
                    candidate_parts = [part for part in candidate.split(sep) if part]
                    max_overlap = min(len(base_parts), len(candidate_parts))
                    for overlap in range(max_overlap, 0, -1):
                        if base_parts[-overlap:] == candidate_parts[:overlap]:
                            candidate_parts = candidate_parts[overlap:]
                            break
                    candidate = sep.join(candidate_parts)
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

        # If the input only specifies a protocol (e.g. "s3") or a protocol root
        # (e.g. "s3://"), avoid normalizing an empty path into ".". Treat this as
        # "no base path provided" so we don't accidentally wrap in DirFileSystem.
        if protocol not in {None, "file", "local"} and resolved_base_path in {"", "/"}:
            return "", "", ""
        if protocol in {None, "file", "local"} and not resolved_base_path:
            return "", "", ""

        # For local filesystems, detect and normalize local paths
        if protocol in {None, "file", "local"}:
            detected_parent, is_local_fs = _detect_local_vs_remote_path(base_path_input)
            if is_local_fs:
                resolved_base_path = detected_parent

                # If the resolved local path is an existing file, root the filesystem
                # at its parent directory.
                try:
                    from pathlib import Path

                    path_obj = Path(resolved_base_path)
                    if path_obj.exists() and path_obj.is_file():
                        resolved_base_path = path_obj.parent.as_posix()
                except Exception:
                    pass

        resolved_base_path = _normalize_path(resolved_base_path)
        cache_path_hint = resolved_base_path

        return resolved_base_path, cache_path_hint, resolved_base_path


def _build_filesystem_with_caching(
    fs: AbstractFileSystem,
    cache_path_hint: Union[str, None],
    cached: bool,
    cache_storage: Union[str, None],
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


def _transform_ssl_for_cloud_protocol(
    protocol: str,
    allow_invalid_certificates: Union[bool, None],
    allow_invalid_certs: Union[bool, None],
    verify: Union[bool, None],
    use_ssl: Union[bool, None],
) -> dict[str, Any]:
    """Transform SSL parameters to format expected by cloud filesystems.

    Args:
        protocol: The filesystem protocol (e.g., 's3', 'gcs', 'azure')
        allow_invalid_certificates: Skip SSL certificate validation
        allow_invalid_certs: Deprecated alias for allow_invalid_certificates
        verify: Whether to verify SSL certificates
        use_ssl: Whether to use SSL/HTTPS

    Returns:
        Dictionary with transformed parameters suitable for cloud filesystems
    """
    # Only transform for cloud protocols
    cloud_protocols = {"s3", "s3a", "s3n", "gcs", "azure", "adl", "abfs"}
    if protocol.lower() not in cloud_protocols:
        return {}

    # Handle deprecated allow_invalid_certs alias
    if allow_invalid_certs is not None:
        if allow_invalid_certificates is None:
            allow_invalid_certificates = allow_invalid_certs
        else:
            import warnings

            warnings.warn(
                "Both allow_invalid_certificates and allow_invalid_certs specified. "
                "Using allow_invalid_certificates (allow_invalid_certs is deprecated).",
                DeprecationWarning,
                stacklevel=4,
            )

    # Transform SSL parameters for cloud filesystems
    # For cloud protocols, SSL parameters should go in client_kwargs
    result = {}

    # Build client_kwargs with SSL parameters
    client_kwargs = {}

    # Handle allow_invalid_certificates -> verify transformation
    if allow_invalid_certificates is not None:
        # For cloud protocols: allow_invalid_certificates=True means verify=False
        client_kwargs["verify"] = not allow_invalid_certificates

    # Handle direct verify parameter (if provided, it takes precedence)
    if verify is not None:
        client_kwargs["verify"] = verify

    # Handle use_ssl parameter
    if use_ssl is not None:
        client_kwargs["use_ssl"] = use_ssl

    # Only include client_kwargs if it has content
    if client_kwargs:
        result["client_kwargs"] = client_kwargs

    return result


def _build_fsspec_kwargs(
    protocol: str,
    storage_options: Union[BaseStorageOptions, dict] | None,
    allow_invalid_certificates: Union[bool, None],
    allow_invalid_certs: Union[bool, None],
    verify: Union[bool, None],
    use_ssl: Union[bool, None],
    **kwargs: Any,
) -> dict[str, Any]:
    """Build kwargs for fsspec.filesystem by merging storage_options and direct parameters.

    Parameter precedence:
    1. Direct SSL parameters (highest precedence)
    2. Storage options kwargs
    3. Other kwargs (lowest precedence)

    Args:
        storage_options: Storage configuration
        allow_invalid_certificates: Skip SSL certificate validation
        allow_invalid_certs: Deprecated alias for allow_invalid_certificates
        verify: Whether to verify SSL certificates
        use_ssl: Whether to use SSL/HTTPS
        **kwargs: Additional filesystem arguments

    Returns:
        kwargs suitable for fsspec.filesystem
    """
    from fsspeckit.storage_options.base import BaseStorageOptions

    # Parameters that should not be taken from storage_options
    # as they are handled directly by filesystem() function
    RESERVED_PARAMS = {
        "use_listings_cache",
        "skip_instance_cache",
    }

    # Start with base kwargs
    fsspec_kwargs = dict(kwargs)

    # Handle deprecated allow_invalid_certs alias
    if allow_invalid_certs is not None:
        if allow_invalid_certificates is None:
            allow_invalid_certificates = allow_invalid_certs
        else:
            import warnings

            warnings.warn(
                "Both allow_invalid_certificates and allow_invalid_certs specified. "
                "Using allow_invalid_certificates (allow_invalid_certs is deprecated).",
                DeprecationWarning,
                stacklevel=3,
            )

    # Add SSL parameters as direct kwargs for cloud protocols
    # Transform SSL parameters for cloud protocols
    ssl_transform = _transform_ssl_for_cloud_protocol(
        protocol,
        allow_invalid_certificates,
        allow_invalid_certs,
        verify,
        use_ssl,
    )

    # Add transformed SSL params to fsspec_kwargs (direct params take precedence)
    for key, value in ssl_transform.items():
        if value is not None:
            fsspec_kwargs[key] = value

    # If storage_options provided, merge their kwargs
    if storage_options is not None:
        if isinstance(storage_options, dict):
            storage_kwargs = storage_options.copy()
        else:
            # BaseStorageOptions has to_fsspec_kwargs method
            storage_kwargs = storage_options.to_fsspec_kwargs()

        # Merge storage kwargs, excluding reserved params
        # (direct params override storage options)
        for key, value in storage_kwargs.items():
            if key not in RESERVED_PARAMS and (
                key not in fsspec_kwargs or fsspec_kwargs[key] is None
            ):
                fsspec_kwargs[key] = value

    return fsspec_kwargs


# Main factory function
def filesystem(
    protocol_or_path: Union[str, None] = "",
    storage_options: Union[BaseStorageOptions, dict] | None = None,
    cached: bool = False,
    cache_storage: Union[str, None] = None,
    verbose: bool = False,
    dirfs: bool = True,
    base_fs: AbstractFileSystem = None,
    use_listings_cache: bool = True,
    skip_instance_cache: bool = False,
    # SSL parameters
    allow_invalid_certificates: Union[bool, None] = None,
    allow_invalid_certs: Union[
        bool, None
    ] = None,  # deprecated alias for allow_invalid_certificates
    verify: Union[bool, None] = None,
    use_ssl: Union[bool, None] = None,
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
        allow_invalid_certificates: Skip SSL certificate validation (for cloud protocols)
        allow_invalid_certs: Deprecated alias for allow_invalid_certificates
        verify: Whether to verify SSL certificates (for cloud protocols)
        use_ssl: Whether to use SSL/HTTPS (for cloud protocols)
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

    # Note: SSL parameters are now explicit parameters, not in kwargs

    provided_protocol: Union[str, None] = None
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

            dir_path: Union[str, Path] = resolved_base_path or Path.cwd()
            fs = DirFileSystem(path=dir_path, fs=fs)
            cache_path_hint = _ensure_string(dir_path)

        return _build_filesystem_with_caching(
            fs, cache_path_hint, cached, cache_storage, verbose
        )

    # Handle other protocols
    # Build kwargs by merging storage_options and direct SSL parameters
    fsspec_kwargs = _build_fsspec_kwargs(
        protocol=protocol,
        storage_options=storage_options,
        allow_invalid_certificates=allow_invalid_certificates,
        allow_invalid_certs=allow_invalid_certs,
        verify=verify,
        use_ssl=use_ssl,
        **kwargs,
    )

    fs = fsspec.filesystem(
        protocol,
        **fsspec_kwargs,
        use_listings_cache=use_listings_cache,
        skip_instance_cache=skip_instance_cache,
    )

    if dirfs and resolved_base_path:
        # When a URI contains a path, wrap the remote filesystem to enforce that root
        fs = DirFileSystem(path=resolved_base_path, fs=fs)

    return _build_filesystem_with_caching(
        fs, cache_path_hint, cached, cache_storage, verbose
    )


def get_filesystem(
    protocol_or_path: Union[str, None] = "",
    storage_options: Union[BaseStorageOptions, dict] | None = None,
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
