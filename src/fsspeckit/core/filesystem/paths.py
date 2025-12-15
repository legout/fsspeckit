"""Path manipulation and protocol detection utilities.

This module contains helper functions for working with filesystem paths including:
- Path normalization and joining
- Protocol detection and parsing
- Local file path detection
"""

import os
import posixpath
import urllib
from pathlib import Path


def _ensure_string(path: str | Path | None) -> str:
    """Ensure the input is a string path.

    Args:
        path: Path to normalize

    Returns:
        String path

    Raises:
        ValueError: If path is None
    """
    if path is None:
        raise ValueError("Path cannot be None")
    return str(path)


def _normalize_path(path: str, sep: str = "/") -> str:
    """Normalize a filesystem path.

    Args:
        path: Path to normalize
        sep: Path separator

    Returns:
        Normalized path
    """
    path = _ensure_string(path)

    # Handle URL-like paths
    if "://" in path:
        # Split protocol and path
        protocol, rest = path.split("://", 1)
        # Normalize the rest of the path
        normalized_rest = posixpath.normpath(rest)
        return f"{protocol}://{normalized_rest}"

    # Handle regular paths
    # Convert backslashes to forward slashes
    normalized = path.replace("\\", "/")
    # Normalize path
    normalized = posixpath.normpath(normalized)

    return normalized


def _join_paths(base: str, rel: str, sep: str = "/") -> str:
    """Join filesystem paths.

    Args:
        base: Base path
        rel: Relative path to join
        sep: Path separator

    Returns:
        Joined path
    """
    base = _normalize_path(base, sep)
    rel = _normalize_path(rel, sep)

    # Handle URL-like paths
    if "://" in base:
        protocol, rest = base.split("://", 1)
        joined = posixpath.join(rest, rel)
        return f"{protocol}://{joined}"

    # Handle regular paths
    return posixpath.join(base, rel)


def _is_within(base: str, target: str, sep: str = "/") -> bool:
    """Check if target path is within base path.

    Args:
        base: Base path
        target: Target path to check
        sep: Path separator

    Returns:
        True if target is within base
    """
    base = _normalize_path(base, sep)
    target = _normalize_path(target, sep)

    # Handle URL-like paths
    if "://" in base:
        protocol, base_rest = base.split("://", 1)
        if "://" in target:
            target_protocol, target_rest = target.split("://", 1)
            if protocol != target_protocol:
                return False
            return _is_within(base_rest, target_rest, sep)

    # Normalize both paths
    base_parts = posixpath.normpath(base).split(sep)
    target_parts = posixpath.normpath(target).split(sep)

    # Check if target is within base
    return tuple(target_parts[: len(base_parts)]) == tuple(base_parts)


def _smart_join(base: str, rel: str, sep: str = "/") -> str:
    """Smart path joining that handles URLs and relative paths.

    Args:
        base: Base path
        rel: Relative path
        sep: Path separator

    Returns:
        Smartly joined path
    """
    base = _normalize_path(base, sep)
    rel = _normalize_path(rel, sep)

    # If base is a URL
    if "://" in base:
        protocol, rest = base.split("://", 1)
        # If rel is absolute, use it
        if rel.startswith(sep) or "://" in rel:
            return rel
        # Otherwise join with base
        joined = posixpath.join(rest, rel)
        return f"{protocol}://{joined}"

    # If rel is absolute, use it
    if rel.startswith(sep):
        return rel

    # Otherwise join
    return posixpath.join(base, rel)


def _protocol_set(protocol: str | tuple[str, ...] | list[str]) -> set[str]:
    """Convert protocol specification to a set.

    Args:
        protocol: Protocol specification

    Returns:
        Set of protocols
    """
    if isinstance(protocol, str):
        return {protocol}
    elif isinstance(protocol, (tuple, list)):
        return set(protocol)
    else:
        return set()


def _protocol_matches(requested: str, candidates: set[str]) -> bool:
    """Check if requested protocol matches candidates.

    Args:
        requested: Requested protocol
        candidates: Set of candidate protocols

    Returns:
        True if match found
    """
    # Direct match
    if requested in candidates:
        return True

    # Check for wildcard
    if "*" in candidates:
        return True

    # Check for partial matches (e.g., "s3" matches "s3n", "s3a")
    for candidate in candidates:
        if candidate.startswith(requested) or requested.startswith(candidate):
            return True

    return False


def _strip_for_fs(fs, url: str) -> str:
    """Strip protocol from URL for filesystem.

    Args:
        fs: Filesystem instance
        url: URL to strip

    Returns:
        URL without protocol
    """
    from fsspec.core import split_protocol

    protocol = split_protocol(url)[0]
    if protocol and protocol in fs.protocol:
        return split_protocol(url)[1]
    return url


def _detect_local_vs_remote_path(path: str) -> tuple[str, bool]:
    """Detect if path is local (filesystem) vs remote (URL-based).

    Args:
        path: Path to check

    Returns:
        Tuple of (normalized_path, is_local_filesystem)
    """
    raw = str(path)

    # Detect URLs before normalizing; os.path.normpath can collapse "://"
    # into ":/" which would cause URL-like paths to be misclassified as local.
    if raw.startswith("http://") or raw.startswith("https://") or "://" in raw:
        return (raw, False)

    normalized = os.path.normpath(raw)
    return (normalized, True)


def _detect_file_vs_directory_path(path: str) -> tuple[str, bool]:
    """Detect if path refers to a file vs directory.

    Args:
        path: Path to check

    Returns:
        Tuple of (normalized_path, is_file)
    """
    raw = str(path)
    normalized = os.path.normpath(raw)

    # Preserve the caller's directory intent. Using normpath() first would remove
    # trailing slashes and make directories indistinguishable from files.
    is_file = not raw.endswith("/")

    return (normalized, is_file)


def _detect_local_file_path(path: str) -> tuple[str, bool]:
    """Detect if path is a local file path (deprecated name).

    This function is deprecated and maintained for backward compatibility.
    Use _detect_local_vs_remote_path() instead for clarity.

    Args:
        path: Path to check

    Returns:
        Tuple of (normalized_path, is_local)
    """
    return _detect_local_vs_remote_path(path)


def _default_cache_storage(cache_path_hint: str | None) -> str:
    """Get default cache storage path.

    Args:
        cache_path_hint: Optional hint for cache path

    Returns:
        Cache storage path
    """
    if cache_path_hint:
        return os.path.expanduser(cache_path_hint)

    # Default to ~/.cache/fsspec
    return os.path.join(os.path.expanduser("~"), ".cache", "fsspec")
