"""Filesystem-aware path normalization and validation utilities.

This module provides utilities to handle path normalization and validation across
different filesystem types (local, S3, GCS, Azure, etc.) for dataset operations.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from fsspec.implementations.local import LocalFileSystem

from fsspeckit.common.logging import get_logger
from fsspeckit.common.security import validate_path as security_validate_path
from fsspeckit.datasets.exceptions import DatasetPathError

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

logger = get_logger(__name__)

# Common cloud and remote protocols supported for dataset operations
SUPPORTED_PROTOCOLS = [
    "s3",
    "s3a",
    "gs",
    "gcs",
    "az",
    "abfs",
    "abfss",
    "file",
    "github",
    "gitlab",
]


def normalize_path(path: str, filesystem: AbstractFileSystem) -> str:
    """Normalize path based on filesystem type.

    Args:
        path: The path to normalize.
        filesystem: The filesystem instance.

    Returns:
        The normalized path.
    """
    if isinstance(filesystem, LocalFileSystem):
        # Local filesystem - use os.path operations
        return os.path.abspath(path)
    elif hasattr(filesystem, "protocol"):
        # Remote filesystem - preserve protocol and structure
        if "://" in path:
            # Already has protocol
            return path
        else:
            # Add protocol based on filesystem
            protocol = filesystem.protocol
            if isinstance(protocol, (list, tuple)):
                protocol = protocol[0]
            return f"{protocol}://{path.lstrip('/')}"
    else:
        # Fallback - return as-is
        return path


def validate_dataset_path(
    path: str, filesystem: AbstractFileSystem, operation: str
) -> None:
    """Comprehensive path validation for dataset operations.

    Args:
        path: The path to validate.
        filesystem: The filesystem instance.
        operation: The operation being performed ('read', 'write', 'merge', etc.)

    Raises:
        DatasetPathError: If the path is invalid for the given operation.
    """
    logger.debug("validating_path", path=path, operation=operation)

    # Basic security validation
    try:
        security_validate_path(path)
    except ValueError as e:
        raise DatasetPathError(
            str(e), operation=operation, details={"path": path}
        ) from e

    # Check path exists for read operations
    if operation in ["read", "merge"]:
        if not filesystem.exists(path):
            raise DatasetPathError(
                f"Dataset path does not exist: {path}",
                operation=operation,
                details={"path": path},
            )

    # Check parent directory exists for write operations
    if operation in ["write", "merge"]:
        try:
            # fsspec's AbstractFileSystem has _parent in recent versions
            parent = filesystem._parent(path)
            if (
                parent
                and parent not in ["", "/", "."]
                and not filesystem.exists(parent)
            ):
                raise DatasetPathError(
                    f"Parent directory does not exist: {parent}",
                    operation=operation,
                    details={"path": path, "parent": parent},
                )
        except (AttributeError, TypeError, ValueError):
            # Fallback or skip if _parent fails
            pass

    # Validate path format
    if "://" in path:
        # Remote path - validate protocol
        protocol = path.split("://")[0].lower()
        if protocol not in SUPPORTED_PROTOCOLS:
            raise DatasetPathError(
                f"Unsupported protocol: {protocol}",
                operation=operation,
                details={
                    "path": path,
                    "protocol": protocol,
                    "supported_protocols": SUPPORTED_PROTOCOLS,
                },
            )
