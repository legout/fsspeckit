"""Filesystem-aware path normalization and validation utilities.

This module provides utilities to handle path normalization and validation across
different filesystem types (local, S3, GCS, Azure, etc.) for dataset operations.

The normalize_path function now delegates to core/filesystem/paths.normalize_path
to provide a unified normalization interface across the codebase.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fsspeckit.common.logging import get_logger
from fsspeckit.common.security import validate_path as security_validate_path
from fsspeckit.core.filesystem.paths import normalize_path as core_normalize_path
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

    This function now delegates to core/filesystem/paths.normalize_path to provide
    unified path normalization across the codebase. The delegation preserves all
    existing behavior while eliminating duplicate normalization logic.

    Args:
        path: The path to normalize.
        filesystem: The filesystem instance.

    Returns:
        The normalized path.
    """
    # Delegate to core normalize_path with filesystem
    return core_normalize_path(path, filesystem=filesystem)


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
        except (AttributeError, TypeError, ValueError):
            parent = None

        if (
            parent
            and parent not in ["", "/", "."]
            and not filesystem.exists(parent)
        ):
            if operation == "write":
                created = False
                for method_name, kwargs in (
                    ("mkdirs", {"exist_ok": True}),
                    ("makedirs", {"exist_ok": True}),
                    ("mkdir", {"create_parents": True}),
                ):
                    method = getattr(filesystem, method_name, None)
                    if method is None:
                        continue
                    try:
                        method(parent, **kwargs)
                        created = True
                        break
                    except TypeError:
                        try:
                            method(parent)
                            created = True
                            break
                        except Exception:
                            continue
                    except Exception:
                        continue

                if created and filesystem.exists(parent):
                    pass
                else:
                    raise DatasetPathError(
                        f"Parent directory does not exist: {parent}",
                        operation=operation,
                        details={"path": path, "parent": parent},
                    )
            else:
                raise DatasetPathError(
                    f"Parent directory does not exist: {parent}",
                    operation=operation,
                    details={"path": path, "parent": parent},
                )

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
