"""Core exception types for fsspeckit path validation.

This module defines the path-validation exception hierarchy that the core
path-normalization layer raises. By living in the core layer (below
``datasets``) it inverts the historical core→datasets import that the
layering check rejects.

Hierarchy
---------
``DatasetError`` is the shared base that carries an optional operation name
and structured details. ``DatasetPathError`` is the only subtype the core
layer raises today — additional dataset-layer error types live in
``fsspeckit.datasets.exceptions`` and inherit from this base.
"""

from __future__ import annotations

from typing import Any


class DatasetError(Exception):
    """Base exception for fsspeckit dataset operations.

    Attributes:
        message: Human-readable error description.
        operation: The operation that failed (e.g., 'read', 'write', 'merge').
        details: Additional structured context about the error.
    """

    def __init__(
        self,
        message: str,
        operation: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.operation = operation
        self.details = details or {}

    def __str__(self) -> str:
        parts = [str(self.args[0])]
        if self.operation:
            parts.append(f"Operation: {self.operation}")
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            parts.append(f"Details: {detail_str}")
        return " | ".join(parts)


class DatasetPathError(DatasetError):
    """Raised when path-related operations fail.

    Used for path normalization failures, missing paths, and invalid
    protocols surfaced through core path validation. The datasets layer
    preserves and enriches this error type at the public boundary.
    """
