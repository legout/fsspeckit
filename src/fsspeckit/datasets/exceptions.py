"""Custom exceptions for dataset operations.

This module provides a consistent exception hierarchy for all dataset operations,
enabling proper error handling and providing clear error categorization.

Error Handling Patterns:
1. Data Integrity Operations (Read/Write/Merge):
   - Fail fast by raising DatasetFileError or DatasetOperationError.
   - Include the 'operation' and 'path' context.
   - Preserve original exception via 'from e'.

2. Optional Metadata/Informational Reads:
   - Log warning or debug message.
   - Use sentinel values (None, 0, or default dicts).
   - Do not interrupt main execution flow.

3. Cleanup Operations:
   - Use try-except blocks.
   - Log warnings for failures but continue cleanup of other resources.
   - Never raise from within a cleanup loop unless catastrophic.

4. Validation:
   - Raise DatasetValidationError for user input issues.
   - Raise DatasetPathError for path-related issues.
"""

from __future__ import annotations

from typing import Any


class DatasetError(Exception):
    """Base exception for all dataset operations.

    Attributes:
        message: Human-readable error description
        operation: The operation that failed (e.g., 'read', 'write', 'merge')
        details: Additional context about the error
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


class DatasetOperationError(DatasetError):
    """Raised when a dataset operation fails.

    Use this for general operation failures that don't fit more specific categories.
    """


class DatasetValidationError(DatasetError):
    """Raised when input validation fails.

    Use this when user-provided input is invalid (e.g., invalid mode, missing columns).
    """


class DatasetFileError(DatasetError):
    """Raised when file I/O operations fail.

    Use this for file read/write errors, permission issues, etc.
    """


class DatasetPathError(DatasetError):
    """Raised when path-related operations fail.

    Use this for path normalization failures, missing paths, invalid protocols, etc.
    """


class DatasetMergeError(DatasetError):
    """Raised when merge operations fail.

    Use this for merge-specific failures (key column issues, schema mismatches, etc.).
    """


class DatasetSchemaError(DatasetError):
    """Raised when schema-related operations fail.

    Use this for schema validation, casting, and compatibility issues.
    """
