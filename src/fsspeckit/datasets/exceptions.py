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


# ``DatasetError`` and ``DatasetPathError`` are defined in the core layer so that
# ``core.filesystem.paths.normalize_path`` can raise path-validation errors
# without importing from ``datasets`` (which would invert the layering). This
# module re-exports them to preserve the historical import surface — every
# existing ``from fsspeckit.datasets.exceptions import DatasetPathError``
# resolves to the same class object that core raises.
from fsspeckit.core.exceptions import DatasetError, DatasetPathError

__all__ = [
    "DatasetError",
    "DatasetFileError",
    "DatasetMergeError",
    "DatasetOperationError",
    "DatasetPathError",
    "DatasetSchemaError",
    "DatasetValidationError",
]


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


# ``DatasetPathError`` is imported from core above; it is listed in __all__
# so existing imports continue to resolve to the core-defined class.


class DatasetMergeError(DatasetError):
    """Raised when merge operations fail.

    Use this for merge-specific failures (key column issues, schema mismatches, etc.).
    """


class DatasetSchemaError(DatasetError):
    """Raised when schema-related operations fail.

    Use this for schema validation, casting, and compatibility issues.
    """
