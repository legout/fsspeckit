"""Security validation helpers for fsspeckit.

This module provides basic validation for paths, codecs, and credential scrubbing
to prevent common security issues like path traversal and credential leakage in logs.
"""

from __future__ import annotations

import re
from typing import Any, ClassVar


# Known-safe compression codecs for parquet operations
VALID_COMPRESSION_CODECS = frozenset(
    {
        "snappy",
        "gzip",
        "lz4",
        "zstd",
        "brotli",
        "uncompressed",
        "none",
    }
)

# Patterns that indicate credential-like values in error messages
_CREDENTIAL_PATTERNS = [
    # AWS-style keys
    re.compile(r"\b(AKIA[A-Z0-9]{16})\b"),  # AWS Access Key ID
    re.compile(r"\b([A-Za-z0-9/+=]{40})\b"),  # AWS Secret Key (40 chars base64-ish)
    # Generic secret/token/key patterns (key=value or "key": "value")
    re.compile(
        r"(secret[_-]?(?:access)?[_-]?key|access[_-]?key[_-]?id|"
        r"session[_-]?token|api[_-]?key|auth[_-]?token|password|"
        r"credential|bearer)[\s]*[=:][\s]*['\"]?([^'\"\s,}\]]{8,})['\"]?",
        re.IGNORECASE,
    ),
    # Bearer tokens
    re.compile(r"\b(Bearer\s+[A-Za-z0-9\-._~+/]+=*)\b", re.IGNORECASE),
    # Connection strings with embedded credentials
    re.compile(
        r"(AccountKey|SharedAccessSignature|sig)=([^;&\s]{10,})",
        re.IGNORECASE,
    ),
]

# Characters that should never appear in filesystem paths
_FORBIDDEN_PATH_CHARS = frozenset(
    {
        "\x00",  # Null byte
        "\x01",
        "\x02",
        "\x03",
        "\x04",
        "\x05",
        "\x06",
        "\x07",  # Control chars
        "\x08",
        "\x0b",
        "\x0c",
        "\x0e",
        "\x0f",  # More control chars
        "\x10",
        "\x11",
        "\x12",
        "\x13",
        "\x14",
        "\x15",
        "\x16",
        "\x17",
        "\x18",
        "\x19",
        "\x1a",
        "\x1b",
        "\x1c",
        "\x1d",
        "\x1e",
        "\x1f",
    }
)


class PathValidator:
    """Secure path validation for DuckDB SQL queries.

    DuckDB's parquet_scan() does not support parameterized file paths,
    requiring careful validation before SQL string interpolation.
    """

    # Strict allowlist: alphanumeric, dots, underscores, hyphens, forward slashes, asterisks, equals signs (for Hive partitions)
    SAFE_PATH_PATTERN: ClassVar[re.Pattern] = re.compile(r"^[a-zA-Z0-9._/\-*:=]+$")

    # Pattern for valid SQL identifiers (column names, table names)
    SQL_IDENTIFIER_PATTERN: ClassVar[re.Pattern] = re.compile(
        r"^[a-zA-Z_][a-zA-Z0-9_]*$"
    )

    # Dangerous patterns for SQL injection - allow * for globs
    DANGEROUS_PATTERNS: ClassVar[re.Pattern] = re.compile(r"'|\"|\;|--|[\x00-\x1f]")

    # Unicode quote variants that could bypass validation
    UNICODE_QUOTES: ClassVar[re.Pattern] = re.compile(
        r"[\u0027\u0060\u00b4\u2018\u2019\u201b\u2032\u2035\uff07\uff40]"
    )

    @classmethod
    def validate_path_for_sql(cls, path: str, max_length: int = 4096) -> None:
        """Validate a file path is safe for SQL interpolation.

        Args:
            path: The file path to validate
            max_length: Maximum allowed path length

        Raises:
            ValueError: If the path is unsafe for SQL interpolation
        """
        if not path:
            raise ValueError("Path cannot be empty")

        if len(path) > max_length:
            raise ValueError(f"Path exceeds maximum length of {max_length}")

        if ".." in path:
            raise ValueError("Path traversal (..) not allowed")

        if cls.DANGEROUS_PATTERNS.search(path):
            raise ValueError("Path contains dangerous SQL characters")

        if cls.UNICODE_QUOTES.search(path):
            raise ValueError("Path contains suspicious Unicode characters")

        if not cls.SAFE_PATH_PATTERN.match(path):
            raise ValueError(
                f"Path contains invalid characters. "
                f"Only alphanumeric, '.', '_', '-', '/', '*', ':' are allowed."
            )

    @classmethod
    def validate_sql_identifier(cls, identifier: str) -> None:
        """Validate that an identifier is safe for SQL use.

        SQL identifiers (column names, table names) must follow strict naming
        rules to prevent injection and ensure compatibility.

        Args:
            identifier: The SQL identifier to validate

        Raises:
            ValueError: If identifier contains invalid characters

        Examples:
            >>> PathValidator.validate_sql_identifier("column_name")
            None
            >>> PathValidator.validate_sql_identifier("column-name")
            ValueError: Invalid SQL identifier
        """
        if not identifier:
            raise ValueError("SQL identifier cannot be empty")

        if not cls.SQL_IDENTIFIER_PATTERN.match(identifier):
            raise ValueError(
                f"Invalid SQL identifier '{identifier}'. "
                f"Must start with letter or underscore, followed by letters, numbers, or underscores only."
            )

    @classmethod
    def escape_for_sql(cls, path: str) -> str:
        """Defense-in-depth: escape single quotes even after validation.

        Args:
            path: Already validated path

        Returns:
            Path with single quotes escaped for SQL string literal
        """
        return path.replace("'", "''")


def validate_path(path: str, base_dir: str | None = None) -> str:
    """Validate a filesystem path for security issues.

    Checks for:
    - Embedded null bytes and control characters
    - Path traversal attempts (../ sequences escaping base_dir)
    - Empty or whitespace-only paths

    Args:
        path: The path to validate.
        base_dir: Optional base directory. If provided, the path must resolve
            to a location within this directory (prevents path traversal).

    Returns:
        The validated path (unchanged if valid).

    Raises:
        ValueError: If the path contains forbidden characters, is empty,
            or escapes the base directory.

    Examples:
        >>> validate_path("/data/file.parquet")
        '/data/file.parquet'

        >>> validate_path("../../../etc/passwd", base_dir="/data")
        ValueError: Path escapes base directory

        >>> validate_path("file\\x00.parquet")
        ValueError: Path contains forbidden characters
    """
    if not path or not path.strip():
        raise ValueError("Path cannot be empty or whitespace-only")

    # Check for forbidden control characters
    for char in path:
        if char in _FORBIDDEN_PATH_CHARS:
            raise ValueError(f"Path contains forbidden control character: {repr(char)}")

    # Check for path traversal when base_dir is specified
    if base_dir is not None:
        import os

        # Normalize both paths for comparison
        base_resolved = os.path.normpath(os.path.abspath(base_dir))

        # Handle relative paths by joining with base
        if not os.path.isabs(path):
            full_path = os.path.join(base_dir, path)
        else:
            full_path = path

        path_resolved = os.path.normpath(os.path.abspath(full_path))

        # Check if resolved path starts with base directory
        if (
            not path_resolved.startswith(base_resolved + os.sep)
            and path_resolved != base_resolved
        ):
            raise ValueError(f"Path '{path}' escapes base directory '{base_dir}'")

    return path


def validate_compression_codec(codec: str) -> str:
    """Validate that a compression codec is in the allowed set.

    This prevents injection of arbitrary values into SQL queries or
    filesystem operations that accept codec parameters.

    Args:
        codec: The compression codec name to validate.

    Returns:
        The validated codec name (lowercased).

    Raises:
        ValueError: If the codec is not in the allowed set.

    Examples:
        >>> validate_compression_codec("snappy")
        'snappy'

        >>> validate_compression_codec("GZIP")
        'gzip'

        >>> validate_compression_codec("malicious; DROP TABLE")
        ValueError: Invalid compression codec
    """
    if not codec or not isinstance(codec, str):
        raise ValueError("Compression codec must be a non-empty string")

    normalized = codec.lower().strip()

    if normalized not in VALID_COMPRESSION_CODECS:
        valid_list = ", ".join(sorted(VALID_COMPRESSION_CODECS - {"none"}))
        raise ValueError(
            f"Invalid compression codec: '{codec}'. Must be one of: {valid_list}"
        )

    return normalized


def scrub_credentials(message: str) -> str:
    """Remove or mask credential-like values from a string.

    This is intended for use before logging error messages that might
    contain sensitive information like access keys or tokens.

    Args:
        message: The string to scrub.

    Returns:
        The string with credential-like values replaced with [REDACTED].

    Examples:
        >>> scrub_credentials("Error: access_key_id=AKIAIOSFODNN7EXAMPLE")
        'Error: access_key_id=[REDACTED]'

        >>> scrub_credentials("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
        '[REDACTED]'
    """
    if not message:
        return message

    result = message

    for pattern in _CREDENTIAL_PATTERNS:
        # Replace matched groups with [REDACTED]
        def redact_match(match: re.Match) -> str:
            groups = match.groups()
            if len(groups) >= 2:
                # Pattern with key=value format - keep the key, redact the value
                return match.group(0).replace(groups[-1], "[REDACTED]")
            else:
                # Single match - redact entire thing
                return "[REDACTED]"

        result = pattern.sub(redact_match, result)

    return result


def scrub_exception(exc: BaseException) -> str:
    """Scrub credentials from an exception's string representation.

    Args:
        exc: The exception to scrub.

    Returns:
        A scrubbed string representation of the exception.
    """
    return scrub_credentials(str(exc))


def validate_columns(
    columns: list[str] | None, valid_columns: list[str]
) -> list[str] | None:
    """Validate that requested columns exist in the schema.

    This is a helper to prevent column injection in SQL-like operations.

    Args:
        columns: List of column names to validate, or None.
        valid_columns: List of valid column names from the schema.

    Returns:
        The validated columns list, or None if columns was None.

    Raises:
        ValueError: If any column is not in the valid set.
    """
    if columns is None:
        return None

    valid_set = set(valid_columns)
    invalid = [col for col in columns if col not in valid_set]

    if invalid:
        raise ValueError(
            f"Invalid column(s): {', '.join(invalid)}. "
            f"Valid columns are: {', '.join(sorted(valid_set))}"
        )

    return columns


def safe_format_error(
    operation: str,
    path: str | None = None,
    error: BaseException | None = None,
    **context: Any,
) -> str:
    """Format an error message with credentials scrubbed.

    Args:
        operation: Description of the operation that failed.
        path: Optional path involved in the operation.
        error: Optional exception that occurred.
        **context: Additional context key-value pairs.

    Returns:
        A formatted, credential-scrubbed error message.
    """
    parts = [f"Failed to {operation}"]

    if path:
        parts.append(f"at '{path}'")

    if error:
        parts.append(f": {scrub_exception(error)}")

    if context:
        context_str = ", ".join(
            f"{k}={scrub_credentials(str(v))}" for k, v in context.items()
        )
        parts.append(f" ({context_str})")

    return " ".join(parts)
