"""Tests for error handling and path normalization functionality.

This module tests the new exception hierarchy and filesystem-aware path
normalization and validation utilities.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from fsspeckit.datasets.exceptions import (
    DatasetError,
    DatasetFileError,
    DatasetMergeError,
    DatasetOperationError,
    DatasetPathError,
    DatasetSchemaError,
    DatasetValidationError,
)
from fsspeckit.datasets.path_utils import normalize_path, validate_dataset_path


class TestExceptionHierarchy:
    """Test the new exception hierarchy."""

    def test_dataset_error_base(self):
        """Test base DatasetError functionality."""
        msg = "Test error message"
        error = DatasetError(msg)

        assert str(error) == msg
        assert error.operation is None
        assert error.details == {}

    def test_dataset_error_with_context(self):
        """Test DatasetError with operation and details."""
        msg = "Test error message"
        operation = "read"
        details = {"path": "/tmp/test", "size": 1024}

        error = DatasetError(msg, operation=operation, details=details)

        assert str(error) == msg
        assert error.operation == operation
        assert error.details == details

    def test_exception_inheritance(self):
        """Test that all exceptions inherit from DatasetError."""
        exceptions = [
            DatasetOperationError("test"),
            DatasetValidationError("test"),
            DatasetFileError("test"),
            DatasetPathError("test"),
            DatasetMergeError("test"),
            DatasetSchemaError("test"),
        ]

        for exc in exceptions:
            assert isinstance(exc, DatasetError)

    def test_exception_string_formatting(self):
        """Test exception string formatting includes context."""
        error = DatasetPathError(
            "Path does not exist",
            operation="read",
            details={"path": "/nonexistent", "protocol": "file"},
        )

        error_str = str(error)
        assert "Path does not exist" in error_str
        assert "Operation: read" in error_str
        assert "Details: path=/nonexistent, protocol=file" in error_str


class TestPathNormalization:
    """Test filesystem-aware path normalization."""

    def test_local_filesystem_normalization(self):
        """Test path normalization for local filesystem."""
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
        path = "relative/path"

        normalized = normalize_path(path, fs)

        # Should be absolute path for local filesystem
        assert os.path.isabs(normalized)
        assert normalized.endswith("relative/path")

    def test_s3_filesystem_normalization(self):
        """Test path normalization for S3 filesystem."""
        fs = Mock()
        fs.protocol = "s3"

        # Path without protocol
        path = "bucket/key"
        normalized = normalize_path(path, fs)
        assert normalized == "s3://bucket/key"

        # Path with protocol (should be preserved)
        path = "s3://bucket/key"
        normalized = normalize_path(path, fs)
        assert normalized == "s3://bucket/key"

    def test_gcs_filesystem_normalization(self):
        """Test path normalization for GCS filesystem."""
        fs = Mock()
        fs.protocol = "gs"

        path = "bucket/key"
        normalized = normalize_path(path, fs)
        assert normalized == "gs://bucket/key"

    def test_github_filesystem_normalization(self):
        """Test path normalization for GitHub filesystem."""
        fs = Mock()
        fs.protocol = "github"

        path = "user/repo"
        normalized = normalize_path(path, fs)
        assert normalized == "github://user/repo"


class TestPathValidation:
    """Test path validation functionality."""

    def test_valid_local_path_validation(self):
        """Test validation of valid local path."""
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Valid path for read operation
            validate_dataset_path(tmpdir, fs, "read")

            # Valid path for write operation
            validate_dataset_path(tmpdir, fs, "write")

    def test_nonexistent_path_validation(self):
        """Test validation fails for nonexistent paths."""
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()

        # Nonexistent path should fail for read operation
        with pytest.raises(DatasetPathError, match="does not exist"):
            validate_dataset_path("/nonexistent/path", fs, "read")

    def test_unsupported_protocol_validation(self):
        """Test validation fails for unsupported protocols."""
        fs = Mock()
        fs.protocol = "unsupported"

        # Should fail for unsupported protocol
        with pytest.raises(DatasetPathError, match="Unsupported protocol"):
            validate_dataset_path("unsupported://bucket/key", fs, "read")

    def test_supported_protocols_validation(self):
        """Test validation passes for supported protocols."""
        fs = Mock()
        fs.protocol = "s3"

        supported_paths = [
            "s3://bucket/key",
            "s3a://bucket/key",
            "gs://bucket/key",
            "gcs://bucket/key",
            "az://bucket/key",
            "abfs://bucket/key",
            "abfss://bucket/key",
            "file:///path/to/file",
            "github://user/repo",
            "gitlab://user/repo",
        ]

        for path in supported_paths:
            # Should not raise exception
            validate_dataset_path(path, fs, "read")


class TestErrorHandlingIntegration:
    """Test integration with dataset operations."""

    def test_pyarrow_io_imports_exceptions(self):
        """Test that PyArrow IO imports the new exceptions."""
        # This test ensures the imports work correctly
        from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO

        # Just verify the class can be imported
        assert PyarrowDatasetIO is not None

    def test_duckdb_dataset_imports_exceptions(self):
        """Test that DuckDB dataset imports the new exceptions."""
        # This test ensures the imports work correctly
        from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

        # Just verify the class can be imported
        assert DuckDBDatasetIO is not None


if __name__ == "__main__":
    pytest.main([__file__])
