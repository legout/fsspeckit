"""Tests for normalize_path API in core."""

import os
import warnings
from pathlib import Path

import pytest
from fsspec.implementations.local import LocalFileSystem


def test_normalize_path_string_only():
    """Test string-only normalization without filesystem."""
    from fsspeckit.core.filesystem import normalize_path

    # Basic path normalization
    assert normalize_path("data/../file.parquet") == "file.parquet"
    assert normalize_path("./data/file.parquet") == "data/file.parquet"

    # Backslash conversion
    assert normalize_path("data\\file.parquet") == "data/file.parquet"

    # URL-like paths
    assert (
        normalize_path("s3://bucket/path/../file.parquet") == "s3://bucket/file.parquet"
    )
    assert (
        normalize_path("gs://bucket/path/../file.parquet") == "gs://bucket/file.parquet"
    )


def test_normalize_path_with_local_filesystem():
    """Test filesystem-aware normalization with LocalFileSystem."""
    from fsspeckit.core.filesystem import normalize_path

    fs = LocalFileSystem()

    # Should return absolute path
    result = normalize_path("data/file.parquet", filesystem=fs)
    assert os.path.isabs(result)

    # Absolute path should remain absolute
    abs_path = "/tmp/test.parquet"
    result = normalize_path(abs_path, filesystem=fs)
    assert result == abs_path


def test_normalize_path_with_validation():
    """Test normalize_path with validation enabled."""
    from fsspeckit.core.filesystem import normalize_path
    from fsspeckit.datasets.exceptions import DatasetPathError

    # Valid path should work
    result = normalize_path("data/file.parquet", validate=True, operation="read")
    assert result == "data/file.parquet"

    # Path with null bytes should fail with ValueError when no operation
    with pytest.raises(ValueError):
        normalize_path("data/file\x00.parquet", validate=True)

    # Path with operation context should raise DatasetPathError
    with pytest.raises(DatasetPathError):
        normalize_path("data/file\x00.parquet", validate=True, operation="read")


def test_normalize_path_with_path_object():
    """Test normalize_path with Path objects."""
    from fsspeckit.core.filesystem import normalize_path

    path_obj = Path("data/../file.parquet")
    result = normalize_path(path_obj)
    assert result == "file.parquet"


def test_normalize_path_preserves_protocol():
    """Test that normalize_path preserves protocol prefixes."""
    from fsspeckit.core.filesystem import normalize_path

    # S3
    result = normalize_path("s3://bucket/path/../file.parquet")
    assert result == "s3://bucket/file.parquet"
    assert result.startswith("s3://")

    # GCS
    result = normalize_path("gs://bucket/path/../file.parquet")
    assert result == "gs://bucket/file.parquet"
    assert result.startswith("gs://")


def test_legacy_normalize_path_deprecated():
    """Test that legacy _normalize_path still works but emits deprecation warning."""
    from fsspeckit.core.filesystem.paths import _normalize_path

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        # Should work as before (note: legacy does normalize)
        result = _normalize_path("data/../file.parquet")
        assert result == "file.parquet"

        # Should emit deprecation warning
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "normalize_path" in str(w[0].message).lower()


def test_normalize_path_available_in_public_api():
    """Test that normalize_path is available in core.filesystem public API."""
    # normalize_path should be importable from core.filesystem
    from fsspeckit.core.filesystem import normalize_path

    assert normalize_path is not None
    assert callable(normalize_path)


def test_normalize_path_empty_and_edge_cases():
    """Test normalize_path with edge cases."""
    from fsspeckit.core.filesystem import normalize_path

    # Root path
    assert normalize_path("/") == "/"

    # Current directory
    assert normalize_path(".") == "."

    # Empty path segments
    assert normalize_path("data//file.parquet") == "data/file.parquet"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
