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


def test_normalize_path_none_raises_value_error():
    """Test that normalize_path raises ValueError for None input."""
    from fsspeckit.core.filesystem import normalize_path
    from typing import Any

    none_path: Any = None
    with pytest.raises(ValueError, match="Path cannot be None"):
        normalize_path(none_path)


def test_normalize_path_windows_backslashes():
    """Test normalize_path converts Windows backslashes to forward slashes."""
    from fsspeckit.core.filesystem import normalize_path

    # Single backslash
    assert normalize_path("data\\file.parquet") == "data/file.parquet"

    # Multiple backslashes
    assert normalize_path("data\\subdir\\file.parquet") == "data/subdir/file.parquet"

    # Mixed slashes
    assert normalize_path("data\\subdir/file.parquet") == "data/subdir/file.parquet"

    # Windows absolute path style
    assert (
        normalize_path("C:\\Users\\data\\file.parquet") == "C:/Users/data/file.parquet"
    )


def test_normalize_path_remote_filesystem_without_protocol():
    """Test normalize_path with remote filesystem when path lacks protocol."""
    from fsspeckit.core.filesystem import normalize_path
    from unittest.mock import Mock

    # S3 without protocol prefix
    fs_s3 = Mock()
    fs_s3.protocol = "s3"
    result = normalize_path("bucket/key/to/file.parquet", filesystem=fs_s3)
    assert result == "s3://bucket/key/to/file.parquet"

    # GCS without protocol prefix
    fs_gcs = Mock()
    fs_gcs.protocol = "gs"
    result = normalize_path("bucket/key/to/file.parquet", filesystem=fs_gcs)
    assert result == "gs://bucket/key/to/file.parquet"

    # Azure without protocol prefix
    fs_az = Mock()
    fs_az.protocol = "abfs"
    result = normalize_path("container/path/to/file.parquet", filesystem=fs_az)
    assert result == "abfs://container/path/to/file.parquet"


def test_normalize_path_remote_filesystem_with_protocol():
    """Test normalize_path with remote filesystem when path has protocol."""
    from fsspeckit.core.filesystem import normalize_path
    from unittest.mock import Mock

    # S3 with protocol prefix - should preserve
    fs_s3 = Mock()
    fs_s3.protocol = "s3"
    result = normalize_path("s3://bucket/path/../file.parquet", filesystem=fs_s3)
    assert result == "s3://bucket/file.parquet"

    # GCS with protocol prefix
    fs_gcs = Mock()
    fs_gcs.protocol = "gs"
    result = normalize_path("gs://my-bucket/path/../data.parquet", filesystem=fs_gcs)
    assert result == "gs://my-bucket/data.parquet"


def test_normalize_path_validate_true_without_filesystem():
    """Test normalize_path with validate=True but no filesystem."""
    from fsspeckit.core.filesystem import normalize_path
    from fsspeckit.datasets.exceptions import DatasetPathError

    # Valid path should pass
    result = normalize_path("data/file.parquet", validate=True)
    assert result == "data/file.parquet"

    # Path with null bytes should fail with ValueError
    with pytest.raises(ValueError):
        normalize_path("data/file\x00.parquet", validate=True)

    # Path with null bytes and operation raises DatasetPathError (operation context wraps ValueError)
    with pytest.raises(DatasetPathError):
        normalize_path("data/file\x00.parquet", validate=True, operation="read")


def test_normalize_path_validate_true_with_local_filesystem():
    """Test normalize_path with validate=True and local filesystem."""
    from fsspeckit.core.filesystem import normalize_path
    from fsspeckit.datasets.exceptions import DatasetPathError
    import tempfile
    import os

    fs = LocalFileSystem()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Valid path for read operation
        result = normalize_path(tmpdir, filesystem=fs, validate=True, operation="read")
        assert os.path.isabs(result)

    # Nonexistent path should fail with DatasetPathError for read operation
    with pytest.raises(DatasetPathError, match="does not exist"):
        normalize_path(
            "/nonexistent/path", filesystem=fs, validate=True, operation="read"
        )


def test_normalize_path_validate_true_with_remote_filesystem():
    """Test normalize_path with validate=True and remote filesystem."""
    from fsspeckit.core.filesystem import normalize_path
    from fsspeckit.datasets.exceptions import DatasetPathError
    from unittest.mock import Mock

    fs = Mock()
    fs.protocol = "s3"
    fs.exists.return_value = True
    fs._parent.return_value = "s3://bucket/parent"

    # Valid path should pass
    result = normalize_path(
        "s3://bucket/key/file.parquet", filesystem=fs, validate=True, operation="read"
    )
    assert result == "s3://bucket/key/file.parquet"

    # Nonexistent path should fail for read
    fs.exists.return_value = False
    with pytest.raises(DatasetPathError, match="does not exist"):
        normalize_path(
            "s3://bucket/nonexistent.parquet",
            filesystem=fs,
            validate=True,
            operation="read",
        )


def test_normalize_path_different_operations():
    """Test normalize_path with different operation contexts."""
    from fsspeckit.core.filesystem import normalize_path
    from fsspeckit.datasets.exceptions import DatasetPathError
    from unittest.mock import Mock
    import tempfile
    import os

    # Test with local filesystem
    fs = LocalFileSystem()
    with tempfile.TemporaryDirectory() as tmpdir:
        # Read operation - path must exist
        result = normalize_path(tmpdir, filesystem=fs, validate=True, operation="read")
        assert os.path.isabs(result)

        # Write operation - parent must exist
        result = normalize_path(tmpdir, filesystem=fs, validate=True, operation="write")
        assert os.path.isabs(result)

    # Test with mock remote filesystem
    fs_remote = Mock()
    fs_remote.protocol = "s3"
    fs_remote.exists.return_value = True
    fs_remote._parent.return_value = "s3://bucket/parent"

    # Merge operation - path must exist
    result = normalize_path(
        "s3://bucket/existing/file.parquet",
        filesystem=fs_remote,
        validate=True,
        operation="merge",
    )
    assert result == "s3://bucket/existing/file.parquet"


def test_normalize_path_all_supported_protocols():
    """Test normalize_path preserves all supported remote protocols."""
    from fsspeckit.core.filesystem import normalize_path
    from unittest.mock import Mock

    protocols = [
        ("s3", "s3://bucket/key/file.parquet"),
        ("s3a", "s3a://bucket/key/file.parquet"),
        ("gs", "gs://bucket/key/file.parquet"),
        ("gcs", "gcs://bucket/key/file.parquet"),
        ("az", "az://container/key/file.parquet"),
        ("abfs", "abfs://container/key/file.parquet"),
        ("abfss", "abfss://container/key/file.parquet"),
        ("file", "file:///path/to/file.parquet"),
        ("github", "github://user/repo/file.parquet"),
        ("gitlab", "gitlab://user/repo/file.parquet"),
    ]

    for protocol, path in protocols:
        fs = Mock()
        fs.protocol = protocol
        result = normalize_path(path)
        assert result.startswith(f"{protocol}://"), (
            f"Protocol {protocol} not preserved in {result}"
        )


def test_normalize_path_pathlib_compatibility():
    """Test normalize_path works with various pathlib.Path inputs."""
    from fsspeckit.core.filesystem import normalize_path
    from pathlib import Path

    # Relative Path object
    path_rel = Path("data/../file.parquet")
    result = normalize_path(path_rel)
    assert result == "file.parquet"

    # Absolute Path object
    path_abs = Path("/tmp/data/file.parquet")
    result = normalize_path(path_abs)
    assert result == "/tmp/data/file.parquet"

    # Path object with ..
    path_with_dots = Path("/tmp/data/../file.parquet")
    result = normalize_path(path_with_dots)
    assert result == "/tmp/file.parquet"


def test_normalize_path_multiple_dot_segments():
    """Test normalize_path handles multiple dot segments correctly."""
    from fsspeckit.core.filesystem import normalize_path

    # Multiple parent references - posixpath.normpath handles within relative path scope
    # a/b/c/../../file.parquet -> a/file.parquet (b/c/.. normalizes to b, then b/.. escapes b scope)
    assert normalize_path("a/b/c/../../file.parquet") == "a/file.parquet"

    # Mixed parent and current directory
    assert normalize_path("a/./b/../c/file.parquet") == "a/c/file.parquet"

    # Deep nesting with parent refs within scope
    assert normalize_path("a/b/c/d/../../file.parquet") == "a/b/file.parquet"

    # URL with multiple parent refs
    assert (
        normalize_path("s3://bucket/a/b/c/../../file.parquet")
        == "s3://bucket/a/file.parquet"
    )

    # Edge case: parent refs that escape completely
    assert normalize_path("../file.parquet") == "../file.parquet"
    assert normalize_path("a/../file.parquet") == "file.parquet"


def test_normalize_path_absolute_vs_relative():
    """Test normalize_path handles absolute and relative paths correctly."""
    from fsspeckit.core.filesystem import normalize_path

    # Relative path stays relative (no filesystem)
    assert normalize_path("data/file.parquet") == "data/file.parquet"
    assert normalize_path("./data/file.parquet") == "data/file.parquet"

    # Absolute path stays absolute
    assert normalize_path("/data/file.parquet") == "/data/file.parquet"
    assert normalize_path("/tmp/../etc/file.parquet") == "/etc/file.parquet"


def test_normalize_path_trailing_slashes():
    """Test normalize_path handles trailing slashes."""
    from fsspeckit.core.filesystem import normalize_path

    # Trailing slash is preserved by posixpath.normpath
    assert normalize_path("data/file/") == "data/file"
    assert normalize_path("s3://bucket/path/") == "s3://bucket/path"
    assert normalize_path("/tmp/dir/") == "/tmp/dir"


def test_normalize_path_remote_filesystem_list_protocol():
    """Test normalize_path handles filesystem with list protocol."""
    from fsspeckit.core.filesystem import normalize_path
    from unittest.mock import Mock

    # Filesystem with tuple protocol
    fs = Mock()
    fs.protocol = ("s3", "s3n", "s3a")
    result = normalize_path("bucket/key/file.parquet", filesystem=fs)
    assert result.startswith("s3://")


def test_normalize_path_datasets_module_available():
    """Test that normalize_path is also available from fsspeckit.datasets."""
    from fsspeckit.datasets import normalize_path as datasets_normalize_path
    from fsspeckit.core.filesystem import normalize_path as core_normalize_path
    from fsspec.implementations.local import LocalFileSystem

    # Both should be available
    assert datasets_normalize_path is not None
    assert core_normalize_path is not None

    # Should produce same results for local filesystem
    fs = LocalFileSystem()
    datasets_result = datasets_normalize_path("data/file.parquet", fs)
    core_result = core_normalize_path("data/file.parquet", filesystem=fs)
    assert datasets_result == core_result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
