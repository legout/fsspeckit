"""Basic tests for fsspec-utils package."""

import posixpath
from pathlib import Path

import pytest


def test_imports():
    """Test that basic imports work."""
    from fsspeckit import AbstractFileSystem, DirFileSystem, filesystem
    from fsspeckit.common.logging import setup_logging
    from fsspeckit.storage_options import AwsStorageOptions, LocalStorageOptions

    assert filesystem is not None
    assert DirFileSystem is not None
    assert AbstractFileSystem is not None
    assert AwsStorageOptions is not None
    assert LocalStorageOptions is not None
    assert setup_logging is not None


def test_local_filesystem():
    """Test local filesystem creation."""
    from fsspeckit import filesystem

    fs = filesystem("file")
    assert fs is not None
    assert hasattr(fs, "ls")
    assert hasattr(fs, "open")


def test_storage_options():
    """Test storage options creation."""
    from fsspeckit.storage_options import AwsStorageOptions, LocalStorageOptions

    # Local options
    local_opts = LocalStorageOptions()
    assert local_opts.protocol == "file"

    # AWS options
    aws_opts = AwsStorageOptions(region="us-east-1")
    assert aws_opts.protocol == "s3"
    assert aws_opts.region == "us-east-1"


def test_logging_setup():
    """Test logging setup."""
    from fsspeckit.common.logging import setup_logging

    # Should not raise any errors
    setup_logging(level="INFO", disable=False)


def test_filesystem_preserves_directory_without_trailing_slash(tmp_path):
    """Ensure filesystem() keeps the last directory component by default."""
    from fsspeckit import DirFileSystem, filesystem

    root = tmp_path / "path" / "to" / "root"
    root.mkdir(parents=True)

    fs = filesystem(root.as_posix())

    assert isinstance(fs, DirFileSystem)
    assert Path(fs.path).resolve() == root.resolve()


def test_filesystem_infers_directory_from_file_path(tmp_path):
    """Ensure filesystem() detects file inputs and returns the parent directory."""
    from fsspeckit import DirFileSystem, filesystem

    root = tmp_path / "data"
    root.mkdir()
    file_path = root / "file.csv"
    file_path.write_text("content", encoding="utf-8")

    fs = filesystem(file_path.as_posix())

    assert isinstance(fs, DirFileSystem)
    assert Path(fs.path).resolve() == root.resolve()


def test_filesystem_directory_with_dotted_parent(tmp_path):
    """Directories with dots in parent names should be preserved."""
    from fsspeckit import DirFileSystem, filesystem

    root = tmp_path / "dataset.v1" / "partition"
    root.mkdir(parents=True)

    fs = filesystem(root.as_posix())

    assert isinstance(fs, DirFileSystem)
    assert Path(fs.path).resolve() == root.resolve()


def test_filesystem_preserves_trailing_dot_directory(tmp_path):
    """Directories ending with a dot should not be treated as files."""
    from fsspeckit import DirFileSystem, filesystem

    root = tmp_path / "abc" / "efg."
    root.mkdir(parents=True)

    fs = filesystem(root.as_posix())

    assert isinstance(fs, DirFileSystem)
    assert Path(fs.path).resolve() == root.resolve()


def test_filesystem_dirfs_child_with_trailing_dot(tmp_path):
    """Relative child directories ending with a dot should be resolved safely."""
    from fsspeckit import DirFileSystem, filesystem

    root = tmp_path / "abc"
    child = root / "efg."
    child.mkdir(parents=True)

    base_fs = filesystem(root.as_posix())
    child_fs = filesystem(child.name, base_fs=base_fs)

    assert isinstance(child_fs, DirFileSystem)
    assert Path(child_fs.path).resolve() == child.resolve()


def test_filesystem_dirfs_with_partial_overlap(tmp_path):
    """Relative child partially overlapping with base should not duplicate segments."""
    from fsspeckit import DirFileSystem, filesystem

    root = tmp_path / "ewn" / "mms2" / "stage1"
    nested = root / "SC"
    nested.mkdir(parents=True)

    base_fs = filesystem(root.as_posix())

    # Provide only the overlapping tail of the path
    child_fs = filesystem("mms2/stage1/SC", base_fs=base_fs)

    assert isinstance(child_fs, DirFileSystem)
    assert Path(child_fs.path).resolve() == nested.resolve()

    # Provide a shorter overlapping tail
    child_fs2 = filesystem("stage1/SC", base_fs=base_fs)

    assert isinstance(child_fs2, DirFileSystem)
    assert Path(child_fs2.path).resolve() == nested.resolve()


def test_filesystem_dirfs_with_relative_path(tmp_path):
    """Relative paths should be resolved against the base DirFileSystem root."""
    from fsspeckit import DirFileSystem, filesystem

    root = tmp_path / "root"
    (root / "nested").mkdir(parents=True)

    base_fs = filesystem(root.as_posix())
    child_fs = filesystem("nested", base_fs=base_fs)

    assert isinstance(child_fs, DirFileSystem)
    assert child_fs.fs is base_fs.fs
    expected_path = posixpath.normpath(posixpath.join(base_fs.path, "nested"))
    assert posixpath.normpath(child_fs.path) == expected_path


def test_filesystem_dirfs_with_explicit_same_path(tmp_path):
    """Explicitly targeting the same directory should reuse the base filesystem."""
    from fsspeckit import filesystem

    root = tmp_path / "root"
    root.mkdir()

    base_fs = filesystem(root.as_posix())
    fs_with_base = filesystem(f"file://{root.as_posix()}", base_fs=base_fs)

    assert fs_with_base is base_fs


def test_filesystem_dirfs_with_explicit_subpath(tmp_path):
    """Explicit child paths should produce a new DirFileSystem anchored within the base."""
    from fsspeckit import DirFileSystem, filesystem

    root = tmp_path / "root"
    sub = root / "nested"
    sub.mkdir(parents=True)

    base_fs = filesystem(root.as_posix())
    child_fs = filesystem(f"file://{sub.as_posix()}", base_fs=base_fs)

    assert isinstance(child_fs, DirFileSystem)
    assert Path(child_fs.path).resolve() == sub.resolve()


def test_filesystem_dirfs_with_mismatched_path_raises(tmp_path):
    """Paths outside the base DirFileSystem should raise a clear error."""
    from fsspeckit import filesystem

    root = tmp_path / "root"
    other = tmp_path / "other"
    root.mkdir()
    other.mkdir()

    base_fs = filesystem(root.as_posix())

    with pytest.raises(ValueError):
        filesystem(f"file://{other.as_posix()}", base_fs=base_fs)


def test_filesystem_dirfs_with_protocol_mismatch(tmp_path):
    """Using an incompatible protocol with base_fs should fail fast."""
    from fsspeckit import filesystem

    root = tmp_path / "root"
    root.mkdir()

    base_fs = filesystem(root.as_posix())

    with pytest.raises(ValueError):
        filesystem("s3://bucket/path", base_fs=base_fs)


def test_filesystem_dirfs_disallows_parent_escape(tmp_path):
    """Relative paths must not escape the base DirFileSystem root."""
    from fsspeckit import filesystem

    root = tmp_path / "root"
    root.mkdir()

    base_fs = filesystem(root.as_posix())

    with pytest.raises(ValueError):
        filesystem("../escape", base_fs=base_fs)


def test_filesystem_local_vs_url_path_resolution(tmp_path):
    """Test that local paths and URL-based protocols are handled correctly."""
    from fsspeckit import DirFileSystem, filesystem

    # Test local path detection
    root = tmp_path / "local" / "path"
    root.mkdir(parents=True)

    fs = filesystem(root.as_posix())
    assert isinstance(fs, DirFileSystem)
    assert Path(fs.path).resolve() == root.resolve()

    # Test URL-based protocol detection
    fs_url = filesystem(f"file://{root.as_posix()}")
    assert isinstance(fs_url, DirFileSystem)
    assert Path(fs_url.path).resolve() == root.resolve()

    # Test that URL paths are not treated as local filesystem paths
    fs_remote = filesystem("memory://bucket/path")
    assert isinstance(fs_remote, DirFileSystem)
    assert fs_remote.path.lstrip("/") == "bucket/path"


def test_filesystem_dirfs_wraps_remote_paths():
    """Remote URIs with paths should be rooted via DirFileSystem when dirfs=True."""
    from fsspeckit import DirFileSystem, filesystem

    fs = filesystem("memory://ewn/mms2/stage1", dirfs=True)
    assert isinstance(fs, DirFileSystem)
    assert fs.path.lstrip("/") == "ewn/mms2/stage1"

    raw_fs = filesystem("memory://ewn/mms2/stage1", dirfs=False)
    assert not isinstance(raw_fs, DirFileSystem)


def test_filesystem_dirfs_ignores_protocol_only_or_root():
    """Protocol-only and protocol-root inputs should not be wrapped by default."""
    from fsspeckit import DirFileSystem, filesystem

    fs_protocol = filesystem("memory", dirfs=True)
    assert not isinstance(fs_protocol, DirFileSystem)

    fs_root = filesystem("memory://", dirfs=True)
    assert not isinstance(fs_root, DirFileSystem)


def test_filesystem_dirfs_ls_defaults_to_names_only():
    """DirFileSystem wrappers should default to `detail=False` for ls()."""
    from fsspeckit import DirFileSystem, filesystem

    fs = filesystem("memory://base", dirfs=True)
    assert isinstance(fs, DirFileSystem)

    fs.makedirs("subdir", exist_ok=True)
    with fs.open("file.txt", "wb") as f:
        f.write(b"x")

    listing = fs.ls("")
    assert all(isinstance(entry, str) for entry in listing)
    assert set(listing) == {"file.txt", "subdir"}

    detailed = fs.ls("", detail=True)
    assert all(isinstance(entry, dict) for entry in detailed)


def test_filesystem_path_helper_functions():
    """Test the new path helper functions for clarity."""
    from fsspeckit.core.filesystem_paths import (
        _detect_file_vs_directory_path,
        _detect_local_vs_remote_path,
    )

    # Test local vs remote detection
    local_path = "/path/to/file"
    normalized, is_local = _detect_local_vs_remote_path(local_path)
    assert is_local is True
    assert normalized == local_path

    # Test URL detection
    url_path = "https://example.com/path"
    normalized, is_local = _detect_local_vs_remote_path(url_path)
    assert is_local is False

    # Test file vs directory detection
    file_path = "/path/to/file"
    normalized, is_file = _detect_file_vs_directory_path(file_path)
    assert is_file is True

    dir_path = "/path/to/dir/"
    normalized, is_file = _detect_file_vs_directory_path(dir_path)
    assert is_file is False


def test_filesystem_cache_path_resolution(tmp_path):
    """Test that cache path hints are derived correctly."""
    from fsspeckit import filesystem

    # Create a directory structure
    root = tmp_path / "dataset"
    subdir = root / "partition"
    subdir.mkdir(parents=True)

    # Test cache path for simple directory
    fs = filesystem(root.as_posix(), cached=True)
    assert hasattr(fs, "is_cache_fs")
    assert fs.is_cache_fs is True

    # Test cache path for nested directory
    fs_nested = filesystem(subdir.as_posix(), cached=True)
    assert hasattr(fs_nested, "is_cache_fs")
    assert fs_nested.is_cache_fs is True


def test_filesystem_base_path_resolution_with_protocols(tmp_path):
    """Test that base path resolution works correctly with different protocols."""
    from fsspeckit import filesystem

    # Create local directory
    root = tmp_path / "base"
    root.mkdir()

    base_fs = filesystem(root.as_posix())

    # Test relative path resolution with base filesystem
    child_fs = filesystem("child", base_fs=base_fs)
    assert Path(child_fs.path).resolve() == (root / "child").resolve()

    # Test absolute path resolution with base filesystem
    abs_path = f"file://{root.as_posix()}/absolute"
    abs_fs = filesystem(abs_path, base_fs=base_fs)
    assert Path(abs_fs.path).resolve() == (root / "absolute").resolve()
