"""Tests for the best-effort directory-creation filesystem view.

Covers the helper used by ``PyarrowDatasetIO.write_dataset`` / ``merge`` so
that writes to object stores do not fail when credentials lack
bucket-creation rights.
"""

from __future__ import annotations

import fsspec
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from fsspeckit.datasets.pyarrow.io import _best_effort_dirs_filesystem


class _BoomFS(MemoryFileSystem):
    """MemoryFS whose ``mkdir`` family raises ``PermissionError``.

    Mimics an object-only account on S3 that can read/write objects but
    cannot create buckets (s3fs then turns the failed ``HeadBucket`` into a
    ``CreateBucket`` attempt, which raises).
    """

    protocol = "boomfs"

    def mkdir(self, path, create_parents=True, **kwargs):  # type: ignore[override]
        raise PermissionError("no bucket-creation rights")

    def mkdirs(self, path, exist_ok=False):  # type: ignore[override]
        raise PermissionError("no bucket-creation rights")

    def makedirs(self, path, exist_ok=False):  # type: ignore[override]
        raise PermissionError("no bucket-creation rights")


def test_view_is_same_type_and_passes_isinstance():
    fs = _BoomFS()
    view = _best_effort_dirs_filesystem(fs)
    assert type(view) is _BoomFS
    # PyArrow accepts fsspec filesystems via isinstance(AbstractFileSystem).
    assert isinstance(view, fsspec.AbstractFileSystem)


def test_view_swallows_permission_error_on_dir_creation():
    view = _best_effort_dirs_filesystem(_BoomFS())
    # No exception raised, returns None:
    assert view.mkdirs("/bucket/some/prefix/path", exist_ok=True) is None
    assert view.mkdir("/bucket/some/prefix/path") is None
    assert view.makedirs("/bucket/some/prefix/path") is None


def test_original_filesystem_is_not_mutated():
    # Crucial for threadpools where concurrent writers share one filesystem.
    fs = _BoomFS()
    _best_effort_dirs_filesystem(fs)
    with pytest.raises(PermissionError):
        fs.mkdirs("/x/y", exist_ok=True)


def test_view_delegates_non_dir_operations():
    fs = _BoomFS()
    view = _best_effort_dirs_filesystem(fs)
    assert view.protocol == fs.protocol
    # Non-dir calls delegate to the underlying (shared) filesystem state.
    assert view.ls("/") == fs.ls("/")


def test_view_returns_original_when_construction_fails():
    # A class without __dict__ (slots) cannot be shallow-copied into a view,
    # so the helper must fall back to returning the original filesystem.
    class _SlotsFS:
        __slots__ = ()

    fs = _SlotsFS()
    assert _best_effort_dirs_filesystem(fs) is fs  # type: ignore[arg-type]
