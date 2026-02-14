"""Tests for dataset backend dependency guards."""

from __future__ import annotations

import pytest


def test_pyarrow_datasetio_requires_pyarrow(monkeypatch) -> None:
    from fsspeckit.common import optional
    from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO

    monkeypatch.setattr(optional, "_PYARROW_AVAILABLE", False)

    with pytest.raises(ImportError, match="pyarrow is required"):
        PyarrowDatasetIO()

    with pytest.raises(ImportError, match="fsspeckit\[datasets\]"):
        PyarrowDatasetIO()


def test_duckdb_connection_requires_duckdb(monkeypatch) -> None:
    from fsspeckit.common import optional
    from fsspeckit.datasets.duckdb.connection import create_duckdb_connection

    monkeypatch.setattr(optional, "_DUCKDB_AVAILABLE", False)

    conn = create_duckdb_connection()
    with pytest.raises(ImportError, match="duckdb is required"):
        _ = conn.connection

    with pytest.raises(ImportError, match="fsspeckit\[sql\]"):
        _ = conn.connection
