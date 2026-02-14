"""Parity contract tests for dataset backends.

These tests validate that DuckDB and PyArrow backends expose consistent
behavior for core dataset operations and return compatible result structures.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from fsspeckit.common.optional import _DUCKDB_AVAILABLE, _PYARROW_AVAILABLE

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

pytestmark = pytest.mark.skipif(
    not (_DUCKDB_AVAILABLE and _PYARROW_AVAILABLE),
    reason="DuckDB and PyArrow dependencies are required",
)


@pytest.fixture
def duckdb_io():
    from fsspeckit.datasets.duckdb.connection import create_duckdb_connection
    from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

    conn = create_duckdb_connection()
    io = DuckDBDatasetIO(conn)
    yield io
    conn.close()


@pytest.fixture
def pyarrow_io():
    from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO

    return PyarrowDatasetIO()


def _write_parquet_parts(path: Path, tables: list[pa.Table]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for idx, table in enumerate(tables):
        pq.write_table(table, path / f"part-{idx}.parquet")


def test_write_dataset_result_parity(tmp_path, duckdb_io, pyarrow_io) -> None:
    data = pa.table({"id": [1, 2, 3], "name": ["A", "B", "C"]})

    duckdb_path = tmp_path / "duckdb_dataset"
    pyarrow_path = tmp_path / "pyarrow_dataset"

    duckdb_result = duckdb_io.write_dataset(data, str(duckdb_path), mode="overwrite")
    pyarrow_result = pyarrow_io.write_dataset(data, str(pyarrow_path), mode="overwrite")

    assert duckdb_result.total_rows == data.num_rows
    assert pyarrow_result.total_rows == data.num_rows
    assert duckdb_result.mode == "overwrite"
    assert pyarrow_result.mode == "overwrite"
    assert duckdb_result.backend == "duckdb"
    assert pyarrow_result.backend == "pyarrow"

    for result in (duckdb_result, pyarrow_result):
        assert len(result.files) >= 1
        assert sum(f.row_count for f in result.files) == result.total_rows
        serialized = result.to_dict()
        assert set(serialized) >= {"files", "total_rows", "mode", "backend"}


def test_merge_upsert_parity(tmp_path, duckdb_io, pyarrow_io) -> None:
    base = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
    source = pa.table({"id": [2, 4], "value": [22, 40]})

    duckdb_path = tmp_path / "duckdb_merge"
    pyarrow_path = tmp_path / "pyarrow_merge"

    duckdb_io.write_dataset(base, str(duckdb_path), mode="overwrite")
    pyarrow_io.write_dataset(base, str(pyarrow_path), mode="overwrite")

    duckdb_result = duckdb_io.merge(
        source,
        str(duckdb_path),
        strategy="upsert",
        key_columns=["id"],
    )
    pyarrow_result = pyarrow_io.merge(
        source,
        str(pyarrow_path),
        strategy="upsert",
        key_columns=["id"],
    )

    assert duckdb_result.inserted == 1
    assert pyarrow_result.inserted == 1
    assert duckdb_result.updated == 1
    assert pyarrow_result.updated == 1
    assert duckdb_result.target_count_after == 4
    assert pyarrow_result.target_count_after == 4


def test_merge_update_requires_existing_target(tmp_path, duckdb_io, pyarrow_io) -> None:
    source = pa.table({"id": [1], "value": [99]})
    missing_path = tmp_path / "missing"

    with pytest.raises(ValueError, match="non-existent target"):
        duckdb_io.merge(
            source,
            str(missing_path),
            strategy="update",
            key_columns=["id"],
        )

    with pytest.raises(ValueError, match="non-existent target"):
        pyarrow_io.merge(
            source,
            str(missing_path),
            strategy="update",
            key_columns=["id"],
        )


def test_compact_parquet_dataset_dry_run_parity(tmp_path, duckdb_io, pyarrow_io) -> None:
    data = pa.table({"id": [1, 2, 3, 4], "value": [10, 20, 30, 40]})
    tables = [data.slice(0, 2), data.slice(2, 2)]
    dataset_path = tmp_path / "compact"
    _write_parquet_parts(dataset_path, tables)

    duckdb_stats = duckdb_io.compact_parquet_dataset(
        str(dataset_path),
        target_rows_per_file=2,
        dry_run=True,
    )
    pyarrow_stats = pyarrow_io.compact_parquet_dataset(
        str(dataset_path),
        target_rows_per_file=2,
        dry_run=True,
    )

    common_keys = {
        "before_file_count",
        "after_file_count",
        "before_total_bytes",
        "after_total_bytes",
        "compacted_file_count",
        "rewritten_bytes",
        "compression_codec",
        "dry_run",
    }
    for key in common_keys:
        assert duckdb_stats[key] == pyarrow_stats[key]


def test_optimize_parquet_dataset_parity(tmp_path, duckdb_io, pyarrow_io) -> None:
    data = pa.table({"id": [1, 2, 3, 4], "value": [10, 20, 30, 40]})
    tables = [data.slice(0, 2), data.slice(2, 2)]

    duckdb_path = tmp_path / "duckdb_optimize"
    pyarrow_path = tmp_path / "pyarrow_optimize"
    _write_parquet_parts(duckdb_path, tables)
    _write_parquet_parts(pyarrow_path, tables)

    duckdb_stats = duckdb_io.optimize_parquet_dataset(
        str(duckdb_path),
        target_rows_per_file=4,
    )
    pyarrow_stats = pyarrow_io.optimize_parquet_dataset(
        str(pyarrow_path),
        target_rows_per_file=4,
    )

    for key in ("before_file_count", "after_file_count", "compacted_file_count"):
        assert duckdb_stats[key] == pyarrow_stats[key]
    assert duckdb_stats["after_file_count"] <= duckdb_stats["before_file_count"]
    assert pyarrow_stats["after_file_count"] <= pyarrow_stats["before_file_count"]
