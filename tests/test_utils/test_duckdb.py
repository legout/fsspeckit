"""Tests for DuckDB backend (connection, IO, maintenance).

This suite focuses on the current, explicit APIs:
- DuckDBConnection / DuckDBDatasetIO
- write_dataset(mode=...)
- merge(strategy=...) is covered in `tests/test_duckdb_merge.py`
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest
from fsspec import filesystem as fsspec_filesystem

from fsspeckit.common.optional import _DUCKDB_AVAILABLE
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection
from fsspeckit.datasets.duckdb.helpers import _unregister_duckdb_table_safely

pytestmark = pytest.mark.skipif(not _DUCKDB_AVAILABLE, reason="duckdb not installed")


@pytest.fixture
def sample_table() -> pa.Table:
    return pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [28, 34, 45, 29, 52],
            "city": ["New York", "London", "Paris", "Tokyo", "Sydney"],
            "amount": [150.50, 89.99, 234.75, 67.25, 412.80],
            "category": ["A", "B", "A", "C", "B"],
            "active": [True, True, False, True, False],
        }
    )


@pytest.fixture
def temp_dir() -> Path:
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


@pytest.fixture
def conn_mgr():
    return create_duckdb_connection(filesystem=fsspec_filesystem("file"))


@pytest.fixture
def io(conn_mgr) -> DuckDBDatasetIO:
    return DuckDBDatasetIO(conn_mgr)


def test_connection_manager_lifecycle(conn_mgr):
    assert conn_mgr.filesystem is not None
    conn = conn_mgr.connection
    assert conn is not None
    conn_mgr.close()
    assert conn_mgr._connection is None  # noqa: SLF001


def test_read_write_parquet_nested_dirs(
    io: DuckDBDatasetIO, sample_table: pa.Table, temp_dir: Path
):
    nested_file = temp_dir / "year=2024" / "month=01" / "data.parquet"
    io.write_parquet(sample_table, str(nested_file))
    assert nested_file.exists()

    out = io.read_parquet(str(nested_file))
    assert out.num_rows == sample_table.num_rows
    assert set(sample_table.column_names).issubset(set(out.column_names))


def test_write_parquet_parent_is_file_error(
    io: DuckDBDatasetIO, sample_table: pa.Table, temp_dir: Path
):
    parent_file = temp_dir / "parent_file.txt"
    parent_file.write_text("not a directory")
    parquet_path = parent_file / "subdir" / "output.parquet"

    with pytest.raises((NotADirectoryError, OSError)):
        io.write_parquet(sample_table, str(parquet_path))


def test_execute_sql_fetch_arrow(conn_mgr, sample_table: pa.Table, temp_dir: Path):
    parquet_file = temp_dir / "data.parquet"
    DuckDBDatasetIO(conn_mgr).write_parquet(sample_table, str(parquet_file))

    rel = conn_mgr.execute_sql(
        f"SELECT * FROM parquet_scan('{parquet_file}') WHERE age > 30"
    )
    result = rel.fetch_arrow_table()
    assert isinstance(result, pa.Table)
    assert result.num_rows < sample_table.num_rows


def test_write_dataset_append_and_overwrite(
    io: DuckDBDatasetIO, sample_table: pa.Table, temp_dir: Path
):
    dataset_dir = temp_dir / "dataset"

    res1 = io.write_dataset(sample_table, str(dataset_dir), mode="append")
    assert res1.total_rows == sample_table.num_rows
    assert len(res1.files) >= 1

    res2 = io.write_dataset(sample_table, str(dataset_dir), mode="append")
    assert res2.total_rows == sample_table.num_rows
    assert len(list(dataset_dir.glob("**/*.parquet"))) >= len(res1.files) + len(
        res2.files
    )

    res3 = io.write_dataset(sample_table, str(dataset_dir), mode="overwrite")
    assert res3.total_rows == sample_table.num_rows


def test_write_dataset_invalid_params(
    io: DuckDBDatasetIO, sample_table: pa.Table, temp_dir: Path
):
    dataset_dir = temp_dir / "dataset"
    with pytest.raises(ValueError, match="mode must be"):
        io.write_dataset(sample_table, str(dataset_dir), mode="invalid")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must be > 0"):
        io.write_dataset(sample_table, str(dataset_dir), max_rows_per_file=0)
    with pytest.raises(ValueError, match="must be > 0"):
        io.write_dataset(sample_table, str(dataset_dir), row_group_size=0)


@pytest.fixture
def fragmented_dataset(io: DuckDBDatasetIO, temp_dir: Path) -> Path:
    dataset_dir = temp_dir / "fragmented"
    dataset_dir.mkdir()
    for i in range(10):
        table = pa.table(
            {
                "id": list(range(i * 50, i * 50 + 50)),
                "group": [i] * 50,
                "value": list(range(50)),
            }
        )
        io.write_parquet(table, str(dataset_dir / f"part_{i}.parquet"))
    return dataset_dir


def test_compact_by_size(io: DuckDBDatasetIO, fragmented_dataset: Path):
    before_files = list(fragmented_dataset.glob("*.parquet"))
    before_rows = io.read_parquet(str(fragmented_dataset)).num_rows

    result = io.compact_parquet_dataset(
        path=str(fragmented_dataset), target_mb_per_file=1
    )
    assert result["dry_run"] is False

    after_files = list(fragmented_dataset.glob("*.parquet"))
    after_rows = io.read_parquet(str(fragmented_dataset)).num_rows
    assert len(after_files) < len(before_files)
    assert after_rows == before_rows


def test_compact_dry_run(io: DuckDBDatasetIO, fragmented_dataset: Path):
    before_files = list(fragmented_dataset.glob("*.parquet"))
    result = io.compact_parquet_dataset(
        path=str(fragmented_dataset), target_mb_per_file=1, dry_run=True
    )
    after_files = list(fragmented_dataset.glob("*.parquet"))
    assert before_files == after_files
    assert result["dry_run"] is True
    assert result["planned_groups"]


def test_optimize_is_compaction(io: DuckDBDatasetIO, fragmented_dataset: Path):
    before_files = list(fragmented_dataset.glob("*.parquet"))
    before_rows = io.read_parquet(str(fragmented_dataset)).num_rows

    result = io.optimize_parquet_dataset(
        path=str(fragmented_dataset), target_mb_per_file=1
    )
    assert result["dry_run"] is False

    after_files = list(fragmented_dataset.glob("*.parquet"))
    after_rows = io.read_parquet(str(fragmented_dataset)).num_rows
    assert len(after_files) < len(before_files)
    assert after_rows == before_rows


def test_unregister_helper_success(conn_mgr):
    conn = conn_mgr.connection
    t = pa.table({"id": [1, 2, 3]})
    conn.register("test_table", t)
    assert ("test_table",) in conn.execute("SHOW TABLES").fetchall()

    _unregister_duckdb_table_safely(conn, "test_table")
    assert ("test_table",) not in conn.execute("SHOW TABLES").fetchall()


def test_unregister_helper_missing_table_does_not_raise(conn_mgr):
    conn = conn_mgr.connection
    _unregister_duckdb_table_safely(conn, "does_not_exist")
