"""Tests for merge_parquet_dataset_pyarrow."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow


def _read_dataset_table(path: str) -> pa.Table:
    dataset = ds.dataset(path)
    return dataset.to_table()


class TestMergeParquetDatasetPyArrow:
    def test_upsert_with_table_source(self, tmp_path):
        target = tmp_path / "target"
        target.mkdir()
        table_a = pa.table({"id": [1, 2], "value": ["a", "b"]})
        table_b = pa.table({"id": [3], "value": ["c"]})
        pq.write_table(table_a, target / "part-0.parquet")
        pq.write_table(table_b, target / "part-1.parquet")

        source = pa.table({"id": [2, 3, 4], "value": ["beta", "gamma", "delta"]})

        stats = merge_parquet_dataset_pyarrow(
            source,
            str(target),
            key_columns="id",
            strategy="upsert",
        )

        assert stats["inserted"] == 1
        assert stats["updated"] == 2
        assert stats["deleted"] == 0
        result = _read_dataset_table(str(target))
        assert result.num_rows == 4
        values = dict(
            zip(result.column("id").to_pylist(), result.column("value").to_pylist())
        )
        assert values == {1: "a", 2: "beta", 3: "gamma", 4: "delta"}

    def test_insert_with_path_source(self, tmp_path):
        target = tmp_path / "target"
        source_dir = tmp_path / "source"
        target.mkdir()
        source_dir.mkdir()
        pq.write_table(
            pa.table({"id": [1], "value": ["keep"]}), target / "part-0.parquet"
        )
        pq.write_table(
            pa.table({"id": [2], "value": ["existing"]}), target / "part-1.parquet"
        )
        pq.write_table(
            pa.table({"id": [2], "value": ["dupe"]}), source_dir / "part-0.parquet"
        )
        pq.write_table(
            pa.table({"id": [3], "value": ["new"]}), source_dir / "part-1.parquet"
        )

        stats = merge_parquet_dataset_pyarrow(
            str(source_dir),
            str(target),
            key_columns="id",
            strategy="insert",
        )

        assert stats["inserted"] == 1
        assert stats["updated"] == 0
        result = _read_dataset_table(str(target))
        values = dict(
            zip(result.column("id").to_pylist(), result.column("value").to_pylist())
        )
        assert values[2] == "existing"
        assert values[3] == "new"

    def test_update_with_composite_keys(self, tmp_path):
        target = tmp_path / "target"
        target.mkdir()
        pq.write_table(
            pa.table(
                {
                    "user_id": [1, 1, 2],
                    "date": ["2025-11-15", "2025-11-16", "2025-11-16"],
                    "value": [10, 20, 30],
                }
            ),
            target / "part-0.parquet",
        )

        source = pa.table(
            {
                "user_id": [1, 3],
                "date": ["2025-11-16", "2025-11-15"],
                "value": [200, 999],
            }
        )

        stats = merge_parquet_dataset_pyarrow(
            source,
            str(target),
            key_columns=["user_id", "date"],
            strategy="update",
        )

        assert stats["inserted"] == 0
        assert stats["updated"] == 1
        table = _read_dataset_table(str(target))
        values = {
            (uid, date): value
            for uid, date, value in zip(
                table.column("user_id").to_pylist(),
                table.column("date").to_pylist(),
                table.column("value").to_pylist(),
            )
        }
        assert values[(1, "2025-11-16")] == 200
        # New key ignored under update strategy
        assert (3, "2025-11-15") not in values

    def test_full_merge_deletes_missing_rows(self, tmp_path):
        target = tmp_path / "target"
        target.mkdir()
        pq.write_table(pa.table({"id": [1, 2, 3]}), target / "part-0.parquet")

        source = pa.table({"id": [2, 4]})
        stats = merge_parquet_dataset_pyarrow(
            source,
            str(target),
            key_columns="id",
            strategy="full_merge",
        )

        assert stats["deleted"] == 2
        assert stats["inserted"] == 1
        table = _read_dataset_table(str(target))
        assert sorted(table.column("id").to_pylist()) == [2, 4]

    def test_deduplicate_keeps_preferred_row(self, tmp_path):
        target = tmp_path / "target"
        target.mkdir()
        pq.write_table(
            pa.table({"id": [1], "value": ["orig"], "ts": [1]}),
            target / "part-0.parquet",
        )

        source = pa.table(
            {
                "id": [1, 1],
                "value": ["old", "new"],
                "ts": [1, 2],
            }
        )

        stats = merge_parquet_dataset_pyarrow(
            source,
            str(target),
            key_columns="id",
            strategy="deduplicate",
            dedup_order_by=["ts"],
        )

        assert stats["updated"] == 1
        table = _read_dataset_table(str(target))
        values = dict(
            zip(table.column("id").to_pylist(), table.column("value").to_pylist())
        )
        assert values[1] == "new"

    def test_null_key_validation(self, tmp_path):
        target = tmp_path / "target"
        target.mkdir()
        pq.write_table(pa.table({"id": [1]}), target / "part-0.parquet")
        source = pa.table({"id": [None]})
        with pytest.raises(ValueError, match="Key column 'id'"):
            merge_parquet_dataset_pyarrow(source, str(target), key_columns="id")

    def test_filtered_scanner_invoked(self, tmp_path, monkeypatch):
        target = tmp_path / "target"
        target.mkdir()
        for idx in range(2):
            pq.write_table(
                pa.table({"id": [idx], "value": [str(idx)]}),
                target / f"part-{idx}.parquet",
            )
        source = pa.table({"id": [0, 1], "value": ["x", "y"]})

        filters: list[ds.Expression | None] = []

        original_dataset = ds.dataset

        class LoggingDataset:
            def __init__(self, inner: ds.Dataset):
                self._inner = inner

            def scanner(self, *args, **kwargs):
                filters.append(kwargs.get("filter"))
                return self._inner.scanner(*args, **kwargs)

            def to_table(self, *args, **kwargs):  # pragma: no cover - safety guard
                raise AssertionError("merge helper must not call to_table directly")

            def __getattr__(self, item):
                return getattr(self._inner, item)

        def wrapped_dataset(*args, **kwargs):
            return LoggingDataset(original_dataset(*args, **kwargs))

        monkeypatch.setattr(ds, "dataset", wrapped_dataset)

        merge_parquet_dataset_pyarrow(
            source,
            str(target),
            key_columns="id",
            strategy="upsert",
        )

        assert any(expr is not None for expr in filters)
