"""Public filesystem maintenance façade tests for issue #46."""

from __future__ import annotations

import importlib

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from fsspeckit.core.maintenance import (
    CompactionPlan,
    MaintenanceBackend,
    MaintenanceResult,
)


def _write_fragmented_dataset(root) -> None:
    root.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), root / "first.parquet")
    pq.write_table(pa.table({"id": [3, 4]}), root / "second.parquet")


def test_filesystem_facade_plans_then_executes_typed_compaction(tmp_path) -> None:
    dataset = tmp_path / "dataset"
    _write_fragmented_dataset(dataset)
    filesystem = fsspec.filesystem("file")

    plan = filesystem.plan_parquet_compaction(str(dataset), target_rows_per_file=4)
    result = filesystem.execute_maintenance_plan(plan)

    assert isinstance(plan, CompactionPlan)
    assert plan.selected_backend == MaintenanceBackend.PYARROW.value
    assert isinstance(result, MaintenanceResult)
    assert result.plan is plan
    assert result.succeeded


def test_filesystem_facade_compaction_preserves_one_call_typed_result(tmp_path) -> None:
    dataset = tmp_path / "dataset"
    _write_fragmented_dataset(dataset)
    filesystem = fsspec.filesystem("file")

    result = filesystem.compact_parquet_dataset(str(dataset), target_rows_per_file=4)

    assert isinstance(result, MaintenanceResult)
    assert result.succeeded
    assert result.plan.selected_backend == MaintenanceBackend.PYARROW.value


def test_legacy_dictionary_maintenance_exports_are_removed() -> None:
    pyarrow_maintenance = importlib.import_module("fsspeckit.datasets.pyarrow")
    pyarrow_implementation = importlib.import_module(
        "fsspeckit.datasets.pyarrow.dataset"
    )
    duckdb_maintenance = importlib.import_module("fsspeckit.datasets.duckdb")
    duckdb_implementation = importlib.import_module("fsspeckit.datasets.duckdb.dataset")

    for name in (
        "compact_parquet_dataset_pyarrow",
        "deduplicate_parquet_dataset_pyarrow",
        "optimize_parquet_dataset_pyarrow",
    ):
        assert name not in pyarrow_maintenance.__all__
        assert not hasattr(pyarrow_implementation, name)

    assert "compact_parquet_dataset_duckdb" not in duckdb_maintenance.__all__
    assert not hasattr(duckdb_implementation, "compact_parquet_dataset_duckdb")
