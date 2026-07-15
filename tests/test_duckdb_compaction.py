"""Migration coverage for removed DuckDB dictionary compaction helpers."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from fsspeckit.core.maintenance import DatasetMaintenanceCoordinator, MaintenanceResult


def test_duckdb_coordinator_compaction_returns_typed_result(tmp_path) -> None:
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), dataset / "first.parquet")
    pq.write_table(pa.table({"id": [3, 4]}), dataset / "second.parquet")

    coordinator = DatasetMaintenanceCoordinator("duckdb")
    plan = coordinator.plan_compaction(str(dataset), target_rows_per_file=4)
    result = coordinator.execute(plan)

    assert isinstance(result, MaintenanceResult)
    assert result.succeeded
    assert result.plan.selected_backend == "duckdb"
