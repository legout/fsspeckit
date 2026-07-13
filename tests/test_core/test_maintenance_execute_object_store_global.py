"""Focused generic fsspec tests for global object-store deduplication (#43).

The memory filesystem exercises the fsspec object-store seam only; these tests
make no claim about real S3-compatible publication behavior.
"""

from __future__ import annotations

import uuid
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from fsspeckit.core.maintenance import (
    BEST_EFFORT_CONCURRENCY_DISCLAIMER,
    BestEffortGlobalRepartitionDeduplicationResult,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    MaintenanceBackend,
    MaintenanceResult,
)


def _root() -> str:
    return f"/global-dedup-{uuid.uuid4().hex[:8]}"


def _write(fs: MemoryFileSystem, path: str, table: pa.Table) -> None:
    buffer = BytesIO()
    pq.write_table(table, buffer)
    fs.pipe(path, buffer.getvalue())


def _read(fs: MemoryFileSystem, path: str) -> pa.Table:
    with fs.open(path, "rb") as handle:
        return pq.read_table(handle)


def _make_plan(
    fs: MemoryFileSystem,
    root: str,
    *,
    key_columns: list[str] | None = None,
    dedup_order_by: list[str] | None = None,
    partition_columns: list[str] | None = None,
    target_rows_per_file: int | None = None,
):
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    return coordinator.plan_global_repartition_deduplication(
        root,
        partition_columns or ["region"],
        filesystem=fs,
        key_columns=key_columns,
        dedup_order_by=dedup_order_by,
        target_rows_per_file=target_rows_per_file,
    )


def _run_plan(
    fs: MemoryFileSystem,
    root: str,
    *,
    key_columns: list[str] | None = None,
    dedup_order_by: list[str] | None = None,
    partition_columns: list[str] | None = None,
    target_rows_per_file: int | None = None,
):
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    plan = coordinator.plan_global_repartition_deduplication(
        root,
        partition_columns or ["region"],
        filesystem=fs,
        key_columns=key_columns,
        dedup_order_by=dedup_order_by,
        target_rows_per_file=target_rows_per_file,
    )
    return plan, coordinator.execute(plan, filesystem=fs)


def _global_sources(fs: MemoryFileSystem, root: str) -> tuple[str, str]:
    first = f"{root}/source-a.parquet"
    second = f"{root}/existing=partition/source-b.parquet"
    _write(
        fs,
        first,
        pa.table(
            {
                "id": [1, 2, 3],
                "region": ["US", "DE", "US"],
                "score": [2, 1, 1],
                "value": ["old", "de", "three"],
            }
        ),
    )
    _write(
        fs,
        second,
        pa.table(
            {
                "id": [1, 4],
                "region": ["DE", "CA"],
                "score": [1, 1],
                "value": ["winner", "four"],
            }
        ),
    )
    return first, second


class TestGlobalRepartitionSuccess:
    def test_global_duplicates_are_removed_and_rows_follow_declared_partitions(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _global_sources(fs, root)

        plan, result = _run_plan(
            fs,
            root,
            key_columns=["id"],
            dedup_order_by=["score"],
        )

        assert isinstance(result, BestEffortGlobalRepartitionDeduplicationResult)
        assert isinstance(result, MaintenanceResult)
        assert result.succeeded
        assert result.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
        assert result.concurrency_disclaimer == BEST_EFFORT_CONCURRENCY_DISCLAIMER
        assert result.destination_partition_columns == ("region",)
        assert set(result.destination_partition_keys) == {
            "region=CA",
            "region=DE",
            "region=US",
        }

        assert result.publication is not None
        live_keys = result.publication.published_files
        assert live_keys
        assert all("/region=" in path for path in live_keys)
        assert all(path.startswith(root + "/region=") for path in live_keys)
        tables = [_read(fs, path) for path in live_keys]
        output = pa.concat_tables(tables)
        assert output.num_rows == 4
        assert sorted(output.column("id").to_pylist()) == [1, 2, 3, 4]
        winner_rows = [
            row
            for row in output.to_pylist()
            if row["id"] == 1
        ]
        assert winner_rows == [
            {"id": 1, "region": "DE", "score": 1, "value": "winner"}
        ]
        assert all(not fs.exists(source) for source in sources)
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 4
        assert result.recovery is None
        assert result.staging_prefix
        assert not fs.exists(result.staging_prefix)
        assert all(
            not path.startswith(root + "/existing=partition")
            for path in live_keys
        )

    def test_max_rows_per_file_is_hard_bound_per_destination_partition(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": [1, 2, 3, 4],
                    "region": ["US", "US", "US", "US"],
                }
            ),
        )

        _, result = _run_plan(fs, root, key_columns=["id"], target_rows_per_file=2)

        assert result.succeeded
        assert result.publication is not None
        assert len(result.publication.published_files) == 2
        for path in result.publication.published_files:
            with fs.open(path, "rb") as handle:
                assert pq.read_metadata(handle).num_rows <= 2


class TestGlobalRepartitionPlanningRequirements:
    @pytest.mark.parametrize(
        "partition_columns",
        [[], ["missing"], ["region", "region"], [""]],
    )
    def test_destination_partition_columns_are_required_and_declared(self, partition_columns):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": [1], "region": ["US"]}),
        )
        coordinator = DatasetMaintenanceCoordinator("pyarrow")

        with pytest.raises(ValueError):
            coordinator.plan_global_repartition_deduplication(
                root,
                partition_columns,
                filesystem=fs,
                key_columns=["id"],
            )


class TestGlobalRepartitionSafety:
    def test_source_drift_prevents_every_source_deletion_and_preserves_recovery(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _global_sources(fs, root)
        plan = _make_plan(fs, root, key_columns=["id"])
        _write(
            fs,
            sources[1],
            pa.table(
                {
                    "id": [1, 4, 5],
                    "region": ["DE", "CA", "US"],
                    "score": [2, 1, 1],
                    "value": ["changed", "four", "five"],
                }
            ),
        )

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        result = coordinator.execute(plan, filesystem=fs)

        assert not result.succeeded
        assert result.drift_detected
        assert result.publication is not None
        assert result.publication.removed_source_files == ()
        assert all(fs.exists(source) for source in sources)
        assert result.recovery is not None
        assert result.recovery.workspace_path == result.staging_prefix
        assert fs.exists(result.staging_prefix)

    def test_copy_failure_preserves_staging_and_sources(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _global_sources(fs, root)
        plan = _make_plan(fs, root, key_columns=["id"])
        original_cat = fs.cat

        def failing_cat(path, **kwargs):
            if "_maintenance_staging" in path:
                raise OSError("injected copy failure")
            return original_cat(path, **kwargs)

        fs.cat = failing_cat  # type: ignore[method-assign]
        try:
            coordinator = DatasetMaintenanceCoordinator("pyarrow")
            result = coordinator.execute(plan, filesystem=fs)
        finally:
            fs.cat = original_cat  # type: ignore[method-assign]

        assert not result.succeeded
        assert result.failed_copies
        assert result.recovery is not None
        assert fs.exists(result.staging_prefix)
        assert all(fs.exists(source) for source in sources)
        assert result.publication is not None
        assert result.publication.removed_source_files == ()

    def test_delete_failure_stops_before_deleting_remaining_sources(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _global_sources(fs, root)
        plan = _make_plan(fs, root, key_columns=["id"])
        original_rm = fs.rm

        def failing_rm(path, recursive=False):
            if path == sources[0]:
                raise OSError("injected delete failure")
            return original_rm(path, recursive=recursive)

        fs.rm = failing_rm  # type: ignore[method-assign]
        try:
            coordinator = DatasetMaintenanceCoordinator("pyarrow")
            result = coordinator.execute(plan, filesystem=fs)
        finally:
            fs.rm = original_rm  # type: ignore[method-assign]

        assert not result.succeeded
        assert result.recovery is not None
        assert fs.exists(result.staging_prefix)
        assert all(fs.exists(source) for source in sources)
        assert result.publication is not None
        assert result.publication.removed_source_files == ()


class TestGlobalRepartitionKeySemantics:
    def test_null_nan_and_strings_use_exact_key_semantics(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/one.parquet",
            pa.table(
                {
                    "id": pa.array([None, float("nan"), 1.0], type=pa.float64()),
                    "code": ["A", "A", "A"],
                    "region": ["US", "US", "US"],
                    "value": ["null-first", "nan-first", "number-first"],
                }
            ),
        )
        _write(
            fs,
            f"{root}/two.parquet",
            pa.table(
                {
                    "id": pa.array([None, float("nan"), 1.0], type=pa.float64()),
                    "code": ["A", "A", "a"],
                    "region": ["DE", "DE", "DE"],
                    "value": ["null-second", "nan-second", "case-distinct"],
                }
            ),
        )

        _, result = _run_plan(fs, root, key_columns=["id", "code"])

        assert result.succeeded
        assert result.actual_metrics is not None
        # null, NaN, and (1, A)/(1, a) => four retained rows.
        assert result.actual_metrics.row_count == 4
        assert result.publication is not None
        output = pa.concat_tables(
            [_read(fs, path) for path in result.publication.published_files]
        )
        assert set(output.column("value").to_pylist()) == {
            "null-first",
            "nan-first",
            "number-first",
            "case-distinct",
        }
    
    def test_destination_partition_paths_escape_ambiguous_values(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": [1, 2, 3, 4],
                    "region": [
                        "a/b",
                        "100%",
                        "__HIVE_DEFAULT_PARTITION__",
                        "nan",
                    ],
                }
            ),
        )

        _, result = _run_plan(fs, root, key_columns=["id"])

        assert result.succeeded
        assert set(result.destination_partition_keys) == {
            "region=100%25",
            "region=__fsspeckit_value____HIVE_DEFAULT_PARTITION__",
            "region=__fsspeckit_value__nan",
            "region=a%2Fb",
        }
