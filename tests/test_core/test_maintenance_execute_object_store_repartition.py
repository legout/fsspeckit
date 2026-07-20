"""Focused generic fsspec tests for pure full-dataset repartition (#60).

The memory filesystem exercises the fsspec object-store seam only; these tests
make no claim about real S3-compatible publication behavior. They mirror the
existing ``test_maintenance_execute_object_store_global.py`` but strip every
deduplication assertion: pure repartition preserves every source row,
including exact duplicates.
"""

from __future__ import annotations

import dataclasses
import uuid
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from fsspeckit.core.maintenance import (
    BEST_EFFORT_CONCURRENCY_DISCLAIMER,
    BestEffortRepartitionResult,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    MaintenanceBackend,
    MaintenanceResult,
    RepartitionPlan,
    RepartitionResult,
    ValidationLevel,
)


def _root() -> str:
    return f"/repartition-{uuid.uuid4().hex[:8]}"


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
    partition_columns: list[str] | None = None,
    target_rows_per_file: int | None = None,
    memory_budget_mb: int | None = None,
):
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    return coordinator.plan_repartition(
        root,
        partition_columns or ["region"],
        filesystem=fs,
        target_rows_per_file=target_rows_per_file,
        memory_budget_mb=memory_budget_mb,
    )


def _run_plan(
    fs: MemoryFileSystem,
    root: str,
    *,
    partition_columns: list[str] | None = None,
    target_rows_per_file: int | None = None,
    memory_budget_mb: int | None = None,
):
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    plan = coordinator.plan_repartition(
        root,
        partition_columns or ["region"],
        filesystem=fs,
        target_rows_per_file=target_rows_per_file,
        memory_budget_mb=memory_budget_mb,
    )
    return plan, coordinator.execute(plan, filesystem=fs)


def _sources(fs: MemoryFileSystem, root: str) -> tuple[str, str]:
    """Two source files with an exact duplicate across them (id=1)."""
    first = f"{root}/source-a.parquet"
    second = f"{root}/existing=partition/source-b.parquet"
    _write(
        fs,
        first,
        pa.table(
            {
                "id": [1, 1, 2],
                "region": ["US", "DE", "US"],
                "value": ["a-dup", "de", "a-three"],
            }
        ),
    )
    _write(
        fs,
        second,
        pa.table(
            {
                "id": [1, 4],
                "region": ["US", "CA"],
                "value": ["b-dup-of-1", "four"],
            }
        ),
    )
    return first, second


# --------------------------------------------------------------------------- #
# Row preservation and typed result
# --------------------------------------------------------------------------- #


class TestRepartitionSuccess:
    def test_exact_duplicates_preserved_and_rows_follow_declared_partitions(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _sources(fs, root)

        plan, result = _run_plan(fs, root)

        assert isinstance(plan, RepartitionPlan)
        assert isinstance(result, BestEffortRepartitionResult)
        assert isinstance(result, RepartitionResult)
        assert isinstance(result, MaintenanceResult)
        assert result.succeeded
        assert result.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
        assert result.concurrency_disclaimer == BEST_EFFORT_CONCURRENCY_DISCLAIMER

        assert result.publication is not None
        live_keys = result.publication.published_files
        assert live_keys
        assert all("/region=" in path for path in live_keys)
        assert all(path.startswith(root + "/region=") for path in live_keys)

        tables = [_read(fs, path) for path in live_keys]
        output = pa.concat_tables(tables)
        assert "region" not in output.column_names
        # Five source rows including the two duplicates of id=1.
        assert output.num_rows == 5
        assert sorted(output.column("id").to_pylist()) == [1, 1, 1, 2, 4]
        # Both duplicates of id=1 survive in the US partition.
        us_values = sorted(
            row["value"]
            for row in output.to_pylist()
            if _live_partition(root, live_keys, fs, row, "region=US")
        )
        assert us_values == ["a-dup", "a-three", "b-dup-of-1"]

        assert all(not fs.exists(source) for source in sources)
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 5
        assert result.recovery is None
        assert result.staging_prefix
        assert not fs.exists(result.staging_prefix)
        # Existing unrelated source partition was replaced.
        assert all(
            not path.startswith(root + "/existing=partition") for path in live_keys
        )

    def test_max_rows_per_file_is_hard_bound_per_destination_partition(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": [1, 2, 3, 4, 5],
                    "region": ["US", "US", "US", "US", "US"],
                }
            ),
        )

        _, result = _run_plan(fs, root, target_rows_per_file=2)

        assert result.succeeded
        assert result.publication is not None
        assert len(result.publication.published_files) == 3
        for path in result.publication.published_files:
            with fs.open(path, "rb") as handle:
                assert pq.read_metadata(handle).num_rows <= 2


def _live_partition(root, live_keys, fs, row, partition_prefix):
    """Return True if *row* lives under the destination partition matching *partition_prefix*."""
    # This helper is only used to group rows by partition for assertion checks.
    # Each live key encodes the destination partition in its path.
    for key in live_keys:
        if partition_prefix in key:
            table = _read(fs, key)
            if any(r == row for r in table.to_pylist()):
                return True
    return False


# --------------------------------------------------------------------------- #
# Planning requirements
# --------------------------------------------------------------------------- #


class TestRepartitionPlanningRequirements:
    @pytest.mark.parametrize(
        "partition_columns",
        [[], ["missing"], ["region", "region"], [""]],
    )
    def test_destination_partition_columns_are_required_and_declared(
        self, partition_columns
    ):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": [1], "region": ["US"]}),
        )
        coordinator = DatasetMaintenanceCoordinator("pyarrow")

        with pytest.raises(ValueError):
            coordinator.plan_repartition(root, partition_columns, filesystem=fs)

    def test_full_distinct_key_scan_rejected_at_plan_time(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": [1], "region": ["US"]}),
        )
        coordinator = DatasetMaintenanceCoordinator("pyarrow")

        with pytest.raises(ValueError, match="FULL_DISTINCT_KEY_SCAN"):
            coordinator.plan_repartition(
                root,
                ["region"],
                filesystem=fs,
                validation_level="full_distinct_key_scan",
            )

    def test_memory_budget_recorded_on_plan(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": [1], "region": ["US"]}),
        )
        plan = _make_plan(fs, root, memory_budget_mb=32)
        assert plan.repartition_memory_budget_mb == 32


# --------------------------------------------------------------------------- #
# Safety: drift, copy failure, delete failure
# --------------------------------------------------------------------------- #


class TestRepartitionSafety:
    def test_source_drift_prevents_every_source_deletion_and_preserves_recovery(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _sources(fs, root)
        plan = _make_plan(fs, root)
        _write(
            fs,
            sources[1],
            pa.table(
                {
                    "id": [1, 4, 5],
                    "region": ["US", "CA", "DE"],
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
        sources = _sources(fs, root)
        plan = _make_plan(fs, root)
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
        sources = _sources(fs, root)
        plan = _make_plan(fs, root)
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


# --------------------------------------------------------------------------- #
# Partition value coverage
# --------------------------------------------------------------------------- #


class TestRepartitionPartitionValues:
    def test_integer_partition_columns_are_path_only_and_hive_readable(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": [1, 2, 3],
                    "year": pa.array([2023, 2024, 2024], type=pa.int64()),
                }
            ),
        )

        _, result = _run_plan(fs, root, partition_columns=["year"])

        assert result.succeeded
        assert result.publication is not None
        for path in result.publication.published_files:
            with fs.open(path, "rb") as handle:
                schema = pq.ParquetFile(handle).schema_arrow
                assert "year" not in schema.names

    def test_null_and_nan_partition_values_use_hive_sentinels(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": [1, 2, 3],
                    "region": pa.array([None, float("nan"), 1.0], type=pa.float64()),
                }
            ),
        )

        _, result = _run_plan(fs, root)

        assert result.succeeded
        live_keys = set(result.publication.published_files)
        assert any("region=__HIVE_DEFAULT_PARTITION__" in k for k in live_keys)
        assert any("region=__HIVE_NAN_PARTITION__" in k for k in live_keys)
        assert any("region=1.0" in k for k in live_keys)

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

        _, result = _run_plan(fs, root)

        assert result.succeeded
        live_keys = set(result.publication.published_files)
        assert any("region=100%25" in k for k in live_keys)
        assert any(
            "region=__fsspeckit_value____HIVE_DEFAULT_PARTITION__" in k
            for k in live_keys
        )
        assert any("region=__fsspeckit_value__nan" in k for k in live_keys)
        assert any("region=a%2Fb" in k for k in live_keys)

    def test_timestamp_derived_partitions_use_explicit_timezone(self):
        from datetime import datetime, timedelta, timezone

        fs = MemoryFileSystem()
        root = _root()
        source_timezone = timezone(timedelta(hours=1))
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": [1, 2],
                    "event_ts": pa.array(
                        [
                            datetime(2024, 1, 1, 0, 30, tzinfo=source_timezone),
                            datetime(2024, 2, 1, 0, 30, tzinfo=source_timezone),
                        ],
                        type=pa.timestamp("us", tz="+01:00"),
                    ),
                }
            ),
        )

        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_repartition(
            root,
            ["year", "year_month"],
            filesystem=fs,
            derived_partition_columns={
                "year": ("year", "event_ts"),
                "year_month": ("strftime", "event_ts", "%Y-%m"),
            },
            partition_timezone="UTC",
        )
        result = coordinator.execute(plan, filesystem=fs)

        assert result.succeeded, result.error
        assert plan.derived_partition_keys[0].timezone == "UTC"
        live_keys = list(result.publication.published_files)
        assert any("/year=2023/year_month=2023-12/" in k for k in live_keys)
        for path in live_keys:
            with fs.open(path, "rb") as handle:
                schema = pq.ParquetFile(handle).schema_arrow
                assert "year" not in schema.names
                assert "year_month" not in schema.names


# --------------------------------------------------------------------------- #
# Memory / spill behavior
# --------------------------------------------------------------------------- #


class TestRepartitionMemorySpill:
    def test_budget_none_is_group_bounded_and_preserves_rows(self):
        fs = MemoryFileSystem()
        root = _root()
        rows = 100
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": list(range(rows)),
                    "region": ["US"] * rows,
                    "payload": ["x" * 50] * rows,
                }
            ),
        )

        plan, result = _run_plan(
            fs, root, target_rows_per_file=25, memory_budget_mb=None
        )

        assert plan.repartition_memory_budget_mb is None
        assert result.succeeded
        assert result.actual_metrics.row_count == rows
        assert result.actual_metrics.file_count == 4

    def test_budget_zero_forces_spill_and_preserves_rows(self):
        """A zero-MB budget forces every bucket to spill; rows are preserved."""
        fs = MemoryFileSystem()
        root = _root()
        rows = 100
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": list(range(rows)),
                    "region": ["US"] * rows,
                    "payload": ["x" * 50] * rows,
                }
            ),
        )

        plan, result = _run_plan(fs, root, target_rows_per_file=25, memory_budget_mb=0)

        assert plan.repartition_memory_budget_mb == 0
        assert result.succeeded
        assert result.actual_metrics.row_count == rows
        assert result.actual_metrics.file_count == 4
        # Staging prefix (including spill prefix) cleaned up on success.
        assert not fs.exists(result.staging_prefix)

    def test_spill_across_multiple_partitions_preserves_rows(self):
        fs = MemoryFileSystem()
        root = _root()
        rows = 200
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": list(range(rows)),
                    "region": ["US"] * (rows // 2) + ["DE"] * (rows // 2),
                    "payload": ["x" * 50] * rows,
                }
            ),
        )

        plan, result = _run_plan(fs, root, target_rows_per_file=25, memory_budget_mb=0)

        assert plan.repartition_memory_budget_mb == 0
        assert result.succeeded
        assert result.actual_metrics.row_count == rows
        us_keys = [k for k in result.publication.published_files if "region=US" in k]
        de_keys = [k for k in result.publication.published_files if "region=DE" in k]
        assert len(us_keys) == 4
        assert len(de_keys) == 4


# --------------------------------------------------------------------------- #
# FULL_DISTINCT_KEY_SCAN rejection at execute time (defense-in-depth)
# --------------------------------------------------------------------------- #


class TestRepartitionExecuteRejection:
    def test_full_distinct_key_scan_rejected_at_execute_time(self):
        """Externally constructed plans cannot bypass the invariant."""
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": [1], "region": ["US"]}),
        )
        plan = _make_plan(fs, root)
        bad_plan = dataclasses.replace(
            plan, validation_level=ValidationLevel.FULL_DISTINCT_KEY_SCAN
        )
        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(ValueError, match="FULL_DISTINCT_KEY_SCAN"):
            coordinator.execute(bad_plan, filesystem=fs)
