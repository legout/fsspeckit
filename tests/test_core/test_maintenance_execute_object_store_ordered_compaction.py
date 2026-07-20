"""Focused generic fsspec tests for partition-ordered compaction (#61).

The memory filesystem exercises the fsspec object-store seam only; these tests
make no claim about real S3-compatible publication behavior. They mirror the
existing ``test_maintenance_execute_object_store_repartition.py`` but verify the
ordered-compaction contract: output is one globally ordered run per physical
partition, validated within files and across adjacent output-file boundaries.
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
    BestEffortOrderedCompactionResult,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    MaintenanceBackend,
    MaintenanceResult,
    OrderedCompactionPlan,
    OrderedCompactionResult,
    SortKey,
    ValidationLevel,
)


def _root() -> str:
    return f"/ordered-{uuid.uuid4().hex[:8]}"


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
    sort_keys=None,
    target_rows_per_file: int | None = None,
    partition_filter=None,
    memory_budget_mb: int | None = None,
    spill_directory: str | None = None,
):
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    return coordinator.plan_ordered_compaction(
        root,
        sort_keys or ["+id"],
        filesystem=fs,
        target_rows_per_file=target_rows_per_file,
        partition_filter=partition_filter,
        memory_budget_mb=memory_budget_mb,
        spill_directory=spill_directory,
    )


def _run_plan(
    fs: MemoryFileSystem,
    root: str,
    *,
    sort_keys=None,
    target_rows_per_file: int | None = None,
    partition_filter=None,
    memory_budget_mb: int | None = None,
    spill_directory: str | None = None,
):
    plan = _make_plan(
        fs,
        root,
        sort_keys=sort_keys,
        target_rows_per_file=target_rows_per_file,
        partition_filter=partition_filter,
        memory_budget_mb=memory_budget_mb,
        spill_directory=spill_directory,
    )
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    return plan, coordinator.execute(plan, filesystem=fs)


def _sources(fs: MemoryFileSystem, root: str) -> tuple[str, str]:
    """Two source files in one partition, deliberately out of order across files."""
    first = f"{root}/region=US/a.parquet"
    second = f"{root}/region=US/b.parquet"
    _write(
        fs,
        first,
        pa.table({"id": [3, 1, 5], "region": ["US", "US", "US"], "v": ["c", "a", "e"]}),
    )
    _write(
        fs,
        second,
        pa.table({"id": [2, 4], "region": ["US", "US"], "v": ["b", "d"]}),
    )
    return first, second


# --------------------------------------------------------------------------- #
# Ordering and typed result
# --------------------------------------------------------------------------- #


class TestOrderedCompactionSuccess:
    def test_order_validated_within_and_across_files(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _sources(fs, root)

        plan, result = _run_plan(fs, root, target_rows_per_file=2)

        assert isinstance(plan, OrderedCompactionPlan)
        assert isinstance(result, BestEffortOrderedCompactionResult)
        assert isinstance(result, OrderedCompactionResult)
        assert isinstance(result, MaintenanceResult)
        assert result.succeeded, result.error
        assert result.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
        assert result.concurrency_disclaimer == BEST_EFFORT_CONCURRENCY_DISCLAIMER

        assert result.publication is not None
        live_keys = sorted(result.publication.published_files)
        assert live_keys
        assert all("/region=US/" in path for path in live_keys)

        ids = []
        prev_max = None
        for key in live_keys:
            table = _read(fs, key)
            key_ids = table.column("id").to_pylist()
            ids.extend(key_ids)
            # Hard per-file row bound.
            assert pq.read_metadata(BytesIO(fs.cat(key))).num_rows <= 2
            # Cross-file boundary ordering.
            if prev_max is not None:
                assert prev_max <= key_ids[0]
            prev_max = key_ids[-1]
        assert ids == [1, 2, 3, 4, 5]
        assert all(not fs.exists(source) for source in sources)
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 5
        assert result.recovery is None
        assert not fs.exists(result.staging_prefix)

    def test_descending_order_via_typed_sort_key(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/region=US/a.parquet",
            pa.table({"id": [3, 1, 5, 2, 4], "region": ["US"] * 5}),
        )
        _, result = _run_plan(
            fs,
            root,
            sort_keys=[SortKey("id", descending=True)],
            target_rows_per_file=2,
        )

        assert result.succeeded, result.error
        ids = []
        for key in sorted(result.publication.published_files):
            ids.extend(_read(fs, key).column("id").to_pylist())
        assert ids == [5, 4, 3, 2, 1]

    def test_multi_partition_each_globally_ordered(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/region=US/a.parquet",
            pa.table({"id": [5, 1, 3], "region": ["US"] * 3}),
        )
        _write(
            fs,
            f"{root}/region=DE/a.parquet",
            pa.table({"id": [9, 2, 8], "region": ["DE"] * 3}),
        )

        _, result = _run_plan(fs, root, target_rows_per_file=10)

        assert result.succeeded, result.error
        us_ids = []
        de_ids = []
        for key in result.publication.published_files:
            ids = _read(fs, key).column("id").to_pylist()
            if "region=US" in key:
                us_ids.extend(ids)
            elif "region=DE" in key:
                de_ids.extend(ids)
        assert us_ids == [1, 3, 5]
        assert de_ids == [2, 8, 9]

    def test_equal_keys_apply_physical_tie_break(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/region=US/a.parquet",
            pa.table({"k": [1, 1], "tag": ["a0", "a1"]}),
        )
        _write(
            fs,
            f"{root}/region=US/b.parquet",
            pa.table({"k": [1, 1], "tag": ["b0", "b1"]}),
        )

        _, result = _run_plan(fs, root, sort_keys=["+k"], target_rows_per_file=10)

        assert result.succeeded, result.error
        key = sorted(result.publication.published_files)[0]
        assert _read(fs, key).column("tag").to_pylist() == ["a0", "a1", "b0", "b1"]


# --------------------------------------------------------------------------- #
# Planning requirements
# --------------------------------------------------------------------------- #


class TestOrderedCompactionPlanningRequirements:
    @pytest.mark.parametrize(
        "sort_keys",
        [[], [""], ["+id", "+id"], ["missing_column"], ["-"]],
    )
    def test_invalid_sort_keys_rejected(self, sort_keys):
        fs = MemoryFileSystem()
        root = _root()
        _write(fs, f"{root}/a.parquet", pa.table({"id": [1]}))
        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(ValueError):
            coordinator.plan_ordered_compaction(root, sort_keys, filesystem=fs)

    def test_full_distinct_key_scan_rejected_at_plan_time(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(fs, f"{root}/a.parquet", pa.table({"id": [1]}))
        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(ValueError, match="FULL_DISTINCT_KEY_SCAN"):
            coordinator.plan_ordered_compaction(
                root,
                ["+id"],
                filesystem=fs,
                validation_level="full_distinct_key_scan",
            )

    def test_memory_budget_and_spill_recorded_on_plan(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(fs, f"{root}/a.parquet", pa.table({"id": [1]}))
        plan = _make_plan(
            fs,
            root,
            memory_budget_mb=32,
            spill_directory=f"{root}/_spill",
        )
        assert plan.sort_memory_budget_mb == 32
        assert plan.sort_spill_directory == f"{root}/_spill"

    def test_partition_filter_restricts_scope(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/region=US/a.parquet",
            pa.table({"id": [3, 1, 2], "region": ["US"] * 3}),
        )
        _write(
            fs,
            f"{root}/region=DE/a.parquet",
            pa.table({"id": [9, 1], "region": ["DE", "DE"]}),
        )
        plan = _make_plan(fs, root, partition_filter=["region=US"])
        assert plan.partition_scope.scope_type.value == "filtered"
        assert len(plan.ordered_groups) == 1


# --------------------------------------------------------------------------- #
# Safety: drift, copy failure, delete failure
# --------------------------------------------------------------------------- #


class TestOrderedCompactionSafety:
    def test_source_drift_prevents_every_source_deletion(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _sources(fs, root)
        plan = _make_plan(fs, root)
        # Mutate one source after planning -> drift.
        _write(
            fs,
            sources[1],
            pa.table({"id": [2, 4, 99], "region": ["US"] * 3, "v": ["b", "d", "z"]}),
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
# Memory / spill behavior (external merge sort)
# --------------------------------------------------------------------------- #


class TestOrderedCompactionMemorySpill:
    def _write_dataset(self, fs, root, rows=10):
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": list(range(rows)),
                    "payload": ["x" * 20] * rows,
                }
            ),
        )

    def test_budget_none_is_partition_bounded_in_memory(self):
        fs = MemoryFileSystem()
        root = _root()
        self._write_dataset(fs, root, rows=10)
        plan, result = _run_plan(fs, root, target_rows_per_file=4)

        assert plan.sort_memory_budget_mb is None
        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 10
        ids = []
        for key in sorted(result.publication.published_files):
            ids.extend(_read(fs, key).column("id").to_pylist())
        assert ids == list(range(10))

    def test_budget_zero_forces_external_merge_sort(self):
        fs = MemoryFileSystem()
        root = _root()
        spill = f"{root}/_spill"
        self._write_dataset(fs, root, rows=10)
        plan, result = _run_plan(
            fs,
            root,
            target_rows_per_file=4,
            memory_budget_mb=0,
            spill_directory=spill,
        )

        assert plan.sort_memory_budget_mb == 0
        assert plan.sort_spill_directory == spill
        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 10
        ids = []
        for key in sorted(result.publication.published_files):
            ids.extend(_read(fs, key).column("id").to_pylist())
        assert ids == list(range(10))
        # Staging cleaned on success.
        assert not fs.exists(result.staging_prefix)

    def test_spill_across_multiple_source_files_preserves_global_order(self):
        fs = MemoryFileSystem()
        root = _root()
        spill = f"{root}/_spill"
        _write(fs, f"{root}/a.parquet", pa.table({"id": [9, 7, 5, 3, 1]}))
        _write(fs, f"{root}/b.parquet", pa.table({"id": [10, 8, 6, 4, 2]}))
        plan, result = _run_plan(
            fs,
            root,
            target_rows_per_file=4,
            memory_budget_mb=0,
            spill_directory=spill,
        )

        assert plan.sort_memory_budget_mb == 0
        assert result.succeeded, result.error
        ids = []
        for key in sorted(result.publication.published_files):
            ids.extend(_read(fs, key).column("id").to_pylist())
        assert ids == list(range(1, 11))

    def test_spill_equal_keys_apply_physical_tie_break(self):
        # heapq.merge's key= argument is not stable across input iterables for
        # equal keys; the merge comparator must fold the physical (run, offset)
        # index into the comparison so the ADR-0006 tie-break survives spill.
        fs = MemoryFileSystem()
        root = _root()
        spill = f"{root}/_spill"
        _write(
            fs,
            f"{root}/a.parquet",
            pa.table({"k": [1, 1, 1], "tag": ["a0", "a1", "a2"]}),
        )
        _write(
            fs,
            f"{root}/b.parquet",
            pa.table({"k": [1, 1, 1], "tag": ["b0", "b1", "b2"]}),
        )
        _, result = _run_plan(
            fs,
            root,
            sort_keys=["+k"],
            target_rows_per_file=10,
            memory_budget_mb=0,
            spill_directory=spill,
        )
        assert result.succeeded, result.error
        tags = []
        for key in sorted(result.publication.published_files):
            tags.extend(_read(fs, key).column("tag").to_pylist())
        assert tags == ["a0", "a1", "a2", "b0", "b1", "b2"]

    def test_budget_zero_without_spill_directory_rejected_at_plan_time(self):
        fs = MemoryFileSystem()
        root = _root()
        self._write_dataset(fs, root, rows=5)
        with pytest.raises(ValueError, match="spill_directory"):
            _make_plan(fs, root, memory_budget_mb=0)


# --------------------------------------------------------------------------- #
# FULL_DISTINCT_KEY_SCAN rejection at execute time (defense-in-depth)
# --------------------------------------------------------------------------- #


class TestOrderedCompactionExecuteRejection:
    def test_full_distinct_key_scan_rejected_at_execute_time(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(fs, f"{root}/a.parquet", pa.table({"id": [1]}))
        plan = _make_plan(fs, root)
        bad_plan = dataclasses.replace(
            plan, validation_level=ValidationLevel.FULL_DISTINCT_KEY_SCAN
        )
        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(ValueError, match="FULL_DISTINCT_KEY_SCAN"):
            coordinator.execute(bad_plan, filesystem=fs)
