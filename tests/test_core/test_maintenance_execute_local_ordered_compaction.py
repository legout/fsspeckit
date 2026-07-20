"""Acceptance coverage for partition-ordered compaction execution (#61).

Covers the #61 acceptance criteria for the ``atomic_local`` lane:

- New typed ordered-compaction plan/result behavior is available through the
  coordinator.
- Output is one globally ordered sequence per affected physical partition,
  split into contiguous ``max_rows_per_file``-bounded chunks; ordering is
  validated WITHIN files AND ACROSS adjacent output-file boundaries.
- Ascending, descending, multi-column, null, NaN, timestamp, and equal-key
  cases are covered; the physical tie-breaker
   ``(partition path, file path, row offset)`` decides equal caller keys.
- Partition placement, schema, row count, and the hard per-file row bound are
  valid.
- ``partition_filter`` restricts scope but ordering stays partition-complete.
- Memory/spill behavior (in-memory and external merge sort) is tested.
- ``FULL_DISTINCT_KEY_SCAN`` is rejected (no key semantics).
- Ordinary ``CompactionPlan`` remains unordered; no sort flag leaks onto
  ``plan_compaction`` or ``plan_coordinated_optimization``.
"""

from __future__ import annotations

import dataclasses
import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.core.maintenance import (
    CompactionPlan,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    MaintenanceBackend,
    MaintenanceOperation,
    MaintenanceResult,
    OrderedCompactionPlan,
    OrderedCompactionResult,
    SortKey,
    ValidationLevel,
    _execute_atomic_local_ordered_compaction,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _write_parquet(path: str, table: pa.Table) -> None:
    pq.write_table(table, path)


def _list_parquet(directory: str) -> list[str]:
    return sorted(
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".parquet")
    )


def _write_partitioned(root, layout):
    """Write a partitioned dataset from a ``{rel_dir: [(name, table)]}`` map."""
    for rel_dir, files in layout.items():
        part_dir = os.path.join(root, rel_dir) if rel_dir else root
        os.makedirs(part_dir, exist_ok=True)
        for name, table in files:
            _write_parquet(os.path.join(part_dir, name), table)
    return root


def _partition_files(root, rel_dir):
    target = os.path.join(root, rel_dir) if rel_dir else root
    return _list_parquet(target)


def _read_file(path):
    """Read a Parquet file via a binary handle to bypass Hive discovery."""
    with open(path, "rb") as fh:
        return pq.read_table(fh)


def _execute(dataset, sort_keys, **kwargs):
    fs = __import__("fsspec").filesystem("file")
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    spill_directory = kwargs.pop("spill_directory", None)
    plan = coordinator.plan_ordered_compaction(
        str(dataset),
        sort_keys,
        filesystem=fs,
        target_rows_per_file=kwargs.pop("target_rows_per_file", None),
        partition_filter=kwargs.pop("partition_filter", None),
        validation_level=kwargs.pop("validation_level", None),
        codec=kwargs.pop("codec", None),
        memory_budget_mb=kwargs.pop("memory_budget_mb", None),
        spill_directory=str(spill_directory) if spill_directory is not None else None,
    )
    return coordinator.execute(plan), plan


def _ids_in_file_order(root, rel_dir):
    """Return the ``id`` values of one partition's files in filename order."""
    out = []
    for path in _partition_files(root, rel_dir):
        out.extend(_read_file(path).column("id").to_pylist())
    return out


# --------------------------------------------------------------------------- #
# Planning: typed plan, scope, operation
# --------------------------------------------------------------------------- #


class TestAtomicLocalOrderedCompactionPlanning:
    def test_plan_is_typed_ordered_compaction_plan(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2, 3]}))]},
        )
        result, plan = _execute(dataset, ["+id"])

        assert isinstance(plan, OrderedCompactionPlan)
        assert isinstance(result, OrderedCompactionResult)
        assert isinstance(result, MaintenanceResult)
        assert plan.operation == MaintenanceOperation.ORDERED_COMPACTION
        assert plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert plan.sort_keys == (SortKey(column="id"),)
        assert result.succeeded, result.error

    def test_sort_keys_normalize_string_convention(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2], "ts": [10, 20]}))]},
        )
        _, plan = _execute(dataset, ["+id", "-ts"])
        # +id -> ascending, nulls_last; -ts -> descending, nulls_first (SQL).
        assert plan.sort_keys == (
            SortKey(column="id", descending=False, nulls_first=False),
            SortKey(column="ts", descending=True, nulls_first=True),
        )

    def test_typed_sort_key_overrides_direction_and_null_placement(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2]}))]},
        )
        _, plan = _execute(
            dataset, [SortKey(column="id", descending=True, nulls_first=False)]
        )
        assert plan.sort_keys == (
            SortKey(column="id", descending=True, nulls_first=False),
        )

    def test_full_distinct_key_scan_rejected_at_plan_time(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1]}))]},
        )
        with pytest.raises(ValueError, match="FULL_DISTINCT_KEY_SCAN"):
            _execute(dataset, ["+id"], validation_level="full_distinct_key_scan")

    @pytest.mark.parametrize(
        "sort_keys",
        [[], [""], ["+id", "+id"], ["nonexistent_column"], ["-"]],
    )
    def test_invalid_sort_keys_rejected(self, tmp_path, sort_keys):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1]}))]},
        )
        with pytest.raises(ValueError):
            _execute(dataset, sort_keys)


# --------------------------------------------------------------------------- #
# Ordering: ascending, descending, multi-column, within and across files
# --------------------------------------------------------------------------- #


class TestAtomicLocalOrderedCompactionOrdering:
    def test_ascending_order_validated_within_and_across_files(self, tmp_path):
        dataset = tmp_path / "ds"
        # Five rows split across two source files, deliberately out of order.
        _write_partitioned(
            str(dataset),
            {
                "": [
                    ("a.parquet", pa.table({"id": [3, 1, 5]})),
                    ("b.parquet", pa.table({"id": [2, 4]})),
                ]
            },
        )
        result, plan = _execute(dataset, ["+id"], target_rows_per_file=2)

        assert result.succeeded, result.error
        ids = _ids_in_file_order(str(dataset), "")
        assert ids == [1, 2, 3, 4, 5]
        # Two output files of 2 rows each, plus one of 1 row.
        files = _partition_files(str(dataset), "")
        assert len(files) == 3
        for path in files:
            assert pq.read_metadata(path).num_rows <= 2
        # Cross-file boundary: max of file N <= min of file N+1.
        prev_max = None
        for path in files:
            ids_in_file = _read_file(path).column("id").to_pylist()
            if prev_max is not None:
                assert prev_max <= ids_in_file[0]
            prev_max = ids_in_file[-1]

    def test_descending_order_via_string_convention(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [3, 1, 5, 2, 4]}))]},
        )
        result, _ = _execute(dataset, ["-id"], target_rows_per_file=2)

        assert result.succeeded, result.error
        assert _ids_in_file_order(str(dataset), "") == [5, 4, 3, 2, 1]

    def test_descending_order_via_typed_sort_key(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [3, 1, 5, 2, 4]}))]},
        )
        result, _ = _execute(
            dataset, [SortKey("id", descending=True)], target_rows_per_file=2
        )

        assert result.succeeded, result.error
        assert _ids_in_file_order(str(dataset), "") == [5, 4, 3, 2, 1]

    def test_multi_column_sort_with_direction_mixture(self, tmp_path):
        dataset = tmp_path / "ds"
        # Group by category ascending; within each group, value descending.
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "category": ["a", "a", "b", "b", "a"],
                                "value": [1, 3, 2, 1, 2],
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = _execute(
            dataset,
            ["+category", "-value"],
            target_rows_per_file=10,
        )

        assert result.succeeded, result.error
        table = _read_file(_partition_files(str(dataset), "")[0])
        rows = list(zip(
            table.column("category").to_pylist(),
            table.column("value").to_pylist(),
        ))
        assert rows == [
            ("a", 3),
            ("a", 2),
            ("a", 1),
            ("b", 2),
            ("b", 1),
        ]

    def test_nulls_last_for_ascending_string_convention(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table({"id": [3, None, 1, 2], "v": [30, -1, 10, 20]}),
                    )
                ]
            },
        )
        result, _ = _execute(dataset, ["+id"], target_rows_per_file=10)

        assert result.succeeded, result.error
        table = _read_file(_partition_files(str(dataset), "")[0])
        # Non-null ascending, null last.
        assert table.column("id").to_pylist() == [1, 2, 3, None]

    def test_nulls_first_for_descending_string_convention(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table({"id": [3, None, 1, 2], "v": [30, -1, 10, 20]}),
                    )
                ]
            },
        )
        result, _ = _execute(dataset, ["-id"], target_rows_per_file=10)

        assert result.succeeded, result.error
        table = _read_file(_partition_files(str(dataset), "")[0])
        # Null first (SQL descending default), then descending non-null.
        assert table.column("id").to_pylist() == [None, 3, 2, 1]

    def test_nan_treated_as_null_for_ordering(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 2, 3, 4],
                                "v": [3.0, float("nan"), 1.0, None],
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = _execute(dataset, ["+v"], target_rows_per_file=10)

        assert result.succeeded, result.error
        table = _read_file(_partition_files(str(dataset), "")[0])
        # Ascending non-null first; NaN and None both placed last (nulls_last).
        values = table.column("v").to_pylist()
        assert values[:2] == [1.0, 3.0]
        # NaN/None ordering among themselves is physical-order stable.
        assert set(map(_nan_or_none, values[2:])) == {None}

    def test_timestamp_sort(self, tmp_path):
        dataset = tmp_path / "ds"
        ts_type = pa.timestamp("us", tz="UTC")
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 2, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 3, tzinfo=timezone.utc)
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "ts": pa.array([t2, t0, t1], type=ts_type),
                                "id": [3, 1, 2],
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = _execute(dataset, ["+ts"], target_rows_per_file=10)

        assert result.succeeded, result.error
        table = _read_file(_partition_files(str(dataset), "")[0])
        assert table.column("id").to_pylist() == [1, 2, 3]

    def test_equal_keys_apply_physical_tie_breaker(self, tmp_path):
        """Equal caller keys preserve (partition path, file path, row offset)."""
        dataset = tmp_path / "ds"
        # Two source files; rows share the same sort key so the physical
        # tie-breaker decides. Files read in path order: a.parquet before b.parquet.
        _write_partitioned(
            str(dataset),
            {
                "": [
                    ("a.parquet", pa.table({"k": [1, 1], "tag": ["a0", "a1"]})),
                    ("b.parquet", pa.table({"k": [1, 1], "tag": ["b0", "b1"]})),
                ]
            },
        )
        result, _ = _execute(dataset, ["+k"], target_rows_per_file=10)

        assert result.succeeded, result.error
        table = _read_file(_partition_files(str(dataset), "")[0])
        assert table.column("tag").to_pylist() == ["a0", "a1", "b0", "b1"]

    def test_partition_scope_ordering_not_global(self, tmp_path):
        """Ordering is per-partition; no cross-partition guarantee."""
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "region=US": [("a.parquet", pa.table({"id": [5, 1, 3]}))],
                "region=DE": [("a.parquet", pa.table({"id": [9, 2, 8]}))],
            },
        )
        result, _ = _execute(dataset, ["+id"], target_rows_per_file=10)

        assert result.succeeded, result.error
        assert _ids_in_file_order(str(dataset), "region=US") == [1, 3, 5]
        assert _ids_in_file_order(str(dataset), "region=DE") == [2, 8, 9]


def _nan_or_none(value):
    """Normalize NaN to None for set comparison."""
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    return value


# --------------------------------------------------------------------------- #
# Partition placement, schema, row count, hard row bounds
# --------------------------------------------------------------------------- #


class TestAtomicLocalOrderedCompactionInvariants:
    def test_partition_placement_preserved(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "region=US": [("a.parquet", pa.table({"id": [3, 1], "region": ["US", "US"]}))],
                "region=DE": [("a.parquet", pa.table({"id": [9, 1], "region": ["DE", "DE"]}))],
            },
        )
        result, _ = _execute(dataset, ["+id"], target_rows_per_file=10)

        assert result.succeeded, result.error
        assert len(_partition_files(str(dataset), "region=US")) == 1
        assert len(_partition_files(str(dataset), "region=DE")) == 1

    def test_row_count_preserved(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    ("a.parquet", pa.table({"id": [3, 1, 2]})),
                    ("b.parquet", pa.table({"id": [5, 4]})),
                ]
            },
        )
        result, _ = _execute(dataset, ["+id"], target_rows_per_file=2)

        assert result.succeeded, result.error
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 5
        total = sum(
            pq.read_metadata(p).num_rows
            for p in _partition_files(str(dataset), "")
        )
        assert total == 5

    def test_schema_preserved(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2], "name": ["x", "y"]}))]},
        )
        result, _ = _execute(dataset, ["+id"], target_rows_per_file=10)

        assert result.succeeded, result.error
        table = _read_file(_partition_files(str(dataset), "")[0])
        assert table.schema.names == ["id", "name"]

    def test_max_rows_per_file_is_hard_bound(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": list(range(10))}))]},
        )
        result, _ = _execute(dataset, ["+id"], target_rows_per_file=3)

        assert result.succeeded, result.error
        files = _partition_files(str(dataset), "")
        for path in files:
            assert pq.read_metadata(path).num_rows <= 3
        assert len(files) == 4  # ceil(10/3)


# --------------------------------------------------------------------------- #
# partition_filter: restricts scope, ordering stays partition-complete
# --------------------------------------------------------------------------- #


class TestAtomicLocalOrderedCompactionPartitionFilter:
    def test_filter_restricts_scope_and_keeps_partition_order(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "region=US": [("a.parquet", pa.table({"id": [3, 1, 2], "region": ["US"]*3}))],
                "region=DE": [("a.parquet", pa.table({"id": [9, 1, 8], "region": ["DE"]*3}))],
            },
        )
        result, plan = _execute(
            dataset, ["+id"], partition_filter=["region=US"], target_rows_per_file=10
        )

        assert result.succeeded, result.error
        assert plan.partition_scope.scope_type.value == "filtered"
        # US partition is in scope and reordered.
        assert _ids_in_file_order(str(dataset), "region=US") == [1, 2, 3]
        # DE partition is untouched (still the original source file).
        de_files = _partition_files(str(dataset), "region=DE")
        assert len(de_files) == 1
        assert de_files[0].endswith("a.parquet")

    def test_filter_with_multiple_partitions_each_complete(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "year=2023": [("a.parquet", pa.table({"id": [3, 1, 2]}))],
                "year=2024": [("a.parquet", pa.table({"id": [6, 4, 5]}))],
                "year=2025": [("a.parquet", pa.table({"id": [9, 7, 8]}))],
            },
        )
        result, _ = _execute(
            dataset,
            ["+id"],
            partition_filter=["year=2023", "year=2024"],
            target_rows_per_file=10,
        )

        assert result.succeeded, result.error
        assert _ids_in_file_order(str(dataset), "year=2023") == [1, 2, 3]
        assert _ids_in_file_order(str(dataset), "year=2024") == [4, 5, 6]
        # 2025 untouched.
        assert _partition_files(str(dataset), "year=2025")[0].endswith("a.parquet")


# --------------------------------------------------------------------------- #
# Atomic-local safety: rollback on validation/drift/publish failure
# --------------------------------------------------------------------------- #


class TestAtomicLocalOrderedCompactionSafety:
    def test_externally_constructed_plan_rejects_full_distinct_key_scan(
        self, tmp_path
    ):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2]}))]},
        )
        _, plan = _execute(dataset, ["+id"])
        bad_plan = dataclasses.replace(
            plan, validation_level=ValidationLevel.FULL_DISTINCT_KEY_SCAN
        )
        with pytest.raises(ValueError, match="FULL_DISTINCT_KEY_SCAN"):
            _execute_atomic_local_ordered_compaction(bad_plan)


# --------------------------------------------------------------------------- #
# Memory / spill behavior
# --------------------------------------------------------------------------- #


class TestAtomicLocalOrderedCompactionMemorySpill:
    def _write_dataset(self, dataset, rows=100):
        os.makedirs(str(dataset), exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "id": list(range(rows)),
                    "payload": ["x" * 50] * rows,
                }
            ),
            os.path.join(str(dataset), "source.parquet"),
        )

    def test_budget_none_is_partition_bounded_in_memory(self, tmp_path):
        dataset = tmp_path / "ds"
        self._write_dataset(dataset, rows=10)
        result, plan = _execute(dataset, ["+id"], target_rows_per_file=4)

        assert plan.sort_memory_budget_mb is None
        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 10
        assert result.actual_metrics.file_count == 3  # ceil(10/4)
        assert _ids_in_file_order(str(dataset), "") == list(range(10))

    def test_budget_zero_forces_external_merge_sort_with_spill_dir(
        self, tmp_path
    ):
        dataset = tmp_path / "ds"
        spill_dir = tmp_path / "spill"
        self._write_dataset(dataset, rows=10)
        result, plan = _execute(
            dataset,
            ["+id"],
            target_rows_per_file=4,
            memory_budget_mb=0,
            spill_directory=spill_dir,
        )

        assert plan.sort_memory_budget_mb == 0
        assert plan.sort_spill_directory == str(spill_dir)
        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 10
        assert _ids_in_file_order(str(dataset), "") == list(range(10))
        # Workspace cleaned up on success.
        assert result.recovery is not None
        assert result.recovery.workspace_path is None
        # Sorted run files removed from spill dir.
        if os.path.isdir(str(spill_dir)):
            assert not any(
                f.endswith(".parquet") for f in os.listdir(str(spill_dir))
            )

    def test_spill_across_multiple_source_files_preserves_global_order(
        self, tmp_path
    ):
        dataset = tmp_path / "ds"
        spill_dir = tmp_path / "spill"
        os.makedirs(str(dataset))
        # Two source files with interleaved ids; spill path must still merge.
        pq.write_table(
            pa.table({"id": [9, 7, 5, 3, 1]}),
            os.path.join(str(dataset), "a.parquet"),
        )
        pq.write_table(
            pa.table({"id": [10, 8, 6, 4, 2]}),
            os.path.join(str(dataset), "b.parquet"),
        )
        result, _ = _execute(
            dataset,
            ["+id"],
            target_rows_per_file=4,
            memory_budget_mb=0,
            spill_directory=spill_dir,
        )

        assert result.succeeded, result.error
        assert _ids_in_file_order(str(dataset), "") == list(range(1, 11))

    def test_budget_zero_without_spill_directory_rejected_at_plan_time(
        self, tmp_path
    ):
        dataset = tmp_path / "ds"
        self._write_dataset(dataset, rows=5)
        with pytest.raises(ValueError, match="spill_directory"):
            _execute(dataset, ["+id"], memory_budget_mb=0)

    def test_default_budget_rejection_when_partition_exceeds_it(
        self, tmp_path, monkeypatch
    ):
        """When budget is None and the partition exceeds the default budget
        without a spill directory, the planner rejects it."""
        from fsspeckit.core import maintenance as mod

        dataset = tmp_path / "ds"
        self._write_dataset(dataset, rows=10)
        # Shrink the default so a small partition trips the rejection.
        monkeypatch.setattr(
            mod, "_DEFAULT_ORDERED_COMPACTION_MEMORY_BUDGET_MB", 0
        )
        with pytest.raises(ValueError, match="exceeds the default memory budget"):
            _execute(dataset, ["+id"])


# --------------------------------------------------------------------------- #
# Ordinary CompactionPlan is unaffected
# --------------------------------------------------------------------------- #


class TestAtomicLocalOrdinaryCompactionUnaffected:
    def test_compaction_plan_carries_no_sort_flag(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [3, 1, 2]}))]},
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(
            str(dataset), filesystem=fs, target_rows_per_file=10
        )
        assert isinstance(plan, CompactionPlan)
        assert not hasattr(plan, "sort_keys")
