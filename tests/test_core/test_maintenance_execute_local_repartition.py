"""Acceptance coverage for pure full-dataset repartition execution (#60).

Covers the #60 acceptance criteria for the ``atomic_local`` lane:

- New typed repartition plan/result behavior is available through the coordinator.
- Local/POSIX publication is atomic for cooperating access and rollback-tested.
- Exact duplicates and total row count are preserved.
- Integer, string, null, NaN, escaped, and derived partition values are covered.
- Existing unrelated partitions/source files are replaced exactly as the
  full-dataset plan specifies.
- Hive reads expose destination partition columns exactly once.
- Memory/spill behavior is tested.
- ``partition_filter`` is rejected (full-dataset scope) and
  ``FULL_DISTINCT_KEY_SCAN`` is rejected (no key semantics).
"""

from __future__ import annotations

import dataclasses
import os

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest

from fsspeckit.core.maintenance import (
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    MaintenanceBackend,
    MaintenanceOperation,
    PartitionScopeType,
    RepartitionPlan,
    RepartitionResult,
    ValidationLevel,
    ValidationOutcome,
    _execute_atomic_local_repartition,
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


def _execute(dataset, **kwargs):
    fs = __import__("fsspec").filesystem("file")
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    plan = coordinator.plan_repartition(
        str(dataset),
        kwargs.pop("partition_columns", ["region"]),
        filesystem=fs,
        target_rows_per_file=kwargs.pop("target_rows_per_file", None),
        validation_level=kwargs.pop("validation_level", None),
        codec=kwargs.pop("codec", None),
        derived_partition_columns=kwargs.pop("derived_partition_columns", None),
        partition_timezone=kwargs.pop("partition_timezone", "UTC"),
        memory_budget_mb=kwargs.pop("memory_budget_mb", None),
    )
    return coordinator.execute(plan), plan


# --------------------------------------------------------------------------- #
# Planning: typed plan, scope, operation
# --------------------------------------------------------------------------- #


class TestAtomicLocalRepartitionPlanning:
    def test_plan_is_typed_repartition_plan(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1], "region": ["US"]}))]},
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_repartition(str(dataset), ["region"], filesystem=fs)

        assert isinstance(plan, RepartitionPlan)
        assert plan.operation == MaintenanceOperation.REPARTITION
        assert plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert plan.partition_scope.scope_type == PartitionScopeType.REPARTITION
        assert plan.partition_scope.partition_columns == ("region",)
        assert plan.partition_columns == ("region",)
        assert plan.repartition_memory_budget_mb is None
        assert len(plan.repartition_groups) == 1

    def test_memory_budget_recorded_on_plan(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1], "region": ["US"]}))]},
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_repartition(
            str(dataset), ["region"], filesystem=fs, memory_budget_mb=64
        )
        assert plan.repartition_memory_budget_mb == 64

    @pytest.mark.parametrize("partition_columns", [[], ["missing"], ["r", "r"], [""]])
    def test_destination_partition_columns_are_required_and_declared(
        self, partition_columns, tmp_path
    ):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1], "region": ["US"]}))]},
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        with pytest.raises(ValueError):
            coordinator.plan_repartition(str(dataset), partition_columns, filesystem=fs)

    def test_full_distinct_key_scan_rejected_at_plan_time(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1], "region": ["US"]}))]},
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        with pytest.raises(ValueError, match="FULL_DISTINCT_KEY_SCAN"):
            coordinator.plan_repartition(
                str(dataset),
                ["region"],
                filesystem=fs,
                validation_level="full_distinct_key_scan",
            )

    def test_full_distinct_key_scan_rejected_at_execute_time(self, tmp_path):
        """Externally constructed plans cannot bypass the invariant."""
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1], "region": ["US"]}))]},
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_repartition(str(dataset), ["region"], filesystem=fs)
        bad_plan = dataclasses.replace(
            plan, validation_level=ValidationLevel.FULL_DISTINCT_KEY_SCAN
        )
        with pytest.raises(ValueError, match="FULL_DISTINCT_KEY_SCAN"):
            _execute_atomic_local_repartition(bad_plan)


# --------------------------------------------------------------------------- #
# Row preservation and cross-partition merge
# --------------------------------------------------------------------------- #


class TestAtomicLocalRepartitionRowPreservation:
    def test_exact_duplicates_preserved_and_rows_follow_declared_partitions(
        self, tmp_path
    ):
        """Every source row, including exact duplicates, appears exactly once."""
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "source=A": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 1, 2],
                                "region": ["US", "DE", "US"],
                                "value": ["a-first", "a-second", "a-third"],
                            }
                        ),
                    )
                ],
                "source=B": [
                    (
                        "b.parquet",
                        pa.table(
                            {
                                "id": [1, 3],
                                "region": ["US", "CA"],
                                "value": ["b-dup-of-1", "b-third"],
                            }
                        ),
                    )
                ],
            },
        )
        result, plan = _execute(dataset)

        assert plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert isinstance(result, RepartitionResult)
        assert result.succeeded, result.error
        assert result.validation is not None and result.validation.succeeded

        # Source partitions fully replaced.
        assert _partition_files(str(dataset), "source=A") == []
        assert _partition_files(str(dataset), "source=B") == []
        # Every source row appears in exactly one destination partition.
        us = _partition_files(str(dataset), "region=US")
        de = _partition_files(str(dataset), "region=DE")
        ca = _partition_files(str(dataset), "region=CA")
        assert len(us) == 1
        assert len(de) == 1
        assert len(ca) == 1
        us_rows = self._read_file(us[0]).to_pydict()
        de_rows = self._read_file(de[0]).to_pydict()
        ca_rows = self._read_file(ca[0]).to_pydict()
        assert sorted(us_rows["value"]) == ["a-first", "a-third", "b-dup-of-1"]
        assert de_rows["value"] == ["a-second"]
        assert ca_rows["value"] == ["b-third"]
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 5
        assert result.actual_metrics.file_count == 3

    @staticmethod
    def _read_file(path):
        with open(path, "rb") as fh:
            return pq.read_table(fh)

    def test_total_row_count_invariant_holds(self, tmp_path):
        """Every source row appears exactly once in output (no loss, no gain)."""
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 2, 3, 1, 2, 1],
                                "region": ["US", "DE", "US", "US", "DE", "CA"],
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = _execute(dataset)

        assert result.succeeded, result.error
        assert result.actual_metrics is not None
        # 6 source rows including duplicates; all preserved.
        assert result.actual_metrics.row_count == 6

    def test_existing_unrelated_partitions_replaced_by_full_dataset_plan(
        self, tmp_path
    ):
        """Unrelated source partitions are replaced exactly as the plan specifies."""
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "legacy=old": [
                    (
                        "old.parquet",
                        pa.table({"id": [1, 2], "region": ["US", "DE"]}),
                    )
                ],
                "legacy=other": [
                    (
                        "other.parquet",
                        pa.table({"id": [3, 4], "region": ["US", "CA"]}),
                    )
                ],
            },
        )
        result, _ = _execute(dataset)

        assert result.succeeded, result.error
        assert _partition_files(str(dataset), "legacy=old") == []
        assert _partition_files(str(dataset), "legacy=other") == []
        assert _partition_files(str(dataset), "region=US")
        assert _partition_files(str(dataset), "region=DE")
        assert _partition_files(str(dataset), "region=CA")


# --------------------------------------------------------------------------- #
# Partition value coverage: integer, string, null, NaN, escaped, derived
# --------------------------------------------------------------------------- #


class TestAtomicLocalRepartitionPartitionValues:
    def test_integer_partition_columns_are_path_only_and_hive_readable(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "source.parquet",
                        pa.table(
                            {
                                "id": [1, 2, 3],
                                "year": pa.array([2023, 2024, 2024], type=pa.int64()),
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = _execute(dataset, partition_columns=["year"])

        assert result.succeeded, result.error
        output_files = _partition_files(str(dataset), "year=2023")
        output_files += _partition_files(str(dataset), "year=2024")
        assert output_files
        assert all("year" not in pq.read_schema(path).names for path in output_files)

        hive_table = pds.dataset(
            str(dataset), format="parquet", partitioning="hive"
        ).to_table()
        assert hive_table.column_names.count("year") == 1
        assert sorted(hive_table["year"].to_pylist()) == [2023, 2024, 2024]

    def test_string_partition_values_follow_declared_partitions(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table({"id": [1, 2], "region": ["US/CA", "DE"]}),
                    )
                ]
            },
        )
        result, _ = _execute(dataset)

        assert result.succeeded, result.error
        # Forward-slash in a string value is URL-escaped in the hive path.
        assert _partition_files(str(dataset), "region=US%2FCA")
        assert _partition_files(str(dataset), "region=DE")

    def test_null_partition_value_uses_hive_default_partition(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table({"id": [1, 2], "region": ["US", None]}),
                    )
                ]
            },
        )
        result, _ = _execute(dataset)

        assert result.succeeded, result.error
        assert _partition_files(str(dataset), "region=US")
        assert _partition_files(str(dataset), "region=__HIVE_DEFAULT_PARTITION__")

    def test_nan_partition_value_uses_hive_nan_partition(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 2],
                                "region": pa.array(
                                    [float("nan"), 1.0], type=pa.float64()
                                ),
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = _execute(dataset)

        assert result.succeeded, result.error
        assert _partition_files(str(dataset), "region=__HIVE_NAN_PARTITION__")
        assert _partition_files(str(dataset), "region=1.0")

    def test_reserved_partition_values_are_escaped(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 2, 3],
                                "region": [
                                    "__HIVE_DEFAULT_PARTITION__",
                                    "nan",
                                    "plain",
                                ],
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = _execute(dataset)

        assert result.succeeded, result.error
        assert _partition_files(
            str(dataset), "region=__fsspeckit_value____HIVE_DEFAULT_PARTITION__"
        )
        assert _partition_files(str(dataset), "region=__fsspeckit_value__nan")
        assert _partition_files(str(dataset), "region=plain")

    def test_timestamp_derived_partitions_use_explicit_timezone(self, tmp_path):
        from datetime import datetime, timedelta, timezone

        dataset = tmp_path / "ds"
        source_timezone = timezone(timedelta(hours=1))
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "source.parquet",
                        pa.table(
                            {
                                "id": [1, 2],
                                "event_ts": pa.array(
                                    [
                                        datetime(
                                            2024, 1, 1, 0, 30, tzinfo=source_timezone
                                        ),
                                        datetime(
                                            2024, 2, 1, 0, 30, tzinfo=source_timezone
                                        ),
                                    ],
                                    type=pa.timestamp("us", tz="+01:00"),
                                ),
                            }
                        ),
                    )
                ]
            },
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_repartition(
            str(dataset),
            ["year", "year_month"],
            filesystem=fs,
            derived_partition_columns={
                "year": ("year", "event_ts"),
                "year_month": ("strftime", "event_ts", "%Y-%m"),
            },
            partition_timezone="UTC",
        )
        result = coordinator.execute(plan)

        assert result.succeeded, result.error
        assert plan.derived_partition_keys[0].timezone == "UTC"
        output_files = sorted(str(path) for path in dataset.rglob("*.parquet"))
        assert any("/year=2023/year_month=2023-12/" in path for path in output_files)
        assert all(
            "year" not in pq.read_schema(path).names
            and "year_month" not in pq.read_schema(path).names
            for path in output_files
        )

        hive_table = pds.dataset(
            str(dataset), format="parquet", partitioning="hive"
        ).to_table()
        assert {"year", "year_month"}.issubset(hive_table.column_names)
        assert sorted(hive_table["year_month"].to_pylist()) == ["2023-12", "2024-01"]


# --------------------------------------------------------------------------- #
# max_rows_per_file hard bound
# --------------------------------------------------------------------------- #


class TestAtomicLocalRepartitionMaxRowsPerFile:
    def test_max_rows_per_file_is_hard_bound(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table({"id": list(range(5)), "region": ["X"] * 5}),
                    )
                ]
            },
        )
        result, _ = _execute(dataset, target_rows_per_file=2)

        assert result.succeeded, result.error
        files = _partition_files(str(dataset), "region=X")
        assert len(files) == 3
        assert all(pq.read_metadata(f).num_rows <= 2 for f in files)
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 5


# --------------------------------------------------------------------------- #
# Hive reads expose partition columns exactly once
# --------------------------------------------------------------------------- #


class TestAtomicLocalRepartitionHiveReads:
    def test_hive_reads_expose_destination_columns_exactly_once(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 2, 3],
                                "region": ["US", "DE", "US"],
                                "extra": ["a", "b", "c"],
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = _execute(dataset)

        assert result.succeeded, result.error
        hive_table = pds.dataset(
            str(dataset), format="parquet", partitioning="hive"
        ).to_table()
        assert hive_table.column_names.count("region") == 1
        assert hive_table.column_names.count("id") == 1
        assert hive_table.column_names.count("extra") == 1
        assert sorted(hive_table["region"].to_pylist()) == ["DE", "US", "US"]
        assert sorted(hive_table["id"].to_pylist()) == [1, 2, 3]


# --------------------------------------------------------------------------- #
# Atomic-local safety: rollback on validation, drift, and publish failure
# --------------------------------------------------------------------------- #


class TestAtomicLocalRepartitionSafety:
    def test_validation_failure_keeps_live_sources(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        original_a = str(dataset / "source=A" / "a.parquet")
        _write_partitioned(
            str(dataset),
            {
                "source=A": [
                    ("a.parquet", pa.table({"id": [1, 2], "region": ["US", "US"]})),
                ]
            },
        )
        import fsspeckit.core.maintenance as maintenance

        monkeypatch.setattr(
            maintenance,
            "_validate_staged_output",
            lambda *_args: ValidationOutcome(
                succeeded=False,
                expected_row_count=1,
                error="injected validation failure",
            ),
        )
        result, _ = _execute(dataset)

        assert not result.succeeded
        assert result.validation is not None and not result.validation.succeeded
        assert self._read_file(original_a).num_rows == 2
        assert not os.path.exists(os.path.join(str(dataset), "region=US"))

    @staticmethod
    def _read_file(path):
        with open(path, "rb") as fh:
            return pq.read_table(fh)

    def test_source_drift_aborts_before_publication(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2], "region": ["US", "DE"]}))]},
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_repartition(str(dataset), ["region"], filesystem=fs)
        source = plan.source_snapshot.files[0].absolute_path
        pq.write_table(
            pa.table({"id": [1, 2, 3], "region": ["US", "DE", "CA"]}),
            source,
        )
        result = coordinator.execute(plan)

        assert not result.succeeded
        assert any(
            phase.phase == "drift_check" and not phase.succeeded
            for phase in result.phase_outcomes
        )
        assert _list_parquet(str(dataset)) == [source]
        assert not os.path.exists(os.path.join(str(dataset), "region=US"))

    def test_publish_failure_rolls_back_source_files(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table({"id": [1, 2, 3], "region": ["US", "DE", "US"]}),
                    )
                ]
            },
        )
        original_file = _list_parquet(str(dataset))[0]
        real_rename = os.rename
        us_prefix = os.path.join(str(dataset), "region=US")
        injected = {"done": False}

        def fail_on_us_publish(src, dst):
            if not injected["done"] and str(dst).startswith(us_prefix):
                injected["done"] = True
                raise OSError("injected repartition publish failure")
            real_rename(src, dst)

        monkeypatch.setattr(os, "rename", fail_on_us_publish)
        result, _ = _execute(dataset)

        assert not result.succeeded
        assert result.publication is not None and not result.publication.succeeded
        assert _list_parquet(str(dataset)) == [original_file]
        assert _partition_files(str(dataset), "region=US") == []
        assert result.recovery is not None and result.recovery.recovered

    def test_workspace_cleaned_up_on_success(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2], "region": ["US", "DE"]}))]},
        )
        result, _ = _execute(dataset)

        assert result.succeeded, result.error
        assert result.recovery is not None and result.recovery.workspace_path is None

    def test_cleanup_failure_reports_retained_backups(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2], "region": ["US", "DE"]}))]},
        )
        import shutil

        real_rmtree = shutil.rmtree

        def fail_cleanup(_path):
            raise OSError("injected cleanup failure")

        monkeypatch.setattr(shutil, "rmtree", fail_cleanup)
        result, _ = _execute(dataset)

        assert result.succeeded
        assert result.recovery is not None
        assert result.recovery.workspace_path is not None
        assert result.recovery.backup_paths
        assert all(os.path.exists(path) for path in result.recovery.backup_paths)
        assert any(
            phase.phase == "cleanup" and not phase.succeeded
            for phase in result.phase_outcomes
        )
        monkeypatch.setattr(shutil, "rmtree", real_rmtree)
        real_rmtree(result.recovery.workspace_path)


# --------------------------------------------------------------------------- #
# Memory / spill behavior
# --------------------------------------------------------------------------- #


class TestAtomicLocalRepartitionMemorySpill:
    def _write_large_dataset(self, dataset, rows=500):
        os.makedirs(str(dataset), exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "id": list(range(rows)),
                    "region": ["US"] * rows,
                    "payload": ["x" * 100] * rows,
                }
            ),
            os.path.join(str(dataset), "source.parquet"),
        )

    def test_budget_none_is_group_bounded_and_preserves_rows(self, tmp_path):
        dataset = tmp_path / "ds"
        self._write_large_dataset(dataset, rows=100)
        result, plan = _execute(dataset, target_rows_per_file=25)

        assert plan.repartition_memory_budget_mb is None
        assert result.succeeded, result.error
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 100
        assert result.actual_metrics.file_count == 4

    def test_budget_zero_forces_spill_and_preserves_rows(self, tmp_path):
        """A zero-MB budget forces every bucket to spill; rows are preserved."""
        dataset = tmp_path / "ds"
        self._write_large_dataset(dataset, rows=100)
        result, plan = _execute(dataset, target_rows_per_file=25, memory_budget_mb=0)

        assert plan.repartition_memory_budget_mb == 0
        assert result.succeeded, result.error
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 100
        assert result.actual_metrics.file_count == 4
        # Workspace (including spill dir) cleaned up on success.
        assert result.recovery is not None
        assert result.recovery.workspace_path is None

    def test_spill_across_multiple_partitions_preserves_rows(self, tmp_path):
        dataset = tmp_path / "ds"
        os.makedirs(str(dataset))
        rows = 200
        pq.write_table(
            pa.table(
                {
                    "id": list(range(rows)),
                    "region": ["US"] * (rows // 2) + ["DE"] * (rows // 2),
                    "payload": ["x" * 100] * rows,
                }
            ),
            os.path.join(str(dataset), "source.parquet"),
        )
        result, plan = _execute(dataset, target_rows_per_file=25, memory_budget_mb=0)

        assert plan.repartition_memory_budget_mb == 0
        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == rows
        # Two destination partitions, each split into rows/2/25 = 4 files.
        assert len(_partition_files(str(dataset), "region=US")) == 4
        assert len(_partition_files(str(dataset), "region=DE")) == 4
