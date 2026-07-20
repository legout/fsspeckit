"""Acceptance coverage for caller-directed schema rewrite execution (#62).

Covers the #62 acceptance criteria for the ``atomic_local`` lane:

- A typed schema-rewrite plan and typed result are available through the
  coordinator.
- The operation does not call or hide dtype inference.
- Narrow numeric, string-to-typed, timestamp unit/timezone, null-only,
  decimal, dictionary, and nested-field cases have explicit policies and
  tests.
- Invalid/lossy conversions fail before live publication.
- Local publication reuses the existing guarantee profile and recovery
  reporting.
- Metadata-preservation rules and intentionally replaced metadata are
  documented.
- Dataset-scale execution is bounded-memory (batch streaming under a
  memory budget).
"""

from __future__ import annotations

import dataclasses
import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.core.maintenance import (
    CastPolicy,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    MaintenanceBackend,
    MaintenanceOperation,
    SchemaRewritePlan,
    SchemaRewriteResult,
    ValidationLevel,
    ValidationOutcome,
    _execute_atomic_local_schema_rewrite,
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


def _write_parquet(path: str, table: pa.Table) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)


def _write_partitioned(root, layout):
    """Write a partitioned dataset from a ``{rel_dir: [(name, table)]}`` map."""
    for rel_dir, files in layout.items():
        part_dir = os.path.join(root, rel_dir) if rel_dir else root
        os.makedirs(part_dir, exist_ok=True)
        for name, table in files:
            _write_parquet(os.path.join(part_dir, name), table)
    return root


def _read_file(path):
    """Read a Parquet file via a binary handle to bypass Hive discovery."""
    with open(path, "rb") as fh:
        return pq.read_table(fh)


def _make_plan(dataset, target_schema, **kwargs):
    import fsspec

    fs = fsspec.filesystem("file")
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    return coordinator.plan_schema_rewrite(
        str(dataset),
        target_schema=target_schema,
        filesystem=fs,
        cast_policy=kwargs.pop("cast_policy", CastPolicy.SAFE),
        target_rows_per_file=kwargs.pop("target_rows_per_file", None),
        validation_level=kwargs.pop("validation_level", None),
        codec=kwargs.pop("codec", None),
        memory_budget_mb=kwargs.pop("memory_budget_mb", None),
        partition_filter=kwargs.pop("partition_filter", None),
    )


def _execute(dataset, target_schema, **kwargs):
    import fsspec

    fs = fsspec.filesystem("file")
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    plan = coordinator.plan_schema_rewrite(
        str(dataset),
        target_schema=target_schema,
        filesystem=fs,
        cast_policy=kwargs.pop("cast_policy", CastPolicy.SAFE),
        target_rows_per_file=kwargs.pop("target_rows_per_file", None),
        validation_level=kwargs.pop("validation_level", None),
        codec=kwargs.pop("codec", None),
        memory_budget_mb=kwargs.pop("memory_budget_mb", None),
        partition_filter=kwargs.pop("partition_filter", None),
    )
    return coordinator.execute(plan), plan


# --------------------------------------------------------------------------- #
# Planning: typed plan, changed fields, cast policy
# --------------------------------------------------------------------------- #


class TestAtomicLocalSchemaRewritePlanning:
    def test_plan_is_typed_schema_rewrite_plan(self, tmp_path):
        dataset = tmp_path / "ds"
        source = pa.schema([("id", pa.int64()), ("value", pa.string())])
        _write_parquet(str(dataset / "a.parquet"), pa.table({"id": [1, 2], "value": ["a", "b"]}))

        plan = _make_plan(dataset, source)

        assert isinstance(plan, SchemaRewritePlan)
        assert plan.operation == MaintenanceOperation.SCHEMA_REWRITE
        assert plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert plan.selected_backend == "pyarrow"
        assert plan.source_schema is not None
        assert plan.target_schema.equals(source)

    def test_result_is_typed_schema_rewrite_result(self, tmp_path):
        dataset = tmp_path / "ds"
        source = pa.schema([("id", pa.int64()), ("value", pa.string())])
        _write_parquet(str(dataset / "a.parquet"), pa.table({"id": [1, 2], "value": ["a", "b"]}))

        result, plan = _execute(dataset, source)

        assert isinstance(result, SchemaRewriteResult)
        from fsspeckit.core.maintenance import MaintenanceResult

        assert isinstance(result, SchemaRewriteResult)
        assert isinstance(result, MaintenanceResult)
        assert result.succeeded
        assert result.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL

    def test_changed_fields_records_only_type_changes(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int32()), "value": ["a", "b"]}),
        )
        target = pa.schema([("id", pa.int64()), ("value", pa.string())])

        plan = _make_plan(dataset, target)

        assert plan.changed_fields == ("id",)
        assert plan.source_schema.field("id").type == pa.int32()
        assert plan.target_schema.field("id").type == pa.int64()

    def test_no_changed_fields_when_schema_is_identical(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": [1, 2], "value": ["a", "b"]}),
        )
        target = pa.schema([("id", pa.int64()), ("value", pa.string())])

        plan = _make_plan(dataset, target)

        assert plan.changed_fields == ()

    def test_cast_policy_accepts_string_argument(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(str(dataset / "a.parquet"), pa.table({"id": [1, 2]}))
        target = pa.schema([("id", pa.int32())])

        plan = _make_plan(dataset, target, cast_policy="loose")

        assert plan.cast_policy == CastPolicy.LOOSE

    def test_invalid_cast_policy_raises(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(str(dataset / "a.parquet"), pa.table({"id": [1]}))

        with pytest.raises(ValueError, match="Unknown cast_policy"):
            _make_plan(dataset, pa.schema([("id", pa.int64())]), cast_policy="bogus")

    def test_strict_rejects_narrowing_at_plan_time(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        with pytest.raises(ValueError, match="STRICT.*promotion"):
            _make_plan(dataset, target, cast_policy=CastPolicy.STRICT)

    def test_strict_allows_widening(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int32())}),
        )
        target = pa.schema([("id", pa.int64())])

        plan = _make_plan(dataset, target, cast_policy=CastPolicy.STRICT)
        assert plan.changed_fields == ("id",)

    def test_target_schema_field_names_must_match_source(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(str(dataset / "a.parquet"), pa.table({"id": [1], "value": ["a"]}))

        with pytest.raises(ValueError, match="same fields"):
            _make_plan(
                dataset, pa.schema([("id", pa.int64()), ("extra", pa.string())])
            )

    def test_target_schema_must_be_pa_schema(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(str(dataset / "a.parquet"), pa.table({"id": [1]}))

        with pytest.raises(ValueError, match="pyarrow.Schema"):
            _make_plan(dataset, [("id", "int64")])  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Cast policies: narrow numeric, string-to-typed, timestamp, null, decimal,
# dictionary, nested
# --------------------------------------------------------------------------- #


class TestAtomicLocalSchemaRewriteCastPolicies:
    def test_safe_narrow_numeric_succeeds_when_values_fit(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 100, 200], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        result, plan = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        table = _read_file(_list_parquet(str(dataset))[0])
        assert table.schema.field("id").type == pa.int32()
        assert sorted(table["id"].to_pylist()) == [1, 100, 200]

    def test_safe_narrow_numeric_fails_when_value_overflows(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2**40], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        result, plan = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert not result.succeeded
        assert any(
            phase.phase == "write" and not phase.succeeded
            for phase in result.phase_outcomes
        )
        # Live dataset is untouched.
        table = _read_file(_list_parquet(str(dataset))[0])
        assert table.schema.field("id").type == pa.int64()

    def test_loose_narrow_numeric_succeeds_when_values_fit(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 100, 200], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.LOOSE)

        assert result.succeeded, result.error
        table = _read_file(_list_parquet(str(dataset))[0])
        assert table.schema.field("id").type == pa.int32()

    def test_loose_narrow_numeric_aborts_on_overflow(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2**40], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.LOOSE)

        assert not result.succeeded
        assert any(
            phase.phase == "write" and not phase.succeeded
            for phase in result.phase_outcomes
        )

    def test_string_to_typed_succeeds_with_valid_strings(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"code": pa.array(["1", "2", "3"], pa.string())}),
        )
        target = pa.schema([("code", pa.int32())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        table = _read_file(_list_parquet(str(dataset))[0])
        assert table.schema.field("code").type == pa.int32()
        assert sorted(table["code"].to_pylist()) == [1, 2, 3]

    def test_string_to_typed_fails_with_invalid_string(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"code": pa.array(["1", "abc", "3"], pa.string())}),
        )
        target = pa.schema([("code", pa.int32())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.LOOSE)

        assert not result.succeeded
        # Live dataset untouched.
        table = _read_file(_list_parquet(str(dataset))[0])
        assert table.schema.field("code").type == pa.string()

    def test_timestamp_unit_widening_strict_allows(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table(
                {
                    "ts": pa.array(
                        [1, 2],
                        type=pa.timestamp("s"),
                    )
                }
            ),
        )
        target = pa.schema([("ts", pa.timestamp("us"))])

        plan = _make_plan(dataset, target, cast_policy=CastPolicy.STRICT)
        assert plan.changed_fields == ("ts",)

    def test_timestamp_timezone_change_rejected_by_strict(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table(
                {
                    "ts": pa.array(
                        [1, 2],
                        type=pa.timestamp("us", tz="UTC"),
                    )
                }
            ),
        )
        target = pa.schema([("ts", pa.timestamp("us", tz="America/New_York"))])

        with pytest.raises(ValueError, match="STRICT.*promotion"):
            _make_plan(dataset, target, cast_policy=CastPolicy.STRICT)

    def test_timestamp_timezone_change_allowed_by_safe(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table(
                {
                    "ts": pa.array(
                        [1, 2],
                        type=pa.timestamp("us", tz="UTC"),
                    )
                }
            ),
        )
        target = pa.schema([("ts", pa.timestamp("us", tz="America/New_York"))])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)
        assert result.succeeded, result.error

    def test_null_only_column_casts_to_any_type(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"col": pa.array([None, None], pa.int64())}),
        )
        target = pa.schema([("col", pa.int32())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        table = _read_file(_list_parquet(str(dataset))[0])
        assert table.schema.field("col").type == pa.int32()
        assert table["col"].null_count == 2

    def test_dictionary_to_value_type_safe(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table(
                {
                    "cat": pa.array(
                        ["a", "b", "a"],
                        type=pa.dictionary(pa.int8(), pa.string()),
                    )
                }
            ),
        )
        target = pa.schema([("cat", pa.string())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        table = _read_file(_list_parquet(str(dataset))[0])
        assert table.schema.field("cat").type == pa.string()
        assert table["cat"].to_pylist() == ["a", "b", "a"]

    def test_list_to_large_list_strict_promotion(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"items": pa.array([[1, 2], [3]], type=pa.list_(pa.int64()))}),
        )
        target = pa.schema([("items", pa.large_list(pa.int64()))])

        plan = _make_plan(dataset, target, cast_policy=CastPolicy.STRICT)
        assert plan.changed_fields == ("items",)

    def test_decimal_precision_narrowing_loose_validates(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table(
                {
                    "amount": pa.array(
                        [1, 2, 3], type=pa.decimal128(38, 0)
                    )
                }
            ),
        )
        target = pa.schema([("amount", pa.decimal128(18, 0))])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.LOOSE)
        assert result.succeeded, result.error

    def test_float_narrowing_safe_succeeds(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"val": pa.array([1.0, 2.0, 3.0], pa.float64())}),
        )
        target = pa.schema([("val", pa.float32())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)
        assert result.succeeded, result.error
        table = _read_file(_list_parquet(str(dataset))[0])
        assert table.schema.field("val").type == pa.float32()

    def test_float_narrowing_loose_allows_precision_loss(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"val": pa.array([1.1, 2.2, 3.3], pa.float64())}),
        )
        target = pa.schema([("val", pa.float32())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.LOOSE)
        # Float precision loss is allowed under LOOSE (not overflow, not null).
        assert result.succeeded, result.error


# --------------------------------------------------------------------------- #
# Row preservation and partition layout preservation
# --------------------------------------------------------------------------- #


class TestAtomicLocalSchemaRewriteRowPreservation:
    def test_rows_preserved_and_partition_layout_unchanged(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "region=US": [
                    ("a.parquet", pa.table({"id": [1, 2], "value": ["a", "b"]})),
                ],
                "region=DE": [
                    ("b.parquet", pa.table({"id": [3, 4], "value": ["c", "d"]})),
                ],
            },
        )
        target = pa.schema([("id", pa.int32()), ("value", pa.string())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 4
        # Partition directories preserved.
        assert os.path.isdir(str(dataset / "region=US"))
        assert os.path.isdir(str(dataset / "region=DE"))
        us_files = _list_parquet(str(dataset / "region=US"))
        de_files = _list_parquet(str(dataset / "region=DE"))
        assert len(us_files) == 1
        assert len(de_files) == 1
        us_table = _read_file(us_files[0])
        assert us_table.schema.field("id").type == pa.int32()
        assert sorted(us_table["id"].to_pylist()) == [1, 2]

    def test_max_rows_per_file_is_hard_bound(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array(list(range(10)), pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        result, _ = _execute(
            dataset, target, cast_policy=CastPolicy.SAFE, target_rows_per_file=3
        )

        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 10
        files = _list_parquet(str(dataset))
        assert len(files) == 4  # ceil(10/3) = 4
        for f in files:
            assert pq.read_metadata(f).num_rows <= 3


# --------------------------------------------------------------------------- #
# Atomic-local safety: rollback on validation, drift, and publish failure
# --------------------------------------------------------------------------- #


class TestAtomicLocalSchemaRewriteSafety:
    def test_lossy_cast_aborts_before_live_publication(self, tmp_path):
        dataset = tmp_path / "ds"
        original_path = str(dataset / "a.parquet")
        _write_parquet(
            original_path,
            pa.table({"id": pa.array([1, 2**40], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.LOOSE)

        assert not result.succeeded
        # Live dataset untouched — still has int64.
        table = _read_file(original_path)
        assert table.schema.field("id").type == pa.int64()
        assert table.num_rows == 2

    def test_source_drift_aborts_before_publication(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int32())}),
        )
        import fsspec

        fs = fsspec.filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_schema_rewrite(
            str(dataset),
            target_schema=pa.schema([("id", pa.int64())]),
            cast_policy=CastPolicy.SAFE,
            filesystem=fs,
        )
        # Mutate source after planning.
        pq.write_table(
            pa.table({"id": pa.array([1, 2, 3], pa.int32())}),
            str(dataset / "a.parquet"),
        )
        result = coordinator.execute(plan)

        assert not result.succeeded
        assert any(
            phase.phase == "drift_check" and not phase.succeeded
            for phase in result.phase_outcomes
        )

    def test_workspace_cleaned_up_on_success(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int32())}),
        )
        target = pa.schema([("id", pa.int64())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        assert result.recovery is not None
        assert result.recovery.workspace_path is None

    def test_cleanup_failure_reports_retained_backups(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int32())}),
        )
        target = pa.schema([("id", pa.int64())])

        real_rmtree = shutil.rmtree

        def fail_cleanup(_path):
            raise OSError("injected cleanup failure")

        monkeypatch.setattr(shutil, "rmtree", fail_cleanup)
        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded
        assert result.recovery is not None
        assert result.recovery.workspace_path is not None
        assert result.recovery.backup_paths
        assert all(os.path.exists(path) for path in result.recovery.backup_paths)
        monkeypatch.setattr(shutil, "rmtree", real_rmtree)
        real_rmtree(result.recovery.workspace_path)


# --------------------------------------------------------------------------- #
# Memory / batch-streaming behavior
# --------------------------------------------------------------------------- #


class TestAtomicLocalSchemaRewriteMemoryBudget:
    def _write_large_dataset(self, dataset, rows=500):
        os.makedirs(str(dataset), exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "id": pa.array(list(range(rows)), pa.int64()),
                    "payload": ["x" * 100] * rows,
                }
            ),
            os.path.join(str(dataset), "source.parquet"),
        )

    def test_budget_none_is_row_batch_bounded_and_preserves_rows(self, tmp_path):
        dataset = tmp_path / "ds"
        self._write_large_dataset(dataset, rows=100)
        target = pa.schema([("id", pa.int32()), ("payload", pa.string())])

        result, plan = _execute(
            dataset, target, cast_policy=CastPolicy.SAFE, target_rows_per_file=25
        )

        assert plan.schema_rewrite_memory_budget_mb is None
        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 100
        assert result.actual_metrics.file_count == 4

    def test_small_budget_preserves_rows_with_batch_streaming(self, tmp_path):
        dataset = tmp_path / "ds"
        self._write_large_dataset(dataset, rows=200)
        target = pa.schema([("id", pa.int32()), ("payload", pa.string())])

        result, plan = _execute(
            dataset,
            target,
            cast_policy=CastPolicy.SAFE,
            target_rows_per_file=50,
            memory_budget_mb=1,
        )

        assert plan.schema_rewrite_memory_budget_mb == 1
        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 200
        assert result.actual_metrics.file_count == 4
        # Workspace cleaned up on success.
        assert result.recovery is not None
        assert result.recovery.workspace_path is None

    def test_budget_streaming_across_multiple_partitions(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "region=US": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": pa.array(list(range(100)), pa.int64()),
                                "payload": ["x"] * 100,
                            }
                        ),
                    )
                ],
                "region=DE": [
                    (
                        "b.parquet",
                        pa.table(
                            {
                                "id": pa.array(list(range(100, 200)), pa.int64()),
                                "payload": ["y"] * 100,
                            }
                        ),
                    )
                ],
            },
        )
        target = pa.schema([("id", pa.int32()), ("payload", pa.string())])

        result, plan = _execute(
            dataset,
            target,
            cast_policy=CastPolicy.SAFE,
            target_rows_per_file=25,
            memory_budget_mb=1,
        )

        assert plan.schema_rewrite_memory_budget_mb == 1
        assert result.succeeded, result.error
        assert result.actual_metrics.row_count == 200


# --------------------------------------------------------------------------- #
# No dtype inference — the publication protocol never calls opt_dtype
# --------------------------------------------------------------------------- #


class TestAtomicLocalSchemaRewriteNoDtypeInference:
    def test_opt_dtype_not_called_during_plan_or_execute(self, tmp_path, monkeypatch):
        import fsspeckit.datasets.polars as polars_mod
        import fsspeckit.datasets.schema as schema_mod

        called = {"count": 0}

        def _spy(*args, **kwargs):
            called["count"] += 1
            raise AssertionError("opt_dtype must not be called by schema rewrite")

        # Patch every opt_dtype function we can find.
        for mod in (polars_mod, schema_mod):
            if hasattr(mod, "opt_dtype"):
                monkeypatch.setattr(mod, "opt_dtype", _spy)

        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int64()), "value": ["a", "b"]}),
        )
        target = pa.schema([("id", pa.int32()), ("value", pa.string())])

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        assert called["count"] == 0


# --------------------------------------------------------------------------- #
# Metadata preservation
# --------------------------------------------------------------------------- #


class TestAtomicLocalSchemaRewriteMetadata:
    def test_target_schema_metadata_is_written_to_output(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int64())}),
        )
        target = pa.schema(
            [("id", pa.int32())],
            metadata={b"schema_key": b"schema_value"},
        )

        result, _ = _execute(dataset, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        table = _read_file(_list_parquet(str(dataset))[0])
        # The target schema's metadata replaces the source metadata — the
        # caller controls the output schema including its metadata.
        md = table.schema.metadata or {}
        assert md.get(b"schema_key") == b"schema_value"


# --------------------------------------------------------------------------- #
# Defense-in-depth: externally constructed plan with wrong operation
# --------------------------------------------------------------------------- #


class TestAtomicLocalSchemaRewriteExecuteRejection:
    def test_full_distinct_key_scan_accepted_for_schema_rewrite(self, tmp_path):
        """Unlike repartition, FULL_DISTINCT_KEY_SCAN is valid for schema rewrite."""
        dataset = tmp_path / "ds"
        _write_parquet(
            str(dataset / "a.parquet"),
            pa.table({"id": pa.array([1, 2], pa.int64())}),
        )

        plan = _make_plan(
            dataset,
            pa.schema([("id", pa.int32())]),
            cast_policy=CastPolicy.SAFE,
            validation_level="full_distinct_key_scan",
        )
        assert plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN
