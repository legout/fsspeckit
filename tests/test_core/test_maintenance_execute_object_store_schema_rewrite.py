"""Focused generic fsspec tests for caller-directed schema rewrite (#62).

The memory filesystem exercises the fsspec object-store seam only; these tests
make no claim about real S3-compatible publication behavior. They mirror the
existing ``test_maintenance_execute_object_store_repartition.py`` but replace
every repartition assertion with schema-rewrite semantics: the operation casts
each source file's columns to the caller-supplied target schema and preserves
the partition layout and row multiplicity.
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
    BestEffortSchemaRewriteResult,
    CastPolicy,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    MaintenanceResult,
    SchemaRewritePlan,
    SchemaRewriteResult,
    ValidationLevel,
)


def _root() -> str:
    return f"/schema-rewrite-{uuid.uuid4().hex[:8]}"


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
    target_schema: pa.Schema,
    *,
    cast_policy: CastPolicy | str = CastPolicy.SAFE,
    target_rows_per_file: int | None = None,
    memory_budget_mb: int | None = None,
    partition_filter: list[str] | None = None,
):
    coordinator = DatasetMaintenanceCoordinator("pyarrow")
    return coordinator.plan_schema_rewrite(
        root,
        target_schema=target_schema,
        cast_policy=cast_policy,
        filesystem=fs,
        target_rows_per_file=target_rows_per_file,
        memory_budget_mb=memory_budget_mb,
        partition_filter=partition_filter,
    )


def _run_plan(
    fs: MemoryFileSystem,
    root: str,
    target_schema: pa.Schema,
    **kwargs,
):
    coordinator = DatasetMaintenanceCoordinator("pyarrow")
    plan = coordinator.plan_schema_rewrite(
        root,
        target_schema=target_schema,
        filesystem=fs,
        **kwargs,
    )
    return plan, coordinator.execute(plan, filesystem=fs)


def _sources(fs: MemoryFileSystem, root: str) -> tuple[str, str]:
    """Two source files with distinct schemas (same column names)."""
    first = f"{root}/source-a.parquet"
    second = f"{root}/region=US/source-b.parquet"
    _write(
        fs,
        first,
        pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "value": ["a", "b"],
            }
        ),
    )
    _write(
        fs,
        second,
        pa.table(
            {
                "id": pa.array([3, 4], pa.int64()),
                "value": ["c", "d"],
            }
        ),
    )
    return first, second


# --------------------------------------------------------------------------- #
# Row preservation, typed result, and partition layout
# --------------------------------------------------------------------------- #


class TestSchemaRewriteSuccess:
    def test_casts_all_files_and_preserves_partition_layout(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _sources(fs, root)
        target = pa.schema([("id", pa.int32()), ("value", pa.string())])

        plan, result = _run_plan(fs, root, target, cast_policy=CastPolicy.SAFE)

        assert isinstance(plan, SchemaRewritePlan)
        assert isinstance(result, BestEffortSchemaRewriteResult)
        assert isinstance(result, SchemaRewriteResult)
        assert isinstance(result, MaintenanceResult)
        assert result.succeeded
        assert result.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
        assert result.concurrency_disclaimer == BEST_EFFORT_CONCURRENCY_DISCLAIMER

        assert result.publication is not None
        live_keys = result.publication.published_files
        assert live_keys
        # Partition layout preserved.
        assert any("region=US" in path for path in live_keys)
        # All output files have the target schema.
        tables = [_read(fs, path) for path in live_keys]
        for table in tables:
            assert table.schema.field("id").type == pa.int32()
            assert table.schema.field("value").type == pa.string()
        output = pa.concat_tables(tables)
        assert output.num_rows == 4
        assert sorted(output["id"].to_pylist()) == [1, 2, 3, 4]

        # Sources removed.
        assert all(not fs.exists(source) for source in sources)
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 4
        assert result.recovery is None
        assert not fs.exists(result.staging_prefix)

    def test_max_rows_per_file_is_hard_bound(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": pa.array(list(range(10)), pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        _, result = _run_plan(
            fs, root, target, cast_policy=CastPolicy.SAFE, target_rows_per_file=3
        )

        assert result.succeeded
        assert result.publication is not None
        assert len(result.publication.published_files) == 4
        for path in result.publication.published_files:
            with fs.open(path, "rb") as handle:
                assert pq.read_metadata(handle).num_rows <= 3


# --------------------------------------------------------------------------- #
# Cast policy coverage
# --------------------------------------------------------------------------- #


class TestSchemaRewriteCastPolicies:
    def test_safe_narrow_numeric_succeeds_when_values_fit(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": pa.array([1, 100, 200], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        _, result = _run_plan(fs, root, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        table = _read(fs, result.publication.published_files[0])
        assert table.schema.field("id").type == pa.int32()

    def test_safe_narrow_numeric_fails_on_overflow(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": pa.array([1, 2**40], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        _, result = _run_plan(fs, root, target, cast_policy=CastPolicy.SAFE)

        assert not result.succeeded
        # Source untouched.
        table = _read(fs, f"{root}/source.parquet")
        assert table.schema.field("id").type == pa.int64()

    def test_loose_narrow_numeric_aborts_on_overflow(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": pa.array([1, 2**40], pa.int64())}),
        )
        target = pa.schema([("id", pa.int32())])

        _, result = _run_plan(fs, root, target, cast_policy=CastPolicy.LOOSE)

        assert not result.succeeded
        assert any(
            phase.phase == "stage" and not phase.succeeded
            for phase in result.phase_outcomes
        )

    def test_string_to_typed_fails_with_invalid_string(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"code": pa.array(["1", "abc", "3"], pa.string())}),
        )
        target = pa.schema([("code", pa.int32())])

        _, result = _run_plan(fs, root, target, cast_policy=CastPolicy.LOOSE)

        assert not result.succeeded

    def test_null_only_column_casts_to_any_type(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"col": pa.array([None, None], pa.int64())}),
        )
        target = pa.schema([("col", pa.int32())])

        _, result = _run_plan(fs, root, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        table = _read(fs, result.publication.published_files[0])
        assert table.schema.field("col").type == pa.int32()
        assert table["col"].null_count == 2

    def test_timestamp_timezone_change_allowed_by_safe(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
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

        _, result = _run_plan(fs, root, target, cast_policy=CastPolicy.SAFE)
        assert result.succeeded, result.error


# --------------------------------------------------------------------------- #
# Planning requirements
# --------------------------------------------------------------------------- #


class TestSchemaRewritePlanningRequirements:
    def test_strict_rejects_narrowing_at_plan_time(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": pa.array([1, 2], pa.int64())}),
        )

        with pytest.raises(ValueError, match="STRICT.*promotion"):
            _make_plan(
                fs,
                root,
                pa.schema([("id", pa.int32())]),
                cast_policy=CastPolicy.STRICT,
            )

    def test_memory_budget_recorded_on_plan(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": [1]}),
        )
        plan = _make_plan(
            fs,
            root,
            pa.schema([("id", pa.int64())]),
            memory_budget_mb=32,
        )
        assert plan.schema_rewrite_memory_budget_mb == 32

    def test_changed_fields_recorded(self):
        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": pa.array([1], pa.int32()), "value": ["a"]}),
        )
        plan = _make_plan(
            fs,
            root,
            pa.schema([("id", pa.int64()), ("value", pa.string())]),
        )
        assert plan.changed_fields == ("id",)


# --------------------------------------------------------------------------- #
# Safety: drift, copy failure, delete failure
# --------------------------------------------------------------------------- #


class TestSchemaRewriteSafety:
    def test_source_drift_prevents_deletion_and_preserves_recovery(self):
        fs = MemoryFileSystem()
        root = _root()
        sources = _sources(fs, root)
        plan = _make_plan(
            fs,
            root,
            pa.schema([("id", pa.int32()), ("value", pa.string())]),
        )
        # Mutate source after planning.
        _write(
            fs,
            sources[1],
            pa.table(
                {
                    "id": pa.array([3, 4, 5], pa.int64()),
                    "value": ["c", "d", "e"],
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
        plan = _make_plan(
            fs,
            root,
            pa.schema([("id", pa.int32()), ("value", pa.string())]),
        )
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


# --------------------------------------------------------------------------- #
# Memory / batch-streaming behavior
# --------------------------------------------------------------------------- #


class TestSchemaRewriteMemoryBudget:
    def test_budget_none_preserves_rows(self):
        fs = MemoryFileSystem()
        root = _root()
        rows = 100
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": pa.array(list(range(rows)), pa.int64()),
                    "payload": ["x" * 50] * rows,
                }
            ),
        )
        target = pa.schema([("id", pa.int32()), ("payload", pa.string())])

        plan, result = _run_plan(
            fs,
            root,
            target,
            cast_policy=CastPolicy.SAFE,
            target_rows_per_file=25,
            memory_budget_mb=None,
        )

        assert plan.schema_rewrite_memory_budget_mb is None
        assert result.succeeded
        assert result.actual_metrics.row_count == rows
        assert result.actual_metrics.file_count == 4

    def test_small_budget_preserves_rows_with_batch_streaming(self):
        fs = MemoryFileSystem()
        root = _root()
        rows = 200
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table(
                {
                    "id": pa.array(list(range(rows)), pa.int64()),
                    "payload": ["x" * 50] * rows,
                }
            ),
        )
        target = pa.schema([("id", pa.int32()), ("payload", pa.string())])

        plan, result = _run_plan(
            fs,
            root,
            target,
            cast_policy=CastPolicy.SAFE,
            target_rows_per_file=50,
            memory_budget_mb=1,
        )

        assert plan.schema_rewrite_memory_budget_mb == 1
        assert result.succeeded
        assert result.actual_metrics.row_count == rows
        assert result.actual_metrics.file_count == 4
        assert not fs.exists(result.staging_prefix)


# --------------------------------------------------------------------------- #
# No dtype inference — the publication protocol never calls opt_dtype
# --------------------------------------------------------------------------- #


class TestSchemaRewriteNoDtypeInference:
    def test_opt_dtype_not_called_during_plan_or_execute(self, monkeypatch):
        import fsspeckit.datasets.polars as polars_mod
        import fsspeckit.datasets.schema as schema_mod

        called = {"count": 0}

        def _spy(*args, **kwargs):
            called["count"] += 1
            raise AssertionError("opt_dtype must not be called by schema rewrite")

        for mod in (polars_mod, schema_mod):
            if hasattr(mod, "opt_dtype"):
                monkeypatch.setattr(mod, "opt_dtype", _spy)

        fs = MemoryFileSystem()
        root = _root()
        _write(
            fs,
            f"{root}/source.parquet",
            pa.table({"id": pa.array([1, 2], pa.int64()), "value": ["a", "b"]}),
        )
        target = pa.schema([("id", pa.int32()), ("value", pa.string())])

        _, result = _run_plan(fs, root, target, cast_policy=CastPolicy.SAFE)

        assert result.succeeded, result.error
        assert called["count"] == 0
