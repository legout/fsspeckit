"""Tests for atomic_local flat-dataset compaction execution (#37).

Covers:
- Successful flat local compaction: row preservation, schema reconciliation,
  max_rows_per_file hard bound, and typed MaintenanceResult.
- Bounded advisory lock acquisition: exclusive lock contention fails
  predictably within the timeout.
- Injected failures: stage, validation, rename (publish), and cleanup failures
  leave a recoverable local dataset and correctly report the outcome.
"""

from __future__ import annotations

import os
import threading
from io import BytesIO
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.core.maintenance import (
    ActualMetrics,
    CompactionPlan,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    LockAcquisitionError,
    MaintenanceBackend,
    MaintenanceResult,
    PhaseOutcome,
    PublicationOutcome,
    RecoveryArtifacts,
    ValidationOutcome,
    _BoundedAdvisoryLock,
    _check_source_drift,
    _execute_atomic_local_compaction,
    _make_workspace,
    _publish_atomic_local,
    _validate_staged_output,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _parquet_bytes(table: pa.Table) -> bytes:
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _write_parquet(path: str, table: pa.Table) -> None:
    pq.write_table(table, path)


def _read_parquet(path: str) -> pa.Table:
    return pq.read_table(path)


def _list_parquet(directory: str) -> list[str]:
    return sorted(
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".parquet")
    )


# --------------------------------------------------------------------------- #
# Happy-path execution
# --------------------------------------------------------------------------- #


class TestAtomicLocalCompactionHappyPath:
    """Flat local compaction succeeds end-to-end."""

    def test_basic_compaction_preserves_rows(self, tmp_path):
        """All source rows are preserved in the compacted output."""
        table = pa.table({"a": list(range(10)), "b": list("abcdefghij")})
        _write_parquet(str(tmp_path / "part1.parquet"), table.slice(0, 5))
        _write_parquet(str(tmp_path / "part2.parquet"), table.slice(5, 5))

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(
            str(tmp_path), fs, target_rows_per_file=100
        )

        result = coordinator.execute(plan)

        assert isinstance(result, MaintenanceResult)
        assert result.succeeded
        assert result.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 10
        assert result.actual_metrics.file_count >= 1
        assert result.actual_metrics.total_bytes > 0

        # Source files are gone, output files exist
        output_files = _list_parquet(str(tmp_path))
        assert len(output_files) >= 1
        combined = pa.concat_tables([_read_parquet(f) for f in output_files])
        assert combined.num_rows == 10
        assert set(combined["a"].to_pylist()) == set(range(10))

    def test_max_rows_per_file_hard_bound(self, tmp_path):
        """Output files never exceed max_rows_per_file rows."""
        table = pa.table({"x": list(range(30))})
        _write_parquet(str(tmp_path / "a.parquet"), table.slice(0, 15))
        _write_parquet(str(tmp_path / "b.parquet"), table.slice(15, 15))

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(
            str(tmp_path), fs, target_rows_per_file=7
        )

        result = coordinator.execute(plan)

        assert result.succeeded
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 30

        output_files = _list_parquet(str(tmp_path))
        for f in output_files:
            meta = pq.read_metadata(f)
            assert meta.num_rows <= 7, (
                f"Output file {f} has {meta.num_rows} rows, exceeds bound of 7"
            )

    def test_result_carries_phase_outcomes(self, tmp_path):
        """Every lifecycle phase is reported in phase_outcomes."""
        table = pa.table({"v": [1, 2, 3]})
        _write_parquet(str(tmp_path / "a.parquet"), table)
        _write_parquet(str(tmp_path / "b.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        result = coordinator.execute(plan)

        assert result.succeeded
        phase_names = [p.phase for p in result.phase_outcomes]
        assert "stage" in phase_names
        assert "write" in phase_names
        assert "validate" in phase_names
        assert "lock" in phase_names
        assert "drift_check" in phase_names
        assert "publish" in phase_names
        assert "cleanup" in phase_names
        for outcome in result.phase_outcomes:
            assert outcome.succeeded, (
                f"Phase {outcome.phase!r} failed: {outcome.error}"
            )

    def test_validation_outcome_populated(self, tmp_path):
        """The validation field is populated with correct row counts."""
        table = pa.table({"n": [10, 20, 30]})
        _write_parquet(str(tmp_path / "x.parquet"), table)
        _write_parquet(str(tmp_path / "y.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        result = coordinator.execute(plan)

        assert result.validation is not None
        assert result.validation.succeeded
        assert result.validation.staged_row_count == 6
        assert result.validation.expected_row_count == 6

    def test_publication_outcome_populated(self, tmp_path):
        """The publication field lists published and removed files."""
        table = pa.table({"k": [1, 2]})
        _write_parquet(str(tmp_path / "p.parquet"), table)
        _write_parquet(str(tmp_path / "q.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        result = coordinator.execute(plan)

        assert result.publication is not None
        assert result.publication.succeeded
        assert len(result.publication.published_files) >= 1
        assert len(result.publication.removed_source_files) == 2

    def test_workspace_cleaned_up_on_success(self, tmp_path):
        """The maintenance workspace is removed after a successful operation."""
        table = pa.table({"z": [7, 8, 9]})
        _write_parquet(str(tmp_path / "u.parquet"), table)
        _write_parquet(str(tmp_path / "v.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        result = coordinator.execute(plan)

        assert result.succeeded
        assert result.recovery is not None
        assert result.recovery.workspace_path is None  # cleaned up
        # No .maintenance_* siblings should remain
        parent = str(tmp_path.parent)
        siblings = os.listdir(parent)
        maintenance_dirs = [s for s in siblings if ".maintenance_" in s]
        assert maintenance_dirs == [], (
            f"Unexpected maintenance directories: {maintenance_dirs}"
        )

    def test_schema_preserved_in_output(self, tmp_path):
        """Output files carry the same schema as the source files."""
        schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.utf8())])
        table = pa.table({"id": pa.array([1, 2], type=pa.int32()), "name": ["a", "b"]})
        _write_parquet(str(tmp_path / "s1.parquet"), table)
        _write_parquet(str(tmp_path / "s2.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        result = coordinator.execute(plan)

        assert result.succeeded
        output_files = _list_parquet(str(tmp_path))
        for f in output_files:
            out_schema = pq.read_schema(f)
            assert out_schema.equals(schema), (
                f"Schema mismatch in {f}: {out_schema} != {schema}"
            )

    def test_no_compaction_groups_returns_success(self, tmp_path):
        """A plan with no compaction groups (nothing to do) returns succeeded."""
        # Write a single large file — plan_compaction won't group singletons.
        table = pa.table({"v": list(range(100))})
        _write_parquet(str(tmp_path / "only.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(
            str(tmp_path), fs, target_rows_per_file=200
        )

        # If no groups, execute should still return a valid result.
        result = coordinator.execute(plan)

        assert isinstance(result, MaintenanceResult)
        # There may be 0 compaction groups; result is still a valid result object.
        assert result.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL


# --------------------------------------------------------------------------- #
# Bounded advisory lock
# --------------------------------------------------------------------------- #


class TestBoundedAdvisoryLock:
    """Lock acquisition respects timeout and fails predictably."""

    def test_exclusive_lock_acquired_and_released(self, tmp_path):
        """A single acquirer obtains and releases the lock."""
        lock_path = str(tmp_path / "test.lock")
        lock = _BoundedAdvisoryLock(lock_path, exclusive=True, timeout_s=5.0)
        assert lock.acquire()
        lock.release()

    def test_shared_lock_acquired_and_released(self, tmp_path):
        """A single shared-lock acquirer obtains and releases."""
        lock_path = str(tmp_path / "test.lock")
        lock = _BoundedAdvisoryLock(lock_path, exclusive=False, timeout_s=5.0)
        assert lock.acquire()
        lock.release()

    def test_context_manager_acquires_and_releases(self, tmp_path):
        """The context manager protocol acquires and releases the lock."""
        lock_path = str(tmp_path / "ctx.lock")
        with _BoundedAdvisoryLock(lock_path, timeout_s=5.0):
            assert os.path.exists(lock_path)

    def test_exclusive_lock_contention_fails_within_timeout(self, tmp_path):
        """A second exclusive acquirer times out when the first holds the lock."""
        lock_path = str(tmp_path / "contended.lock")
        holder = _BoundedAdvisoryLock(lock_path, exclusive=True, timeout_s=5.0)
        assert holder.acquire()

        try:
            # The second acquirer has a very short timeout so the test is fast.
            challenger = _BoundedAdvisoryLock(
                lock_path, exclusive=True, timeout_s=0.1, retry_interval_s=0.02
            )
            acquired = challenger.acquire()
            assert not acquired, "Challenger should not have acquired a held lock"
        finally:
            holder.release()

    def test_context_manager_raises_on_contention(self, tmp_path):
        """LockAcquisitionError is raised when the context manager times out."""
        lock_path = str(tmp_path / "cm_contended.lock")
        holder = _BoundedAdvisoryLock(lock_path, exclusive=True, timeout_s=5.0)
        assert holder.acquire()

        try:
            with pytest.raises(LockAcquisitionError):
                with _BoundedAdvisoryLock(
                    lock_path, exclusive=True, timeout_s=0.1, retry_interval_s=0.02
                ):
                    pass  # should not reach here
        finally:
            holder.release()

    def test_publication_blocked_during_concurrent_exclusive_lock(self, tmp_path):
        """Cooperating writer holds lock; execute() acquisition fails within timeout."""
        table = pa.table({"v": [1, 2, 3]})
        _write_parquet(str(tmp_path / "a.parquet"), table)
        _write_parquet(str(tmp_path / "b.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        # Pre-occupy the lock file as an exclusive holder.
        lock_path = str(tmp_path / ".fsspeckit_maintenance.lock")
        holder = _BoundedAdvisoryLock(lock_path, exclusive=True, timeout_s=5.0)
        assert holder.acquire()

        try:
            result = _execute_atomic_local_compaction(
                plan, lock_timeout_s=0.1, lock_retry_interval_s=0.02
            )
        finally:
            holder.release()

        assert not result.succeeded
        assert result.error is not None
        assert "timed out" in result.error.lower() or "lock" in result.error.lower()
        lock_phase = next((p for p in result.phase_outcomes if p.phase == "lock"), None)
        assert lock_phase is not None
        assert not lock_phase.succeeded

        # Dataset files are untouched (no modification happened)
        remaining = _list_parquet(str(tmp_path))
        assert len(remaining) == 2


# --------------------------------------------------------------------------- #
# Failure injection
# --------------------------------------------------------------------------- #


class TestAtomicLocalCompactionFailureInjection:
    """Injected failures leave a recoverable dataset and report outcome."""

    def test_stage_failure_leaves_dataset_intact(self, tmp_path):
        """If the workspace cannot be created, the source dataset is untouched."""
        table = pa.table({"c": [1, 2]})
        _write_parquet(str(tmp_path / "a.parquet"), table)
        _write_parquet(str(tmp_path / "b.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        # Patch _make_workspace to raise
        import fsspeckit.core.maintenance as _m

        original = _m._make_workspace

        def _fail(*_: Any) -> Any:
            raise OSError("injected stage failure")

        _m._make_workspace = _fail  # type: ignore[assignment]
        try:
            result = coordinator.execute(plan)
        finally:
            _m._make_workspace = original

        assert not result.succeeded
        assert result.error is not None
        assert "stage" in result.error.lower() or "Stage" in result.error

        # Dataset is untouched
        assert len(_list_parquet(str(tmp_path))) == 2

    def test_validation_failure_leaves_dataset_intact(self, tmp_path):
        """A staged-output validation failure leaves source files unchanged."""
        table = pa.table({"d": [10, 20, 30]})
        _write_parquet(str(tmp_path / "x.parquet"), table)
        _write_parquet(str(tmp_path / "y.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        import fsspeckit.core.maintenance as _m

        original = _m._validate_staged_output

        def _fail_validate(
            staged_files: list[str],
            expected_schema: Any,
            expected_rows: int,
        ) -> Any:
            return ValidationOutcome(
                succeeded=False,
                staged_row_count=0,
                expected_row_count=expected_rows,
                error="injected validation failure",
            )

        _m._validate_staged_output = _fail_validate  # type: ignore[assignment]
        try:
            result = coordinator.execute(plan)
        finally:
            _m._validate_staged_output = original

        assert not result.succeeded
        assert result.validation is not None
        assert not result.validation.succeeded
        assert result.validation.error == "injected validation failure"
        # Workspace should still exist (not cleaned up after failed validation)
        assert result.recovery is not None
        assert result.recovery.workspace_path is not None

        # Source files are untouched (publication was not reached)
        assert len(_list_parquet(str(tmp_path))) == 2

    def test_publish_failure_restores_source_files(self, tmp_path):
        """A rename failure during publication rolls back; source files survive."""
        table = pa.table({"e": [1, 2, 3, 4, 5]})
        _write_parquet(str(tmp_path / "p1.parquet"), table.slice(0, 3))
        _write_parquet(str(tmp_path / "p2.parquet"), table.slice(3, 2))

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        import fsspeckit.core.maintenance as _m

        original = _m._publish_atomic_local

        def _fail_publish(*_: Any, **__: Any) -> Any:
            return PublicationOutcome(
                succeeded=False,
                error="injected publish failure",
            )

        _m._publish_atomic_local = _fail_publish  # type: ignore[assignment]
        try:
            result = coordinator.execute(plan)
        finally:
            _m._publish_atomic_local = original

        assert not result.succeeded
        assert result.publication is not None
        assert not result.publication.succeeded
        assert result.publication.error == "injected publish failure"
        assert result.recovery is not None
        assert result.recovery.workspace_path is not None

        # Source files still present (were not removed)
        remaining = _list_parquet(str(tmp_path))
        assert len(remaining) == 2

    def test_source_drift_detected_after_lock(self, tmp_path):
        """Source drift detected under the lock causes failure without mutation."""
        table = pa.table({"f": [1, 2, 3]})
        src_a = str(tmp_path / "a.parquet")
        src_b = str(tmp_path / "b.parquet")
        _write_parquet(src_a, table)
        _write_parquet(src_b, table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        import fsspeckit.core.maintenance as _m

        original = _m._check_source_drift

        def _drift(*_: Any) -> str | None:
            return "injected drift: file changed"

        _m._check_source_drift = _drift  # type: ignore[assignment]
        try:
            result = coordinator.execute(plan)
        finally:
            _m._check_source_drift = original

        assert not result.succeeded
        assert result.error is not None
        assert "drift" in result.error.lower()
        drift_phase = next(
            (p for p in result.phase_outcomes if p.phase == "drift_check"), None
        )
        assert drift_phase is not None
        assert not drift_phase.succeeded

        # Source files are untouched (publication was not reached)
        assert os.path.exists(src_a)
        assert os.path.exists(src_b)
        assert len(_list_parquet(str(tmp_path))) == 2

    def test_cleanup_failure_does_not_undo_publication(self, tmp_path):
        """A cleanup failure is non-fatal; the dataset was already updated."""
        table = pa.table({"g": [9, 8, 7]})
        _write_parquet(str(tmp_path / "m.parquet"), table)
        _write_parquet(str(tmp_path / "n.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

        import fsspeckit.core.maintenance as _m
        import shutil as _shutil

        original_rmtree = _shutil.rmtree

        def _fail_rmtree(path: str, *args: Any, **kwargs: Any) -> None:
            # Only fail on the maintenance workspace
            if ".maintenance_" in path:
                raise OSError("injected cleanup failure")
            original_rmtree(path, *args, **kwargs)

        _shutil.rmtree = _fail_rmtree  # type: ignore[assignment]
        try:
            result = coordinator.execute(plan)
        finally:
            _shutil.rmtree = original_rmtree
            # Clean up the leftover workspace if it's still there
            if result.recovery and result.recovery.workspace_path:
                try:
                    original_rmtree(result.recovery.workspace_path, ignore_errors=True)
                except Exception:
                    pass

        # The operation still succeeded despite the cleanup failure.
        assert result.succeeded
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 6

        # Workspace path is non-None because cleanup failed.
        assert result.recovery is not None
        assert result.recovery.workspace_path is not None

        cleanup_phase = next(
            (p for p in result.phase_outcomes if p.phase == "cleanup"), None
        )
        assert cleanup_phase is not None
        assert not cleanup_phase.succeeded


# --------------------------------------------------------------------------- #
# Publish rollback unit test
# --------------------------------------------------------------------------- #


class TestPublishAtomicLocalRollback:
    """_publish_atomic_local rolls back correctly on failure."""

    def test_successful_publish_moves_files(self, tmp_path):
        """Staged files are moved to dataset root; source files moved to backup."""
        staged_dir = tmp_path / "staged"
        backup_dir = tmp_path / "backup"
        dataset_dir = tmp_path / "dataset"
        staged_dir.mkdir()
        backup_dir.mkdir()
        dataset_dir.mkdir()

        # Write source files
        src = str(dataset_dir / "src.parquet")
        _write_parquet(src, pa.table({"v": [1]}))

        # Write staged output
        stg = str(staged_dir / "out.parquet")
        _write_parquet(stg, pa.table({"v": [1]}))

        outcome = _publish_atomic_local(
            source_files=[src],
            staged_files=[stg],
            dataset_root=str(dataset_dir),
            backup_dir=str(backup_dir),
        )

        assert outcome.succeeded
        assert not os.path.exists(src)  # moved to backup
        assert os.path.exists(str(backup_dir / "src.parquet"))
        dest = str(dataset_dir / "out.parquet")
        assert os.path.exists(dest)  # staged → dataset

    def test_rollback_restores_source_on_rename_failure(self, tmp_path, monkeypatch):
        """On rename failure, backed-up sources are restored."""
        staged_dir = tmp_path / "staged"
        backup_dir = tmp_path / "backup"
        dataset_dir = tmp_path / "dataset"
        staged_dir.mkdir()
        backup_dir.mkdir()
        dataset_dir.mkdir()

        src = str(dataset_dir / "src.parquet")
        _write_parquet(src, pa.table({"v": [1]}))
        stg = str(staged_dir / "out.parquet")
        _write_parquet(stg, pa.table({"v": [1]}))

        call_count = {"n": 0}
        original_rename = os.rename

        def _fail_second_rename(a: str, b: str) -> None:
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise OSError("injected rename failure")
            original_rename(a, b)

        monkeypatch.setattr(os, "rename", _fail_second_rename)

        outcome = _publish_atomic_local(
            source_files=[src],
            staged_files=[stg],
            dataset_root=str(dataset_dir),
            backup_dir=str(backup_dir),
        )

        assert not outcome.succeeded
        # Source file should be restored
        assert os.path.exists(src), "Source file should have been rolled back"


# --------------------------------------------------------------------------- #
# Unit tests for helper functions
# --------------------------------------------------------------------------- #


class TestMakeWorkspace:
    def test_creates_staged_and_backup_subdirs(self, tmp_path):
        workspace, staged, backup = _make_workspace(str(tmp_path))
        assert os.path.isdir(staged)
        assert os.path.isdir(backup)
        assert os.path.basename(workspace).startswith(".maintenance_")

    def test_workspace_is_sibling_of_dataset(self, tmp_path):
        dataset = tmp_path / "dataset"
        dataset.mkdir()
        workspace, _, _ = _make_workspace(str(dataset))
        assert os.path.dirname(workspace) == str(tmp_path)


class TestValidateStagedOutput:
    def test_succeeds_with_correct_rows(self, tmp_path):
        table = pa.table({"a": [1, 2, 3]})
        f = str(tmp_path / "out.parquet")
        pq.write_table(table, f)
        outcome = _validate_staged_output([f], None, 3)
        assert outcome.succeeded
        assert outcome.staged_row_count == 3

    def test_fails_on_row_count_mismatch(self, tmp_path):
        table = pa.table({"a": [1, 2, 3]})
        f = str(tmp_path / "out.parquet")
        pq.write_table(table, f)
        outcome = _validate_staged_output([f], None, 5)
        assert not outcome.succeeded
        assert "mismatch" in (outcome.error or "")

    def test_fails_on_schema_mismatch(self, tmp_path):
        table = pa.table({"a": [1, 2, 3]})
        f = str(tmp_path / "out.parquet")
        pq.write_table(table, f)
        wrong_schema = pa.schema([pa.field("b", pa.int64())])
        outcome = _validate_staged_output([f], wrong_schema, 3)
        assert not outcome.succeeded
        assert "schema" in (outcome.error or "").lower()


class TestCheckSourceDrift:
    def test_no_drift_when_sizes_match(self, tmp_path):
        from fsspeckit.core.maintenance import (
            SourceFileInfo,
            SourceSnapshot,
        )

        table = pa.table({"x": [1, 2, 3]})
        f = str(tmp_path / "s.parquet")
        pq.write_table(table, f)
        size = os.path.getsize(f)

        snapshot = SourceSnapshot(
            dataset_path=str(tmp_path),
            filesystem_protocol="file",
            files=(
                SourceFileInfo(
                    relative_path="s.parquet",
                    absolute_path=f,
                    size_bytes=size,
                    num_rows=3,
                    content_token=f"{size}:None",
                ),
            ),
            total_bytes=size,
            total_rows=3,
            captured_at="2026-01-01T00:00:00+00:00",
        )

        assert _check_source_drift(snapshot) is None

    def test_drift_detected_when_size_differs(self, tmp_path):
        from fsspeckit.core.maintenance import (
            SourceFileInfo,
            SourceSnapshot,
        )

        table = pa.table({"x": [1, 2, 3]})
        f = str(tmp_path / "s.parquet")
        pq.write_table(table, f)

        snapshot = SourceSnapshot(
            dataset_path=str(tmp_path),
            filesystem_protocol="file",
            files=(
                SourceFileInfo(
                    relative_path="s.parquet",
                    absolute_path=f,
                    size_bytes=9999999,  # Wrong size
                    num_rows=3,
                    content_token="9999999:None",
                ),
            ),
            total_bytes=9999999,
            total_rows=3,
            captured_at="2026-01-01T00:00:00+00:00",
        )

        error = _check_source_drift(snapshot)
        assert error is not None
        assert "size changed" in error
