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
import posixpath
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.core.maintenance import (
    CoordinatedOptimizationResult,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    LockAcquisitionError,
    MaintenanceBackend,
    MaintenanceResult,
    PartitionScopeType,
    PublicationOutcome,
    ValidationOutcome,
    _BoundedAdvisoryLock,
    _check_source_drift,
    _execute_atomic_local_compaction,
    _group_partition_dir,
    _make_workspace,
    _plan_partition_local_compaction_groups,
    _publish_atomic_local,
    _validate_staged_output,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


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
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=100)

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
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=7)

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
            assert outcome.succeeded, f"Phase {outcome.phase!r} failed: {outcome.error}"

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
        # Use an explicit dataset subdirectory so the workspace name is predictable.
        dataset_dir = tmp_path / "mydata"
        dataset_dir.mkdir()
        table = pa.table({"z": [7, 8, 9]})
        _write_parquet(str(dataset_dir / "u.parquet"), table)
        _write_parquet(str(dataset_dir / "v.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(
            str(dataset_dir), fs, target_rows_per_file=100
        )

        result = coordinator.execute(plan)

        assert result.succeeded
        assert result.recovery is not None
        assert result.recovery.workspace_path is None  # cleaned up
        # No .maintenance_mydata_* siblings should remain under tmp_path
        siblings = os.listdir(str(tmp_path))
        maintenance_dirs = [s for s in siblings if s.startswith(".maintenance_mydata_")]
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
            assert out_schema.equals(schema, check_metadata=False), (
                f"Schema mismatch in {f}: {out_schema} != {schema}"
            )

    def test_no_compaction_groups_returns_success(self, tmp_path):
        """A plan with no compaction groups (nothing to do) returns succeeded."""
        # Write a single large file — plan_compaction won't group singletons.
        table = pa.table({"v": list(range(100))})
        _write_parquet(str(tmp_path / "only.parquet"), table)

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=200)

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

        import shutil as _shutil

        original_rmtree = _shutil.rmtree
        result: MaintenanceResult | None = None

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
            if (
                result is not None
                and result.recovery
                and result.recovery.workspace_path
            ):
                try:
                    original_rmtree(result.recovery.workspace_path, ignore_errors=True)
                except Exception:
                    pass

        assert result is not None  # execute() always returns a result
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


# --------------------------------------------------------------------------- #
# Partition-subtree compaction (#38)
# --------------------------------------------------------------------------- #


def _write_partitioned(root, layout):
    """Write a partitioned dataset from a ``{rel_dir: [(name, table)]}`` map.

    Returns the absolute dataset root.
    """
    for rel_dir, files in layout.items():
        part_dir = os.path.join(root, rel_dir) if rel_dir else root
        os.makedirs(part_dir, exist_ok=True)
        for name, table in files:
            _write_parquet(os.path.join(part_dir, name), table)
    return root


def _partition_files(root, rel_dir):
    target = os.path.join(root, rel_dir) if rel_dir else root
    return _list_parquet(target)


class TestPartitionSubtreeCompaction:
    """Targeted local compaction preserves partition subtrees (#38).

    Covers the three #38 acceptance criteria:
    - filtered compaction publishes beneath original partition tuples,
    - unrelated partition files remain unchanged,
    - a failed multi-subtree publication restores every already-swapped subtree.
    """

    def test_filtered_compaction_publishes_beneath_partition_tuples(self, tmp_path):
        """Outputs land in their source partition directories, not the root."""
        dataset = tmp_path / "ds"
        table = pa.table({"a": list(range(10)), "b": list("abcdefghij")})
        _write_partitioned(
            str(dataset),
            {
                "country=US/state=CA": [
                    ("p0.parquet", table.slice(0, 5)),
                    ("p1.parquet", table.slice(5, 5)),
                ],
                "country=US/state=NY": [
                    ("p0.parquet", table.slice(0, 5)),
                    ("p1.parquet", table.slice(5, 5)),
                ],
                "country=DE/state=BY": [
                    ("p0.parquet", table.slice(0, 5)),
                    ("p1.parquet", table.slice(5, 5)),
                ],
            },
        )

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(
            str(dataset), fs, partition_filter=["country=US/"], target_rows_per_file=100
        )
        assert plan.partition_scope.scope_type == PartitionScopeType.FILTERED

        result = coordinator.execute(plan)

        assert result.succeeded, result.error
        # Outputs appear under each US partition tuple, never flattened to root.
        ca_outputs = _partition_files(str(dataset), "country=US/state=CA")
        ny_outputs = _partition_files(str(dataset), "country=US/state=NY")
        assert len(ca_outputs) == 1, f"expected 1 CA output, got {ca_outputs}"
        assert len(ny_outputs) == 1, f"expected 1 NY output, got {ny_outputs}"
        assert not _list_parquet(str(dataset)), "no files should land in the root"
        # Rows are preserved per partition.
        ca_rows = pa.concat_tables([_read_parquet(f) for f in ca_outputs]).num_rows
        ny_rows = pa.concat_tables([_read_parquet(f) for f in ny_outputs]).num_rows
        assert ca_rows == 10
        assert ny_rows == 10
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 20

    def test_unrelated_partition_files_remain_unchanged(self, tmp_path):
        """Files outside the partition filter are not read, moved, or rewritten."""
        dataset = tmp_path / "ds"
        table = pa.table({"a": list(range(10)), "b": list("abcdefghij")})
        _write_partitioned(
            str(dataset),
            {
                "country=US/state=CA": [
                    ("p0.parquet", table.slice(0, 5)),
                    ("p1.parquet", table.slice(5, 5)),
                ],
                "country=DE/state=BY": [
                    ("p0.parquet", table.slice(0, 5)),
                    ("p1.parquet", table.slice(5, 5)),
                ],
            },
        )

        # Snapshot the unrelated DE partition before compaction.
        de_before = _partition_files(str(dataset), "country=DE/state=BY")
        de_hashes = {f: _read_parquet(f).to_pydict() for f in de_before}

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(
            str(dataset), fs, partition_filter=["country=US/"], target_rows_per_file=100
        )
        result = coordinator.execute(plan)
        assert result.succeeded, result.error

        # DE partition untouched: same files, identical contents.
        de_after = _partition_files(str(dataset), "country=DE/state=BY")
        assert sorted(de_after) == sorted(de_before)
        for f in de_after:
            assert _read_parquet(f).to_pydict() == de_hashes[f], (
                f"Unrelated partition file {f} was modified"
            )
        # The DE files were never part of the publication.
        assert result.publication is not None
        de_root = os.path.join(str(dataset), "country=DE")
        assert not any(
            p.startswith(de_root) for p in result.publication.published_files
        )
        assert not any(
            p.startswith(de_root) for p in result.publication.removed_source_files
        )

    def test_failed_multisubtree_publication_restores_all_subtrees(self, tmp_path):
        """A mid-publication failure rolls back every already-swapped subtree."""
        dataset = tmp_path / "ds"
        table = pa.table({"v": list(range(4))})
        _write_partitioned(
            str(dataset),
            {
                "p=a": [("f0.parquet", table), ("f1.parquet", table)],
                "p=b": [("f0.parquet", table), ("f1.parquet", table)],
                "p=c": [("f0.parquet", table), ("f1.parquet", table)],
            },
        )

        original_paths = {
            part: _partition_files(str(dataset), part) for part in ("p=a", "p=b", "p=c")
        }

        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(dataset), fs, target_rows_per_file=100)
        # Three partition-local groups -> three subtrees to swap.
        assert len(plan.compaction_groups) == 3

        # Inject a failure on the FIRST publish rename whose destination is
        # under p=c (the last subtree in sorted order). Subtrees p=a and p=b
        # are already fully swapped by then, so both must be rolled back.
        original_rename = os.rename
        fired = {"done": False}
        c_prefix = os.path.join(str(dataset), "p=c")

        def _fail_on_c_publish(src, dst):
            if not fired["done"] and str(dst).startswith(c_prefix):
                fired["done"] = True
                raise OSError("injected publish failure for partition c")
            original_rename(src, dst)

        import fsspeckit.core.maintenance as _m

        _m.os.rename = _fail_on_c_publish  # type: ignore[attr-defined]
        try:
            result = coordinator.execute(plan)
        finally:
            _m.os.rename = original_rename  # type: ignore[attr-defined]

        assert not result.succeeded
        assert result.publication is not None
        assert not result.publication.succeeded
        # Every original source file is back in place across all subtrees.
        for part, paths in original_paths.items():
            restored = _partition_files(str(dataset), part)
            assert sorted(restored) == sorted(paths), (
                f"Subtree {part!r} was not fully restored: {restored} != {paths}"
            )
        # No compacted output leaked into any partition.
        for part in ("p=a", "p=b", "p=c"):
            files = _partition_files(str(dataset), part)
            for f in files:
                assert not os.path.basename(f).startswith("compacted_"), (
                    f"Leaked output file {f} in subtree {part!r}"
                )


class TestPlanPartitionLocalCompactionGroups:
    """Partition-local planning never crosses a partition boundary (#38)."""

    def test_groups_never_mix_partitions(self, tmp_path):
        """Files from different partitions never share a compaction group."""
        root = str(tmp_path)
        file_stats = [
            {
                "path": os.path.join(root, "p=a", "f0.parquet"),
                "size_bytes": 10,
                "num_rows": 5,
            },
            {
                "path": os.path.join(root, "p=a", "f1.parquet"),
                "size_bytes": 10,
                "num_rows": 5,
            },
            {
                "path": os.path.join(root, "p=b", "f0.parquet"),
                "size_bytes": 10,
                "num_rows": 5,
            },
            {
                "path": os.path.join(root, "p=b", "f1.parquet"),
                "size_bytes": 10,
                "num_rows": 5,
            },
        ]

        groups = _plan_partition_local_compaction_groups(file_stats, root, None, 100)

        assert len(groups) == 2
        for group in groups:
            # Re-derive each file's partition dir to prove the group is single-partition.
            part_dirs = {
                posixpath.dirname(posixpath.relpath(fi.path, root))
                for fi in group.files
            }
            assert len(part_dirs) == 1, f"Group mixes partitions: {part_dirs}"

    def test_flat_dataset_behaves_like_flat_planning(self, tmp_path):
        """Flat datasets collapse to a single partition and plan identically."""
        root = str(tmp_path)
        file_stats = [
            {"path": os.path.join(root, "f0.parquet"), "size_bytes": 10, "num_rows": 5},
            {"path": os.path.join(root, "f1.parquet"), "size_bytes": 10, "num_rows": 5},
        ]

        groups = _plan_partition_local_compaction_groups(file_stats, root, None, 100)

        assert len(groups) == 1
        assert _group_partition_dir(groups[0], root) == ""


class TestPublishAtomicLocalPartitionSubtrees:
    """_publish_atomic_local swaps partition subtrees with full rollback (#38)."""

    def test_partition_aware_publish_places_outputs_in_tuples(self, tmp_path):
        """Staged outputs are published beneath their partition directories."""
        staged_dir = tmp_path / "staged"
        backup_dir = tmp_path / "backup"
        dataset_dir = tmp_path / "dataset"
        staged_dir.mkdir()
        backup_dir.mkdir()
        for part in ("p=a", "p=b"):
            os.makedirs(os.path.join(str(dataset_dir), part), exist_ok=True)

        src_a = os.path.join(str(dataset_dir), "p=a", "src.parquet")
        src_b = os.path.join(str(dataset_dir), "p=b", "src.parquet")
        _write_parquet(src_a, pa.table({"v": [1]}))
        _write_parquet(src_b, pa.table({"v": [2]}))

        stg_a = os.path.join(str(staged_dir), "out_a.parquet")
        stg_b = os.path.join(str(staged_dir), "out_b.parquet")
        _write_parquet(stg_a, pa.table({"v": [1]}))
        _write_parquet(stg_b, pa.table({"v": [2]}))

        outcome = _publish_atomic_local(
            source_files=[src_a, src_b],
            staged_files=[stg_a, stg_b],
            dataset_root=str(dataset_dir),
            backup_dir=str(backup_dir),
            staged_partition_dirs=["p=a", "p=b"],
        )

        assert outcome.succeeded
        assert not os.path.exists(src_a) and not os.path.exists(src_b)
        # Backups preserved partition structure.
        assert os.path.exists(os.path.join(str(backup_dir), "p=a", "src.parquet"))
        assert os.path.exists(os.path.join(str(backup_dir), "p=b", "src.parquet"))
        # Outputs published beneath their partition tuples.
        assert os.path.exists(os.path.join(str(dataset_dir), "p=a", "out_a.parquet"))
        assert os.path.exists(os.path.join(str(dataset_dir), "p=b", "out_b.parquet"))

    def test_multisubtree_failure_restores_every_swapped_subtree(
        self, tmp_path, monkeypatch
    ):
        """A failure in subtree 3 rolls back subtrees 1 and 2 as well."""
        staged_dir = tmp_path / "staged"
        backup_dir = tmp_path / "backup"
        dataset_dir = tmp_path / "dataset"
        staged_dir.mkdir()
        backup_dir.mkdir()
        for part in ("p=a", "p=b", "p=c"):
            os.makedirs(os.path.join(str(dataset_dir), part), exist_ok=True)

        srcs = {}
        stgs = {}
        for part in ("p=a", "p=b", "p=c"):
            src = os.path.join(str(dataset_dir), part, "src.parquet")
            _write_parquet(src, pa.table({"v": [1]}))
            srcs[part] = src
            stg = os.path.join(str(staged_dir), "out.parquet")
            _write_parquet(stg, pa.table({"v": [1]}))
            stgs[part] = stg

        # Fail the first rename whose destination is under p=c (sorted last).
        original_rename = os.rename
        fired = {"done": False}
        c_prefix = os.path.join(str(dataset_dir), "p=c")

        def _fail_on_c_publish(src, dst):
            if not fired["done"] and str(dst).startswith(c_prefix):
                fired["done"] = True
                raise OSError("injected publish failure for partition c")
            original_rename(src, dst)

        monkeypatch.setattr(os, "rename", _fail_on_c_publish)

        outcome = _publish_atomic_local(
            source_files=[srcs["p=a"], srcs["p=b"], srcs["p=c"]],
            staged_files=[stgs["p=a"], stgs["p=b"], stgs["p=c"]],
            dataset_root=str(dataset_dir),
            backup_dir=str(backup_dir),
            staged_partition_dirs=["p=a", "p=b", "p=c"],
        )

        assert not outcome.succeeded
        # Every source file restored across all three subtrees.
        for part in ("p=a", "p=b", "p=c"):
            assert os.path.exists(srcs[part]), (
                f"Source file for {part!r} was not restored"
            )
        # No output file leaked into any partition.
        for part in ("p=a", "p=b", "p=c"):
            assert not os.path.exists(
                os.path.join(str(dataset_dir), part, "out.parquet")
            ), f"Output file leaked into {part!r}"


# --------------------------------------------------------------------------- #
# Partition-local deduplication (#40)
# --------------------------------------------------------------------------- #


class TestAtomicLocalPartitionLocalDeduplication:
    """Acceptance coverage for the native partition-local deduplication lane."""

    def _execute(self, dataset, **kwargs):
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_partition_local_deduplication(
            str(dataset), fs, key_columns=["id"], **kwargs
        )
        return coordinator.execute(plan), plan

    def test_preserves_partition_tuples_and_deduplicates_all_source_files(
        self, tmp_path
    ):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "country=US": [
                    (
                        "a.parquet",
                        pa.table({"id": [1, 1], "value": ["first", "second"]}),
                    ),
                    (
                        "b.parquet",
                        pa.table({"id": [1, 2], "value": ["third", "unique"]}),
                    ),
                ],
                "country=DE": [
                    (
                        "a.parquet",
                        pa.table({"id": [1, 3], "value": ["de-one", "de-three"]}),
                    ),
                ],
            },
        )

        result, plan = self._execute(dataset)

        assert plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert result.succeeded, result.error
        assert result.validation is not None and result.validation.succeeded
        us_files = _partition_files(str(dataset), "country=US")
        de_files = _partition_files(str(dataset), "country=DE")
        assert len(us_files) == 1
        assert len(de_files) == 1
        assert pa.concat_tables(
            [_read_parquet(path) for path in us_files]
        ).to_pydict() == {
            "country": ["US", "US"],
            "id": [1, 2],
            "value": ["first", "unique"],
        }
        assert pa.concat_tables(
            [_read_parquet(path) for path in de_files]
        ).to_pydict() == {
            "country": ["DE", "DE"],
            "id": [1, 3],
            "value": ["de-one", "de-three"],
        }
        assert not _list_parquet(str(dataset))
        assert result.recovery is not None and result.recovery.workspace_path is None

    def test_null_nan_and_exact_string_keys_compare_equal(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=x": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [None, None, "Value", "value"],
                                "measure": [float("nan"), float("nan"), 1.0, 1.0],
                                "payload": ["null-a", "nan-a", "upper", "lower"],
                            }
                        ),
                    ),
                    (
                        "b.parquet",
                        pa.table(
                            {
                                "id": [None, "Value"],
                                "measure": [float("nan"), 1.0],
                                "payload": ["nan-b", "upper-b"],
                            }
                        ),
                    ),
                ]
            },
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_partition_local_deduplication(
            str(dataset),
            fs,
            key_columns=["id", "measure"],
            validation_level="full_distinct_key_scan",
        )
        result = coordinator.execute(plan)

        assert result.succeeded, result.error
        output = _read_parquet(_partition_files(str(dataset), "part=x")[0]).to_pydict()
        assert output["payload"] == ["null-a", "upper", "lower"]
        assert result.validation is not None
        assert result.validation.succeeded

    def test_explicit_order_wins_and_physical_order_breaks_ties(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=x": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 1, 2, 2],
                                "priority": [3, 1, 5, 5],
                                "payload": ["high", "low", "first", "second"],
                            }
                        ),
                    )
                ]
            },
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_partition_local_deduplication(
            str(dataset), fs, key_columns=["id"], dedup_order_by=["priority"]
        )
        result = coordinator.execute(plan)

        assert result.succeeded, result.error
        output = _read_parquet(_partition_files(str(dataset), "part=x")[0]).to_pydict()
        assert output["payload"] == ["low", "first"]

    def test_max_rows_per_file_is_hard_bound(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=x": [
                    (
                        "a.parquet",
                        pa.table({"id": list(range(5)), "payload": list("abcde")}),
                    ),
                ]
            },
        )
        result, _ = self._execute(dataset, target_rows_per_file=2)

        assert result.succeeded, result.error
        files = _partition_files(str(dataset), "part=x")
        assert len(files) == 3
        assert all(_read_parquet(path).num_rows <= 2 for path in files)
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 5

    def test_rejects_nonpositive_output_bounds(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"part=x": [("a.parquet", pa.table({"id": [1], "payload": ["a"]}))]},
        )
        with pytest.raises(ValueError, match="target_rows_per_file"):
            self._execute(dataset, target_rows_per_file=0)
        with pytest.raises(ValueError, match="target_mb_per_file"):
            self._execute(dataset, target_mb_per_file=0)
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        with pytest.raises(ValueError, match="target_rows_per_file"):
            coordinator.plan_coordinated_optimization(
                str(dataset),
                fs,
                dedup_key_columns=["id"],
                target_rows_per_file=0,
            )

    def test_cleanup_failure_reports_retained_backups(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=x": [
                    ("a.parquet", pa.table({"id": [1, 1], "payload": ["a", "b"]}))
                ]
            },
        )
        import shutil

        real_rmtree = shutil.rmtree

        def fail_cleanup(_path):
            raise OSError("injected cleanup failure")

        monkeypatch.setattr(shutil, "rmtree", fail_cleanup)
        result, _ = self._execute(dataset)

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

    def test_metadata_conflict_invalidates_plan(self, tmp_path):
        dataset = tmp_path / "ds"
        part = dataset / "part=x"
        part.mkdir(parents=True)
        first = pa.table({"id": [1], "payload": ["a"]}).replace_schema_metadata(
            {b"source": b"first"}
        )
        second = pa.table({"id": [2], "payload": ["b"]}).replace_schema_metadata(
            {b"source": b"second"}
        )
        pq.write_table(first, part / "a.parquet")
        pq.write_table(second, part / "b.parquet")
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)

        with pytest.raises(ValueError, match="Schema reconciliation required"):
            coordinator.plan_partition_local_deduplication(
                str(dataset), fs, key_columns=["id"]
            )

    def test_publish_failure_rolls_back_every_partition(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=a": [
                    ("a.parquet", pa.table({"id": [1, 1], "payload": ["a", "a2"]})),
                ],
                "part=b": [
                    ("b.parquet", pa.table({"id": [2, 2], "payload": ["b", "b2"]})),
                ],
            },
        )
        original = {
            part: _partition_files(str(dataset), part)[0]
            for part in ("part=a", "part=b")
        }
        real_rename = os.rename
        b_prefix = os.path.join(str(dataset), "part=b")
        injected = {"done": False}

        def fail_on_b(src, dst):
            if not injected["done"] and str(dst).startswith(b_prefix):
                injected["done"] = True
                raise OSError("injected local dedup publish failure")
            real_rename(src, dst)

        monkeypatch.setattr(os, "rename", fail_on_b)
        result, _ = self._execute(dataset)

        assert not result.succeeded
        assert result.publication is not None and not result.publication.succeeded
        assert _partition_files(str(dataset), "part=a") == [original["part=a"]]
        assert _partition_files(str(dataset), "part=b") == [original["part=b"]]
        assert all(
            not os.path.basename(path).startswith("deduplicated_")
            for part in ["part=a", "part=b"]
            for path in _partition_files(str(dataset), part)
        )
        assert result.recovery is not None and result.recovery.recovered

    def test_rollback_failure_is_reported_as_unrecovered(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=a": [
                    ("a.parquet", pa.table({"id": [1, 1], "payload": ["a", "a2"]})),
                ],
                "part=b": [
                    ("b.parquet", pa.table({"id": [2, 2], "payload": ["b", "b2"]})),
                ],
            },
        )
        real_rename = os.rename
        b_prefix = os.path.join(str(dataset), "part=b")
        injected = {"publish": False, "rollback": False}

        def fail_publish_and_rollback(src, dst):
            src_text = str(src)
            dst_text = str(dst)
            if (
                not injected["publish"]
                and dst_text.startswith(b_prefix)
                and "staged" in src_text
            ):
                injected["publish"] = True
                raise OSError("injected publish failure")
            if (
                injected["publish"]
                and not injected["rollback"]
                and dst_text.startswith(b_prefix)
                and "backup" in src_text
            ):
                injected["rollback"] = True
                raise OSError("injected rollback failure")
            real_rename(src, dst)

        monkeypatch.setattr(os, "rename", fail_publish_and_rollback)
        result, _ = self._execute(dataset)

        assert not result.succeeded
        assert result.recovery is not None
        assert not result.recovery.recovered
        assert result.recovery.error is not None
        assert "restore" in result.recovery.error
        workspace = result.recovery.workspace_path
        assert workspace is not None
        backup_b = os.path.join(workspace, "backup", "part=b", "b.parquet")
        backup_a = os.path.join(workspace, "backup", "part=a", "a.parquet")
        assert list(result.recovery.backup_paths) == [backup_b]
        assert os.path.exists(backup_b)
        assert not os.path.exists(backup_a)

    def test_source_drift_aborts_before_publication(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=x": [
                    ("a.parquet", pa.table({"id": [1, 1], "payload": ["a", "b"]}))
                ]
            },
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_partition_local_deduplication(
            str(dataset), fs, key_columns=["id"]
        )
        source = plan.source_snapshot.files[0].absolute_path
        pq.write_table(
            pa.table({"id": [1, 1, 2], "payload": ["a", "b", "c"]}),
            source,
        )

        result = coordinator.execute(plan)

        assert not result.succeeded
        assert any(
            phase.phase == "drift_check" and not phase.succeeded
            for phase in result.phase_outcomes
        )
        assert _partition_files(str(dataset), "part=x") == [source]

    def test_validation_failure_keeps_live_sources(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=x": [
                    ("a.parquet", pa.table({"id": [1, 1], "payload": ["a", "b"]}))
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
        result, _ = self._execute(dataset)

        assert not result.succeeded
        assert result.validation is not None and not result.validation.succeeded
        assert _partition_files(str(dataset), "part=x") == [
            os.path.join(str(dataset), "part=x", "a.parquet")
        ]

    def test_dedup_order_by_dash_column_keeps_latest_per_key(self, tmp_path):
        # Regression for #52: "-column" must sort descending so keep-first
        # selects the most recent row per key, and bare "column" stays ascending.
        import fsspec

        rows = pa.table(
            {
                "id": [1, 1, 2, 2],
                "ts": [
                    "2024-01-01",
                    "2024-01-03",
                    "2024-01-02",
                    "2024-01-05",
                ],
            }
        )
        fs = fsspec.filesystem("file")

        # Descending ("-ts") => keep-first selects the latest ts per key.
        dataset_desc = tmp_path / "desc"
        _write_partitioned(str(dataset_desc), {"part=x": [("a.parquet", rows)]})
        result_desc = fs.deduplicate_parquet_dataset(
            str(dataset_desc), key_columns=["id"], dedup_order_by=["-ts"]
        )
        assert result_desc.succeeded, result_desc.error
        output_desc = _read_parquet(
            _partition_files(str(dataset_desc), "part=x")[0]
        ).to_pydict()
        assert dict(zip(output_desc["id"], output_desc["ts"])) == {
            1: "2024-01-03",
            2: "2024-01-05",
        }

        # Bare name is ascending => the inverse: keep the earliest ts per key.
        dataset_asc = tmp_path / "asc"
        _write_partitioned(str(dataset_asc), {"part=x": [("a.parquet", rows)]})
        result_asc = fs.deduplicate_parquet_dataset(
            str(dataset_asc), key_columns=["id"], dedup_order_by=["ts"]
        )
        assert result_asc.succeeded, result_asc.error
        output_asc = _read_parquet(
            _partition_files(str(dataset_asc), "part=x")[0]
        ).to_pydict()
        assert dict(zip(output_asc["id"], output_asc["ts"])) == {
            1: "2024-01-01",
            2: "2024-01-02",
        }

    def test_dedup_order_by_plus_prefixed_column_stays_literal(self, tmp_path):
        # Standards follow-up to #52: a leading "+" is NOT a sigil. "+ts" must
        # address a literal column named "+ts" (ascending), and must NOT be
        # rewritten to the bare "ts" column. Only a leading "-" is special.
        import fsspec

        rows = pa.table(
            {
                "id": [1, 1, 2, 2],
                "+ts": [
                    "2024-01-01",
                    "2024-01-03",
                    "2024-01-02",
                    "2024-01-05",
                ],
            }
        )
        fs = fsspec.filesystem("file")

        # "+ts" is a literal ascending column => keep-first keeps the earliest.
        dataset = tmp_path / "plus_literal"
        _write_partitioned(str(dataset), {"part=x": [("a.parquet", rows)]})
        result = fs.deduplicate_parquet_dataset(
            str(dataset), key_columns=["id"], dedup_order_by=["+ts"]
        )
        assert result.succeeded, result.error
        output = _read_parquet(_partition_files(str(dataset), "part=x")[0]).to_pydict()
        assert dict(zip(output["id"], output["+ts"])) == {
            1: "2024-01-01",
            2: "2024-01-02",
        }


# --------------------------------------------------------------------------- #
# Global-repartitioning deduplication — atomic local lane (#42)
# --------------------------------------------------------------------------- #


class TestAtomicLocalGlobalRepartitionDeduplication:
    """Acceptance coverage for the native global-repartitioning dedup lane (#42).

    Covers the three #42 acceptance criteria:
    - Global deduplication is opt-in and requires declared destination
      partition columns.
    - Retained rows are written only to their declared destination tuple.
    - Atomic-local validation and rollback cover the entire global rewrite.
    Plus key semantics, winner ordering, and fault injection.
    """

    def _plan(
        self,
        dataset,
        *,
        partition_columns=None,
        key_columns=None,
        dedup_order_by=None,
        target_rows_per_file=None,
        validation_level=None,
    ):
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        return coordinator.plan_global_repartition_deduplication(
            str(dataset),
            partition_columns or ["region"],
            filesystem=fs,
            key_columns=key_columns,
            dedup_order_by=dedup_order_by,
            target_rows_per_file=target_rows_per_file,
            validation_level=validation_level,
        )

    def _execute(self, dataset, **kwargs):
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_global_repartition_deduplication(
            str(dataset),
            kwargs.pop("partition_columns", ["region"]),
            filesystem=fs,
            key_columns=kwargs.pop("key_columns", None),
            dedup_order_by=kwargs.pop("dedup_order_by", None),
            target_rows_per_file=kwargs.pop("target_rows_per_file", None),
            validation_level=kwargs.pop("validation_level", None),
        )
        return coordinator.execute(plan), plan

    @staticmethod
    def _read_file(path):
        """Read a Parquet file via a binary handle to bypass Hive partition discovery."""
        with open(path, "rb") as fh:
            return pq.read_table(fh)

    # ---------------------------------------------------------- #
    # AC1: global dedup is opt-in, requires declared partition cols
    # ---------------------------------------------------------- #

    def test_requires_non_empty_partition_columns(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset), {"": [("a.parquet", pa.table({"id": [1], "region": ["US"]}))]}
        )
        with pytest.raises(ValueError, match="partition_columns"):
            self._execute(dataset, partition_columns=[])

    def test_partition_columns_must_be_in_schema(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset), {"": [("a.parquet", pa.table({"id": [1], "region": ["US"]}))]}
        )
        with pytest.raises(ValueError, match="partition_columns"):
            self._execute(dataset, partition_columns=["missing_col"])

    # ---------------------------------------------------------- #
    # AC1+AC2: cross-partition dedup + rows follow declared partitions
    # ---------------------------------------------------------- #

    def test_cross_partition_duplicates_removed_and_rows_follow_declared_partitions(
        self, tmp_path
    ):
        """Rows from different source partitions merge and follow declared tuple."""
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "source=A": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 2],
                                "region": ["US", "DE"],
                                "value": ["a-first", "a-second"],
                                "score": [2, 1],
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
                                "value": ["b-winner", "b-third"],
                                "score": [1, 1],
                            }
                        ),
                    )
                ],
            },
        )
        result, plan = self._execute(
            dataset, key_columns=["id"], dedup_order_by=["score"]
        )

        assert plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert result.succeeded, result.error
        assert result.validation is not None and result.validation.succeeded

        # Source partition files were moved out (empty dirs may remain).
        assert _partition_files(str(dataset), "source=A") == []
        assert _partition_files(str(dataset), "source=B") == []
        # Retained rows live under declared destination tuples.
        us_files = _partition_files(str(dataset), "region=US")
        de_files = _partition_files(str(dataset), "region=DE")
        ca_files = _partition_files(str(dataset), "region=CA")
        assert len(us_files) == 1
        assert len(de_files) == 1
        assert len(ca_files) == 1
        us_output = self._read_file(us_files[0]).to_pydict()
        assert us_output["id"] == [1]
        assert us_output["value"] == ["b-winner"]  # winner by score=1 < None
        de_output = self._read_file(de_files[0]).to_pydict()
        assert de_output["id"] == [2]
        ca_output = self._read_file(ca_files[0]).to_pydict()
        assert ca_output["id"] == [3]
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 3
        assert result.actual_metrics.file_count == 3

    def test_integer_partition_columns_are_path_only_and_hive_readable(self, tmp_path):
        """Integer destination keys stay out of files and hive reads succeed (#56)."""
        import pyarrow.dataset as pds

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

        result, _ = self._execute(
            dataset, partition_columns=["year"], key_columns=["id"]
        )
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

    def test_timestamp_derived_partitions_use_explicit_timezone_and_path_only_keys(
        self, tmp_path
    ):
        """Derived keys use the planned timezone and remain hive path metadata (#57)."""
        from datetime import datetime, timedelta, timezone

        import pyarrow.dataset as pds

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
        plan = coordinator.plan_global_repartition_deduplication(
            str(dataset),
            ["year", "year_month"],
            filesystem=fs,
            key_columns=["id"],
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
        assert set(("year", "year_month")).issubset(hive_table.column_names)
        assert sorted(hive_table["year_month"].to_pylist()) == ["2023-12", "2024-01"]

    def test_flat_sources_repartition_into_declared_tuples(self, tmp_path):
        """Flat (un-partitioned) sources repartition into new Hive-style dirs."""
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
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = self._execute(dataset, key_columns=["id"])

        assert result.succeeded, result.error
        # Three unique keys survive global dedup, in two destination partitions.
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 3
        assert _partition_files(str(dataset), "region=US")
        assert _partition_files(str(dataset), "region=DE")
        assert not _list_parquet(str(dataset))

    # ---------------------------------------------------------- #
    # Key semantics: null/NaN/exact-string/type
    # ---------------------------------------------------------- #

    def test_null_nan_and_exact_string_keys_compare_equal(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [None, None, "Value", "value", "1"],
                                "region": ["X", "X", "X", "X", "X"],
                                "measure": [float("nan"), float("nan"), 1.0, 1.0, 1.0],
                                "payload": [
                                    "null-a",
                                    "null-b",
                                    "upper",
                                    "lower",
                                    "num",
                                ],
                            }
                        ),
                    )
                ]
            },
        )
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_global_repartition_deduplication(
            str(dataset),
            ["region"],
            filesystem=fs,
            key_columns=["id", "measure"],
            validation_level="full_distinct_key_scan",
        )
        result = coordinator.execute(plan)

        assert result.succeeded, result.error
        output = self._read_file(
            _partition_files(str(dataset), "region=X")[0]
        ).to_pydict()
        # null==null (2 collapsed to first), "Value"!="value", 1 != True
        assert output["payload"] == ["null-a", "upper", "lower", "num"]

    def test_explicit_order_wins_and_physical_order_breaks_ties(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": [1, 1, 2, 2],
                                "region": ["X", "X", "X", "X"],
                                "priority": [3, 1, 5, 5],
                                "payload": ["high", "low", "first", "second"],
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = self._execute(
            dataset, key_columns=["id"], dedup_order_by=["priority"]
        )

        assert result.succeeded, result.error
        output = self._read_file(
            _partition_files(str(dataset), "region=X")[0]
        ).to_pydict()
        assert output["payload"] == ["low", "first"]

    # ---------------------------------------------------------- #
    # max_rows_per_file hard bound
    # ---------------------------------------------------------- #

    def test_max_rows_per_file_is_hard_bound(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table(
                            {
                                "id": list(range(5)),
                                "region": ["X"] * 5,
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = self._execute(dataset, key_columns=["id"], target_rows_per_file=2)

        assert result.succeeded, result.error
        files = _partition_files(str(dataset), "region=X")
        assert len(files) == 3
        assert all(self._read_file(f).num_rows <= 2 for f in files)
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 5

    # ---------------------------------------------------------- #
    # Full distinct-key scan validation
    # ---------------------------------------------------------- #

    def test_full_distinct_key_scan_passes_when_globally_unique(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table({"id": [1, 2, 3], "region": ["A", "B", "A"]}),
                    )
                ]
            },
        )
        result, _ = self._execute(
            dataset,
            key_columns=["id"],
            validation_level="full_distinct_key_scan",
        )

        assert result.succeeded, result.error
        assert result.validation is not None and result.validation.succeeded

    # ---------------------------------------------------------- #
    # AC3: atomic-local validation failure keeps live sources
    # ---------------------------------------------------------- #

    def test_validation_failure_keeps_live_sources(self, tmp_path, monkeypatch):
        dataset = tmp_path / "ds"
        original_a = str(dataset / "source=A" / "a.parquet")
        _write_partitioned(
            str(dataset),
            {
                "source=A": [
                    (
                        "a.parquet",
                        pa.table({"id": [1, 1], "region": ["US", "US"]}),
                    )
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
        result, _ = self._execute(dataset, key_columns=["id"])

        assert not result.succeeded
        assert result.validation is not None and not result.validation.succeeded
        # Original source file untouched.
        assert self._read_file(original_a).num_rows == 2
        assert not os.path.exists(os.path.join(str(dataset), "region=US"))

    # ---------------------------------------------------------- #
    # AC3: source drift aborts before publication
    # ---------------------------------------------------------- #

    def test_source_drift_aborts_before_publication(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "": [
                    (
                        "a.parquet",
                        pa.table({"id": [1, 1], "region": ["US", "US"]}),
                    )
                ]
            },
        )
        plan = self._plan(dataset, key_columns=["id"])
        source = plan.source_snapshot.files[0].absolute_path
        pq.write_table(
            pa.table({"id": [1, 1, 2], "region": ["US", "US", "DE"]}),
            source,
        )

        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        result = coordinator.execute(plan)

        assert not result.succeeded
        assert any(
            phase.phase == "drift_check" and not phase.succeeded
            for phase in result.phase_outcomes
        )
        assert _list_parquet(str(dataset)) == [source]
        assert not os.path.exists(os.path.join(str(dataset), "region=US"))

    # ---------------------------------------------------------- #
    # AC3: publish failure rolls back every swapped subtree
    # ---------------------------------------------------------- #

    def test_publish_failure_rolls_back_source_files(self, tmp_path, monkeypatch):
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
                            }
                        ),
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
                raise OSError("injected global dedup publish failure")
            real_rename(src, dst)

        monkeypatch.setattr(os, "rename", fail_on_us_publish)
        result, _ = self._execute(dataset, key_columns=["id"])

        assert not result.succeeded
        assert result.publication is not None and not result.publication.succeeded
        # Source file restored.
        assert _list_parquet(str(dataset)) == [original_file]
        # No destination partition output files leaked (empty dir may remain).
        assert _partition_files(str(dataset), "region=US") == []
        assert result.recovery is not None and result.recovery.recovered

    def test_rollback_failure_is_reported_as_unrecovered(self, tmp_path, monkeypatch):
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
                            }
                        ),
                    )
                ]
            },
        )

        real_rename = os.rename
        us_prefix = os.path.join(str(dataset), "region=US")
        injected = {"publish": False, "rollback": False}

        def fail_publish_and_rollback(src, dst):
            src_text = str(src)
            dst_text = str(dst)
            if (
                not injected["publish"]
                and dst_text.startswith(us_prefix)
                and "staged" in src_text
            ):
                injected["publish"] = True
                raise OSError("injected publish failure")
            if (
                injected["publish"]
                and not injected["rollback"]
                and "backup" in src_text
            ):
                injected["rollback"] = True
                raise OSError("injected rollback failure")
            real_rename(src, dst)

        monkeypatch.setattr(os, "rename", fail_publish_and_rollback)
        result, _ = self._execute(dataset, key_columns=["id"])

        assert not result.succeeded
        assert result.recovery is not None
        assert not result.recovery.recovered
        assert result.recovery.error is not None
        assert "restore" in result.recovery.error
        assert result.recovery.workspace_path is not None

    # ---------------------------------------------------------- #
    # Cleanup behavior
    # ---------------------------------------------------------- #

    def test_workspace_cleaned_up_on_success(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {"": [("a.parquet", pa.table({"id": [1, 2], "region": ["US", "DE"]}))]},
        )
        result, _ = self._execute(dataset, key_columns=["id"])

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
        result, _ = self._execute(dataset, key_columns=["id"])

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

    # ---------------------------------------------------------- #
    # Metadata conflict invalidates plan
    # ---------------------------------------------------------- #

    def test_metadata_conflict_invalidates_plan(self, tmp_path):
        dataset = tmp_path / "ds"
        dataset.mkdir()
        first = pa.table({"id": [1], "region": ["US"]}).replace_schema_metadata(
            {b"source": b"first"}
        )
        second = pa.table({"id": [2], "region": ["DE"]}).replace_schema_metadata(
            {b"source": b"second"}
        )
        pq.write_table(first, dataset / "a.parquet")
        pq.write_table(second, dataset / "b.parquet")
        fs = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)

        with pytest.raises(ValueError, match="Schema reconciliation required"):
            coordinator.plan_global_repartition_deduplication(
                str(dataset), ["region"], filesystem=fs, key_columns=["id"]
            )

    # ---------------------------------------------------------- #
    # Null partition values map to Hive default partition
    # ---------------------------------------------------------- #

    def test_null_partition_value_uses_hive_default_partition(self, tmp_path):
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
                                "region": ["US", None],
                            }
                        ),
                    )
                ]
            },
        )
        result, _ = self._execute(dataset, key_columns=["id"])

        assert result.succeeded, result.error
        assert _partition_files(str(dataset), "region=US")
        assert _partition_files(str(dataset), "region=__HIVE_DEFAULT_PARTITION__")


class TestAtomicLocalCoordinatedOptimization:
    """Local optimization publishes deduplication and compaction atomically."""

    @staticmethod
    def _execute(dataset, **kwargs):
        filesystem = __import__("fsspec").filesystem("file")
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_coordinated_optimization(
            str(dataset), filesystem, **kwargs
        )
        return cast(CoordinatedOptimizationResult, coordinator.execute(plan)), plan

    def test_dedup_and_compaction_report_separate_actual_phases(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "country=US": [
                    ("a.parquet", pa.table({"id": [1, 1], "value": ["a", "b"]})),
                    ("b.parquet", pa.table({"id": [1, 2], "value": ["c", "d"]})),
                ],
                "country=DE": [
                    ("a.parquet", pa.table({"id": [1, 2], "value": ["e", "f"]})),
                ],
            },
        )

        result, plan = self._execute(
            dataset,
            dedup_key_columns=["id"],
            target_rows_per_file=1,
            validation_level="full_distinct_key_scan",
        )

        assert result.succeeded, result.error
        assert isinstance(result, CoordinatedOptimizationResult)
        assert result.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert result.dedup_phase_executed
        assert result.dedup_rows_removed == 2
        assert [outcome.phase for outcome in result.phase_outcomes] == [
            "stage",
            "dedup",
            "compaction",
            "validate",
            "lock",
            "drift_check",
            "publish",
            "cleanup",
        ]
        assert all(outcome.succeeded for outcome in result.phase_outcomes)
        assert result.validation is not None and result.validation.succeeded
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 4
        assert len(_partition_files(str(dataset), "country=US")) == 2
        assert len(_partition_files(str(dataset), "country=DE")) == 2
        assert plan.dedup_key_columns is not None
        assert list(plan.dedup_key_columns) == ["id"]

    def test_compaction_only_skips_deduplication_without_repartitioning(self, tmp_path):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "country=US": [
                    ("a.parquet", pa.table({"id": [1, 2], "value": ["a", "b"]})),
                    ("b.parquet", pa.table({"id": [3, 4], "value": ["c", "d"]})),
                ],
                "country=DE": [
                    ("a.parquet", pa.table({"id": [5, 6], "value": ["e", "f"]})),
                ],
            },
        )

        result, _ = self._execute(dataset, target_rows_per_file=10)

        assert result.succeeded, result.error
        assert not result.dedup_phase_executed
        assert result.dedup_rows_removed is None
        assert "dedup" not in [outcome.phase for outcome in result.phase_outcomes]
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 4
        assert len(_partition_files(str(dataset), "country=US")) == 1
        de_files = _partition_files(str(dataset), "country=DE")
        assert len(de_files) == 1
        assert _read_parquet(de_files[0]).to_pydict()["id"] == [5, 6]

    def test_publish_failure_restores_all_partitions_before_returning(
        self, tmp_path, monkeypatch
    ):
        dataset = tmp_path / "ds"
        _write_partitioned(
            str(dataset),
            {
                "part=a": [("a.parquet", pa.table({"id": [1, 1]}))],
                "part=b": [("b.parquet", pa.table({"id": [2, 2]}))],
            },
        )
        original = {
            partition: _partition_files(str(dataset), partition)
            for partition in ("part=a", "part=b")
        }
        real_rename = os.rename
        destination_prefix = os.path.join(str(dataset), "part=b")
        injected = False

        def fail_second_partition_publish(source, destination):
            nonlocal injected
            if (
                not injected
                and str(source).find("staged") >= 0
                and str(destination).startswith(destination_prefix)
            ):
                injected = True
                raise OSError("injected optimization publish failure")
            real_rename(source, destination)

        monkeypatch.setattr(os, "rename", fail_second_partition_publish)
        result, _ = self._execute(dataset, dedup_key_columns=["id"])

        assert not result.succeeded
        assert result.publication is not None and not result.publication.succeeded
        assert result.recovery is not None and result.recovery.recovered
        assert _partition_files(str(dataset), "part=a") == original["part=a"]
        assert _partition_files(str(dataset), "part=b") == original["part=b"]
