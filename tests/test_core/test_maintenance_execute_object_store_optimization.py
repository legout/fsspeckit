"""Focused generic fsspec tests for coordinated object-store optimization (#45).

Uses ``fsspec.implementations.memory.MemoryFileSystem`` as the generic
object-store stand-in.  These tests cover the fsspec API path only; they make
no claim about real S3/GCS/Azure publication behavior.

Acceptance criteria verified here:

- AC1: Optimization returns phase outcomes plus best-effort guarantee and
  recovery details.
- AC2: A failure in either phase (dedup or compaction) prevents unsafe deletion
  of still-valid sources.
- AC3: Recovery artifacts (staging prefix, partial live outputs) are reported
  on failure; no automatic rollback is claimed.
"""

from __future__ import annotations

import uuid
from io import BytesIO
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from fsspeckit.core.maintenance import (
    BEST_EFFORT_CONCURRENCY_DISCLAIMER,
    CoordinatedOptimizationResult,
    DatasetMaintenanceCoordinator,
    GuaranteeLevel,
    MaintenanceBackend,
    MaintenanceResult,
    SchemaOutcome,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _root() -> str:
    return f"/optimization-{uuid.uuid4().hex[:8]}"


def _write(fs: MemoryFileSystem, path: str, table: pa.Table) -> None:
    buffer = BytesIO()
    pq.write_table(table, buffer)
    fs.pipe(path, buffer.getvalue())


def _read(fs: MemoryFileSystem, path: str) -> pa.Table:
    with fs.open(path, "rb") as handle:
        return pq.read_table(handle)


def _row_count(fs: MemoryFileSystem, path: str) -> int:
    with fs.open(path, "rb") as handle:
        return pq.read_metadata(handle).num_rows


def _total_rows(fs: MemoryFileSystem, root: str) -> int:
    total = 0
    for path in fs.find(root):
        if path.endswith(".parquet"):
            total += _row_count(fs, path)
    return total


def _make_plan(
    fs: MemoryFileSystem,
    root: str,
    *,
    dedup_key_columns: list[str] | None = None,
    dedup_order_by: list[str] | None = None,
    target_rows_per_file: int | None = None,
):
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    return coordinator.plan_coordinated_optimization(
        root,
        filesystem=fs,
        dedup_key_columns=dedup_key_columns,
        dedup_order_by=dedup_order_by,
        target_rows_per_file=target_rows_per_file,
    )


def _run_plan(
    fs: MemoryFileSystem,
    root: str,
    *,
    dedup_key_columns: list[str] | None = None,
    dedup_order_by: list[str] | None = None,
    target_rows_per_file: int | None = None,
):
    coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
    plan = coordinator.plan_coordinated_optimization(
        root,
        filesystem=fs,
        dedup_key_columns=dedup_key_columns,
        dedup_order_by=dedup_order_by,
        target_rows_per_file=target_rows_per_file,
    )
    return plan, coordinator.execute(plan, filesystem=fs)


# --------------------------------------------------------------------------- #
# AC1: Optimization returns phase outcomes + guarantee + recovery details
# --------------------------------------------------------------------------- #


class TestCoordinatedOptimizationSuccess:
    """Happy-path: dedup + compaction compose into one output set."""

    def test_dedup_then_compaction_removes_duplicates_and_compacts(self):
        fs = MemoryFileSystem()
        root = _root()
        t1 = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        t2 = pa.table({"id": [1, 2, 4], "v": ["a2", "b2", "d"]})
        _write(fs, f"{root}/f1.parquet", t1)
        _write(fs, f"{root}/f2.parquet", t2)

        plan, result = _run_plan(fs, root, dedup_key_columns=["id"])

        assert result.succeeded
        assert isinstance(result, CoordinatedOptimizationResult)
        assert isinstance(result, MaintenanceResult)
        assert result.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE

        # Phase composition: dedup → stage → validate → publish → drift_check → cleanup
        phase_names = [p.phase for p in result.phase_outcomes]
        assert phase_names == [
            "dedup",
            "stage",
            "validate",
            "publish",
            "drift_check",
            "cleanup",
        ]
        assert all(p.succeeded for p in result.phase_outcomes)

        # Dedup phase metadata
        assert result.dedup_phase_executed
        assert result.dedup_rows_removed == 2  # two duplicates removed

        # Sources deleted
        assert result.untouched_source_keys == ()
        assert not fs.exists(f"{root}/f1.parquet")
        assert not fs.exists(f"{root}/f2.parquet")

        # Output has exactly 4 unique rows
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 4
        assert _total_rows(fs, root) == 4

    def test_coordinated_optimization_reconciles_string_large_string(self):
        """Coordinated dedup+compaction supports compatible schemas (#65)."""
        fs = MemoryFileSystem()
        root = _root()
        # Two files with a shared duplicate key but offset-width string types.
        _write(
            fs,
            f"{root}/f1.parquet",
            pa.table(
                {
                    "id": [1, 2],
                    "v": pa.array(["a", "b"], type=pa.string()),
                }
            ),
        )
        _write(
            fs,
            f"{root}/f2.parquet",
            pa.table(
                {
                    "id": [1, 3],
                    "v": pa.array(["a2", "c"], type=pa.large_string()),
                }
            ),
        )

        plan, result = _run_plan(fs, root, dedup_key_columns=["id"])

        assert plan.schema_outcome == SchemaOutcome.LOSSLESS_PROMOTED
        assert result.succeeded
        assert result.dedup_phase_executed
        assert result.dedup_rows_removed == 1  # duplicate key id=1 removed

        # 3 unique rows survive, all written as large_string.
        assert result.actual_metrics is not None
        assert result.actual_metrics.row_count == 3
        assert _total_rows(fs, root) == 3
        for path in fs.find(root):
            if path.endswith(".parquet"):
                schema = pq.ParquetFile(fs.open(path, "rb")).schema_arrow
                assert schema.field("v").type == pa.large_string()

    def test_compaction_only_skips_dedup_phase(self):
        fs = MemoryFileSystem()
        root = _root()
        t1 = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        t2 = pa.table({"id": [4, 5, 6], "v": ["d", "e", "f"]})
        _write(fs, f"{root}/f1.parquet", t1)
        _write(fs, f"{root}/f2.parquet", t2)

        plan, result = _run_plan(fs, root, target_rows_per_file=10)

        assert result.succeeded
        assert not result.dedup_phase_executed
        assert result.dedup_rows_removed is None

        # No dedup phase
        phase_names = [p.phase for p in result.phase_outcomes]
        assert "dedup" not in phase_names
        assert phase_names[0] == "stage"

        # All 6 rows preserved
        assert result.actual_metrics.row_count == 6
        assert _total_rows(fs, root) == 6

    def test_result_carry_best_effort_disclaimer_and_recovery_fields(self):
        fs = MemoryFileSystem()
        root = _root()
        t = pa.table({"id": [1, 2], "v": ["a", "b"]})
        _write(fs, f"{root}/f1.parquet", t)

        plan, result = _run_plan(fs, root, dedup_key_columns=["id"])

        assert result.concurrency_disclaimer == BEST_EFFORT_CONCURRENCY_DISCLAIMER
        assert "no distributed lock" in result.concurrency_disclaimer
        # On success, staging is cleaned up and recovery is None.
        assert result.recovery is None


# --------------------------------------------------------------------------- #
# AC2: A failure in either phase prevents unsafe deletion
# --------------------------------------------------------------------------- #


class TestDedupPhaseFailurePreventsDeletion:
    """If the dedup phase fails, no sources are deleted."""

    def test_dedup_failure_preserves_all_sources(self):
        fs = MemoryFileSystem()
        root = _root()
        # Plan with valid files, then corrupt one AFTER planning to trigger a
        # dedup-phase read failure at execution time.
        t1 = pa.table({"id": [1, 2], "v": ["a", "b"]})
        t2 = pa.table({"id": [1, 3], "v": ["a2", "c"]})
        _write(fs, f"{root}/f1.parquet", t1)
        _write(fs, f"{root}/f2.parquet", t2)

        plan = _make_plan(fs, root, dedup_key_columns=["id"])

        # Corrupt one source AFTER planning so the dedup read fails at execute.
        fs.pipe(f"{root}/f2.parquet", b"corrupted parquet")

        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        result = coordinator.execute(plan, filesystem=fs)

        assert not result.succeeded
        assert result.dedup_phase_executed

        # Dedup phase should be the first failing phase
        phase_names = [p.phase for p in result.phase_outcomes]
        assert "dedup" in phase_names
        dedup_phase = next(p for p in result.phase_outcomes if p.phase == "dedup")
        assert not dedup_phase.succeeded
        assert dedup_phase.error is not None

        # No live copies, no source deletions
        assert result.copied_live_keys == ()
        assert result.staged_keys == ()

        # All sources preserved (still on disk)
        assert fs.exists(f"{root}/f1.parquet")
        assert fs.exists(f"{root}/f2.parquet")

        # Recovery artifacts present
        assert result.recovery is not None
        assert result.recovery.workspace_path is not None


class TestStagePhaseFailurePreventsDeletion:
    """If the stage phase fails, no sources are deleted."""

    def test_stage_failure_preserves_sources(self):
        fs = MemoryFileSystem()
        root = _root()
        t1 = pa.table({"id": [1, 2], "v": ["a", "b"]})
        _write(fs, f"{root}/f1.parquet", t1)

        plan = _make_plan(fs, root, dedup_key_columns=["id"])
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)

        # Patch pq.write_table to fail during staging
        with patch(
            "pyarrow.parquet.write_table", side_effect=RuntimeError("disk full")
        ):
            result = coordinator.execute(plan, filesystem=fs)

        assert not result.succeeded

        phase_names = [p.phase for p in result.phase_outcomes]
        assert "stage" in phase_names
        stage_phase = next(p for p in result.phase_outcomes if p.phase == "stage")
        assert not stage_phase.succeeded

        # Source preserved
        assert fs.exists(f"{root}/f1.parquet")
        assert result.copied_live_keys == ()


class TestDriftPreventsSourceDeletion:
    """If source drift is detected after copy, no sources are deleted."""

    def test_drift_after_copy_preserves_all_sources(self):
        fs = MemoryFileSystem()
        root = _root()
        t1 = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        t2 = pa.table({"id": [1, 2, 4], "v": ["a2", "b2", "d"]})
        _write(fs, f"{root}/f1.parquet", t1)
        _write(fs, f"{root}/f2.parquet", t2)

        plan = _make_plan(fs, root, dedup_key_columns=["id"])
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)

        original_pipe = fs.pipe

        def drift_after_copies(path, data, *args, **kwargs):
            """Simulate source drift once staged outputs start going live."""
            # Allow staging writes normally.
            if "_maintenance_staging" in path:
                return original_pipe(path, data, *args, **kwargs)
            # First live copy: succeed, then corrupt source f1.
            if "optimized" in path:
                result = original_pipe(path, data, *args, **kwargs)
                # Corrupt a source file so drift is detected.
                fs.pipe(f"{root}/f1.parquet", b"corrupted")
                return result
            return original_pipe(path, data, *args, **kwargs)

        with patch.object(fs, "pipe", side_effect=drift_after_copies):
            result = coordinator.execute(plan, filesystem=fs)

        assert not result.succeeded
        assert result.drift_detected

        # Drift check phase failed
        drift_phase = next(p for p in result.phase_outcomes if p.phase == "drift_check")
        assert not drift_phase.succeeded
        assert drift_phase.error is not None

        # No source deleted despite some live copies existing.
        # Both source files (even corrupted one) must be preserved.
        assert result.untouched_source_keys  # non-empty
        # The optimized outputs should exist (partial live output = recovery artifact)
        assert len(result.copied_live_keys) >= 1


class TestCopyFailurePreventsSourceDeletion:
    """If copy to live keys fails, staging and partial outputs are retained."""

    def test_copy_failure_preserves_sources_and_staging(self):
        fs = MemoryFileSystem()
        root = _root()
        t1 = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        t2 = pa.table({"id": [1, 2, 4], "v": ["a2", "b2", "d"]})
        _write(fs, f"{root}/f1.parquet", t1)
        _write(fs, f"{root}/f2.parquet", t2)

        plan = _make_plan(fs, root, dedup_key_columns=["id"])
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)

        # Make live writes fail by patching pipe to fail on live paths
        original_pipe = fs.pipe

        def fail_on_live(path, data, *args, **kwargs):
            if "_maintenance_staging" not in path and "optimized" in path:
                raise OSError("object store write error")
            return original_pipe(path, data, *args, **kwargs)

        with patch.object(fs, "pipe", side_effect=fail_on_live):
            result = coordinator.execute(plan, filesystem=fs)

        assert not result.succeeded
        assert len(result.failed_copies) > 0

        # Publish phase failed
        publish_phase = next(p for p in result.phase_outcomes if p.phase == "publish")
        assert not publish_phase.succeeded

        # Staging retained as recovery artifact
        assert result.staged_keys  # non-empty
        assert result.recovery is not None
        assert result.recovery.workspace_path is not None

        # Sources preserved
        assert result.untouched_source_keys  # all sources untouched


# --------------------------------------------------------------------------- #
# AC3: Recovery artifacts reported on failure
# --------------------------------------------------------------------------- #


class TestRecoveryArtifacts:
    """Recovery fields are populated correctly on failure."""

    def test_recovery_workspace_path_on_dedup_failure(self):
        fs = MemoryFileSystem()
        root = _root()
        t = pa.table({"id": [1, 2], "v": ["a", "b"]})
        _write(fs, f"{root}/f1.parquet", t)

        plan = _make_plan(fs, root, dedup_key_columns=["id"])

        # Corrupt after planning to trigger dedup-phase failure at execute.
        fs.pipe(f"{root}/f1.parquet", b"corrupted")

        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        result = coordinator.execute(plan, filesystem=fs)

        assert not result.succeeded
        assert result.recovery is not None
        assert result.recovery.workspace_path is not None
        assert "_maintenance_staging" in result.recovery.workspace_path

    def test_staging_prefix_reported_on_failure(self):
        fs = MemoryFileSystem()
        root = _root()
        t = pa.table({"id": [1, 2], "v": ["a", "b"]})
        _write(fs, f"{root}/f1.parquet", t)

        plan = _make_plan(fs, root, dedup_key_columns=["id"])

        fs.pipe(f"{root}/f1.parquet", b"corrupted")

        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        result = coordinator.execute(plan, filesystem=fs)

        assert result.staging_prefix
        assert "_maintenance_staging" in result.staging_prefix


# --------------------------------------------------------------------------- #
# Typed reporting contract
# --------------------------------------------------------------------------- #


class TestTypedReportingContract:
    """The result is a typed MaintenanceResult with phase composition metadata."""

    def test_result_is_coordinated_optimization_result(self):
        fs = MemoryFileSystem()
        root = _root()
        t = pa.table({"id": [1, 2], "v": ["a", "b"]})
        _write(fs, f"{root}/f1.parquet", t)

        _, result = _run_plan(fs, root, dedup_key_columns=["id"])

        assert isinstance(result, CoordinatedOptimizationResult)
        assert isinstance(result, MaintenanceResult)
        assert result.plan.operation.value == "coordinated_optimization"

    def test_guarantee_is_best_effort_object_store(self):
        fs = MemoryFileSystem()
        root = _root()
        t = pa.table({"id": [1], "v": ["a"]})
        _write(fs, f"{root}/f1.parquet", t)

        _, result = _run_plan(fs, root, dedup_key_columns=["id"])

        assert result.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE

    def test_validation_outcome_populated(self):
        fs = MemoryFileSystem()
        root = _root()
        t1 = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        t2 = pa.table({"id": [1, 2, 4], "v": ["a2", "b2", "d"]})
        _write(fs, f"{root}/f1.parquet", t1)
        _write(fs, f"{root}/f2.parquet", t2)

        _, result = _run_plan(fs, root, dedup_key_columns=["id"])

        assert result.validation is not None
        assert result.validation.succeeded
        # After dedup: 6 input rows - 2 duplicates = 4 output rows
        assert result.validation.staged_row_count == 4
        assert result.validation.expected_row_count == 4

    def test_publication_outcome_populated_on_success(self):
        fs = MemoryFileSystem()
        root = _root()
        t1 = pa.table({"id": [1, 2], "v": ["a", "b"]})
        _write(fs, f"{root}/f1.parquet", t1)

        _, result = _run_plan(fs, root, dedup_key_columns=["id"])

        assert result.publication is not None
        assert result.publication.succeeded
        assert len(result.publication.published_files) >= 1
        assert len(result.publication.removed_source_files) == 1

    def test_actual_metrics_populated_on_success_only(self):
        fs = MemoryFileSystem()
        root = _root()
        t = pa.table({"id": [1, 2], "v": ["a", "b"]})
        _write(fs, f"{root}/f1.parquet", t)

        # Success
        _, ok_result = _run_plan(fs, root, dedup_key_columns=["id"])
        assert ok_result.actual_metrics is not None
        assert ok_result.actual_metrics.file_count >= 1
        assert ok_result.actual_metrics.total_bytes > 0

        # Failure — plan with valid data, then corrupt before execute
        fs2 = MemoryFileSystem()
        root2 = _root()
        t2 = pa.table({"id": [1, 2], "v": ["a", "b"]})
        _write(fs2, f"{root2}/f1.parquet", t2)
        coord2 = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan2 = coord2.plan_coordinated_optimization(
            root2, filesystem=fs2, dedup_key_columns=["id"]
        )
        fs2.pipe(f"{root2}/f1.parquet", b"corrupted")
        fail_result = coord2.execute(plan2, filesystem=fs2)
        assert fail_result.actual_metrics is None


# --------------------------------------------------------------------------- #
# Execute seam: filesystem required
# --------------------------------------------------------------------------- #


class TestExecuteSeamRequiresFilesystem:
    """execute() for best_effort optimization without filesystem → ValueError."""

    def test_missing_filesystem_raises_value_error(self):
        fs = MemoryFileSystem()
        root = _root()
        t = pa.table({"id": [1], "v": ["a"]})
        _write(fs, f"{root}/f1.parquet", t)

        plan = _make_plan(fs, root, dedup_key_columns=["id"])
        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)

        with pytest.raises(ValueError, match="requires the filesystem"):
            coordinator.execute(plan)


# --------------------------------------------------------------------------- #
# Phase composition: max_rows_per_file splitting
# --------------------------------------------------------------------------- #


class TestMaxRowsPerFileBound:
    """max_rows_per_file is enforced as a hard output bound."""

    def test_output_files_respect_row_bound(self):
        fs = MemoryFileSystem()
        root = _root()
        # 5 rows, all unique keys
        t = pa.table({"id": [1, 2, 3, 4, 5], "v": ["a", "b", "c", "d", "e"]})
        _write(fs, f"{root}/f1.parquet", t)

        plan, result = _run_plan(
            fs, root, dedup_key_columns=["id"], target_rows_per_file=2
        )

        assert result.succeeded
        # 5 rows / 2 per file = 3 files (2 + 2 + 1)
        live_files = [p for p in fs.find(root) if p.endswith(".parquet")]
        assert len(live_files) == 3
        for path in live_files:
            assert _row_count(fs, path) <= 2


# --------------------------------------------------------------------------- #
# Partition preservation: compaction-only must never cross partition boundaries
# --------------------------------------------------------------------------- #
#
# #35 explicitly defers repartitioning; optimization must preserve partition
# placement.  The no-dedup path uses partition-local compaction groups so a
# ``country=DE`` file is never mixed with a ``country=US`` file.


class TestPartitionPreservation:
    """Optimization must never move rows across partition boundaries."""

    def test_compaction_only_preserves_partition_boundaries(self):
        fs = MemoryFileSystem()
        root = _root()
        # Two files in different partitions
        t_de = pa.table({"id": [1, 2], "v": ["a", "b"]})
        t_us = pa.table({"id": [3, 4], "v": ["c", "d"]})
        _write(fs, f"{root}/country=DE/f1.parquet", t_de)
        _write(fs, f"{root}/country=US/f1.parquet", t_us)

        plan, result = _run_plan(fs, root, target_rows_per_file=10)

        assert result.succeeded

        # Each partition should have its own output file
        de_files = [p for p in fs.find(f"{root}/country=DE") if p.endswith(".parquet")]
        us_files = [p for p in fs.find(f"{root}/country=US") if p.endswith(".parquet")]
        assert len(de_files) == 1
        assert len(us_files) == 1
        assert _row_count(fs, de_files[0]) == 2
        assert _row_count(fs, us_files[0]) == 2

    def test_dedup_then_compaction_preserves_partition_boundaries(self):
        fs = MemoryFileSystem()
        root = _root()
        # Duplicate keys within each partition
        t_de = pa.table({"id": [1, 1, 2], "v": ["a", "a2", "b"]})
        t_us = pa.table({"id": [3, 3, 4], "v": ["c", "c2", "d"]})
        _write(fs, f"{root}/country=DE/f1.parquet", t_de)
        _write(fs, f"{root}/country=US/f1.parquet", t_us)

        plan, result = _run_plan(fs, root, dedup_key_columns=["id"])

        assert result.succeeded
        assert result.dedup_rows_removed == 2  # one dup per partition

        # Each partition has its own deduplicated output
        de_files = [p for p in fs.find(f"{root}/country=DE") if p.endswith(".parquet")]
        us_files = [p for p in fs.find(f"{root}/country=US") if p.endswith(".parquet")]
        assert len(de_files) == 1
        assert len(us_files) == 1
        assert _row_count(fs, de_files[0]) == 2  # ids 1, 2
        assert _row_count(fs, us_files[0]) == 2  # ids 3, 4
