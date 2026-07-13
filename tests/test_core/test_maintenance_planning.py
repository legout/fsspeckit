"""Tests for typed maintenance planning and guarantee classification."""

from __future__ import annotations

import posixpath
import uuid
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec import filesystem as fsspec_filesystem
from fsspec.implementations.memory import MemoryFileSystem

from fsspeckit.core.maintenance import (
    CompactionPlan,
    CoordinatedOptimizationPlan,
    DatasetMaintenanceCoordinator,
    GlobalRepartitionDeduplicationPlan,
    GuaranteeLevel,
    MaintenanceBackend,
    PartitionLocalDeduplicationPlan,
    PartitionScopeType,
    SchemaOutcome,
    ValidationLevel,
)


def _parquet_bytes(table: pa.Table) -> bytes:
    """Serialize a PyArrow table to Parquet bytes."""
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _memory_root() -> str:
    """Return a unique in-memory dataset root to avoid cross-test leakage."""
    return f"/dataset-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sample_table() -> pa.Table:
    """A small table suitable for writing Parquet files."""
    return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


class TestCoordinatorPlanning:
    """Plan creation without file mutation."""

    def test_plan_compaction_local_atomic(self, sample_table, tmp_path):
        """Local filesystem plans receive the atomic-local guarantee."""
        fs = fsspec_filesystem("file")
        a = tmp_path / "a.parquet"
        b = tmp_path / "b.parquet"
        pq.write_table(sample_table, str(a))
        pq.write_table(sample_table, str(b))

        coordinator = DatasetMaintenanceCoordinator(MaintenanceBackend.PYARROW)
        plan = coordinator.plan_compaction(str(tmp_path), fs, target_rows_per_file=10)

        assert isinstance(plan, CompactionPlan)
        assert plan.operation.value == "compaction"
        assert plan.guarantee_level == GuaranteeLevel.ATOMIC_LOCAL
        assert plan.selected_backend == "pyarrow"
        assert plan.selected_codec == "snappy"
        assert plan.max_rows_per_file == 10
        assert plan.target_byte_size is None
        assert plan.validation_level == ValidationLevel.DEFAULT
        assert plan.schema_outcome == SchemaOutcome.LOSSLESS_PRESERVED
        assert plan.partition_scope.scope_type == PartitionScopeType.FULL
        assert len(plan.compaction_groups) >= 1
        assert len(plan.source_snapshot.files) == 2
        assert plan.source_snapshot.total_rows == 6
        assert plan.schema is not None

        # Planning is lock-free and leaves the dataset untouched.
        assert {p.name for p in tmp_path.iterdir()} == {"a.parquet", "b.parquet"}

    def test_plan_compaction_memory_best_effort(self, sample_table):
        """Non-local fsspec filesystems receive best-effort object-store semantics."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(root, fs, target_rows_per_file=10)

        assert plan.guarantee_level == GuaranteeLevel.BEST_EFFORT_OBJECT_STORE
        assert plan.source_snapshot.filesystem_protocol == "memory"
        assert len(plan.source_snapshot.files) == 2

    def test_plan_partition_local_deduplication(self, sample_table):
        """Partition-local deduplication plans capture key columns and scope."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("duckdb")
        plan = coordinator.plan_partition_local_deduplication(
            root, fs, key_columns=["a"]
        )

        assert isinstance(plan, PartitionLocalDeduplicationPlan)
        expected_key_columns = ("a",)
        assert plan.dedup_key_columns == expected_key_columns
        # Omitted business order intentionally falls back to snapshot-local
        # physical order, not implicit key-column ordering.
        assert plan.dedup_order_by is None
        assert plan.partition_scope.scope_type == PartitionScopeType.FULL
        assert len(plan.dedup_groups) >= 1

    def test_plan_global_repartition_deduplication(self, sample_table):
        """Global deduplication plans expose repartition scope and columns."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_global_repartition_deduplication(
            dataset_path=root,
            partition_columns=["b"],
            filesystem=fs,
            key_columns=["a"],
        )

        assert isinstance(plan, GlobalRepartitionDeduplicationPlan)
        expected_partition_columns = ("b",)
        assert plan.partition_columns == expected_partition_columns
        assert plan.partition_scope.scope_type == PartitionScopeType.REPARTITION
        expected_partition_scope_columns = ("b",)
        assert (
            plan.partition_scope.partition_columns == expected_partition_scope_columns
        )
        expected_key_columns = ("a",)
        assert plan.dedup_key_columns == expected_key_columns

    def test_plan_global_repartition_requires_partition_columns(self, sample_table):
        """Global deduplication requires non-empty partition columns."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(
            ValueError, match="partition_columns must be a non-empty list"
        ):
            coordinator.plan_global_repartition_deduplication(
                root, partition_columns=[], filesystem=fs, key_columns=["a"]
            )

    def test_plan_coordinated_optimization(self, sample_table):
        """Optimization plans represent optional deduplication plus compaction."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_coordinated_optimization(
            root, fs, target_rows_per_file=10
        )

        assert isinstance(plan, CoordinatedOptimizationPlan)
        assert plan.dedup_key_columns is None
        assert len(plan.optimization_groups) >= 1

    def test_plan_coordinated_optimization_with_dedup(self, sample_table):
        """Optimization with dedup key columns captures the dedup phase."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_coordinated_optimization(
            root, fs, dedup_key_columns=["a"], target_rows_per_file=10
        )

        assert plan.dedup_key_columns is not None
        expected_key_columns = ("a",)
        assert plan.dedup_key_columns == expected_key_columns
        assert len(plan.optimization_groups) >= 1

    def test_plan_exposes_output_bounds(self, sample_table):
        """Plans expose hard row bound and advisory byte-size target."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(
            root, fs, target_rows_per_file=100, target_mb_per_file=1
        )

        assert plan.max_rows_per_file == 100
        assert plan.target_byte_size == 1024 * 1024

    def test_plan_validation_level_full(self, sample_table):
        """Validation level is pinned in the plan."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_partition_local_deduplication(
            root,
            fs,
            key_columns=["a"],
            validation_level=ValidationLevel.FULL_DISTINCT_KEY_SCAN,
        )

        assert plan.validation_level == ValidationLevel.FULL_DISTINCT_KEY_SCAN

    def test_plan_partition_scope_filtered(self, sample_table):
        """Partition filters are reflected in the plan scope."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/part=1/a.parquet", _parquet_bytes(sample_table))
        fs.pipe(f"{root}/part=2/b.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(
            root, fs, target_rows_per_file=10, partition_filter=["part=1"]
        )

        assert plan.partition_scope.scope_type == PartitionScopeType.FILTERED
        expected_filter = ("part=1",)
        assert plan.partition_scope.partition_filter == expected_filter
        assert len(plan.source_snapshot.files) == 1

    def test_source_snapshot_content_tokens(self, sample_table):
        """Source snapshots capture drift-detection tokens per file."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(root, fs, target_rows_per_file=10)

        assert len(plan.source_snapshot.files) == 1
        file_info = plan.source_snapshot.files[0]
        assert file_info.relative_path == "a.parquet"
        assert file_info.absolute_path == f"{root}/a.parquet"
        assert file_info.size_bytes > 0
        assert file_info.num_rows == 3
        assert file_info.content_token
        assert plan.source_snapshot.total_bytes == file_info.size_bytes
        assert plan.source_snapshot.total_rows == 3

    def test_execute_seam_blocked(self, sample_table):
        """best_effort_object_store execute() is deferred to issue #39."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(root, fs, target_rows_per_file=10)

        # Memory filesystem receives best_effort_object_store; its execution
        # path is deferred to issue #39.  nosec B608 — not SQL.
        with pytest.raises(NotImplementedError) as exc_info:
            coordinator.execute(plan)  # nosec B608

        assert "#39" in str(exc_info.value)


class TestSchemaReconciliation:
    """Schema reconciliation during planning."""

    def test_plan_schema_preserved_when_files_match(self):
        """Identical schemas across files receive a lossless-preserved outcome."""
        root = _memory_root()
        fs = MemoryFileSystem()
        table = pa.table({"a": [1], "b": ["x"]})
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(table))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(root, fs, target_rows_per_file=10)

        assert plan.schema_outcome == SchemaOutcome.LOSSLESS_PRESERVED
        assert plan.schema is not None

    def test_plan_schema_reconciliation_required_on_mismatch(self):
        """Differing schemas across files invalidate the plan."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(pa.table({"a": [1], "b": ["x"]})))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(pa.table({"a": [2], "c": ["y"]})))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(ValueError, match="Schema reconciliation required"):
            coordinator.plan_compaction(root, fs, target_rows_per_file=10)

    def test_partition_local_deduplication_respects_boundaries(self, sample_table):
        """Partition-local deduplication plans separate groups per partition."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(f"{root}/part=1/a.parquet", _parquet_bytes(sample_table))
        fs.pipe(f"{root}/part=2/b.parquet", _parquet_bytes(sample_table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_partition_local_deduplication(
            root, fs, target_rows_per_file=10
        )

        # Two files in two different partitions must never be grouped together.
        assert len(plan.dedup_groups) == 2
        partition_dirs = {
            posixpath.basename(posixpath.dirname(group.files[0].path))
            for group in plan.dedup_groups
        }
        assert partition_dirs == {"part=1", "part=2"}


class TestPlanImmutability:
    """Plans and their nested group structures are immutable."""

    def test_compaction_plan_is_deeply_immutable(self):
        """Mutating a nested group or file inside a plan is rejected."""
        from dataclasses import FrozenInstanceError

        root = _memory_root()
        fs = MemoryFileSystem()
        table = pa.table({"a": [1], "b": ["x"]})
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(table))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(table))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(root, fs, target_rows_per_file=10)

        assert len(plan.compaction_groups) >= 1
        group = plan.compaction_groups[0]
        with pytest.raises(FrozenInstanceError):
            group.files = ()  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            group.files[0].size_bytes = 0  # type: ignore[misc]
