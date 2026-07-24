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
    CompactionSkipReason,
    CoordinatedOptimizationPlan,
    DatasetMaintenanceCoordinator,
    FileInfo,
    GlobalRepartitionDeduplicationPlan,
    GuaranteeLevel,
    MaintenanceBackend,
    PartitionLocalDeduplicationPlan,
    PartitionScopeType,
    SchemaOutcome,
    ValidationLevel,
    _collect_dataset_stats,
    _parquet_codecs,
    _reconcile_schema,
    collect_dataset_stats,
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

    def test_compaction_plan_exposes_singleton_skip_reason(
        self, sample_table, tmp_path
    ):
        fs = fsspec_filesystem("file")
        pq.write_table(sample_table, str(tmp_path / "healthy.parquet"))

        plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
            str(tmp_path), fs, target_rows_per_file=100
        )

        assert not plan.compaction_groups
        assert len(plan.skipped_files) == 1
        assert plan.skipped_files[0].reason == CompactionSkipReason.SINGLETON_PARTITION
        assert plan.skipped_files[0].file.path.endswith("healthy.parquet")

    def test_explicit_codec_rewrites_healthy_singleton(self, sample_table, tmp_path):
        fs = fsspec_filesystem("file")
        pq.write_table(
            sample_table,
            str(tmp_path / "gzip.parquet"),
            compression="gzip",
        )
        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(
            str(tmp_path),
            fs,
            target_rows_per_file=100,
            codec="zstd",
        )

        assert len(plan.compaction_groups) == 1
        assert not plan.skipped_files
        result = coordinator.execute(plan)
        assert result.succeeded, result.error

        output_files = list(tmp_path.glob("*.parquet"))
        assert len(output_files) == 1
        parquet_file = pq.ParquetFile(output_files[0])
        codecs = {
            parquet_file.metadata.row_group(row_group)
            .column(column)
            .compression.lower()
            for row_group in range(parquet_file.metadata.num_row_groups)
            for column in range(parquet_file.metadata.num_columns)
        }
        assert codecs == {"zstd"}

    def test_derived_partition_planning_requires_unambiguous_timestamp(self, tmp_path):
        fs = fsspec_filesystem("file")
        table = pa.table(
            {
                "id": [1],
                "event_ts": pa.array([0], type=pa.timestamp("us")),
                "ingested_ts": pa.array([0], type=pa.timestamp("us")),
            }
        )
        pq.write_table(table, str(tmp_path / "data.parquet"))
        coordinator = DatasetMaintenanceCoordinator("pyarrow")

        with pytest.raises(ValueError, match="exactly one timestamp.*candidates"):
            coordinator.plan_global_repartition_deduplication(
                str(tmp_path),
                ["year"],
                filesystem=fs,
                derived_partition_columns={"year": ("year", "auto")},
            )

    def test_derived_partition_planning_validates_function_type_and_collision(
        self, sample_table, tmp_path
    ):
        fs = fsspec_filesystem("file")
        pq.write_table(sample_table, str(tmp_path / "data.parquet"))
        coordinator = DatasetMaintenanceCoordinator("pyarrow")

        with pytest.raises(ValueError, match="Unknown derived partition function"):
            coordinator.plan_global_repartition_deduplication(
                str(tmp_path),
                ["year"],
                filesystem=fs,
                derived_partition_columns={"year": ("quarter", "a")},
            )
        with pytest.raises(ValueError, match="collides with the source schema"):
            coordinator.plan_global_repartition_deduplication(
                str(tmp_path),
                ["a"],
                filesystem=fs,
                derived_partition_columns={"a": ("year", "a")},
            )
        with pytest.raises(ValueError, match="must be timestamp"):
            coordinator.plan_global_repartition_deduplication(
                str(tmp_path),
                ["year"],
                filesystem=fs,
                derived_partition_columns={"year": ("year", "a")},
            )

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

    def test_plan_schema_promotes_string_to_large_string(self):
        """``string`` plus ``large_string`` reconcile to ``large_string``."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(
            f"{root}/a.parquet",
            _parquet_bytes(pa.table({"value": pa.array(["a"], type=pa.string())})),
        )
        fs.pipe(
            f"{root}/b.parquet",
            _parquet_bytes(
                pa.table({"value": pa.array(["b"], type=pa.large_string())})
            ),
        )

        plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
            root, fs, target_rows_per_file=10
        )

        assert plan.schema_outcome == SchemaOutcome.LOSSLESS_PROMOTED
        assert plan.schema is not None
        target = plan.schema
        assert target.field("value").type == pa.large_string()

    def test_plan_schema_promotes_binary_to_large_binary(self):
        """``binary`` plus ``large_binary`` reconcile to ``large_binary``."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(
            f"{root}/a.parquet",
            _parquet_bytes(pa.table({"value": pa.array([b"a"], type=pa.binary())})),
        )
        fs.pipe(
            f"{root}/b.parquet",
            _parquet_bytes(
                pa.table({"value": pa.array([b"b"], type=pa.large_binary())})
            ),
        )

        plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
            root, fs, target_rows_per_file=10
        )

        assert plan.schema_outcome == SchemaOutcome.LOSSLESS_PROMOTED
        assert plan.schema is not None
        target = plan.schema
        assert target.field("value").type == pa.large_binary()

    def test_plan_schema_promotes_nested_list_string(self):
        """Offset-width promotion recurses into nested list item types."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(
            f"{root}/a.parquet",
            _parquet_bytes(
                pa.table({"value": pa.array([["a"]], type=pa.list_(pa.string()))})
            ),
        )
        fs.pipe(
            f"{root}/b.parquet",
            _parquet_bytes(
                pa.table(
                    {
                        "value": pa.array(
                            [["b"]],
                            type=pa.large_list(pa.large_string()),
                        )
                    }
                )
            ),
        )

        plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
            root, fs, target_rows_per_file=10
        )

        assert plan.schema_outcome == SchemaOutcome.LOSSLESS_PROMOTED
        assert plan.schema is not None
        target = plan.schema
        assert target.field("value").type == pa.large_list(pa.large_string())

    def test_plan_schema_promotes_integer_widening(self):
        """Same-signed integer width differences promote to the wider type."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(
            f"{root}/a.parquet",
            _parquet_bytes(pa.table({"value": pa.array([1], type=pa.int32())})),
        )
        fs.pipe(
            f"{root}/b.parquet",
            _parquet_bytes(pa.table({"value": pa.array([2], type=pa.int64())})),
        )

        plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
            root, fs, target_rows_per_file=10
        )

        assert plan.schema_outcome == SchemaOutcome.LOSSLESS_PROMOTED
        assert plan.schema is not None
        target = plan.schema
        assert target.field("value").type == pa.int64()

    def test_plan_schema_incompatible_types_still_reject(self):
        """Genuinely incompatible types invalidate the plan before mutation."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(
            f"{root}/a.parquet",
            _parquet_bytes(pa.table({"value": pa.array([1], type=pa.int64())})),
        )
        fs.pipe(
            f"{root}/b.parquet",
            _parquet_bytes(pa.table({"value": pa.array(["x"], type=pa.string())})),
        )

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(ValueError, match="Schema reconciliation required"):
            coordinator.plan_compaction(root, fs, target_rows_per_file=10)

    def test_plan_schema_rejects_metadata_conflict(self):
        """Field metadata conflicts invalidate the plan."""
        root = _memory_root()
        fs = MemoryFileSystem()
        base = pa.table(
            {"value": pa.array(["a"], type=pa.string())},
        )
        with_meta = base.replace_schema_metadata(None).cast(
            pa.schema([pa.field("value", pa.string(), metadata={b"k": b"v"})])
        )
        fs.pipe(f"{root}/a.parquet", _parquet_bytes(base))
        fs.pipe(f"{root}/b.parquet", _parquet_bytes(with_meta))

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(ValueError, match="Schema reconciliation required"):
            coordinator.plan_compaction(root, fs, target_rows_per_file=10)

    def test_plan_schema_rejects_nested_struct_metadata_conflict(self):
        """Nested struct field metadata conflicts invalidate the plan."""
        root = _memory_root()
        fs = MemoryFileSystem()
        matching_meta = pa.field("nested", pa.string(), metadata={b"k": b"v"})
        conflicting_meta = pa.field("nested", pa.string(), metadata={b"k": b"x"})
        fs.pipe(
            f"{root}/a.parquet",
            _parquet_bytes(
                pa.table({"nested": pa.array([["a"]], type=pa.list_(matching_meta))})
            ),
        )
        fs.pipe(
            f"{root}/b.parquet",
            _parquet_bytes(
                pa.table(
                    {"nested": pa.array([["b"]], type=pa.large_list(conflicting_meta))}
                )
            ),
        )

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        with pytest.raises(ValueError, match="Schema reconciliation required"):
            coordinator.plan_compaction(root, fs, target_rows_per_file=10)

    def test_plan_schema_rejects_nullability_mismatch(self):
        """Nullability differences invalidate the plan (order-independent)."""
        root = _memory_root()
        fs = MemoryFileSystem()
        fs.pipe(
            f"{root}/a.parquet",
            _parquet_bytes(
                pa.table({"value": pa.array(["a"], type=pa.string())}).cast(
                    pa.schema([pa.field("value", pa.string(), nullable=False)])
                )
            ),
        )
        fs.pipe(
            f"{root}/b.parquet",
            _parquet_bytes(
                pa.table({"value": pa.array(["b"], type=pa.large_string())})
            ),
        )

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


class TestFooterReadConsolidation:
    """Issue #66: planning reads each Parquet footer exactly once.

    ``collect_dataset_stats`` captures ``schema_arrow`` and the codec set in
    the single footer open it already performs for ``num_rows``; the schema
    and codec consumers reuse that cache instead of re-opening each footer.
    """

    def test_planning_opens_each_footer_once(self, sample_table, tmp_path, monkeypatch):
        """A codec-aware planning pass opens each footer once, not three times.

        Before the fix each in-scope file was opened three times during
        planning (rows + schema + codecs). ``collect_dataset_stats`` now
        harvests all three from a single open, so the whole pass opens each
        footer exactly once.
        """
        fs = fsspec_filesystem("file")
        for name in ("a", "b", "c"):
            pq.write_table(sample_table, str(tmp_path / f"{name}.parquet"))

        original_parquet_file = pq.ParquetFile
        footer_opens = {"count": 0}

        def counting_parquet_file(*args, **kwargs):
            footer_opens["count"] += 1
            return original_parquet_file(*args, **kwargs)

        monkeypatch.setattr(pq, "ParquetFile", counting_parquet_file)

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(
            str(tmp_path), fs, target_rows_per_file=100, codec="zstd"
        )

        # Three files, one footer open each during the whole planning pass —
        # down from three per file before consolidation.
        assert footer_opens["count"] == 3
        assert len(plan.source_snapshot.files) == 3

    def test_planning_stats_capture_footer_metadata(self, sample_table, tmp_path):
        """Planning captures schema + codecs from the single footer open."""
        fs = fsspec_filesystem("file")
        path = tmp_path / "gzip.parquet"
        pq.write_table(sample_table, str(path), compression="gzip")

        result = _collect_dataset_stats(
            fs, str(tmp_path), None, capture_footer_metadata=True
        )
        file_info = result["files"][0]

        assert file_info["num_rows"] == sample_table.num_rows
        # Cached schema matches a direct footer read.
        expected_schema = pq.ParquetFile(str(path)).schema_arrow
        assert file_info["schema_arrow"].equals(expected_schema)
        # Cached codec set matches a direct footer read.
        assert file_info["codecs"] == frozenset({"gzip"})

    def test_public_collect_dataset_stats_contract_unchanged(
        self, sample_table, tmp_path
    ):
        """The public stats dict exposes only path/size/num_rows (no footer objects)."""
        fs = fsspec_filesystem("file")
        pq.write_table(sample_table, str(tmp_path / "a.parquet"))

        stats = collect_dataset_stats(str(tmp_path), fs)
        file_info = stats["files"][0]

        # No pyarrow schema/frozenset leaks through the supported public surface.
        assert set(file_info.keys()) == {"path", "size_bytes", "num_rows"}
        assert stats["total_rows"] == sample_table.num_rows

    def test_parquet_codecs_uses_cache_without_opening(self):
        """A cached codec set is returned directly, ignoring the filesystem."""
        cached = frozenset({"zstd"})
        assert _parquet_codecs(MemoryFileSystem(), "/ignored.parquet", cached) == cached

    def test_parquet_codecs_falls_back_to_opening(self, sample_table, tmp_path):
        """Without a cache, _parquet_codecs opens the footer (backwards-compat)."""
        fs = fsspec_filesystem("file")
        path = tmp_path / "snappy.parquet"
        pq.write_table(sample_table, str(path), compression="snappy")
        assert _parquet_codecs(fs, str(path)) == frozenset({"snappy"})

    def test_reconcile_schema_falls_back_without_cache(self, tmp_path):
        """file_stats lacking the cached schema still reconcile by opening."""
        fs = fsspec_filesystem("file")
        table = pa.table({"a": [1]})
        a = tmp_path / "a.parquet"
        b = tmp_path / "b.parquet"
        pq.write_table(table, str(a))
        pq.write_table(table, str(b))
        # file_stats lack the cached "schema_arrow" key, like older callers.
        file_stats = [
            {"path": str(a), "size_bytes": 10, "num_rows": 1},
            {"path": str(b), "size_bytes": 10, "num_rows": 1},
        ]
        outcome, schema = _reconcile_schema(fs, file_stats)
        assert outcome == SchemaOutcome.LOSSLESS_PRESERVED
        assert schema is not None


class TestPrecollectedFileStats:
    """Issue #67: planning accepts pre-collected file stats to skip the footer scan.

    Callers that maintain a Parquet ``_metadata`` sidecar (e.g. pydala2) can
    hand per-file ``{path, size_bytes, num_rows}`` plus an optional
    schema/codec snapshot to the planner. When supplied, planning skips the
    ``fs.ls`` walk and the footer scan while still applying the partition
    filter, source-snapshot capture, schema reconciliation, and grouping.
    """

    def test_planning_skips_walk_and_footer_scan(
        self, sample_table, tmp_path, monkeypatch
    ):
        """Supplying file_stats performs zero fs.ls walks and zero footer opens."""
        fs = fsspec_filesystem("file")
        for name in ("a", "b"):
            pq.write_table(sample_table, str(tmp_path / f"{name}.parquet"))

        # Capture a real schema from one file to simulate a sidecar snapshot.
        schema = pq.ParquetFile(str(tmp_path / "a.parquet")).schema_arrow
        file_stats = [
            {
                "path": str(tmp_path / "a.parquet"),
                "size_bytes": (tmp_path / "a.parquet").stat().st_size,
                "num_rows": sample_table.num_rows,
                "schema_arrow": schema,
                "codecs": frozenset({"snappy"}),
            },
            {
                "path": str(tmp_path / "b.parquet"),
                "size_bytes": (tmp_path / "b.parquet").stat().st_size,
                "num_rows": sample_table.num_rows,
                "schema_arrow": schema,
                "codecs": frozenset({"snappy"}),
            },
        ]

        original_parquet_file = pq.ParquetFile
        footer_opens = {"count": 0}

        def counting_parquet_file(*args, **kwargs):
            footer_opens["count"] += 1
            return original_parquet_file(*args, **kwargs)

        import fsspeckit.core.maintenance as maint

        walk_calls = {"count": 0}
        original_discover = maint._discover_parquet_files

        def counting_discover(*args, **kwargs):
            walk_calls["count"] += 1
            return original_discover(*args, **kwargs)

        monkeypatch.setattr(pq, "ParquetFile", counting_parquet_file)
        monkeypatch.setattr(maint, "_discover_parquet_files", counting_discover)

        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(
            str(tmp_path), fs, target_rows_per_file=100, file_stats=file_stats
        )

        # Zero footer opens and zero directory walks — the whole win of #67.
        assert footer_opens["count"] == 0
        assert walk_calls["count"] == 0
        assert len(plan.source_snapshot.files) == 2
        assert plan.source_snapshot.total_rows == sample_table.num_rows * 2
        assert plan.schema_outcome == SchemaOutcome.LOSSLESS_PRESERVED

    def test_planning_still_applies_partition_filter(self, sample_table, tmp_path):
        """partition_filter restricts pre-collected stats to matching partitions."""
        fs = fsspec_filesystem("file")
        (tmp_path / "date=2025-01-01").mkdir()
        (tmp_path / "date=2025-01-02").mkdir()
        pq.write_table(sample_table, str(tmp_path / "date=2025-01-01" / "a.parquet"))
        pq.write_table(sample_table, str(tmp_path / "date=2025-01-02" / "b.parquet"))

        file_stats = [
            {
                "path": str(tmp_path / "date=2025-01-01" / "a.parquet"),
                "size_bytes": 10,
                "num_rows": sample_table.num_rows,
            },
            {
                "path": str(tmp_path / "date=2025-01-02" / "b.parquet"),
                "size_bytes": 10,
                "num_rows": sample_table.num_rows,
            },
        ]

        plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
            str(tmp_path),
            fs,
            target_rows_per_file=100,
            partition_filter=["date=2025-01-01"],
            file_stats=file_stats,
        )

        scoped_paths = {f.absolute_path for f in plan.source_snapshot.files}
        assert len(scoped_paths) == 1
        assert str(tmp_path / "date=2025-01-01" / "a.parquet") in scoped_paths
        assert plan.partition_scope.scope_type == PartitionScopeType.FILTERED

    def test_planning_uses_supplied_schema_without_footer_reads(
        self, sample_table, tmp_path
    ):
        """A supplied schema_arrow makes schema reconciliation footer-free."""
        fs = fsspec_filesystem("file")
        pq.write_table(sample_table, str(tmp_path / "a.parquet"))
        schema = pq.ParquetFile(str(tmp_path / "a.parquet")).schema_arrow

        file_stats = [
            {
                "path": str(tmp_path / "a.parquet"),
                "size_bytes": 10,
                "num_rows": sample_table.num_rows,
                "schema_arrow": schema,
            }
        ]

        footer_opens = {"count": 0}
        original_parquet_file = pq.ParquetFile

        def counting_parquet_file(*args, **kwargs):
            footer_opens["count"] += 1
            return original_parquet_file(*args, **kwargs)

        pq.ParquetFile = counting_parquet_file
        try:
            plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
                str(tmp_path), fs, target_rows_per_file=100, file_stats=file_stats
            )
        finally:
            pq.ParquetFile = original_parquet_file

        assert footer_opens["count"] == 0
        assert plan.schema is not None
        assert plan.schema.equals(schema)

    def test_codec_aware_planning_uses_supplied_codecs(self, sample_table, tmp_path):
        """Supplied codecs drive force-rewrite without reopening the footer."""
        fs = fsspec_filesystem("file")
        pq.write_table(sample_table, str(tmp_path / "a.parquet"))
        schema = pq.ParquetFile(str(tmp_path / "a.parquet")).schema_arrow

        file_stats = [
            {
                "path": str(tmp_path / "a.parquet"),
                "size_bytes": 10,
                "num_rows": sample_table.num_rows,
                "schema_arrow": schema,
                "codecs": frozenset({"gzip"}),
            }
        ]

        footer_opens = {"count": 0}
        original_parquet_file = pq.ParquetFile

        def counting_parquet_file(*args, **kwargs):
            footer_opens["count"] += 1
            return original_parquet_file(*args, **kwargs)

        pq.ParquetFile = counting_parquet_file
        try:
            plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
                str(tmp_path),
                fs,
                target_rows_per_file=100,
                codec="zstd",
                file_stats=file_stats,
            )
        finally:
            pq.ParquetFile = original_parquet_file

        # The mismatched cached codec forces the singleton into a rewrite group.
        assert footer_opens["count"] == 0
        assert len(plan.compaction_groups) == 1

    def test_file_stats_accepts_fileinfo_objects(self, sample_table, tmp_path):
        """FileInfo objects are accepted alongside dicts."""
        fs = fsspec_filesystem("file")
        pq.write_table(sample_table, str(tmp_path / "a.parquet"))

        file_stats = [
            FileInfo(
                path=str(tmp_path / "a.parquet"),
                size_bytes=10,
                num_rows=sample_table.num_rows,
            )
        ]

        plan = DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
            str(tmp_path), fs, target_rows_per_file=100, file_stats=file_stats
        )
        assert len(plan.source_snapshot.files) == 1

    def test_empty_after_partition_filter_raises(self, sample_table, tmp_path):
        """Stats matching no partition filter raise FileNotFoundError."""
        fs = fsspec_filesystem("file")
        file_stats = [
            {
                "path": str(tmp_path / "date=2025-01-01" / "a.parquet"),
                "size_bytes": 10,
                "num_rows": 1,
            }
        ]
        with pytest.raises(FileNotFoundError):
            DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
                str(tmp_path),
                fs,
                partition_filter=["date=2099-01-01"],
                file_stats=file_stats,
            )

    def test_empty_file_stats_list_raises(self, sample_table, tmp_path):
        """An explicitly empty file_stats list is a ValueError, not a walk."""
        fs = fsspec_filesystem("file")
        with pytest.raises(ValueError, match="non-empty"):
            DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
                str(tmp_path), fs, file_stats=[]
            )

    @pytest.mark.parametrize("missing", ["size_bytes", "num_rows"])
    def test_missing_required_key_raises(self, sample_table, tmp_path, missing):
        """Each entry must carry path, size_bytes and num_rows."""
        fs = fsspec_filesystem("file")
        entry = {"path": str(tmp_path / "a.parquet"), "size_bytes": 1, "num_rows": 1}
        entry.pop(missing)
        with pytest.raises(ValueError, match="must include"):
            DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
                str(tmp_path), fs, file_stats=[entry]
            )

    def test_negative_values_raise(self, sample_table, tmp_path):
        """Negative size_bytes or num_rows are rejected."""
        fs = fsspec_filesystem("file")
        entry = {
            "path": str(tmp_path / "a.parquet"),
            "size_bytes": -1,
            "num_rows": 1,
        }
        with pytest.raises(ValueError, match="non-negative"):
            DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
                str(tmp_path), fs, file_stats=[entry]
            )

    def test_repartition_threads_file_stats(self, sample_table, tmp_path):
        """file_stats is threaded through plan_repartition (no partition_filter)."""
        fs = fsspec_filesystem("file")
        for name in ("a", "b"):
            pq.write_table(
                pa.table({"id": [1, 2], "region": ["eu", "us"]}),
                str(tmp_path / f"{name}.parquet"),
            )
        schema = pq.ParquetFile(str(tmp_path / "a.parquet")).schema_arrow
        file_stats = [
            {
                "path": str(tmp_path / "a.parquet"),
                "size_bytes": 10,
                "num_rows": 2,
                "schema_arrow": schema,
            },
            {
                "path": str(tmp_path / "b.parquet"),
                "size_bytes": 10,
                "num_rows": 2,
                "schema_arrow": schema,
            },
        ]
        from fsspeckit.core.maintenance import RepartitionPlan

        plan = DatasetMaintenanceCoordinator("pyarrow").plan_repartition(
            str(tmp_path),
            partition_columns=["region"],
            filesystem=fs,
            file_stats=file_stats,
        )
        assert isinstance(plan, RepartitionPlan)
        assert len(plan.source_snapshot.files) == 2

    def test_execute_plan_from_advisory_sizes_no_false_drift(
        self, sample_table, tmp_path
    ):
        """A plan built from advisory sizes executes without false drift.

        The source snapshot records the true on-disk size (via fs.info), so
        an advisory sidecar ``size_bytes`` used only for grouping never trips
        drift detection at execution time (#67).
        """
        fs = fsspec_filesystem("file")
        for name in ("a", "b"):
            pq.write_table(sample_table, str(tmp_path / f"{name}.parquet"))
        schema = pq.ParquetFile(str(tmp_path / "a.parquet")).schema_arrow

        # Advisory size_bytes deliberately wrong (1) vs the real on-disk size.
        file_stats = [
            {
                "path": str(tmp_path / "a.parquet"),
                "size_bytes": 1,
                "num_rows": sample_table.num_rows,
                "schema_arrow": schema,
                "codecs": frozenset({"snappy"}),
            },
            {
                "path": str(tmp_path / "b.parquet"),
                "size_bytes": 1,
                "num_rows": sample_table.num_rows,
                "schema_arrow": schema,
                "codecs": frozenset({"snappy"}),
            },
        ]
        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        plan = coordinator.plan_compaction(
            str(tmp_path), fs, target_rows_per_file=100, file_stats=file_stats
        )
        # The snapshot size is the real on-disk size, not the advisory 1.
        assert all(f.size_bytes > 1 for f in plan.source_snapshot.files)
        result = coordinator.execute(plan)  # nosec B608 - not a SQL sink
        assert result.succeeded, result.error

    def test_three_key_only_stats_still_open_footer_for_schema(
        self, sample_table, tmp_path
    ):
        """Without a supplied schema, reconciliation falls back to the footer.

        The required three keys {path, size_bytes, num_rows} skip the walk but
        not the schema read; zero-footer planning needs the optional snapshot.
        """
        fs = fsspec_filesystem("file")
        pq.write_table(sample_table, str(tmp_path / "a.parquet"))
        file_stats = [
            {
                "path": str(tmp_path / "a.parquet"),
                "size_bytes": (tmp_path / "a.parquet").stat().st_size,
                "num_rows": sample_table.num_rows,
            }
        ]
        footer_opens = {"count": 0}
        original_parquet_file = pq.ParquetFile

        def counting_parquet_file(*args, **kwargs):
            footer_opens["count"] += 1
            return original_parquet_file(*args, **kwargs)

        pq.ParquetFile = counting_parquet_file
        try:
            DatasetMaintenanceCoordinator("pyarrow").plan_compaction(
                str(tmp_path), fs, target_rows_per_file=100, file_stats=file_stats
            )
        finally:
            pq.ParquetFile = original_parquet_file
        # Schema reconciliation opened the footer (no snapshot was supplied).
        assert footer_opens["count"] >= 1

    def test_file_stats_threaded_through_dedup_and_ordered(
        self, sample_table, tmp_path
    ):
        """file_stats threads through deduplication and ordered-compaction too."""
        fs = fsspec_filesystem("file")
        for name in ("a", "b"):
            pq.write_table(sample_table, str(tmp_path / f"{name}.parquet"))
        schema = pq.ParquetFile(str(tmp_path / "a.parquet")).schema_arrow
        file_stats = [
            {
                "path": str(tmp_path / f"{name}.parquet"),
                "size_bytes": (tmp_path / f"{name}.parquet").stat().st_size,
                "num_rows": sample_table.num_rows,
                "schema_arrow": schema,
            }
            for name in ("a", "b")
        ]
        coordinator = DatasetMaintenanceCoordinator("pyarrow")
        dedup_plan = coordinator.plan_partition_local_deduplication(
            str(tmp_path), fs, key_columns=["a"], file_stats=file_stats
        )
        assert len(dedup_plan.source_snapshot.files) == 2
        ordered_plan = coordinator.plan_ordered_compaction(
            str(tmp_path), sort_keys=["a"], filesystem=fs, file_stats=file_stats
        )
        assert len(ordered_plan.source_snapshot.files) == 2
