"""Tests for backend-neutral maintenance execution templates."""

from typing import Any

from fsspec.implementations.memory import MemoryFileSystem

from fsspeckit.core.maintenance import (
    CompactionGroup,
    FileInfo,
    MaintenanceStats,
    execute_compaction_template,
)


class FakeCompactRecorder:
    """Records compaction callback invocations."""

    def __init__(self) -> None:
        self.calls: list[tuple[CompactionGroup, str]] = []

    def __call__(self, group: CompactionGroup, output_path: str) -> None:
        self.calls.append((group, output_path))


def make_stats(**overrides: Any) -> MaintenanceStats:
    """Create a baseline MaintenanceStats with optional overrides."""
    defaults: dict[str, Any] = {
        "before_file_count": 2,
        "after_file_count": 1,
        "before_total_bytes": 200,
        "after_total_bytes": 100,
        "compacted_file_count": 1,
        "rewritten_bytes": 200,
    }
    defaults.update(overrides)
    return MaintenanceStats(**defaults)


class TestExecuteCompactionTemplate:
    """Tests for execute_compaction_template."""

    def test_dry_run_returns_planned_groups_without_callback(self):
        """Dry-run should return planned groups without invoking compact_group_fn."""
        files = [FileInfo(path="a.parquet", size_bytes=10, num_rows=1)]
        group = CompactionGroup(files=tuple(files))
        planned_stats = make_stats()
        recorder = FakeCompactRecorder()
        fs = MemoryFileSystem()

        result = execute_compaction_template(
            groups=[group],
            planned_stats=planned_stats,
            dataset_path="/dataset",
            compact_group_fn=recorder,
            filesystem=fs,
            dry_run=True,
        )

        expected = planned_stats.to_dict()
        expected["planned_groups"] = [["a.parquet"]]
        assert result == expected
        assert recorder.calls == []

    def test_empty_groups_returns_stats_without_callback(self):
        """Empty groups should return stats without invoking compact_group_fn."""
        planned_stats = make_stats()
        recorder = FakeCompactRecorder()
        fs = MemoryFileSystem()

        result = execute_compaction_template(
            groups=[],
            planned_stats=planned_stats,
            dataset_path="/dataset",
            compact_group_fn=recorder,
            filesystem=fs,
            dry_run=False,
        )

        assert result == planned_stats.to_dict()
        assert "planned_groups" not in result
        assert recorder.calls == []

    def test_normal_execution_compacts_and_removes_originals(self):
        """Normal execution should invoke callback and remove original files."""
        fs = MemoryFileSystem()
        fs.pipe("/dataset/g1/a.parquet", b"data-a")
        fs.pipe("/dataset/g1/b.parquet", b"data-b")
        fs.pipe("/dataset/g2/c.parquet", b"data-c")

        group1 = CompactionGroup(
            files=(
                FileInfo(path="/dataset/g1/a.parquet", size_bytes=5, num_rows=1),
                FileInfo(path="/dataset/g1/b.parquet", size_bytes=5, num_rows=1),
            )
        )
        group2 = CompactionGroup(
            files=(FileInfo(path="/dataset/g2/c.parquet", size_bytes=5, num_rows=1),)
        )
        planned_stats = make_stats(
            before_file_count=3,
            after_file_count=2,
            before_total_bytes=15,
            after_total_bytes=10,
            compacted_file_count=2,
            rewritten_bytes=15,
        )
        recorder = FakeCompactRecorder()

        result = execute_compaction_template(
            groups=[group1, group2],
            planned_stats=planned_stats,
            dataset_path="/dataset",
            compact_group_fn=recorder,
            filesystem=fs,
            dry_run=False,
        )

        assert result == planned_stats.to_dict()
        assert len(recorder.calls) == 2

        # Verify callback received the correct groups and generated output paths.
        assert recorder.calls[0][0] is group1
        assert recorder.calls[1][0] is group2
        for _, output_path in recorder.calls:
            assert output_path.startswith("/dataset/compacted-")
            assert output_path.endswith(".parquet")

        # Original files should be removed.
        assert not fs.exists("/dataset/g1/a.parquet")
        assert not fs.exists("/dataset/g1/b.parquet")
        assert not fs.exists("/dataset/g2/c.parquet")

    def test_dry_run_with_empty_groups(self):
        """Dry-run with empty groups returns stats without planned_groups."""
        planned_stats = make_stats()
        recorder = FakeCompactRecorder()
        fs = MemoryFileSystem()

        result = execute_compaction_template(
            groups=[],
            planned_stats=planned_stats,
            dataset_path="/dataset",
            compact_group_fn=recorder,
            filesystem=fs,
            dry_run=True,
        )

        expected = planned_stats.to_dict()
        expected["planned_groups"] = []
        assert result == expected
        assert recorder.calls == []
