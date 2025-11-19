"""Tests for the backend-neutral maintenance core module."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from fsspeckit.core.maintenance import (
    FileInfo,
    MaintenanceStats,
    CompactionGroup,
    collect_dataset_stats,
    plan_compaction_groups,
    plan_optimize_groups,
)


class TestFileInfo:
    """Test FileInfo dataclass."""

    def test_file_info_creation(self):
        """Test FileInfo creation with valid data."""
        file_info = FileInfo("test.parquet", 1024, 100)
        assert file_info.path == "test.parquet"
        assert file_info.size_bytes == 1024
        assert file_info.num_rows == 100

    def test_file_info_validation(self):
        """Test FileInfo validation rejects invalid data."""
        with pytest.raises(ValueError, match="size_bytes must be >= 0"):
            FileInfo("test.parquet", -1, 100)

        with pytest.raises(ValueError, match="num_rows must be >= 0"):
            FileInfo("test.parquet", 1024, -1)


class TestMaintenanceStats:
    """Test MaintenanceStats dataclass."""

    def test_maintenance_stats_creation(self):
        """Test MaintenanceStats creation with valid data."""
        stats = MaintenanceStats(
            before_file_count=10,
            after_file_count=5,
            before_total_bytes=1000,
            after_total_bytes=800,
            compacted_file_count=5,
            rewritten_bytes=500,
        )
        assert stats.before_file_count == 10
        assert stats.after_file_count == 5
        assert stats.compressed_file_count == 5
        assert stats.dry_run is False  # Default value

    def test_maintenance_stats_validation(self):
        """Test MaintenanceStats validation rejects invalid data."""
        with pytest.raises(ValueError, match="before_file_count must be >= 0"):
            MaintenanceStats(
                before_file_count=-1,
                after_file_count=5,
                before_total_bytes=1000,
                after_total_bytes=800,
                compacted_file_count=5,
                rewritten_bytes=500,
            )

    def test_maintenance_stats_to_dict(self):
        """Test MaintenanceStats to_dict conversion."""
        stats = MaintenanceStats(
            before_file_count=10,
            after_file_count=5,
            before_total_bytes=1000,
            after_total_bytes=800,
            compacted_file_count=5,
            rewritten_bytes=500,
            zorder_columns=["col1", "col2"],
            planned_groups=[["file1", "file2"]],
        )

        result = stats.to_dict()

        assert result["before_file_count"] == 10
        assert result["after_file_count"] == 5
        assert result["zorder_columns"] == ["col1", "col2"]
        assert result["planned_groups"] == [["file1", "file2"]]


class TestCompactionGroup:
    """Test CompactionGroup dataclass."""

    def test_compaction_group_creation(self):
        """Test CompactionGroup creation with valid data."""
        files = [
            FileInfo("file1.parquet", 512, 50),
            FileInfo("file2.parquet", 256, 25),
        ]
        group = CompactionGroup(files=files)

        assert group.file_count == 2
        assert group.total_size_bytes == 768
        assert group.total_rows == 75

    def test_compaction_group_single_file(self):
        """Test CompactionGroup with single file."""
        file = FileInfo("single.parquet", 1024, 100)
        group = CompactionGroup(files=[file])

        assert group.file_count == 1
        assert group.total_size_bytes == 1024
        assert group.total_rows == 100

    def test_compaction_group_validation(self):
        """Test CompactionGroup validation rejects empty files list."""
        with pytest.raises(ValueError, match="CompactionGroup must contain at least one file"):
            CompactionGroup(files=[])

    def test_compaction_group_file_paths(self):
        """Test CompactionGroup file_paths method."""
        files = [
            FileInfo("path/file1.parquet", 512, 50),
            FileInfo("path/file2.parquet", 256, 25),
        ]
        group = CompactionGroup(files=files)

        paths = group.file_paths()
        assert len(paths) == 2
        assert "path/file1.parquet" in paths
        assert "path/file2.parquet" in paths


class TestPlanCompactionGroups:
    """Test plan_compaction_groups function."""

    def test_plan_compaction_groups_basic(self):
        """Test basic compaction grouping logic."""
        files = [
            FileInfo("small1.parquet", 10, 10),
            FileInfo("small2.parquet", 20, 20),
            FileInfo("small3.parquet", 15, 15),
            FileInfo("large.parquet", 1000, 1000),  # Should be left alone
        ]

        result = plan_compaction_groups(files, target_mb_per_file=64)

        assert "groups" in result
        assert "untouched_files" in result
        assert "planned_stats" in result
        assert "planned_groups" in result

        # Check that large file is untouched
        untouched_paths = [f.path for f in result["untouched_files"]]
        assert "large.parquet" in untouched_paths

    def test_plan_compaction_groups_by_rows(self):
        """Test compaction grouping by row count."""
        files = [
            FileInfo("file1.parquet", 100, 10),
            FileInfo("file2.parquet", 100, 15),
            FileInfo("file3.parquet", 100, 20),
            FileInfo("file4.parquet", 100, 100),  # Should be left alone
        ]

        result = plan_compaction_groups(files, target_rows_per_file=50)

        # Files with 10, 15, 20 rows should be grouped (total 45 < 50)
        # File with 100 rows should be left alone
        untouched_paths = [f.path for f in result["untouched_files"]]
        assert "file4.parquet" in untouched_paths

    def test_plan_compaction_groups_no_thresholds(self):
        """Test that providing no thresholds raises ValueError."""
        files = [FileInfo("test.parquet", 100, 10)]

        with pytest.raises(ValueError, match="Must provide at least one of"):
            plan_compaction_groups(files, target_mb_per_file=None, target_rows_per_file=None)

    def test_plan_compaction_groups_invalid_thresholds(self):
        """Test that invalid thresholds raise ValueError."""
        files = [FileInfo("test.parquet", 100, 10)]

        with pytest.raises(ValueError, match="target_mb_per_file must be > 0"):
            plan_compaction_groups(files, target_mb_per_file=0)

        with pytest.raises(ValueError, match="target_rows_per_file must be > 0"):
            plan_compaction_groups(files, target_rows_per_file=0)

    def test_plan_compaction_groups_dict_input(self):
        """Test that dict input (legacy format) works."""
        files_dict = [
            {"path": "file1.parquet", "size_bytes": 100, "num_rows": 10},
            {"path": "file2.parquet", "size_bytes": 200, "num_rows": 20},
        ]

        result = plan_compaction_groups(files_dict, target_mb_per_file=1)

        # Should work without errors and create FileInfo objects internally
        assert len(result["groups"]) >= 0
        assert result["planned_stats"].before_file_count == 2

    def test_plan_compaction_groups_no_groups(self):
        """Test behavior when no compaction groups are needed."""
        files = [
            FileInfo("large1.parquet", 2000, 200),  # Above threshold
            FileInfo("large2.parquet", 3000, 300),  # Above threshold
        ]

        result = plan_compaction_groups(files, target_mb_per_file=1)

        # No groups should be created
        assert len(result["groups"]) == 0
        assert result["planned_stats"].compacted_file_count == 0

    def test_plan_compaction_groups_dry_run_stats(self):
        """Test that dry_run stats are properly set."""
        files = [
            FileInfo("file1.parquet", 100, 10),
            FileInfo("file2.parquet", 100, 15),
        ]

        result = plan_compaction_groups(files, target_mb_per_file=1)
        stats = result["planned_stats"]

        assert stats.dry_run is True
        assert stats.compression_codec is None  # Should be None, set by caller


class TestPlanOptimizeGroups:
    """Test plan_optimize_groups function."""

    def test_plan_optimize_groups_basic(self):
        """Test basic optimization planning."""
        files = [
            FileInfo("file1.parquet", 100, 10),
            FileInfo("file2.parquet", 200, 20),
        ]
        zorder_columns = ["col1"]

        result = plan_optimize_groups(
            files,
            zorder_columns=zorder_columns,
            target_mb_per_file=1
        )

        assert "groups" in result
        assert "planned_stats" in result
        stats = result["planned_stats"]
        assert stats.zorder_columns == zorder_columns

    def test_plan_optimize_groups_empty_zorder_columns(self):
        """Test that empty zorder_columns raises ValueError."""
        files = [FileInfo("test.parquet", 100, 10)]

        with pytest.raises(ValueError, match="zorder_columns must be a non-empty list"):
            plan_optimize_groups(files, zorder_columns=[])

    def test_plan_optimize_groups_invalid_thresholds(self):
        """Test that invalid thresholds raise ValueError."""
        files = [FileInfo("test.parquet", 100, 10)]

        with pytest.raises(ValueError, match="target_mb_per_file must be > 0"):
            plan_optimize_groups(files, zorder_columns=["col1"], target_mb_per_file=0)

    def test_plan_optimize_groups_with_schema_validation(self):
        """Test schema validation with mock schema."""
        files = [FileInfo("test.parquet", 100, 10)]

        # Mock schema with available columns
        mock_schema = Mock()
        mock_schema.column_names = ["col1", "col2"]

        # Test with valid column
        result = plan_optimize_groups(
            files,
            zorder_columns=["col1"],
            sample_schema=mock_schema
        )
        assert "groups" in result

        # Test with missing column
        with pytest.raises(ValueError, match="Missing z-order columns"):
            plan_optimize_groups(
                files,
                zorder_columns=["missing_col"],
                sample_schema=mock_schema
            )

    def test_plan_optimize_groups_dict_input(self):
        """Test that dict input (legacy format) works."""
        files_dict = [
            {"path": "file1.parquet", "size_bytes": 100, "num_rows": 10},
        ]
        mock_schema = Mock()
        mock_schema.column_names = ["col1"]

        result = plan_optimize_groups(
            files_dict,
            zorder_columns=["col1"],
            sample_schema=mock_schema
        )

        assert "groups" in result
        assert result["planned_stats"].before_file_count == 1

    def test_plan_optimize_groups_single_files(self):
        """Test that single files are included in optimization groups (unlike compaction)."""
        files = [
            FileInfo("single.parquet", 1000, 100),  # Large file, but should be optimized
            FileInfo("single2.parquet", 800, 80),   # Another single file
        ]
        mock_schema = Mock()
        mock_schema.column_names = ["col1"]

        result = plan_optimize_groups(
            files,
            zorder_columns=["col1"],
            sample_schema=mock_schema,
            target_mb_per_file=2  # 2MB threshold
        )

        # Optimization should include single files (unlike compaction)
        total_files_in_groups = sum(len(group.files) for group in result["groups"])
        assert total_files_in_groups >= 2


@pytest.fixture
def mock_filesystem():
    """Mock filesystem for testing collect_dataset_stats."""
    fs = Mock()
    fs.exists.return_value = True
    fs.ls.return_value = ["test.parquet"]
    fs.info.return_value = {"size": 1000}
    fs.open.return_value.__enter__ = Mock()
    fs.open.return_value.__exit__ = Mock()
    fs.isdir.return_value = False
    return fs


class TestCollectDatasetStats:
    """Test collect_dataset_stats function."""

    @patch('fsspeckit.core.maintenance.pq.ParquetFile')
    def test_collect_dataset_stats_basic(self, mock_parquet_file, mock_filesystem):
        """Test basic dataset stats collection."""
        # Mock parquet file metadata
        mock_metadata = Mock()
        mock_metadata.num_rows = 100
        mock_parquet_file.return_value.metadata = mock_metadata

        with patch('fsspeckit.core.maintenance.fsspec_filesystem', return_value=mock_filesystem):
            result = collect_dataset_stats("test_path", mock_filesystem)

        assert "files" in result
        assert "total_bytes" in result
        assert "total_rows" in result
        assert result["total_bytes"] == 1000
        assert result["total_rows"] == 100
        assert len(result["files"]) == 1

    @patch('fsspeckit.core.maintenance.fsspec_filesystem')
    def test_collect_dataset_stats_path_not_found(self, mock_fsspec):
        """Test that non-existent path raises FileNotFoundError."""
        mock_fs = Mock()
        mock_fs.exists.return_value = False
        mock_fsspec.return_value = mock_fs

        with pytest.raises(FileNotFoundError, match="Dataset path.*does not exist"):
            collect_dataset_stats("nonexistent_path")

    @patch('fsspeckit.core.maintenance.fsspec_filesystem')
    @patch('fsspeckit.core.maintenance.pq.ParquetFile')
    @patch('fsspeckit.core.maintenance.pq.read_table')
    def test_collect_dataset_stats_fallback_to_read_table(
        self, mock_read_table, mock_parquet_file, mock_fsspec
    ):
        """Test fallback to read_table when ParquetFile fails."""
        mock_fs = Mock()
        mock_fs.exists.return_value = True
        mock_fs.ls.return_value = ["test.parquet"]
        mock_fs.info.return_value = {"size": 1000}
        mock_fs.open.return_value.__enter__ = Mock()
        mock_fs.open.return_value.__exit__ = Mock()
        mock_fs.isdir.return_value = False
        mock_fsspec.return_value = mock_fs

        # ParquetFile raises exception
        mock_parquet_file.side_effect = Exception("Failed to read metadata")

        # read_table succeeds
        mock_table = Mock()
        mock_table.num_rows = 100
        mock_read_table.return_value = mock_table

        result = collect_dataset_stats("test_path")

        assert result["total_rows"] == 100

    @patch('fsspeckit.core.maintenance.fsspec_filesystem')
    def test_collect_dataset_stats_partition_filter(self, mock_fsspec):
        """Test partition filtering functionality."""
        mock_fs = Mock()
        mock_fs.exists.return_value = True
        mock_fs.ls.return_value = [
            "date=2025-01-01/file1.parquet",
            "date=2025-01-02/file2.parquet",
            "other/file3.parquet"
        ]
        mock_fs.info.return_value = {"size": 1000}
        mock_fs.open.return_value.__enter__ = Mock()
        mock_fs.open.return_value.__exit__ = Mock()
        mock_fs.isdir.return_value = False
        mock_fsspec.return_value = mock_fs

        with patch('fsspeckit.core.maintenance.pq.ParquetFile') as mock_pq:
            mock_metadata = Mock()
            mock_metadata.num_rows = 100
            mock_pq.return_value.metadata = mock_metadata

            # Test partition filter
            result = collect_dataset_stats(
                "test_path",
                mock_fs,
                partition_filter=["date=2025-01-01"]
            )

            # Should only include files matching partition filter
            assert len(result["files"]) == 1
            assert "date=2025-01-01/file1.parquet" in [f["path"] for f in result["files"]]


if __name__ == "__main__":
    pytest.main([__file__])