"""Tests for DuckDB parquet dataset compaction functionality."""

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.datasets.duckdb.connection import create_duckdb_connection
from fsspeckit.datasets.duckdb.dataset import (
    collect_dataset_stats_duckdb,
    compact_parquet_dataset_duckdb,
    DuckDBDatasetIO,
)


@pytest.fixture
def sample_table():
    """Create a sample PyArrow table for testing."""
    return pa.table(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"],
            "value": [10.5, 20.3, 30.1, 40.9, 50.7, 60.5, 70.3, 80.1, 90.9, 100.7],
        }
    )


@pytest.fixture
def duckdb_connection():
    """Create a DuckDB connection for testing."""
    return create_duckdb_connection()


class TestCollectDatasetStatsDuckdb:
    """Tests for collect_dataset_stats_duckdb function."""

    def test_collect_stats_basic(self, tmp_path, sample_table):
        """Test basic stats collection."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Write a single parquet file
        pq.write_table(sample_table, dataset_dir / "data.parquet")

        stats = collect_dataset_stats_duckdb(str(dataset_dir))

        assert "files" in stats
        assert "total_bytes" in stats
        assert "total_rows" in stats
        assert len(stats["files"]) == 1
        assert stats["total_rows"] == 10

    def test_collect_stats_multiple_files(self, tmp_path, sample_table):
        """Test stats collection with multiple files."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Write multiple files
        for i in range(3):
            chunk = sample_table.slice(i * 3, 3)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        stats = collect_dataset_stats_duckdb(str(dataset_dir))

        assert len(stats["files"]) == 3
        assert stats["total_rows"] == 9  # 3 files * 3 rows each

    def test_collect_stats_partition_filter(self, tmp_path, sample_table):
        """Test stats collection with partition filter."""
        dataset_dir = tmp_path / "dataset"
        (dataset_dir / "date=2025-01-01").mkdir(parents=True)
        (dataset_dir / "date=2025-01-02").mkdir(parents=True)

        pq.write_table(sample_table, dataset_dir / "date=2025-01-01" / "data.parquet")
        pq.write_table(sample_table, dataset_dir / "date=2025-01-02" / "data.parquet")

        stats = collect_dataset_stats_duckdb(
            str(dataset_dir), partition_filter=["date=2025-01-01"]
        )

        assert len(stats["files"]) == 1
        assert stats["total_rows"] == 10


class TestCompactParquetDatasetDuckdb:
    """Tests for compact_parquet_dataset_duckdb function."""

    def test_compact_basic(self, tmp_path, sample_table):
        """Test basic compaction of multiple small files."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Create multiple small files
        for i in range(5):
            chunk = sample_table.slice(i * 2, 2)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        # Compact with a target that should group files
        result = compact_parquet_dataset_duckdb(
            str(dataset_dir), target_mb_per_file=1
        )

        assert result["before_file_count"] == 5
        assert result["after_file_count"] < 5  # Files should be compacted
        assert result["compacted_file_count"] > 0

        # Verify data integrity
        files = list(dataset_dir.glob("*.parquet"))
        total_rows = 0
        for f in files:
            table = pq.read_table(f)
            total_rows += table.num_rows
        assert total_rows == 10

    def test_compact_dry_run(self, tmp_path, sample_table):
        """Test dry-run mode doesn't modify files."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Create multiple small files
        for i in range(3):
            chunk = sample_table.slice(i * 3, 3)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        file_count_before = len(list(dataset_dir.glob("*.parquet")))

        result = compact_parquet_dataset_duckdb(
            str(dataset_dir), target_mb_per_file=1, dry_run=True
        )

        assert result["dry_run"] is True
        assert "planned_groups" in result

        # Files should not be modified
        file_count_after = len(list(dataset_dir.glob("*.parquet")))
        assert file_count_before == file_count_after

    def test_compact_with_target_rows(self, tmp_path, sample_table):
        """Test compaction with target_rows_per_file."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Create multiple small files
        for i in range(5):
            chunk = sample_table.slice(i * 2, 2)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        result = compact_parquet_dataset_duckdb(
            str(dataset_dir), target_rows_per_file=5
        )

        assert result["before_file_count"] == 5
        # Files 1-2 (4 rows) and 3-4 (4 rows) get compacted, file 5 (2 rows) stays
        assert result["after_file_count"] == 3  # 2 compacted + 1 singleton left

    def test_compact_with_compression(self, tmp_path, sample_table):
        """Test compaction with compression change."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Create uncompressed files
        for i in range(3):
            chunk = sample_table.slice(i * 3, 3)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet", compression=None)

        result = compact_parquet_dataset_duckdb(
            str(dataset_dir), target_mb_per_file=1, compression="snappy"
        )

        assert result["compression_codec"] == "snappy"
        assert result["after_file_count"] < 3

    def test_compact_no_compaction_needed(self, tmp_path, sample_table):
        """Test when files are already large enough."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Write a single file
        pq.write_table(sample_table, dataset_dir / "large.parquet")

        result = compact_parquet_dataset_duckdb(
            str(dataset_dir), target_mb_per_file=1000  # Very high threshold
        )

        # No compaction should occur (single file, under threshold)
        assert result["after_file_count"] == 1

    def test_compact_partition_filter(self, tmp_path, sample_table):
        """Test compaction with partition filter."""
        dataset_dir = tmp_path / "dataset"
        (dataset_dir / "date=2025-01-01").mkdir(parents=True)
        (dataset_dir / "date=2025-01-02").mkdir(parents=True)

        # Create files in different partitions
        for i in range(3):
            chunk = sample_table.slice(i * 3, 3)
            pq.write_table(chunk, dataset_dir / "date=2025-01-01" / f"part_{i}.parquet")

        for i in range(2):
            chunk = sample_table.slice(i * 5, 5)
            pq.write_table(chunk, dataset_dir / "date=2025-01-02" / f"part_{i}.parquet")

        # Compact only one partition
        result = compact_parquet_dataset_duckdb(
            str(dataset_dir),
            target_mb_per_file=1,
            partition_filter=["date=2025-01-01"],
        )

        # Should only compact files in the filtered partition
        assert result["before_file_count"] == 3  # Only files matching filter

    def test_compact_empty_dataset(self, tmp_path):
        """Test compaction on empty dataset raises error."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        with pytest.raises(FileNotFoundError, match="No parquet files found"):
            compact_parquet_dataset_duckdb(str(dataset_dir))

    def test_compact_invalid_thresholds(self, tmp_path, sample_table):
        """Test compaction with invalid thresholds raises error."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        pq.write_table(sample_table, dataset_dir / "data.parquet")

        with pytest.raises(ValueError, match="target_mb_per_file must be > 0"):
            compact_parquet_dataset_duckdb(str(dataset_dir), target_mb_per_file=0)


class TestDuckDBDatasetIOCompact:
    """Tests for DuckDBDatasetIO.compact_parquet_dataset method."""

    def test_class_method_basic(self, tmp_path, sample_table, duckdb_connection):
        """Test the class method for compaction."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Create multiple small files
        for i in range(4):
            chunk = sample_table.slice(i * 2, 2)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        io = DuckDBDatasetIO(duckdb_connection)

        result = io.compact_parquet_dataset(str(dataset_dir), target_mb_per_file=1)

        assert "before_file_count" in result
        assert "after_file_count" in result
        assert result["before_file_count"] == 4

    def test_class_method_dry_run(self, tmp_path, sample_table, duckdb_connection):
        """Test dry-run via class method."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        pq.write_table(sample_table, dataset_dir / "data.parquet")

        io = DuckDBDatasetIO(duckdb_connection)

        result = io.compact_parquet_dataset(
            str(dataset_dir), target_mb_per_file=1, dry_run=True
        )

        assert result["dry_run"] is True

    def test_class_method_with_compression(self, tmp_path, sample_table, duckdb_connection):
        """Test compression change via class method."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        for i in range(3):
            chunk = sample_table.slice(i * 3, 3)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        io = DuckDBDatasetIO(duckdb_connection)

        result = io.compact_parquet_dataset(
            str(dataset_dir), target_mb_per_file=1, compression="snappy"
        )

        assert result["compression_codec"] == "snappy"


class TestCompactCorrectness:
    """Tests for data correctness after compaction."""

    def test_data_preservation(self, tmp_path, sample_table):
        """Test that all data is preserved after compaction."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Create multiple files with all data
        for i in range(5):
            chunk = sample_table.slice(i * 2, 2)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        # Get original data
        original_tables = []
        for f in sorted(dataset_dir.glob("*.parquet")):
            original_tables.append(pq.read_table(f))
        original_combined = pa.concat_tables(original_tables)
        original_combined = original_combined.sort_by("id")

        # Compact
        compact_parquet_dataset_duckdb(str(dataset_dir), target_mb_per_file=1)

        # Read compacted data
        compacted_tables = []
        for f in sorted(dataset_dir.glob("*.parquet")):
            compacted_tables.append(pq.read_table(f))
        compacted_combined = pa.concat_tables(compacted_tables)
        compacted_combined = compacted_combined.sort_by("id")

        # Verify data is the same
        assert original_combined.num_rows == compacted_combined.num_rows
        assert original_combined.column_names == compacted_combined.column_names

    def test_schema_preservation(self, tmp_path, sample_table):
        """Test that schema is preserved after compaction."""
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()

        # Create multiple files
        for i in range(3):
            chunk = sample_table.slice(i * 3, 3)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        original_schema = pq.read_table(dataset_dir / "part_0.parquet").schema

        # Compact
        compact_parquet_dataset_duckdb(str(dataset_dir), target_mb_per_file=1)

        # Check schema of compacted files
        for f in dataset_dir.glob("*.parquet"):
            schema = pq.read_table(f).schema
            assert schema.names == original_schema.names
            assert schema.types == original_schema.types
