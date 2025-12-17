"""Tests for write_dataset API across PyArrow and DuckDB backends."""

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from fsspeckit.datasets.write_result import FileWriteMetadata, WriteDatasetResult
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
from fsspeckit.common.optional import _PYARROW_AVAILABLE, _DUCKDB_AVAILABLE

# Skip all tests if pyarrow is not available
pytestmark = pytest.mark.skipif(not _PYARROW_AVAILABLE, reason="pyarrow not available")


@pytest.fixture
def sample_table():
    """Create a sample PyArrow table for testing."""
    return pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "value": [150.50, 89.99, 234.75, 67.25, 412.80],
        }
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


class TestWriteDatasetResultTypes:
    """Tests for WriteDatasetResult and FileWriteMetadata dataclasses."""

    def test_file_write_metadata_creation(self):
        """Test FileWriteMetadata can be created with valid data."""
        metadata = FileWriteMetadata(
            path="/tmp/file.parquet", row_count=100, size_bytes=1024
        )
        assert metadata.path == "/tmp/file.parquet"
        assert metadata.row_count == 100
        assert metadata.size_bytes == 1024

    def test_file_write_metadata_validation(self):
        """Test FileWriteMetadata validates row_count >= 0."""
        with pytest.raises(ValueError, match="row_count must be >= 0"):
            FileWriteMetadata(path="/tmp/file.parquet", row_count=-1)

    def test_file_write_metadata_size_validation(self):
        """Test FileWriteMetadata validates size_bytes >= 0."""
        with pytest.raises(ValueError, match="size_bytes must be >= 0"):
            FileWriteMetadata(path="/tmp/file.parquet", row_count=100, size_bytes=-1)

    def test_write_dataset_result_creation(self):
        """Test WriteDatasetResult can be created with valid data."""
        files = [
            FileWriteMetadata(path="/tmp/file1.parquet", row_count=50),
            FileWriteMetadata(path="/tmp/file2.parquet", row_count=50),
        ]
        result = WriteDatasetResult(
            files=files, total_rows=100, mode="append", backend="pyarrow"
        )
        assert len(result.files) == 2
        assert result.total_rows == 100
        assert result.mode == "append"
        assert result.backend == "pyarrow"

    def test_write_dataset_result_mode_validation(self):
        """Test WriteDatasetResult validates mode values."""
        with pytest.raises(ValueError, match="mode must be"):
            WriteDatasetResult(
                files=[], total_rows=0, mode="invalid", backend="pyarrow"
            )

    def test_write_dataset_result_backend_validation(self):
        """Test WriteDatasetResult validates backend values."""
        with pytest.raises(ValueError, match="backend must be"):
            WriteDatasetResult(files=[], total_rows=0, mode="append", backend="invalid")

    def test_write_dataset_result_row_count_validation(self):
        """Test WriteDatasetResult validates sum of file row counts matches total."""
        files = [
            FileWriteMetadata(path="/tmp/file1.parquet", row_count=50),
            FileWriteMetadata(path="/tmp/file2.parquet", row_count=30),
        ]
        # This should fail because 50 + 30 != 100
        with pytest.raises(ValueError, match="does not match total_rows"):
            WriteDatasetResult(
                files=files, total_rows=100, mode="append", backend="pyarrow"
            )

    def test_write_dataset_result_to_dict(self):
        """Test WriteDatasetResult.to_dict() produces correct format."""
        files = [
            FileWriteMetadata(path="/tmp/file1.parquet", row_count=50, size_bytes=1024),
        ]
        result = WriteDatasetResult(
            files=files, total_rows=50, mode="append", backend="pyarrow"
        )
        result_dict = result.to_dict()

        assert "files" in result_dict
        assert "total_rows" in result_dict
        assert "mode" in result_dict
        assert "backend" in result_dict
        assert len(result_dict["files"]) == 1
        assert result_dict["files"][0]["path"] == "/tmp/file1.parquet"
        assert result_dict["files"][0]["row_count"] == 50
        assert result_dict["files"][0]["size_bytes"] == 1024


class TestPyArrowWriteDataset:
    """Tests for PyArrow write_dataset implementation."""

    def test_write_dataset_append_mode(self, sample_table, temp_dir):
        """Test write_dataset with append mode preserves existing files."""
        dataset_dir = temp_dir / "dataset"
        io = PyarrowDatasetIO()

        # First write
        result1 = io.write_dataset(sample_table, str(dataset_dir), mode="append")
        assert result1.mode == "append"
        assert result1.backend == "pyarrow"
        assert result1.total_rows == 5
        assert len(result1.files) >= 1

        # Capture files from first write
        first_files = list(dataset_dir.glob("**/*.parquet"))
        assert len(first_files) >= 1

        # Second write (append)
        result2 = io.write_dataset(sample_table, str(dataset_dir), mode="append")
        assert result2.total_rows == 5
        assert len(result2.files) >= 1

        # Check that old files are preserved
        all_files = list(dataset_dir.glob("**/*.parquet"))
        assert len(all_files) >= 2  # Should have files from both writes

    def test_write_dataset_overwrite_mode(self, sample_table, temp_dir):
        """Test write_dataset with overwrite mode removes existing parquet files."""
        dataset_dir = temp_dir / "dataset"
        io = PyarrowDatasetIO()

        # First write
        result1 = io.write_dataset(sample_table, str(dataset_dir), mode="append")
        first_files = list(dataset_dir.glob("**/*.parquet"))
        assert len(first_files) >= 1

        # Second write (overwrite)
        result2 = io.write_dataset(sample_table, str(dataset_dir), mode="overwrite")
        assert result2.mode == "overwrite"
        assert result2.total_rows == 5

        # Check that we have new files but same total count
        all_files = list(dataset_dir.glob("**/*.parquet"))
        assert len(all_files) >= 1

    def test_write_dataset_overwrite_preserves_non_parquet(
        self, sample_table, temp_dir
    ):
        """Test overwrite mode preserves non-parquet files."""
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir(parents=True)
        io = PyarrowDatasetIO()

        # Create a non-parquet file
        readme_file = dataset_dir / "README.txt"
        readme_file.write_text("This is a dataset")

        # Write dataset
        io.write_dataset(sample_table, str(dataset_dir), mode="append")
        assert readme_file.exists()

        # Overwrite
        result = io.write_dataset(sample_table, str(dataset_dir), mode="overwrite")
        assert result.mode == "overwrite"
        # README should still exist
        assert readme_file.exists()
        assert readme_file.read_text() == "This is a dataset"

    def test_write_dataset_returns_file_metadata(self, sample_table, temp_dir):
        """Test write_dataset returns metadata for written files."""
        dataset_dir = temp_dir / "dataset"
        io = PyarrowDatasetIO()

        result = io.write_dataset(sample_table, str(dataset_dir), mode="append")

        assert len(result.files) >= 1
        for file_meta in result.files:
            assert isinstance(file_meta, FileWriteMetadata)
            assert file_meta.path
            assert file_meta.row_count >= 0
            # Size may or may not be available depending on implementation
            if file_meta.size_bytes is not None:
                assert file_meta.size_bytes >= 0

        # Verify total rows matches
        total_rows_from_files = sum(f.row_count for f in result.files)
        assert total_rows_from_files == result.total_rows

    def test_write_dataset_with_compression(self, sample_table, temp_dir):
        """Test write_dataset with different compression codecs."""
        dataset_dir = temp_dir / "dataset"
        io = PyarrowDatasetIO()

        result = io.write_dataset(
            sample_table, str(dataset_dir), mode="append", compression="gzip"
        )
        assert result.total_rows == 5
        assert len(result.files) >= 1

    def test_write_dataset_invalid_mode(self, sample_table, temp_dir):
        """Test write_dataset raises error for invalid mode."""
        dataset_dir = temp_dir / "dataset"
        io = PyarrowDatasetIO()

        with pytest.raises(ValueError, match="mode must be"):
            io.write_dataset(sample_table, str(dataset_dir), mode="invalid")  # type: ignore


@pytest.mark.skipif(not _DUCKDB_AVAILABLE, reason="duckdb not available")
class TestDuckDBWriteDataset:
    """Tests for DuckDB write_dataset implementation."""

    @pytest.fixture
    def duckdb_io(self):
        """Create a DuckDB IO instance for testing."""
        from fsspeckit.datasets.duckdb.connection import create_duckdb_connection
        from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

        conn = create_duckdb_connection()
        return DuckDBDatasetIO(conn)

    def test_write_dataset_append_mode(self, sample_table, temp_dir, duckdb_io):
        """Test DuckDB write_dataset with append mode."""
        dataset_dir = temp_dir / "dataset"

        # First write
        result1 = duckdb_io.write_dataset(sample_table, str(dataset_dir), mode="append")
        assert result1.mode == "append"
        assert result1.backend == "duckdb"
        assert result1.total_rows == 5
        assert len(result1.files) >= 1

        # Capture files from first write
        first_files = list(dataset_dir.glob("**/*.parquet"))
        assert len(first_files) >= 1

        # Second write (append)
        result2 = duckdb_io.write_dataset(sample_table, str(dataset_dir), mode="append")
        assert result2.total_rows == 5

        # Check that old files are preserved
        all_files = list(dataset_dir.glob("**/*.parquet"))
        assert len(all_files) >= 2  # Should have files from both writes

    def test_write_dataset_overwrite_mode(self, sample_table, temp_dir, duckdb_io):
        """Test DuckDB write_dataset with overwrite mode."""
        dataset_dir = temp_dir / "dataset"

        # First write
        result1 = duckdb_io.write_dataset(sample_table, str(dataset_dir), mode="append")
        first_files = list(dataset_dir.glob("**/*.parquet"))
        assert len(first_files) >= 1

        # Second write (overwrite)
        result2 = duckdb_io.write_dataset(
            sample_table, str(dataset_dir), mode="overwrite"
        )
        assert result2.mode == "overwrite"
        assert result2.total_rows == 5

    def test_write_dataset_returns_file_metadata(
        self, sample_table, temp_dir, duckdb_io
    ):
        """Test DuckDB write_dataset returns file metadata."""
        dataset_dir = temp_dir / "dataset"

        result = duckdb_io.write_dataset(sample_table, str(dataset_dir), mode="append")

        assert len(result.files) >= 1
        for file_meta in result.files:
            assert isinstance(file_meta, FileWriteMetadata)
            assert file_meta.path
            assert file_meta.row_count >= 0
            if file_meta.size_bytes is not None:
                assert file_meta.size_bytes >= 0

        # Verify total rows matches
        total_rows_from_files = sum(f.row_count for f in result.files)
        assert total_rows_from_files == result.total_rows

    def test_write_dataset_overwrite_preserves_non_parquet(
        self, sample_table, temp_dir, duckdb_io
    ):
        """Test DuckDB overwrite mode preserves non-parquet files."""
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir(parents=True)

        # Create a non-parquet file
        readme_file = dataset_dir / "README.txt"
        readme_file.write_text("This is a dataset")

        # Write dataset
        duckdb_io.write_dataset(sample_table, str(dataset_dir), mode="append")
        assert readme_file.exists()

        # Overwrite
        result = duckdb_io.write_dataset(
            sample_table, str(dataset_dir), mode="overwrite"
        )
        assert result.mode == "overwrite"
        # README should still exist
        assert readme_file.exists()
        assert readme_file.read_text() == "This is a dataset"
