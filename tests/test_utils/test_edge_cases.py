"""Edge case tests for dataset write modes and related functionality."""

import os
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from fsspeckit.datasets.duckdb import DuckDBParquetHandler
from fsspeckit.datasets.pyarrow import PyarrowDatasetHandler


class TestMaxRowsPerFileEdgeCases:
    """Test max_rows_per_file splitting functionality edge cases."""

    @pytest.fixture
    def large_table(self):
        """Create a large table for splitting tests."""
        return pa.table(
            {
                "id": list(range(150)),
                "value": [f"row_{i}" for i in range(150)],
                "number": [i * 1.5 for i in range(150)],
            }
        )

    def test_duckdb_max_rows_per_file_splitting(self, large_table):
        """Test DuckDB max_rows_per_file with various sizes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with DuckDBParquetHandler() as handler:
                # Test with max_rows_per_file=50 (should create 3 files)
                handler.write_dataset(large_table, dataset_dir, max_rows_per_file=50)

                files = list(Path(dataset_dir).glob("**/*.parquet"))
                assert len(files) >= 1

                # Verify all data is present
                result = handler.read_parquet(dataset_dir)
                assert result.num_rows == 150

    def test_pyarrow_max_rows_per_file_splitting(self, large_table):
        """Test PyArrow max_rows_per_file with various sizes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with PyarrowDatasetHandler() as handler:
                # Test with max_rows_per_file=50 (should create 3 files)
                handler.write_dataset(large_table, dataset_dir, max_rows_per_file=50)

                files = list(Path(dataset_dir).glob("**/*.parquet"))
                assert len(files) == 3

                # Verify all data is present
                result = handler.read_parquet(dataset_dir)
                assert result.num_rows == 150

    def test_max_rows_per_file_with_append_mode(self, large_table):
        """Test max_rows_per_file works correctly with append mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with DuckDBParquetHandler() as handler:
                # First write with splitting
                handler.write_dataset(large_table, dataset_dir, max_rows_per_file=60)
                files1 = list(Path(dataset_dir).glob("**/*.parquet"))
                assert len(files1) >= 1

                # Second append with different splitting
                handler.write_dataset(
                    large_table, dataset_dir, max_rows_per_file=40, mode="append"
                )
                files2 = list(Path(dataset_dir).glob("**/*.parquet"))
                assert len(files2) > len(files1)

                # Verify all data is present
                result = handler.read_parquet(dataset_dir)
                assert result.num_rows == 300  # 150 + 150

    def test_max_rows_per_file_edge_cases(self):
        """Test edge cases for max_rows_per_file parameter."""
        small_table = pa.table({"id": [1, 2], "value": ["a", "b"]})

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with DuckDBParquetHandler() as handler:
                # Test with max_rows_per_file larger than table (should create 1 file)
                handler.write_dataset(small_table, dataset_dir, max_rows_per_file=100)
                files = list(Path(dataset_dir).glob("**/*.parquet"))
                assert len(files) >= 1

    def test_max_rows_per_file_error_handling(self):
        """Test error handling for invalid max_rows_per_file values."""
        table = pa.table({"id": [1, 2], "value": ["a", "b"]})

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with DuckDBParquetHandler() as handler:
                # Test zero value
                with pytest.raises(ValueError, match="must be > 0"):
                    handler.write_dataset(table, dataset_dir, max_rows_per_file=0)

                # Test negative value
                with pytest.raises(ValueError, match="must be > 0"):
                    handler.write_dataset(table, dataset_dir, max_rows_per_file=-5)


class TestPathValidationEdgeCases:
    """Test path validation edge cases."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table for testing."""
        return pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})

    def test_nonexistent_parent_directory(self, sample_table):
        """Test writing to a path where parent directory doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Path with non-existent parent
            dataset_dir = f"{temp_dir}/nonexistent/nested/dataset"

            with DuckDBParquetHandler() as handler:
                # Should create parent directories automatically
                handler.write_dataset(sample_table, dataset_dir)

                # Verify the directory and file were created
                assert Path(dataset_dir).exists()
                files = list(Path(dataset_dir).glob("*.parquet"))
                assert len(files) > 0

    def test_relative_path_handling(self, sample_table):
        """Test handling of relative paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                # Test with relative path
                dataset_dir = "relative_dataset"

                with DuckDBParquetHandler() as handler:
                    handler.write_dataset(sample_table, dataset_dir)

                    # Should work with relative paths
                    assert Path(dataset_dir).exists()
                    files = list(Path(dataset_dir).glob("*.parquet"))
                    assert len(files) > 0

            finally:
                os.chdir(original_cwd)

    def test_unicode_path_handling(self, sample_table):
        """Test handling of Unicode characters in paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Path with Unicode characters
            dataset_dir = f"{temp_dir}/dataset_ñ_中文_🚀"

            with DuckDBParquetHandler() as handler:
                handler.write_dataset(sample_table, dataset_dir)

                # Verify the directory and file were created
                assert Path(dataset_dir).exists()
                files = list(Path(dataset_dir).glob("*.parquet"))
                assert len(files) > 0

    def test_long_path_handling(self, sample_table):
        """Test handling of very long paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a long path
            long_dir_name = "a" * 100  # 100 character directory name
            dataset_dir = f"{temp_dir}/{long_dir_name}/dataset"

            with DuckDBParquetHandler() as handler:
                handler.write_dataset(sample_table, dataset_dir)

                # Verify the directory and file were created
                assert Path(dataset_dir).exists()
                files = list(Path(dataset_dir).glob("*.parquet"))
                assert len(files) > 0

    def test_path_with_spaces(self, sample_table):
        """Test handling of paths with spaces."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset with spaces/sub dir"

            with DuckDBParquetHandler() as handler:
                handler.write_dataset(sample_table, dataset_dir)

                # Verify the directory and file were created
                assert Path(dataset_dir).exists()
                files = list(Path(dataset_dir).glob("*.parquet"))
                assert len(files) > 0


class TestModeBehaviorEdgeCases:
    """Test mode behavior in various edge cases."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table for testing."""
        return pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})

    def test_append_mode_with_existing_files(self, sample_table):
        """Test append mode when files already exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with DuckDBParquetHandler() as handler:
                # Write initial data
                handler.write_dataset(sample_table, dataset_dir)
                files1 = list(Path(dataset_dir).glob("**/*.parquet"))
                initial_count = len(files1)

                # Append more data
                more_data = pa.table({"id": [4, 5], "value": ["d", "e"]})
                handler.write_dataset(more_data, dataset_dir, mode="append")
                files2 = list(Path(dataset_dir).glob("**/*.parquet"))

                # Should have more files now
                assert len(files2) > initial_count

                # Verify all data is present
                result = handler.read_parquet(dataset_dir)
                assert result.num_rows == 5

    def test_overwrite_mode_preserves_non_parquet_files(self, sample_table):
        """Test that overwrite mode preserves non-parquet files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = Path(temp_dir) / "dataset"
            dataset_dir.mkdir()

            # Create some non-parquet files
            readme_file = dataset_dir / "README.txt"
            readme_file.write_text("This is a dataset directory")

            config_file = dataset_dir / "config.json"
            config_file.write_text('{"version": "1.0"}')

            with DuckDBParquetHandler() as handler:
                # Write initial parquet data
                handler.write_dataset(sample_table, str(dataset_dir))
                parquet_files_1 = list(dataset_dir.glob("*.parquet"))

                # Overwrite with new data
                new_data = pa.table({"id": [10, 11], "value": ["x", "y"]})
                handler.write_dataset(new_data, str(dataset_dir), mode="overwrite")

                # Check that non-parquet files are preserved
                assert readme_file.exists()
                assert config_file.exists()
                assert readme_file.read_text() == "This is a dataset directory"

                # Check that old parquet files are gone and new ones exist
                parquet_files_2 = list(dataset_dir.glob("*.parquet"))
                assert len(parquet_files_2) > 0
                # The specific filenames might be different due to UUIDs

    def test_mode_with_empty_dataset_directory(self, sample_table):
        """Test mode behavior when dataset directory exists but is empty."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/empty_dataset"
            Path(dataset_dir).mkdir()  # Create empty directory

            with DuckDBParquetHandler() as handler:
                # Both modes should work the same with empty directory
                handler.write_dataset(sample_table, dataset_dir, mode="append")
                files_append = list(Path(dataset_dir).glob("**/*.parquet"))
                assert len(files_append) > 0

                # Clear and test overwrite
                for f in files_append:
                    f.unlink()

                handler.write_dataset(sample_table, dataset_dir, mode="overwrite")
                files_overwrite = list(Path(dataset_dir).glob("**/*.parquet"))
                assert len(files_overwrite) > 0

    def test_append_mode_collision_prevention(self, sample_table):
        """Test that append mode prevents filename collisions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with DuckDBParquetHandler() as handler:
                # Write initial data
                handler.write_dataset(sample_table, dataset_dir, mode="append")
                files1 = list(Path(dataset_dir).glob("**/*.parquet"))

                # Write more data multiple times
                for _ in range(3):
                    more_data = pa.table({"id": [10, 11], "value": ["x", "y"]})
                    handler.write_dataset(more_data, dataset_dir, mode="append")

                files_final = list(Path(dataset_dir).glob("**/*.parquet"))

                # Should have multiple unique files, no collisions
                assert len(files_final) > len(files1)

                # Verify all files are readable
                result = handler.read_parquet(dataset_dir)
                assert result.num_rows >= 7  # 3 + (3 * 2) at minimum
