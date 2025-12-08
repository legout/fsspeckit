"""Test universal I/O helpers in fsspeckit.core.ext_io."""

import json
import tempfile
import csv
from unittest.mock import Mock, patch, MagicMock
import pytest

# Import the functions we want to test
from fsspeckit.core.ext_io import (
    read_files,
    write_files,
    write_file,
    READ_HANDLERS,
    WRITE_HANDLERS,
)


class MockFileSystem:
    """Mock filesystem for testing I/O operations."""

    def __init__(self):
        self.files = {}
        self.exists_data = {}
        self.rm_calls = []

    def glob(self, pattern):
        """Mock glob that returns files matching pattern."""
        return [f for f in self.files.keys() if pattern in f]

    def open(self, path, mode="r"):
        """Mock file opening."""
        if path not in self.files:
            raise FileNotFoundError(f"File not found: {path}")

        class MockFile:
            def __init__(self, content, mode):
                self.content = content
                self.mode = mode
                self.position = 0

            def read(self):
                return self.content

            def write(self, data):
                self.content = data
                return len(data)

            def readlines(self):
                return self.content.split("\n")

        return MockFile(self.files[path], mode)

    def exists(self, path):
        """Mock existence check."""
        return path in self.exists_data

    def rm(self, path, recursive=False):
        """Mock file removal."""
        self.rm_calls.append((path, recursive))
        if path in self.exists_data:
            del self.exists_data[path]


class TestUniversalIO:
    """Test the universal I/O helpers."""

    def setup_method(self):
        """Set up test fixtures."""
        self.fs = MockFileSystem()
        self.temp_dir = tempfile.mkdtemp()

    def test_dispatch_handler_mappings(self):
        """Test that handler mappings are properly configured."""
        # Test READ_HANDLERS
        assert "json" in READ_HANDLERS
        assert "csv" in READ_HANDLERS
        assert "parquet" in READ_HANDLERS
        assert len(READ_HANDLERS) == 3

        # Test WRITE_HANDLERS
        assert "json" in WRITE_HANDLERS
        assert "csv" in WRITE_HANDLERS
        assert "parquet" in WRITE_HANDLERS
        assert len(WRITE_HANDLERS) == 3

    def test_read_files_delegates_to_json_handler(self):
        """Test that read_files with format='json' delegates to JSON handler."""
        path = f"{self.temp_dir}/test.json"

        # Mock the JSON handler
        mock_json_handler = Mock(return_value={"test": "data"})

        # Patch the READ_HANDLERS mapping
        with patch.dict(READ_HANDLERS, {"json": mock_json_handler}):
            result = read_files(self.fs, path, format="json")

        # Verify the JSON handler was called with correct arguments
        mock_json_handler.assert_called_once()
        call_args = mock_json_handler.call_args
        assert call_args[1]["self"] == self.fs
        assert call_args[1]["path"] == path

    def test_read_files_delegates_to_csv_handler(self):
        """Test that read_files with format='csv' delegates to CSV handler."""
        path = f"{self.temp_dir}/test.csv"

        # Mock the CSV handler
        mock_csv_handler = Mock(return_value={"test": "data"})

        # Patch the READ_HANDLERS mapping
        with patch.dict(READ_HANDLERS, {"csv": mock_csv_handler}):
            result = read_files(self.fs, path, format="csv")

        # Verify the CSV handler was called with correct arguments
        mock_csv_handler.assert_called_once()
        call_args = mock_csv_handler.call_args
        assert call_args[1]["self"] == self.fs
        assert call_args[1]["path"] == path

    def test_read_files_delegates_to_parquet_handler(self):
        """Test that read_files with format='parquet' delegates to Parquet handler."""
        path = f"{self.temp_dir}/test.parquet"

        # Mock the Parquet handler
        mock_parquet_handler = Mock(return_value={"test": "data"})

        # Patch the READ_HANDLERS mapping
        with patch.dict(READ_HANDLERS, {"parquet": mock_parquet_handler}):
            result = read_files(self.fs, path, format="parquet")

        # Verify the Parquet handler was called with correct arguments
        mock_parquet_handler.assert_called_once()
        call_args = mock_parquet_handler.call_args
        assert call_args[1]["self"] == self.fs
        assert call_args[1]["path"] == path

    def test_read_files_passes_format_specific_args(self):
        """Test that read_files passes format-specific arguments correctly."""
        path = f"{self.temp_dir}/test.json"

        # Mock the JSON handler
        mock_json_handler = Mock(return_value={"test": "data"})

        # Test with JSON-specific arguments
        with patch.dict(READ_HANDLERS, {"json": mock_json_handler}):
            result = read_files(
                self.fs,
                path,
                format="json",
                jsonlines=True,
                batch_size=10,
                include_file_path=True,
            )

        # Verify the JSON handler was called with all arguments including jsonlines
        mock_json_handler.assert_called_once()
        call_args = mock_json_handler.call_args
        assert call_args[1]["jsonlines"] is True
        assert call_args[1]["batch_size"] == 10
        assert call_args[1]["include_file_path"] is True

    def test_write_files_single_path_string_json(self):
        """Test write_files with single string path for JSON format."""
        test_data = {"id": 1, "name": "test"}
        path = f"{self.temp_dir}/test.json"

        # Mock the filesystem
        self.fs.exists_data[path] = False

        # Test write_files with single path
        write_files(self.fs, test_data, path, format="json")

        # Verify file was created
        assert path in self.fs.files

    def test_write_files_single_path_list_json(self):
        """Test write_files with list containing single path for JSON format."""
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        paths = [f"{self.temp_dir}/test1.json", f"{self.temp_dir}/test2.json"]

        # Mock the filesystem
        for path in paths:
            self.fs.exists_data[path] = False

        # Test write_files with list of paths
        write_files(self.fs, test_data, paths, format="json")

        # Verify files were created
        for path in paths:
            assert path in self.fs.files

    def test_write_files_single_path_string_csv(self):
        """Test write_files with single string path for CSV format."""
        test_data = {"id": [1, 2], "name": ["test1", "test2"]}
        path = f"{self.temp_dir}/test.csv"

        # Mock the filesystem
        self.fs.exists_data[path] = False

        # Test write_files with single path and CSV format
        write_files(self.fs, test_data, path, format="csv")

        # Verify file was created
        assert path in self.fs.files

    def test_write_files_multiple_paths_csv(self):
        """Test write_files with multiple paths for CSV format."""
        test_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
        ]
        paths = [f"{self.temp_dir}/test1.csv", f"{self.temp_dir}/test2.csv"]

        # Mock the filesystem
        for path in paths:
            self.fs.exists_data[path] = False

        # Test write_files with multiple paths
        write_files(self.fs, test_data, paths, format="csv")

        # Verify files were created
        for path in paths:
            assert path in self.fs.files

    def test_write_files_single_path_string_parquet(self):
        """Test write_files with single string path for Parquet format."""
        test_data = {"id": [1, 2], "name": ["test1", "test2"]}
        path = f"{self.temp_dir}/test.parquet"

        # Mock the filesystem
        self.fs.exists_data[path] = False

        # Test write_files with single path and Parquet format
        write_files(self.fs, test_data, path, format="parquet")

        # Verify file was created
        assert path in self.fs.files

    def test_write_files_multiple_paths_parquet(self):
        """Test write_files with multiple paths for Parquet format."""
        test_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
        ]
        paths = [f"{self.temp_dir}/test1.parquet", f"{self.temp_dir}/test2.parquet"]

        # Mock the filesystem
        for path in paths:
            self.fs.exists_data[path] = False

        # Test write_files with multiple paths
        write_files(self.fs, test_data, paths, format="parquet")

        # Verify files were created
        for path in paths:
            assert path in self.fs.files

    def test_write_files_use_threads_false(self):
        """Test write_files with use_threads=False."""
        test_data = [{"id": i, "name": f"test{i}"} for i in range(3)]
        paths = [f"{self.temp_dir}/test{i}.json" for i in range(3)]

        # Mock the filesystem
        for path in paths:
            self.fs.exists_data[path] = False

        # Test write_files with threading disabled
        write_files(self.fs, test_data, paths, format="json", use_threads=False)

        # Verify all files were created
        for path in paths:
            assert path in self.fs.files

    def test_write_files_use_threads_true(self):
        """Test write_files with use_threads=True."""
        test_data = [{"id": i, "name": f"test{i}"} for i in range(3)]
        paths = [f"{self.temp_dir}/test{i}.json" for i in range(3)]

        # Mock the filesystem
        for path in paths:
            self.fs.exists_data[path] = False

        # Test write_files with threading enabled
        write_files(self.fs, test_data, paths, format="json", use_threads=True)

        # Verify all files were created
        for path in paths:
            assert path in self.fs.files

    def test_write_files_different_modes(self):
        """Test write_files with different write modes."""
        test_data = {"id": 1, "name": "test"}
        path = f"{self.temp_dir}/test.json"

        # Test append mode
        self.fs.exists_data[path] = False
        write_files(self.fs, test_data, path, format="json", mode="append")
        assert path in self.fs.files

        # Test overwrite mode
        write_files(self.fs, test_data, path, format="json", mode="overwrite")
        assert path in self.fs.files

        # Test error_if_exists mode
        self.fs.exists_data[path] = True
        with pytest.raises(FileExistsError):
            write_files(self.fs, test_data, path, format="json", mode="error_if_exists")

    def test_write_file_dispatch(self):
        """Test write_file format dispatch."""
        test_data = {"id": 1, "name": "test"}
        path = f"{self.temp_dir}/test.json"

        # Mock the filesystem
        self.fs.exists_data[path] = False

        # Test write_file with JSON format
        write_file(self.fs, test_data, path, format="json")

        # Verify file was created
        assert path in self.fs.files

    def test_write_file_delegates_to_json_handler(self):
        """Test that write_file with format='json' delegates to JSON handler."""
        test_data = {"id": 1, "name": "test"}
        path = f"{self.temp_dir}/test.json"

        # Mock the JSON handler
        mock_json_handler = Mock()

        # Patch the WRITE_HANDLERS mapping
        with patch.dict(WRITE_HANDLERS, {"json": mock_json_handler}):
            write_file(self.fs, test_data, path, format="json")

        # Verify the JSON handler was called with correct arguments
        mock_json_handler.assert_called_once()
        call_args = mock_json_handler.call_args
        assert call_args[0][0] == self.fs  # self
        assert call_args[0][1] == test_data  # data
        assert call_args[0][2] == path  # path

    def test_write_file_delegates_to_csv_handler(self):
        """Test that write_file with format='csv' delegates to CSV handler."""
        test_data = {"id": 1, "name": "test"}
        path = f"{self.temp_dir}/test.csv"

        # Mock the CSV handler
        mock_csv_handler = Mock()

        # Patch the WRITE_HANDLERS mapping
        with patch.dict(WRITE_HANDLERS, {"csv": mock_csv_handler}):
            write_file(self.fs, test_data, path, format="csv")

        # Verify the CSV handler was called with correct arguments
        mock_csv_handler.assert_called_once()
        call_args = mock_csv_handler.call_args
        assert call_args[0][0] == self.fs  # self
        assert call_args[0][1] == test_data  # data
        assert call_args[0][2] == path  # path

    def test_write_file_delegates_to_parquet_handler(self):
        """Test that write_file with format='parquet' delegates to Parquet handler."""
        test_data = {"id": 1, "name": "test"}
        path = f"{self.temp_dir}/test.parquet"

        # Mock the Parquet handler
        mock_parquet_handler = Mock()

        # Patch the WRITE_HANDLERS mapping
        with patch.dict(WRITE_HANDLERS, {"parquet": mock_parquet_handler}):
            write_file(self.fs, test_data, path, format="parquet")

        # Verify the Parquet handler was called with correct arguments
        mock_parquet_handler.assert_called_once()
        call_args = mock_parquet_handler.call_args
        assert call_args[0][0] == self.fs  # self
        assert call_args[0][1] == test_data  # data
        assert call_args[0][2] == path  # path

    def test_write_file_invalid_format(self):
        """Test write_file with invalid format raises ValueError."""
        test_data = {"id": 1, "name": "test"}
        path = f"{self.temp_dir}/test.txt"

        # Test write_file with invalid format
        with pytest.raises(ValueError, match="Unsupported format"):
            write_file(self.fs, test_data, path, format="invalid")

    def test_read_files_invalid_format(self):
        """Test read_files with invalid format raises ValueError."""
        path = f"{self.temp_dir}/test.txt"

        # Test read_files with invalid format
        with pytest.raises(ValueError, match="Unsupported format"):
            read_files(self.fs, path, format="invalid")

    def test_write_files_data_path_length_mismatch(self):
        """Test write_files handles data/path length mismatch correctly."""
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        # Only one path for two data items
        paths = [f"{self.temp_dir}/test.json"]

        # Should raise ValueError for incompatible lengths
        with pytest.raises(ValueError, match="compatible lengths"):
            write_files(self.fs, test_data, paths, format="json")


class TestLazyImports:
    """Test that optional dependencies are handled lazily."""

    def test_import_without_optional_dependencies(self):
        """Test that importing ext_io doesn't require optional dependencies."""
        # This test should pass even if optional dependencies are not installed
        # The import should work due to TYPE_CHECKING and lazy loading
        import fsspeckit.core.ext_io

        assert hasattr(fsspeckit.core.ext_io, "read_files")
        assert hasattr(fsspeckit.core.ext_io, "write_files")
        assert hasattr(fsspeckit.core.ext_io, "write_file")

    @patch("fsspeckit.core.ext_io._import_polars")
    def test_missing_polars_raises_import_error(self, mock_import):
        """Test that missing polars raises appropriate ImportError."""
        mock_import.side_effect = ImportError("polars is required")

        # This should raise ImportError when trying to use functionality that requires polars
        from fsspeckit.common.optional import _import_polars

        with pytest.raises(ImportError, match="polars is required"):
            _import_polars()

    @patch("fsspeckit.core.ext_io._import_pyarrow")
    def test_missing_pyarrow_raises_import_error(self, mock_import):
        """Test that missing pyarrow raises appropriate ImportError."""
        mock_import.side_effect = ImportError("pyarrow is required")

        # This should raise ImportError when trying to use functionality that requires pyarrow
        from fsspeckit.common.optional import _import_pyarrow

        with pytest.raises(ImportError, match="pyarrow is required"):
            _import_pyarrow()

    @patch("fsspeckit.core.ext_io._import_pandas")
    def test_missing_pandas_raises_import_error(self, mock_import):
        """Test that missing pandas raises appropriate ImportError."""
        mock_import.side_effect = ImportError("pandas is required")

        # This should raise ImportError when trying to use functionality that requires pandas
        from fsspeckit.common.optional import _import_pandas

        with pytest.raises(ImportError, match="pandas is required"):
            _import_pandas()
