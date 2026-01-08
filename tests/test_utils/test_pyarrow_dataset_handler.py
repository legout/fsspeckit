"""Tests for PyArrow dataset handler."""

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from fsspeckit import filesystem
from fsspeckit.datasets.pyarrow import PyarrowDatasetHandler, PyarrowDatasetIO


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


class TestPyarrowDatasetIOInit:
    """Tests for PyarrowDatasetIO initialization."""

    def test_init_default(self):
        """Test initialization with default parameters."""
        io = PyarrowDatasetIO()
        assert io is not None
        assert io._filesystem is not None

    def test_init_with_filesystem(self):
        """Test initialization with filesystem instance."""
        fs = filesystem("file")
        io = PyarrowDatasetIO(filesystem=fs)
        assert io._filesystem is fs


class TestPyarrowDatasetHandlerContextManager:
    """Tests for context manager functionality."""

    def test_context_manager(self):
        """Test context manager protocol."""
        with PyarrowDatasetHandler() as handler:
            assert handler is not None

    def test_handler_inherits_from_io(self):
        """Test that handler inherits from PyarrowDatasetIO."""
        handler = PyarrowDatasetHandler()
        assert isinstance(handler, PyarrowDatasetIO)


class TestPyarrowDatasetIOReadWrite:
    """Tests for read and write operations."""

    def test_write_and_read_single_file(self, sample_table, temp_dir):
        """Test writing and reading a single parquet file."""
        parquet_file = temp_dir / "data.parquet"

        io = PyarrowDatasetIO()

        # Write
        io.write_parquet(sample_table, str(parquet_file))
        assert parquet_file.exists()

        # Read
        result = io.read_parquet(str(parquet_file))
        assert isinstance(result, pa.Table)
        assert result.num_rows == sample_table.num_rows
        assert result.column_names == sample_table.column_names

    def test_write_dataset_basic(self, sample_table, temp_dir):
        """Test basic dataset write."""
        dataset_dir = temp_dir / "dataset"

        io = PyarrowDatasetIO()
        io.write_dataset(sample_table, str(dataset_dir))

        assert dataset_dir.exists()
        files = list(dataset_dir.glob("**/*.parquet"))
        assert len(files) >= 1

    def test_read_parquet_with_columns(self, sample_table, temp_dir):
        """Test reading with column selection."""
        parquet_file = temp_dir / "data.parquet"

        io = PyarrowDatasetIO()
        io.write_parquet(sample_table, str(parquet_file))

        result = io.read_parquet(str(parquet_file), columns=["id", "name"])
        assert result.num_columns == 2
        assert result.column_names == ["id", "name"]


class TestPyarrowDatasetIOMaintenance:
    """Tests for maintenance operations."""

    def test_compact_dataset(self, sample_table, temp_dir):
        """Test dataset compaction."""
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir(parents=True, exist_ok=True)

        io = PyarrowDatasetIO()

        # Create multiple small files
        for i in range(5):
            chunk = sample_table.slice(i % sample_table.num_rows, 1)
            file_path = dataset_dir / f"part_{i}.parquet"
            io.write_parquet(chunk, str(file_path))

        # Compact
        result = io.compact_parquet_dataset(str(dataset_dir), target_mb_per_file=1)
        assert "before_file_count" in result
        assert "after_file_count" in result

    def test_compact_dry_run(self, sample_table, temp_dir):
        """Test dry run compaction."""
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir(parents=True, exist_ok=True)

        io = PyarrowDatasetIO()
        io.write_dataset(sample_table, str(dataset_dir))

        result = io.compact_parquet_dataset(
            str(dataset_dir), target_mb_per_file=1, dry_run=True
        )
        assert result["dry_run"] is True

    def test_optimize_dataset(self, sample_table, temp_dir):
        """Test dataset optimization."""
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir(parents=True, exist_ok=True)

        io = PyarrowDatasetIO()
        io.write_dataset(sample_table, str(dataset_dir))

        result = io.optimize_parquet_dataset(str(dataset_dir), target_mb_per_file=64)
        assert "before_file_count" in result


class TestPyarrowDatasetHandlerAPISymmetry:
    """Tests to verify API symmetry with DuckDB handler."""

    def test_has_read_parquet(self):
        """Test that handler has read_parquet method."""
        handler = PyarrowDatasetHandler()
        assert hasattr(handler, "read_parquet")
        assert callable(handler.read_parquet)

    def test_has_write_parquet(self):
        """Test that handler has write_parquet method."""
        handler = PyarrowDatasetHandler()
        assert hasattr(handler, "write_parquet")
        assert callable(handler.write_parquet)

    def test_has_write_dataset(self):
        """Test that handler has write_dataset method."""
        handler = PyarrowDatasetHandler()
        assert hasattr(handler, "write_dataset")
        assert callable(handler.write_dataset)

    def test_has_merge(self):
        """Test that handler has merge method."""
        handler = PyarrowDatasetHandler()
        assert hasattr(handler, "merge")
        assert callable(handler.merge)

    def test_has_compact_parquet_dataset(self):
        """Test that handler has compact_parquet_dataset method."""
        handler = PyarrowDatasetHandler()
        assert hasattr(handler, "compact_parquet_dataset")
        assert callable(handler.compact_parquet_dataset)

    def test_has_optimize_parquet_dataset(self):
        """Test that handler has optimize_parquet_dataset method."""
        handler = PyarrowDatasetHandler()
        assert hasattr(handler, "optimize_parquet_dataset")
        assert callable(handler.optimize_parquet_dataset)

    def test_has_modern_api_methods(self):
        """Test that handler has modern dataset API methods."""
        handler = PyarrowDatasetHandler()
        assert hasattr(handler, "write_dataset")
        assert hasattr(handler, "merge")
        # Legacy convenience methods should be removed
        legacy_methods = [
            "insert_dataset",
            "upsert_dataset",
            "update_dataset",
            "deduplicate_dataset",
        ]
        for method in legacy_methods:
            assert not hasattr(
                handler, method
            ), f"Legacy method {method} should have been removed"


class TestOptionalDependencyHandling:
    """Tests for optional dependency handling."""

    def test_import_error_without_pyarrow(self, monkeypatch):
        """Test that ImportError is raised when PyArrow is not available."""
        # This test verifies the lazy import pattern
        import fsspeckit.common.optional as optional_module

        # Temporarily set availability to False
        original = optional_module._PYARROW_AVAILABLE
        monkeypatch.setattr(optional_module, "_PYARROW_AVAILABLE", False)

        try:
            # Re-import to trigger check
            from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO

            with pytest.raises(ImportError, match="pyarrow is required"):
                PyarrowDatasetIO()
        finally:
            monkeypatch.setattr(optional_module, "_PYARROW_AVAILABLE", original)
