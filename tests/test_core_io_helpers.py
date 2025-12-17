"""Test threading behavior in JSON and CSV readers."""

import json
import tempfile
from unittest.mock import patch

import pyarrow as pa
import pytest

# Import to trigger registration
import fsspeckit.core.ext  # noqa: F401


class MockFileSystem:
    """Mock filesystem for testing core IO helpers."""

    def __init__(self):
        self.files = {}
        self.files_written = []  # Track files written by write_json

    def glob(self, pattern):
        # Simple mock that returns files matching pattern
        return [f for f in self.files.keys() if pattern in f or f.endswith(pattern)]

    def open(self, path, mode="r"):
        # Return file-like object
        if path not in self.files:
            raise FileNotFoundError(f"File not found: {path}")

        class MockFile:
            def __init__(self, content):
                self.content = content
                self.position = 0

            def read(self):
                return self.content

            def readlines(self):
                return self.content.split("\n")

            def write(self, data):
                # For string, just replace content. For bytes, decode? 
                # Simplistic mock:
                if isinstance(data, bytes):
                    self.content = data.decode('utf-8')
                else:
                    self.content = data
                # Update the filesystem dict as well?
                # The parent 'open' doesn't update 'files'. 
                # But 'files_written' should be updated.
                # However, MockFile doesn't have reference to FS.
                # We can't update FS from here easily without refactoring.
                # But test_write_json_behavior checks fs.files_written?
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

            def seek(self, offset, whence=0):
                if whence == 0:
                    self.position = offset
                elif whence == 1:
                    self.position += offset
                elif whence == 2:
                    self.position = len(self.content) + offset
                return self.position

            def tell(self):
                return self.position
        
        # We need to handle writing differently because we need to update fs.files_written
        mock_file = MockFile(self.files.get(path, ""))
        
        # Monkey patch write to update parent
        original_write = mock_file.write
        def side_effect_write(data):
            self.files_written.append(path)
            self.files[path] = data
            return original_write(data)
        mock_file.write = side_effect_write

        return mock_file


class TestThreadingBehavior:
    """Test that use_threads=True and use_threads=False produce same data."""

    def test_json_use_threads_behavior(self, tmp_path):
        """Test that use_threads parameter works correctly in JSON reader."""
        # Import the functions directly from the module
        from fsspeckit.core.ext.csv import _read_csv
        from fsspeckit.core.ext.json import _read_json

        # Create test JSON files
        test_data = [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"},
            {"id": 3, "value": "test3"},
        ]

        files = []
        for i, data in enumerate(test_data):
            file_path = tmp_path / f"test_{i}.json"
            with open(file_path, "w") as f:
                json.dump(data, f)
            files.append(str(file_path))

        fs = MockFileSystem()
        fs.files = {f: json.dumps(data) for f, data in zip(files, test_data)}

        # Test with threading enabled
        data_threaded = _read_json(fs, files, use_threads=True, as_dataframe=False)

        # Test with threading disabled
        data_sequential = _read_json(
            fs, files, use_threads=False, as_dataframe=False
        )

        # Both should produce the same DataFrames
        assert len(data_threaded) == len(data_sequential) == len(test_data)
        for i in range(len(test_data)):
            # Compare dict content directly
            # data_threaded is list of dicts or DataFrames depending on implementation of _read_json
            # The test calls it with as_dataframe=False, so expected list of dicts/list of lists?
            # _read_json returns dict or list of dicts.
            # Compare directly
            assert data_threaded[i] == data_sequential[i]

    def test_csv_use_threads_behavior(self, tmp_path):
        """Test that use_threads parameter works correctly in CSV reader."""
        import csv

        # Import the functions directly from the module
        from fsspeckit.core.ext.csv import _read_csv
        from fsspeckit.core.ext.json import _read_json

        # Create test CSV files
        test_data = [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"},
            {"id": 3, "value": "test3"},
        ]

        files = []
        for i, data in enumerate(test_data):
            file_path = tmp_path / f"test_{i}.csv"
            with open(file_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["id", "value"])
                writer.writeheader()
                writer.writerow(data)
            files.append(str(file_path))

        fs = MockFileSystem()
        # Fix: Each file should contain its own CSV content, not all concatenated content.
        fs.files = {
            f: ",".join(["id", "value"]) + "\n" + ",".join([str(d["id"]), d["value"]])
            for f, d in zip(files, test_data)
        }

        # Test with threading enabled
        dfs_threaded = _read_csv(fs, files, use_threads=True, concat=False)

        # Test with threading disabled
        dfs_sequential = _read_csv(fs, files, use_threads=False, concat=False)

        # Both should produce the same DataFrames
        assert len(dfs_threaded) == len(dfs_sequential) == len(test_data)
        for i in range(len(test_data)):
            # Compare DataFrame content (convert to dict for comparison)
            dict_threaded = dfs_threaded[i].to_dict(as_series=False)
            dict_sequential = dfs_sequential[i].to_dict(as_series=False)
            assert dict_threaded == dict_sequential


class TestJoblibAvailability:
    """Test joblib handling in CSV/Parquet helpers."""

    def test_all_helpers_use_threads_false_by_default(self):
        """Test that all CSV/Parquet helpers default to use_threads=False."""
        import inspect

        # Import all the helper functions
        import fsspeckit.core.ext.csv as csv_module
        import fsspeckit.core.ext.io as io_module
        import fsspeckit.core.ext.json as json_module
        import fsspeckit.core.ext.parquet as parquet_module

        # List of functions to check
        helper_functions = [
            csv_module._read_csv,
            csv_module._read_csv_batches,
            csv_module.read_csv,
            parquet_module._read_parquet,
            parquet_module._read_parquet_batches,
            parquet_module.read_parquet,
            json_module._read_json,
            json_module._read_json_batches,
            json_module.read_json,
            io_module.read_files,
            io_module.write_files,
        ]

        # Check that each function has use_threads parameter with default False
        for func in helper_functions:
            sig = inspect.signature(func)
            assert "use_threads" in sig.parameters, (
                f"{func.__name__} missing use_threads parameter"
            )

            param = sig.parameters["use_threads"]
            assert param.default is False, (
                f"{func.__name__} use_threads default should be False, got {param.default}"
            )

    def test_joblib_lazy_import_behavior(self, monkeypatch):
        """Test that joblib is only imported when needed."""
        import sys

        # Remove joblib from sys.modules if present
        if "joblib" in sys.modules:
            monkeypatch.setitem(sys.modules, "joblib", None)

        # Check that joblib availability is determined at import time
        import fsspeckit.common.optional as optional_mod

        # joblib availability should be determined by importlib.util.find_spec
        # This test verifies the module doesn't fail to import even without joblib
        assert hasattr(optional_mod, "_JOBLIB_AVAILABLE")
        assert isinstance(optional_mod._JOBLIB_AVAILABLE, bool)

        # The _import_joblib function should raise ImportError when joblib is not available
        if not optional_mod._JOBLIB_AVAILABLE:
            with pytest.raises(ImportError) as exc_info:
                optional_mod._import_joblib()

            assert "joblib" in str(exc_info.value).lower()
            assert "fsspeckit[datasets]" in str(exc_info.value).lower()

    def test_run_parallel_without_joblib_raises_clear_error(self, monkeypatch):
        """Test that run_parallel fails gracefully with clear error when joblib missing."""
        import sys

        # Simulate joblib not being available
        import fsspeckit.common.optional as optional_mod

        original_joblib_available = optional_mod._JOBLIB_AVAILABLE
        optional_mod._JOBLIB_AVAILABLE = False

        # Remove joblib from sys.modules if present
        if "joblib" in sys.modules:
            monkeypatch.setitem(sys.modules, "joblib", None)

        try:
            from fsspeckit.common.misc import run_parallel

            # run_parallel should raise ImportError with clear message when joblib is missing
            with pytest.raises(ImportError) as exc_info:
                run_parallel(lambda x: x * 2, [1, 2, 3])

            # Verify error message mentions joblib and installation
            assert "joblib" in str(exc_info.value).lower()
            assert "fsspeckit[datasets]" in str(exc_info.value).lower()

        finally:
            # Restore original state
            optional_mod._JOBLIB_AVAILABLE = original_joblib_available


class TestWritePyArrowDataset:
    """Test PyArrow dataset I/O helper workflows."""

    def test_write_standard_dataset(self, tmp_path):
        """Test standard dataset write without merge strategy."""
        from fsspec.implementations.local import LocalFileSystem

        from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

        fs = LocalFileSystem()
        io = PyarrowDatasetIO(filesystem=fs)

        table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        dataset_path = str(tmp_path / "dataset")

        io.write_dataset(data=table, path=dataset_path, mode="append")

        # Verify dataset was created
        assert fs.exists(dataset_path)
        assert any(fs.glob(f"{dataset_path}/**/*.parquet"))

        # Read back and verify
        result_table = _read_dataset_table(dataset_path)
        assert result_table.num_rows == 3
        assert result_table.column_names == ["id", "value"]

    def test_write_with_upsert_strategy(self, tmp_path):
        """Test upsert merge strategy."""
        from fsspec.implementations.local import LocalFileSystem

        from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

        fs = LocalFileSystem()
        io = PyarrowDatasetIO(filesystem=fs)

        # Create initial dataset
        initial = pa.table({"id": [1, 2], "value": ["a", "b"]})
        dataset_path = str(tmp_path / "dataset")
        io.write_dataset(data=initial, path=dataset_path, mode="overwrite")

        # Upsert with new and updated data
        upsert_data = pa.table({"id": [2, 3], "value": ["updated", "c"]})
        io.merge(
            data=upsert_data, path=dataset_path, strategy="upsert", key_columns="id"
        )

        # Verify results
        result = _read_dataset_table(dataset_path)
        assert result.num_rows == 3
        values = dict(
            zip(result.column("id").to_pylist(), result.column("value").to_pylist())
        )
        assert values[1] == "a"  # Unchanged
        assert values[2] == "updated"  # Updated
        assert values[3] == "c"  # Inserted

    def test_write_with_insert_strategy(self, tmp_path):
        """Test insert-only merge strategy."""
        from fsspec.implementations.local import LocalFileSystem

        from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

        fs = LocalFileSystem()
        io = PyarrowDatasetIO(filesystem=fs)

        # Create initial dataset
        initial = pa.table({"id": [1, 2], "value": ["a", "b"]})
        dataset_path = str(tmp_path / "dataset")
        io.write_dataset(data=initial, path=dataset_path, mode="overwrite")

        # Insert with new and existing data
        insert_data = pa.table({"id": [2, 3], "value": ["dupe", "c"]})
        io.merge(
            data=insert_data, path=dataset_path, strategy="insert", key_columns="id"
        )

        # Verify only new rows were inserted
        result = _read_dataset_table(dataset_path)
        assert result.num_rows == 3
        values = dict(
            zip(result.column("id").to_pylist(), result.column("value").to_pylist())
        )
        assert values[1] == "a"  # Unchanged
        assert values[2] == "b"  # Unchanged (was "dupe" in source)
        assert values[3] == "c"  # Inserted

    def test_write_with_update_strategy(self, tmp_path):
        """Test update-only merge strategy."""
        from fsspec.implementations.local import LocalFileSystem

        from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

        fs = LocalFileSystem()
        io = PyarrowDatasetIO(filesystem=fs)

        # Create initial dataset
        initial = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        dataset_path = str(tmp_path / "dataset")
        io.write_dataset(data=initial, path=dataset_path, mode="overwrite")

        # Update existing and try to insert new
        update_data = pa.table({"id": [2, 4], "value": ["updated", "new"]})
        io.merge(
            data=update_data, path=dataset_path, strategy="update", key_columns="id"
        )

        # Verify only existing rows were updated
        result = _read_dataset_table(dataset_path)
        assert result.num_rows == 3
        values = dict(
            zip(result.column("id").to_pylist(), result.column("value").to_pylist())
        )
        assert values[1] == "a"  # Unchanged
        assert values[2] == "updated"  # Updated
        assert values[3] == "c"  # Unchanged (new row was ignored)

    def test_write_with_deduplicate_strategy(self, tmp_path):
        """Test deduplicate merge strategy is not supported by merge()."""
        from fsspec.implementations.local import LocalFileSystem

        from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

        fs = LocalFileSystem()
        io = PyarrowDatasetIO(filesystem=fs)

        # Create initial dataset
        initial = pa.table({"id": [1], "value": ["original"], "ts": [1]})
        dataset_path = str(tmp_path / "dataset")
        io.write_dataset(data=initial, path=dataset_path, mode="overwrite")

        # Deduplicate with multiple rows
        dedup_data = pa.table(
            {
                "id": [1, 1, 2],
                "value": ["old", "new", "c"],
                "ts": [1, 2, 1],
            }
        )
        with pytest.raises(ValueError, match="strategy must be one of"):
            io.merge(  # type: ignore[arg-type]
                data=dedup_data,
                path=dataset_path,
                strategy="deduplicate",
                key_columns="id",
            )

    def test_write_with_full_merge_strategy(self, tmp_path):
        """Test full_merge strategy is not supported by merge()."""
        from fsspec.implementations.local import LocalFileSystem

        from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

        fs = LocalFileSystem()
        io = PyarrowDatasetIO(filesystem=fs)

        # Create initial dataset
        initial = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        dataset_path = str(tmp_path / "dataset")
        io.write_dataset(data=initial, path=dataset_path, mode="overwrite")

        # Full merge with subset
        merge_data = pa.table({"id": [2, 4], "value": ["updated", "d"]})
        with pytest.raises(ValueError, match="strategy must be one of"):
            io.merge(  # type: ignore[arg-type]
                data=merge_data,
                path=dataset_path,
                strategy="full_merge",
                key_columns="id",
            )


def _read_dataset_table(path: str) -> pa.Table:
    """Read a dataset as a single table."""
    import pyarrow.dataset as ds

    dataset = ds.dataset(path)
    return dataset.to_table()
