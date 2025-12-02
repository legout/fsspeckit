"""Test threading behavior in JSON and CSV readers."""

import tempfile
import json
from unittest.mock import patch

import pytest


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

        return MockFile(self.files[path])


class TestThreadingBehavior:
    """Test that use_threads=True and use_threads=False produce same data."""

    def test_json_use_threads_behavior(self, tmp_path):
        """Test that use_threads parameter works correctly in JSON reader."""
        # Import the functions directly from the module
        from fsspeckit.core.ext import _read_json, _read_csv

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
        data_threaded = _read_json(files, fs=fs, use_threads=True, as_dataframe=False)

        # Test with threading disabled
        data_sequential = _read_json(
            files, fs=fs, use_threads=False, as_dataframe=False
        )
        fs.files = {f: csv_content for f, data in zip(files, test_data)}

        # Test with threading enabled
        dfs_threaded = _read_csv(files, use_threads=True, concat=False)

        # Test with threading disabled
        dfs_sequential = _read_csv(files, use_threads=False, concat=False)

        # Both should produce the same DataFrames
        assert len(dfs_threaded) == len(dfs_sequential) == len(test_data)
        for i in range(len(test_data)):
            # Compare DataFrame content (convert to dict for comparison)
            dict_threaded = dfs_threaded[i].to_dict()
            dict_sequential = dfs_sequential[i].to_dict()
            assert dict_threaded == dict_sequential

    def test_csv_use_threads_behavior(self, tmp_path):
        """Test that use_threads parameter works correctly in CSV reader."""
        import csv

        # Import the functions directly from the module
        from fsspeckit.core.ext import _read_json, _read_csv

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
                writer.writerow(["id", "value"])
                writer.writerow(data)
            files.append(str(file_path))

        fs = MockFileSystem()
        csv_content = "\n".join(
            [
                ",".join(["id", "value"]) + "\n" + ",".join([str(d["id"]), d["value"]])
                for d in test_data
            ]
        )
        fs.files = {f: csv_content for f, data in zip(files, test_data)}

        # Test with threading enabled
        dfs_threaded = _read_csv(files, use_threads=True, concat=False)

        # Test with threading disabled
        dfs_sequential = _read_csv(files, use_threads=False, concat=False)

        # Both should produce the same DataFrames
        assert len(dfs_threaded) == len(dfs_sequential) == len(test_data)
        for i in range(len(test_data)):
            # Compare DataFrame content (convert to dict for comparison)
            dict_threaded = dfs_threaded[i].to_dict()
            dict_sequential = dfs_sequential[i].to_dict()
            assert dict_threaded == dict_sequential
