"""Backend consistency validation test for fsspeckit.

This test validates that both DuckDB and PyArrow backends produce
identical results after the critical bug fixes and refactoring to inherit
from BaseDatasetHandler.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Any
import pyarrow as pa

# Test Configuration
SKIP_SLOW_TESTS = True
TEMP_DIR_PREFIX = "fsspeckit_backend_test_"


class BackendConsistencyValidator:
    """Main validator class for backend consistency testing."""

    def __init__(self, temp_dir: Path):
        """Initialize validator with temporary directory."""
        self.temp_dir = temp_dir
        self.duckdb_backend = None
        self.pyarrow_backend = None
        self.test_results = []

    def setup_backends(self):
        """Initialize both backends."""
        try:
            # Import DuckDB backend
            from fsspeckit.datasets.duckdb.connection import create_duckdb_connection
            from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

            conn = create_duckdb_connection()
            self.duckdb_backend = DuckDBDatasetIO(conn)

            # Import PyArrow backend
            from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO

            self.pyarrow_backend = PyarrowDatasetIO()

        except ImportError as e:
            pytest.skip(f"Backend import failed: {e}")

    def compare_results(
        self, operation: str, duckdb_result: Any, pyarrow_result: Any
    ) -> bool:
        """Compare results from both backends."""
        try:
            # Handle different result types
            if isinstance(duckdb_result, pa.Table) and isinstance(
                pyarrow_result, pa.Table
            ):
                return self._compare_tables(duckdb_result, pyarrow_result)
            else:
                return duckdb_result == pyarrow_result

        except Exception as e:
            self.test_results.append(
                {"operation": operation, "status": "ERROR", "error": str(e)}
            )
            return False

    def _compare_tables(self, table1: pa.Table, table2: pa.Table) -> bool:
        """Compare two PyArrow tables for exact equality."""
        try:
            # Check schemas
            if table1.schema != table2.schema:
                return False

            # Check row counts
            if len(table1) != len(table2):
                return False

            # Sort by all columns to ensure consistent ordering
            if len(table1) > 0:
                sort_columns = table1.column_names
                sorted1 = table1.sort_by(sort_columns)
                sorted2 = table2.sort_by(sort_columns)
                return sorted1.equals(sorted2)
            else:
                return True  # Both tables are empty

        except Exception as e:
            return False


def generate_test_data_variants():
    """Generate various test data scenarios."""

    # Basic test data
    basic_data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "value": [10.5, 20.3, 30.7, 40.1, 50.9],
        "active": [True, False, True, False, True],
        "category": ["A", "B", "A", "C", "B"],
    }

    # Large dataset for performance testing
    large_data = {
        "id": list(range(1000)),
        "value": [i * 0.5 for i in range(1000)],
        "category": ["X", "Y", "Z"] * 334,
        "active": [i % 2 == 0 for i in range(1000)],
    }

    # Multiple key columns test data
    multi_key_data = {
        "user_id": [1, 1, 2, 2, 3],
        "session_id": [100, 101, 200, 201, 300],
        "timestamp": [
            "2023-01-01",
            "2023-01-02",
            "2023-01-01",
            "2023-01-03",
            "2023-01-02",
        ],
        "event": ["login", "click", "login", "logout", "login"],
        "duration": [0, 5, 0, 15, 0],
    }

    return {
        "basic": pa.Table.from_pydict(basic_data),
        "large": pa.Table.from_pydict(large_data),
        "multi_key": pa.Table.from_pydict(multi_key_data),
    }


class TestBackendConsistency:
    """Main test class for backend consistency validation."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        temp_dir = Path(tempfile.mkdtemp(prefix=TEMP_DIR_PREFIX))
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def validator(self, temp_dir):
        """Create validator instance."""
        val = BackendConsistencyValidator(temp_dir)
        val.setup_backends()
        return val

    @pytest.fixture
    def test_data(self):
        """Generate test data for all scenarios."""
        return generate_test_data_variants()

    def test_basic_read_write_consistency(self, validator, test_data):
        """Test that both backends produce identical results for basic read/write operations."""

        for data_name, data_table in test_data.items():
            if data_name == "large" and SKIP_SLOW_TESTS:
                continue

            # Test single file read/write
            duckdb_file = validator.temp_dir / f"duckdb_{data_name}.parquet"
            pyarrow_file = validator.temp_dir / f"pyarrow_{data_name}.parquet"

            # Write with both backends
            validator.duckdb_backend.write_parquet(data_table, str(duckdb_file))
            validator.pyarrow_backend.write_parquet(data_table, str(pyarrow_file))

            # Read with both backends
            duckdb_result = validator.duckdb_backend.read_parquet(str(duckdb_file))
            pyarrow_result = validator.pyarrow_backend.read_parquet(str(pyarrow_file))

            # Compare results
            assert validator.compare_results(
                f"basic_read_write_{data_name}", duckdb_result, pyarrow_result
            ), f"Basic read/write failed for {data_name}"

    def test_dataset_write_append_consistency(self, validator, test_data):
        """Test dataset append operations between backends."""

        basic_data = test_data["basic"]

        # Create datasets
        duckdb_dataset = validator.temp_dir / "duckdb_dataset"
        pyarrow_dataset = validator.temp_dir / "pyarrow_dataset"

        # Split data into two parts for append testing
        part1 = basic_data.slice(0, 3)
        part2 = basic_data.slice(3, 2)

        # Write initial data
        validator.duckdb_backend.write_dataset(
            part1, str(duckdb_dataset), mode="overwrite"
        )
        validator.pyarrow_backend.write_dataset(
            part1, str(pyarrow_dataset), mode="overwrite"
        )

        # Append second part
        validator.duckdb_backend.write_dataset(
            part2, str(duckdb_dataset), mode="append"
        )
        validator.pyarrow_backend.write_dataset(
            part2, str(pyarrow_dataset), mode="append"
        )

        # Read results
        duckdb_result = validator.duckdb_backend.read_parquet(str(duckdb_dataset))
        pyarrow_result = validator.pyarrow_backend.read_parquet(str(pyarrow_dataset))

        # Compare appended results
        assert validator.compare_results(
            "dataset_append", duckdb_result, pyarrow_result
        ), "Dataset append operation produced different results"

    def test_empty_dataset_consistency(self, validator):
        """Test handling of empty datasets."""

        # Create empty table
        empty_table = pa.Table.from_pydict({"id": [], "name": [], "value": []})

        # Test empty dataset operations
        empty_dataset = validator.temp_dir / "empty_dataset"

        # Write empty dataset
        validator.duckdb_backend.write_dataset(
            empty_table, str(empty_dataset), mode="overwrite"
        )
        validator.pyarrow_backend.write_dataset(
            empty_table, str(empty_dataset), mode="overwrite"
        )

        # Read back and compare
        duckdb_result = validator.duckdb_backend.read_parquet(str(empty_dataset))
        pyarrow_result = validator.pyarrow_backend.read_parquet(str(empty_dataset))

        assert validator.compare_results(
            "empty_dataset", duckdb_result, pyarrow_result
        ), "Empty dataset handling produced different results"


if __name__ == "__main__":
    # Simple test runner
    pytest.main([__file__, "-v", "--tb=short"])
