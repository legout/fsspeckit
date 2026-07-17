"""Correctness validation tests for PyArrow dataset operations.

These tests ensure that PyArrow optimizations produce identical results
to previous implementations and handle all edge cases correctly.
"""

import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO


class TestPyArrowCorrectnessValidation:
    """Correctness tests comparing PyArrow results with expected outputs."""

    @pytest.fixture
    def test_datasets(self):
        """Create various test datasets for correctness testing."""
        with tempfile.TemporaryDirectory() as tmp:
            datasets = {}

            # Dataset 1: Simple duplicates
            simple_dir = Path(tmp) / "simple_dup"
            simple_dir.mkdir()
            simple_table = pa.table(
                {
                    "id": [1, 2, 1, 2, 3],
                    "value": ["a", "b", "a", "b", "c"],
                }
            )
            pq.write_table(simple_table, simple_dir / "data.parquet")
            datasets["simple"] = str(simple_dir)

            # Dataset 2: Multiple columns with duplicates
            multi_dir = Path(tmp) / "multi_col"
            multi_dir.mkdir()
            multi_table = pa.table(
                {
                    "id": [1, 2, 1, 2, 3, 1],
                    "name": ["Alice", "Bob", "Alice", "Bob", "Charlie", "Alice"],
                    "age": [25, 30, 25, 30, 35, 25],
                    "score": [85.5, 90.2, 85.5, 90.2, 78.9, 85.5],
                }
            )
            pq.write_table(multi_table, multi_dir / "data.parquet")
            datasets["multi_column"] = str(multi_dir)

            # Dataset 3: Exact duplicates
            exact_dir = Path(tmp) / "exact_dup"
            exact_dir.mkdir()
            exact_table = pa.table(
                {
                    "id": [1, 2, 1, 2],
                    "value": ["x", "y", "x", "y"],  # Exact duplicates
                }
            )
            # Write same data twice to create exact duplicates
            pq.write_table(exact_table, exact_dir / "part1.parquet")
            pq.write_table(exact_table, exact_dir / "part2.parquet")
            datasets["exact_dup"] = str(exact_dir)

            # Dataset 4: Large dataset with ordering
            large_dir = Path(tmp) / "large_order"
            large_dir.mkdir()
            num_rows = 10000
            ids = list(range(1000)) * 10  # 10 duplicates per ID
            values = np.random.randn(num_rows).tolist()
            timestamps = list(range(num_rows))  # Sequential timestamps
            categories = [f"cat_{i % 100}" for i in range(num_rows)]

            large_table = pa.table(
                {
                    "id": ids,
                    "value": values,
                    "timestamp": timestamps,
                    "category": categories,
                }
            )
            pq.write_table(large_table, large_dir / "data.parquet")
            datasets["large_ordered"] = str(large_dir)

            # Dataset 5: Complex data types
            complex_dir = Path(tmp) / "complex_types"
            complex_dir.mkdir()
            complex_table = pa.table(
                {
                    "id": [1, 2, 1, 2],
                    "tags": [["tag1", "tag2"], ["tag3"], ["tag1", "tag2"], ["tag3"]],
                    "metadata": [
                        {"key": "val1"},
                        {"key": "val2"},
                        {"key": "val1"},
                        {"key": "val2"},
                    ],
                    "values": [[1.0, 2.0], [3.0], [1.0, 2.0], [3.0]],
                }
            )
            pq.write_table(complex_table, complex_dir / "data.parquet")
            datasets["complex_types"] = str(complex_dir)

            yield datasets


class TestPyArrowMergeCorrectness:
    """Correctness tests for merge operations."""

    @pytest.fixture
    def merge_test_data(self):
        """Create test data for merge operations."""
        with tempfile.TemporaryDirectory() as tmp:
            # Target dataset
            target_dir = Path(tmp) / "target"
            target_dir.mkdir()

            target_table = pa.table(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "value": [10, 20, 30],
                }
            )
            pq.write_table(target_table, target_dir / "target.parquet")

            # Source data
            source_table = pa.table(
                {
                    "id": [2, 3, 4, 5],
                    "name": ["Bob Updated", "Charlie", "David", "Eve"],
                    "value": [25, 35, 40, 50],
                }
            )

            yield str(target_dir), source_table

    def test_merge_insert_correctness(self, merge_test_data):
        """Test correctness of INSERT merge strategy."""
        target_dir, source_table = merge_test_data

        handler = PyarrowDatasetIO()
        result = handler.merge(
            data=source_table,
            path=target_dir,
            strategy="insert",
            key_columns=["id"],
        )

        # Should only insert new keys (4, 5)
        assert result.inserted == 2
        assert result.updated == 0
        assert result.target_count_after == 5

        # Verify final data
        final_table = pq.read_table(Path(target_dir) / "target.parquet")
        final_data = final_table.to_pydict()

        # Should have original 3 + 2 new = 5 rows
        assert len(final_data["id"]) == 5
        assert set(final_data["id"]) == {1, 2, 3, 4, 5}

        # Original values should be preserved
        assert final_data["value"][final_data["id"].index(1)] == 10
        assert final_data["value"][final_data["id"].index(2)] == 20

    def test_merge_update_correctness(self, merge_test_data):
        """Test correctness of UPDATE merge strategy."""
        target_dir, source_table = merge_test_data

        handler = PyarrowDatasetIO()
        result = handler.merge(
            data=source_table,
            path=target_dir,
            strategy="update",
            key_columns=["id"],
        )

        # Should only update existing keys (2, 3)
        assert result.inserted == 0
        assert result.updated == 2
        assert result.target_count_after == 3

        # Verify final data
        final_table = pq.read_table(Path(target_dir) / "target.parquet")
        final_data = final_table.to_pydict()

        # Should still have 3 rows
        assert len(final_data["id"]) == 3
        assert set(final_data["id"]) == {1, 2, 3}

        # Updated values should be present
        id_2_idx = final_data["id"].index(2)
        id_3_idx = final_data["id"].index(3)
        assert final_data["value"][id_2_idx] == 25  # Updated
        assert final_data["value"][id_3_idx] == 35  # Updated

    def test_merge_upsert_correctness(self, merge_test_data):
        """Test correctness of UPSERT merge strategy."""
        target_dir, source_table = merge_test_data

        handler = PyarrowDatasetIO()
        result = handler.merge(
            data=source_table,
            path=target_dir,
            strategy="upsert",
            key_columns=["id"],
        )

        # Should update 2 and insert 2
        assert result.inserted == 2
        assert result.updated == 2
        assert result.target_count_after == 5

        # Verify final data
        final_table = pq.read_table(Path(target_dir) / "target.parquet")
        final_data = final_table.to_pydict()

        # Should have original 3 + 2 new = 5 rows
        assert len(final_data["id"]) == 5
        assert set(final_data["id"]) == {1, 2, 3, 4, 5}

        # Updated values
        id_2_idx = final_data["id"].index(2)
        id_3_idx = final_data["id"].index(3)
        assert final_data["value"][id_2_idx] == 25  # Updated
        assert final_data["value"][id_3_idx] == 35  # Updated

        # New values
        id_4_idx = final_data["id"].index(4)
        id_5_idx = final_data["id"].index(5)
        assert final_data["value"][id_4_idx] == 40  # New
        assert final_data["value"][id_5_idx] == 50  # New

    def test_merge_with_missing_columns(self):
        """Test merge when source has missing columns."""
        with tempfile.TemporaryDirectory() as tmp:
            target_dir = Path(tmp) / "target"
            target_dir.mkdir()

            # Target has extra column
            target_table = pa.table(
                {
                    "id": [1, 2],
                    "name": ["Alice", "Bob"],
                    "value": [10, 20],
                    "category": ["A", "B"],
                }
            )
            pq.write_table(target_table, target_dir / "target.parquet")

            # Source missing 'category'
            source_table = pa.table(
                {
                    "id": [2, 3],
                    "name": ["Bob Updated", "Charlie"],
                    "value": [25, 30],
                }
            )

            handler = PyarrowDatasetIO()
            result = handler.merge(
                data=source_table,
                path=str(target_dir),
                strategy="upsert",
                key_columns=["id"],
            )

            assert result.updated == 1
            assert result.inserted == 1

            # Verify final table has all columns
            final_table = pq.read_table(target_dir / "target.parquet")
            assert "category" in final_table.column_names

            # Missing column should have nulls for new rows
            final_data = final_table.to_pydict()
            category_for_id_3 = final_data["category"][final_data["id"].index(3)]
            assert category_for_id_3 is None


class TestPyArrowEdgeCaseCorrectness:
    """Correctness tests for edge cases and error scenarios."""

    def test_read_parquet_sql_string_filter(self, tmp_path):
        """read_parquet with a SQL-string filter resolves via fsspeckit.sql.filters.

        Regression test for issue #50: a dangling import of the removed
        ``fsspeckit.common.sql_filters`` module raised ModuleNotFoundError whenever
        a SQL string was passed as ``filters`` to ``read_parquet``.
        """
        table = pa.table({"x": [1, 2, 3]})
        path = tmp_path / "data.parquet"
        pq.write_table(table, path)

        result = PyarrowDatasetIO().read_parquet(str(path), filters="x > 1")

        assert result.num_rows == 2
        assert set(result.column("x").to_pylist()) == {2, 3}
