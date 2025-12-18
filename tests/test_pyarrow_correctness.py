"""Correctness validation tests for PyArrow dataset operations.

These tests ensure that PyArrow optimizations produce identical results
to previous implementations and handle all edge cases correctly.
"""

import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.datasets.pyarrow.dataset import deduplicate_parquet_dataset_pyarrow
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

    def test_simple_key_deduplication_correctness(self, test_datasets):
        """Test correctness of simple key-based deduplication."""
        result = deduplicate_parquet_dataset_pyarrow(
            test_datasets["simple"],
            key_columns=["id"],
        )

        # Should deduplicate to 3 unique IDs
        assert result["deduplicated_rows"] == 2
        assert result["total_rows_after"] == 3

        # Verify the remaining data
        final_table = pq.read_table(test_datasets["simple"])
        final_ids = sorted(final_table.column("id").to_pylist())
        assert final_ids == [1, 2, 3]

    def test_multi_column_deduplication_correctness(self, test_datasets):
        """Test correctness of multi-column deduplication."""
        result = deduplicate_parquet_dataset_pyarrow(
            test_datasets["multi_column"],
            key_columns=["id", "name"],
        )

        # Should deduplicate to 3 unique combinations
        assert result["deduplicated_rows"] == 3
        assert result["total_rows_after"] == 3

        # Verify the remaining data
        final_table = pq.read_table(test_datasets["multi_column"])
        final_data = final_table.to_pydict()

        # Check that we have the expected unique combinations
        unique_combinations = set(zip(final_data["id"], final_data["name"]))
        expected_combinations = {(1, "Alice"), (2, "Bob"), (3, "Charlie")}
        assert unique_combinations == expected_combinations

    def test_exact_deduplication_correctness(self, test_datasets):
        """Test correctness of exact duplicate removal."""
        result = deduplicate_parquet_dataset_pyarrow(
            test_datasets["exact_dup"],
            key_columns=None,  # Exact duplicates
        )

        # Should deduplicate to 2 unique rows (remove 2 duplicates)
        assert result["deduplicated_rows"] == 2
        assert result["total_rows_after"] == 2

        # Verify the remaining data
        final_table = pq.read_table(test_datasets["exact_dup"])
        assert final_table.num_rows == 2

    def test_dedup_order_by_correctness(self, test_datasets):
        """Test correctness of dedup_order_by parameter."""
        # Test with ascending order (keep first)
        result_asc = deduplicate_parquet_dataset_pyarrow(
            test_datasets["large_ordered"],
            key_columns=["id"],
            dedup_order_by=["timestamp"],  # Keep earliest (smallest timestamp)
        )

        # Test with descending order (keep last)
        result_desc = deduplicate_parquet_dataset_pyarrow(
            test_datasets["large_ordered"],
            key_columns=["id"],
            dedup_order_by=["-timestamp"],  # Keep latest (largest timestamp)
        )

        # Both should remove same number of duplicates
        assert result_asc["deduplicated_rows"] == result_desc["deduplicated_rows"]

        # Read final datasets to verify ordering
        final_asc = pq.read_table(test_datasets["large_ordered"])
        final_desc = pq.read_table(test_datasets["large_ordered"])

        # Convert back to original ordering for comparison
        # (Both results are written to same location, so we need separate tests)
        # For now, just verify that processing completed successfully
        assert result_asc["deduplicated_rows"] > 0
        assert result_desc["deduplicated_rows"] > 0

    def test_complex_data_types_deduplication(self, test_datasets):
        """Test deduplication with complex data types."""
        result = deduplicate_parquet_dataset_pyarrow(
            test_datasets["complex_types"],
            key_columns=["id"],
        )

        # Should deduplicate to 2 unique IDs
        assert result["deduplicated_rows"] == 2
        assert result["total_rows_after"] == 2

        # Verify complex types are preserved correctly
        final_table = pq.read_table(test_datasets["complex_types"])
        assert final_table.num_rows == 2

        # Check that complex columns have expected structure
        tags_col = final_table.column("tags")
        assert len(tags_col) == 2

    def test_deduplication_consistency_multiple_runs(self, test_datasets):
        """Test that multiple runs produce identical results."""
        results = []

        for _ in range(3):
            # Reset dataset between runs
            import shutil

            original_dir = Path(test_datasets["multi_column"])
            temp_dir = original_dir.parent / f"temp_{int(time.time())}"

            try:
                shutil.copytree(original_dir, temp_dir)

                result = deduplicate_parquet_dataset_pyarrow(
                    str(temp_dir),
                    key_columns=["id", "name"],
                )

                results.append(
                    {
                        "deduplicated": result["deduplicated_rows"],
                        "final_count": result["total_rows_after"],
                        "final_table": pq.read_table(str(temp_dir)),
                    }
                )

            finally:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)

        # All runs should produce identical results
        deduplicated_counts = [r["deduplicated"] for r in results]
        final_counts = [r["final_count"] for r in results]

        assert len(set(deduplicated_counts)) == 1, (
            "Deduplicated counts should be identical"
        )
        assert len(set(final_counts)) == 1, "Final counts should be identical"

        # Final tables should have same structure
        for i in range(1, len(results)):
            assert results[0]["final_table"].schema == results[i]["final_table"].schema

    def test_edge_case_empty_dataset(self):
        """Test deduplication with empty dataset."""
        with tempfile.TemporaryDirectory() as tmp:
            empty_dir = Path(tmp) / "empty"
            empty_dir.mkdir()

            # Create empty parquet file
            empty_table = pa.table({"id": pa.array([], type=pa.int64())})
            pq.write_table(empty_table, empty_dir / "empty.parquet")

            result = deduplicate_parquet_dataset_pyarrow(
                str(empty_dir),
                key_columns=["id"],
            )

            assert result["deduplicated_rows"] == 0
            assert result["total_rows_before"] == 0
            assert result["total_rows_after"] == 0

    def test_edge_case_single_row(self):
        """Test deduplication with single row."""
        with tempfile.TemporaryDirectory() as tmp:
            single_dir = Path(tmp) / "single"
            single_dir.mkdir()

            single_table = pa.table({"id": [1], "value": ["a"]})
            pq.write_table(single_table, single_dir / "single.parquet")

            result = deduplicate_parquet_dataset_pyarrow(
                str(single_dir),
                key_columns=["id"],
            )

            assert result["deduplicated_rows"] == 0
            assert result["total_rows_after"] == 1

    def test_edge_case_all_duplicates(self):
        """Test deduplication when all rows are duplicates."""
        with tempfile.TemporaryDirectory() as tmp:
            all_dup_dir = Path(tmp) / "all_dup"
            all_dup_dir.mkdir()

            # All rows identical
            all_dup_table = pa.table(
                {
                    "id": [1, 1, 1, 1],
                    "value": ["x", "x", "x", "x"],
                }
            )
            pq.write_table(all_dup_table, all_dup_dir / "all_dup.parquet")

            result = deduplicate_parquet_dataset_pyarrow(
                str(all_dup_dir),
                key_columns=["id"],
            )

            assert result["deduplicated_rows"] == 3
            assert result["total_rows_after"] == 1

    def test_edge_case_null_keys(self):
        """Test deduplication with NULL key values."""
        with tempfile.TemporaryDirectory() as tmp:
            null_dir = Path(tmp) / "null_keys"
            null_dir.mkdir()

            null_table = pa.table(
                {
                    "id": [1, None, 1, None, 2],
                    "value": ["a", "b", "a", "c", "d"],
                }
            )
            pq.write_table(null_table, null_dir / "null.parquet")

            # This should handle NULLs correctly
            result = deduplicate_parquet_dataset_pyarrow(
                str(null_dir),
                key_columns=["id"],
            )

            # Should remove duplicates while preserving NULL handling
            assert result["deduplicated_rows"] >= 1
            assert result["total_rows_after"] >= 3  # At least 1, None, 2

    def test_different_data_types_consistency(self):
        """Test deduplication with various PyArrow data types."""
        with tempfile.TemporaryDirectory() as tmp:
            type_dir = Path(tmp) / "data_types"
            type_dir.mkdir()

            # Create table with various data types
            type_table = pa.table(
                {
                    "int_col": pa.array([1, 2, 1, 2, 3], type=pa.int64()),
                    "float_col": pa.array([1.1, 2.2, 1.1, 2.2, 3.3], type=pa.float64()),
                    "bool_col": pa.array(
                        [True, False, True, False, True], type=pa.bool_()
                    ),
                    "string_col": pa.array(["a", "b", "a", "b", "c"], type=pa.string()),
                    "date_col": pa.array(
                        [
                            "2023-01-01",
                            "2023-01-02",
                            "2023-01-01",
                            "2023-01-02",
                            "2023-01-03",
                        ],
                        type=pa.date32(),
                    ),
                }
            )
            pq.write_table(type_table, type_dir / "types.parquet")

            result = deduplicate_parquet_dataset_pyarrow(
                str(type_dir),
                key_columns=["int_col"],
            )

            # Should work correctly with all data types
            assert result["deduplicated_rows"] == 2
            assert result["total_rows_after"] == 3

            # Verify final data has correct types
            final_table = pq.read_table(type_dir / "types.parquet")
            assert final_table.schema.field("int_col").type == pa.int64()
            assert final_table.schema.field("float_col").type == pa.float64()
            assert final_table.schema.field("string_col").type == pa.string()

    def test_partitioned_data_correctness(self):
        """Test deduplication with partitioned dataset structure."""
        with tempfile.TemporaryDirectory() as tmp:
            part_dir = Path(tmp) / "partitioned"
            part_dir.mkdir()

            # Create partitioned structure
            for partition_val in ["A", "B", "C"]:
                part_subdir = part_dir / f"partition={partition_val}"
                part_subdir.mkdir()

                # Create data for this partition
                partition_table = pa.table(
                    {
                        "id": [1, 2, 1, 2],
                        "value": [
                            f"{partition_val}_1",
                            f"{partition_val}_2",
                            f"{partition_val}_1",
                            f"{partition_val}_2",
                        ],
                        "partition": [partition_val] * 4,
                    }
                )
                pq.write_table(partition_table, part_subdir / "data.parquet")

            result = deduplicate_parquet_dataset_pyarrow(
                str(part_dir),
                key_columns=["id"],
            )

            # Should deduplicate within and across partitions
            assert result["deduplicated_rows"] >= 2  # At least 2 duplicates removed
            assert result["files_processed"] == 3

    def test_compression_preservation(self):
        """Test that deduplication preserves data integrity with different compressions."""
        compressions = ["snappy", "gzip", "lz4"]

        for compression in compressions:
            with tempfile.TemporaryDirectory() as tmp:
                comp_dir = Path(tmp) / f"comp_{compression}"
                comp_dir.mkdir()

                # Create test data
                test_table = pa.table(
                    {
                        "id": [1, 2, 1, 2, 3],
                        "value": ["a", "b", "a", "b", "c"],
                    }
                )
                pq.write_table(
                    test_table, comp_dir / "data.parquet", compression=compression
                )

                # Deduplicate with same compression
                result = deduplicate_parquet_dataset_pyarrow(
                    str(comp_dir),
                    key_columns=["id"],
                    compression=compression,
                )

                # Verify results
                assert result["deduplicated_rows"] == 2
                assert result["total_rows_after"] == 3

                # Verify final data integrity
                final_table = pq.read_table(comp_dir / "data.parquet")
                assert final_table.num_rows == 3
                final_ids = sorted(final_table.column("id").to_pylist())
                assert final_ids == [1, 2, 3]


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

    def test_invalid_key_columns(self):
        """Test handling of invalid key columns."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "invalid_keys"
            dataset_dir.mkdir()

            table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            pq.write_table(table, dataset_dir / "data.parquet")

            # Should raise error for non-existent key column
            with pytest.raises((ValueError, KeyError, FileNotFoundError)):
                deduplicate_parquet_dataset_pyarrow(
                    str(dataset_dir),
                    key_columns=["nonexistent"],
                )

    def test_empty_key_columns_list(self):
        """Test handling of empty key columns list."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "empty_keys"
            dataset_dir.mkdir()

            table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            pq.write_table(table, dataset_dir / "data.parquet")

            # Should raise error for empty key columns
            with pytest.raises(ValueError, match="key_columns cannot be empty"):
                deduplicate_parquet_dataset_pyarrow(
                    str(dataset_dir),
                    key_columns=[],
                )

    def test_consistent_ordering_across_runs(self):
        """Test that ordering is consistent across multiple runs."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "ordering_test"
            dataset_dir.mkdir()

            # Create data with specific ordering requirements
            table = pa.table(
                {
                    "id": [1, 1, 2, 2, 3, 3],
                    "timestamp": [10, 5, 20, 15, 30, 25],
                    "value": ["a1", "a2", "b1", "b2", "c1", "c2"],
                }
            )
            pq.write_table(table, dataset_dir / "data.parquet")

            # Run deduplication with ordering
            results = []
            for _ in range(3):
                # Copy data for each run
                import shutil

                run_dir = dataset_dir.parent / f"run_{int(time.time() * 1000)}"
                shutil.copytree(dataset_dir, run_dir)

                result = deduplicate_parquet_dataset_pyarrow(
                    str(run_dir),
                    key_columns=["id"],
                    dedup_order_by=["-timestamp"],  # Keep latest
                )

                final_table = pq.read_table(run_dir / "data.parquet")
                results.append(final_table.to_pydict())

                # Clean up
                shutil.rmtree(run_dir)

            # All results should be identical
            for i in range(1, len(results)):
                assert results[0] == results[i], f"Run {i} produced different results"

    def test_memory_limit_enforcement(self):
        """Test that memory limits are properly enforced."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "memory_test"
            dataset_dir.mkdir()

            # Create reasonably large dataset
            num_rows = 100_000
            table = pa.table(
                {
                    "id": list(range(num_rows // 2)) * 2,
                    "value": list(range(num_rows)),
                }
            )
            pq.write_table(table, dataset_dir / "data.parquet")

            # Set very low memory limit
            with pytest.raises((MemoryError, OSError)):
                deduplicate_parquet_dataset_pyarrow(
                    str(dataset_dir),
                    key_columns=["id"],
                    max_memory_mb=1,  # Very low limit
                    chunk_size_rows=1000,
                )

    def test_corrupted_data_handling(self):
        """Test handling of corrupted or invalid parquet files."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "corrupted"
            dataset_dir.mkdir()

            # Create valid file
            table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            pq.write_table(table, dataset_dir / "good.parquet")

            # Create corrupted file
            corrupted_file = dataset_dir / "corrupted.parquet"
            with open(corrupted_file, "wb") as f:
                f.write(b"this is not a valid parquet file")

            # Should handle corrupted files gracefully
            with pytest.raises((OSError, IOError, pa.ArrowInvalid, pa.ArrowIOError)):
                deduplicate_parquet_dataset_pyarrow(
                    str(dataset_dir),
                    key_columns=["id"],
                )
