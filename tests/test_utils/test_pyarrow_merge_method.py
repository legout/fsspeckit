"""Tests for PyarrowDatasetIO.merge() method with incremental rewrite strategies."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from fsspeckit.common.optional import _DUCKDB_AVAILABLE
from fsspeckit.core.incremental import MergeResult
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

pytestmark = pytest.mark.skipif(not _DUCKDB_AVAILABLE, reason="DuckDB not available")

from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection


def _read_dataset_table(path: str) -> pa.Table:
    dataset = ds.dataset(path)
    return dataset.to_table()


def _count_parquet_files(path) -> int:
    """Count parquet files in a directory."""
    import os

    count = 0
    for root, _, files in os.walk(path):
        count += sum(1 for f in files if f.endswith(".parquet"))
    return count


class TestPyarrowMergeInsertStrategy:
    """Test INSERT strategy: append only new keys."""

    def test_insert_only_new_keys(self, tmp_path):
        """INSERT should only add rows with keys not in target."""
        target = tmp_path / "dataset"
        target.mkdir()

        # Create target with existing keys
        existing = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        pq.write_table(existing, target / "part-0.parquet")

        # Create source with mix of existing and new keys
        source = pa.table({"id": [2, 3, 4, 5], "value": ["B", "C", "D", "E"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="insert",
            key_columns=["id"],
        )

        # Should only insert keys 4 and 5
        assert result.inserted == 2
        assert result.updated == 0
        assert result.deleted == 0
        assert result.source_count == 4
        assert result.target_count_after == 5

        # Verify data
        final = _read_dataset_table(str(target))
        assert final.num_rows == 5
        ids = set(final.column("id").to_pylist())
        assert ids == {1, 2, 3, 4, 5}

        # Verify original values preserved for existing keys
        values_dict = dict(
            zip(final.column("id").to_pylist(), final.column("value").to_pylist())
        )
        assert values_dict[2] == "b"  # Original value, not "B"
        assert values_dict[3] == "c"  # Original value, not "C"
        assert values_dict[4] == "D"  # New value
        assert values_dict[5] == "E"  # New value

    def test_insert_all_existing_keys(self, tmp_path):
        """INSERT with all existing keys should insert nothing."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        pq.write_table(existing, target / "part-0.parquet")

        # Source with only existing keys
        source = pa.table({"id": [1, 2], "value": ["A", "B"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="insert",
            key_columns=["id"],
        )

        assert result.inserted == 0
        assert result.updated == 0
        assert result.target_count_after == 3

        # Verify data unchanged
        final = _read_dataset_table(str(target))
        values_dict = dict(
            zip(final.column("id").to_pylist(), final.column("value").to_pylist())
        )
        assert values_dict[1] == "a"
        assert values_dict[2] == "b"

    def test_insert_to_empty_dataset(self, tmp_path):
        """INSERT to non-existent dataset should write all data."""
        target = tmp_path / "dataset"
        target.mkdir()

        source = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="insert",
            key_columns=["id"],
        )

        assert result.inserted == 3
        assert result.updated == 0
        assert result.target_count_before == 0
        assert result.target_count_after == 3


class TestPyarrowMergeUpdateStrategy:
    """Test UPDATE strategy: rewrite only affected files."""

    def test_update_only_existing_keys(self, tmp_path):
        """UPDATE should only modify rows with matching keys."""
        target = tmp_path / "dataset"
        target.mkdir()

        # Create target with existing keys
        existing = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        pq.write_table(existing, target / "part-0.parquet")

        # Source with updates for some keys
        source = pa.table({"id": [2, 3], "value": ["UPDATED_B", "UPDATED_C"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="update",
            key_columns=["id"],
        )

        assert result.inserted == 0
        assert result.updated == 2
        assert result.deleted == 0
        assert result.target_count_after == 3

        # Verify updates applied
        final = _read_dataset_table(str(target))
        values_dict = dict(
            zip(final.column("id").to_pylist(), final.column("value").to_pylist())
        )
        assert values_dict[1] == "a"  # Unchanged
        assert values_dict[2] == "UPDATED_B"
        assert values_dict[3] == "UPDATED_C"

    def test_update_with_new_keys_ignored(self, tmp_path):
        """UPDATE should ignore keys not in target."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table({"id": [1, 2], "value": ["a", "b"]})
        pq.write_table(existing, target / "part-0.parquet")

        # Source with existing and new keys
        source = pa.table({"id": [2, 3, 4], "value": ["UPDATED", "NEW1", "NEW2"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="update",
            key_columns=["id"],
        )

        # Should only update key 2, ignore 3 and 4
        assert result.inserted == 0
        assert result.updated == 1
        assert result.target_count_after == 2

        final = _read_dataset_table(str(target))
        ids = set(final.column("id").to_pylist())
        assert ids == {1, 2}  # No new keys added

    def test_update_empty_dataset_error(self, tmp_path):
        """UPDATE to non-existent dataset should raise error."""
        target = tmp_path / "dataset"
        target.mkdir()

        source = pa.table({"id": [1, 2], "value": ["a", "b"]})

        io = PyarrowDatasetIO()
        with pytest.raises(ValueError, match="UPDATE strategy requires"):
            io.merge(
                data=source,
                path=str(target),
                strategy="update",
                key_columns=["id"],
            )


class TestPyarrowMergeUpsertStrategy:
    """Test UPSERT strategy: update affected + append inserts."""

    def test_upsert_updates_and_inserts(self, tmp_path):
        """UPSERT should update existing and insert new keys."""
        target = tmp_path / "dataset"
        target.mkdir()

        # Create target
        existing = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        pq.write_table(existing, target / "part-0.parquet")

        # Source with updates and inserts
        source = pa.table({"id": [2, 3, 4, 5], "value": ["B", "C", "D", "E"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="upsert",
            key_columns=["id"],
        )

        assert result.inserted == 2  # Keys 4, 5
        assert result.updated == 2  # Keys 2, 3
        assert result.deleted == 0
        assert result.target_count_after == 5

        # Verify data
        final = _read_dataset_table(str(target))
        assert final.num_rows == 5
        values_dict = dict(
            zip(final.column("id").to_pylist(), final.column("value").to_pylist())
        )
        assert values_dict[1] == "a"  # Unchanged
        assert values_dict[2] == "B"  # Updated
        assert values_dict[3] == "C"  # Updated
        assert values_dict[4] == "D"  # Inserted
        assert values_dict[5] == "E"  # Inserted

    def test_upsert_only_updates(self, tmp_path):
        """UPSERT with all existing keys should only update."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        pq.write_table(existing, target / "part-0.parquet")

        source = pa.table({"id": [1, 2], "value": ["UPDATED_A", "UPDATED_B"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="upsert",
            key_columns=["id"],
        )

        assert result.inserted == 0
        assert result.updated == 2
        assert result.target_count_after == 3

    def test_upsert_only_inserts(self, tmp_path):
        """UPSERT with all new keys should only insert."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table({"id": [1, 2], "value": ["a", "b"]})
        pq.write_table(existing, target / "part-0.parquet")

        source = pa.table({"id": [3, 4, 5], "value": ["c", "d", "e"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="upsert",
            key_columns=["id"],
        )

        assert result.inserted == 3
        assert result.updated == 0
        assert result.target_count_after == 5

    def test_upsert_to_empty_dataset(self, tmp_path):
        """UPSERT to non-existent dataset should write all data."""
        target = tmp_path / "dataset"
        target.mkdir()

        source = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="upsert",
            key_columns=["id"],
        )

        assert result.inserted == 3
        assert result.updated == 0
        assert result.target_count_before == 0
        assert result.target_count_after == 3


class TestPyarrowMergeFilePreservation:
    """Test that unaffected files are preserved unchanged."""

    def test_update_preserves_unaffected_files(self, tmp_path):
        """UPDATE should not rewrite files without matching keys."""
        target = tmp_path / "dataset"
        target.mkdir()

        # Create multiple files with different key ranges
        file1 = pa.table({"id": [1, 2], "value": ["a", "b"]})
        file2 = pa.table({"id": [10, 20], "value": ["j", "t"]})
        file3 = pa.table({"id": [100, 200], "value": ["big1", "big2"]})

        pq.write_table(file1, target / "part-0.parquet")
        pq.write_table(file2, target / "part-1.parquet")
        pq.write_table(file3, target / "part-2.parquet")

        # Update only keys in file1
        source = pa.table({"id": [1, 2], "value": ["UPDATED_A", "UPDATED_B"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="update",
            key_columns=["id"],
        )

        # Should have rewritten only 1 file, preserved 2
        assert len(result.rewritten_files) == 1
        assert len(result.preserved_files) == 2

        # Verify file count unchanged
        assert _count_parquet_files(target) == 3

        # Verify data correctness
        final = _read_dataset_table(str(target))
        values_dict = dict(
            zip(final.column("id").to_pylist(), final.column("value").to_pylist())
        )
        assert values_dict[1] == "UPDATED_A"
        assert values_dict[2] == "UPDATED_B"
        assert values_dict[10] == "j"  # Unchanged
        assert values_dict[20] == "t"  # Unchanged
        assert values_dict[100] == "big1"  # Unchanged
        assert values_dict[200] == "big2"  # Unchanged

    def test_insert_preserves_all_files(self, tmp_path):
        """INSERT should not modify any existing files."""
        target = tmp_path / "dataset"
        target.mkdir()

        # Create multiple files
        file1 = pa.table({"id": [1, 2], "value": ["a", "b"]})
        file2 = pa.table({"id": [10, 20], "value": ["j", "t"]})

        pq.write_table(file1, target / "part-0.parquet")
        pq.write_table(file2, target / "part-1.parquet")

        initial_file_count = _count_parquet_files(target)

        # Insert new keys
        source = pa.table({"id": [100, 200], "value": ["new1", "new2"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="insert",
            key_columns=["id"],
        )

        # Should have preserved all original files
        assert len(result.preserved_files) == 2
        assert len(result.rewritten_files) == 0
        assert len(result.inserted_files) > 0

        # Should have more files now
        assert _count_parquet_files(target) > initial_file_count


class TestPyarrowMergeMetadata:
    """Test that merge results include correct file metadata."""

    def test_result_contains_file_metadata(self, tmp_path):
        """Merge result should include paths of affected files."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table({"id": [1, 2], "value": ["a", "b"]})
        pq.write_table(existing, target / "part-0.parquet")

        source = pa.table({"id": [2, 3], "value": ["B", "C"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="upsert",
            key_columns=["id"],
        )

        # Should have rewritten files and inserted files
        assert len(result.rewritten_files) > 0
        assert len(result.inserted_files) > 0

        # All file paths should be absolute
        for path in result.rewritten_files + result.inserted_files:
            assert path.endswith(".parquet")

    def test_result_files_include_rewritten_and_inserted_entries(self, tmp_path):
        """MergeResult.files should include rewritten + inserted file metadata."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table({"id": [1, 2], "value": ["a", "b"]})
        pq.write_table(existing, target / "part-0.parquet")

        source = pa.table({"id": [2, 3], "value": ["UPDATED_B", "NEW_C"]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="upsert",
            key_columns=["id"],
        )

        ops = {m.operation for m in result.files}
        assert "rewritten" in ops
        assert "inserted" in ops

        rewritten_meta = [m for m in result.files if m.operation == "rewritten"]
        inserted_meta = [m for m in result.files if m.operation == "inserted"]

        assert {m.path for m in rewritten_meta} == set(result.rewritten_files)
        assert {m.path for m in inserted_meta} == set(result.inserted_files)

        for m in rewritten_meta + inserted_meta:
            assert m.path.endswith(".parquet")
            assert m.row_count > 0

    def test_result_strategy_recorded(self, tmp_path):
        """Merge result should record the strategy used."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table({"id": [1], "value": ["a"]})
        pq.write_table(existing, target / "part-0.parquet")

        source = pa.table({"id": [2], "value": ["b"]})

        io = PyarrowDatasetIO()

        # Test insert
        result = io.merge(
            data=source,
            path=str(target),
            strategy="insert",
            key_columns=["id"],
        )
        assert result.strategy == "insert"

        # Test update
        source_update = pa.table({"id": [1], "value": ["UPDATED"]})
        result = io.merge(
            data=source_update,
            path=str(target),
            strategy="update",
            key_columns=["id"],
        )
        assert result.strategy == "update"

        # Test upsert
        result = io.merge(
            data=source,
            path=str(target),
            strategy="upsert",
            key_columns=["id"],
        )
        assert result.strategy == "upsert"


class TestPyarrowMergeInvariants:
    """Test merge invariants: null keys, partition immutability."""

    def test_null_keys_rejected(self, tmp_path):
        """Merge should reject source data with NULL keys."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table({"id": [1, 2], "value": ["a", "b"]})
        pq.write_table(existing, target / "part-0.parquet")

        # Source with NULL key
        source = pa.table({"id": [2, None, 3], "value": ["B", "NULL_KEY", "C"]})

        io = PyarrowDatasetIO()
        with pytest.raises(ValueError, match="NULL"):
            io.merge(
                data=source,
                path=str(target),
                strategy="upsert",
                key_columns=["id"],
            )

    @pytest.mark.skip(reason="Partition immutability check not yet implemented")
    def test_partition_immutability_enforced(self, tmp_path):
        """Merge should reject changes to partition columns for existing keys."""
        target = tmp_path / "dataset"
        target.mkdir()

        # Create target with partition column
        existing = pa.table(
            {"id": [1, 2], "partition": ["A", "A"], "value": ["a", "b"]}
        )
        pq.write_table(existing, target / "part-0.parquet")

        # Source trying to change partition for existing key
        source = pa.table(
            {
                "id": [2],
                "partition": ["B"],  # Different partition!
                "value": ["UPDATED"],
            }
        )

        io = PyarrowDatasetIO()
        with pytest.raises(ValueError, match="partition"):
            io.merge(
                data=source,
                path=str(target),
                strategy="upsert",
                key_columns=["id"],
                partition_columns=["partition"],
            )

    def test_composite_keys_supported(self, tmp_path):
        """Merge should work with composite (multi-column) keys."""
        target = tmp_path / "dataset"
        target.mkdir()

        existing = pa.table(
            {
                "user_id": [1, 1, 2],
                "date": ["2025-01-01", "2025-01-02", "2025-01-01"],
                "value": [10, 20, 30],
            }
        )
        pq.write_table(existing, target / "part-0.parquet")

        # Update one composite key
        source = pa.table({"user_id": [1], "date": ["2025-01-02"], "value": [999]})

        io = PyarrowDatasetIO()
        result = io.merge(
            data=source,
            path=str(target),
            strategy="update",
            key_columns=["user_id", "date"],
        )

        assert result.updated == 1
        assert result.target_count_after == 3

        final = _read_dataset_table(str(target))
        # Find the updated row
        for i in range(final.num_rows):
            if (
                final.column("user_id")[i].as_py() == 1
                and final.column("date")[i].as_py() == "2025-01-02"
            ):
                assert final.column("value")[i].as_py() == 999


class TestDuckDBMergeMethods:
    """Test DuckDB merge methods directly."""

    def setup_method(self):
        """Setup for each test method."""
        self.connection = create_duckdb_connection()
        self.handler = DuckDBDatasetIO(self.connection)

    def teardown_method(self):
        """Cleanup after each test method."""
        self.connection.close()

    def test_merge_upsert_basic(self, tmp_path):
        """Test _merge_upsert with basic data."""
        existing_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10, 20, 30],
            }
        )

        source_data = pa.table(
            {
                "id": [2, 3, 4],
                "name": ["Bob Updated", "Charlie", "David"],
                "value": [25, 35, 40],
            }
        )

        result = self.handler._merge_upsert(existing_data, source_data, ["id"])

        assert result.num_rows == 4
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(result_dict["id"], result_dict["name"], result_dict["value"])
        )
        expected_rows = sorted(
            [
                (1, "Alice", 10),
                (2, "Bob Updated", 25),
                (3, "Charlie", 35),
                (4, "David", 40),
            ]
        )
        assert result_rows == expected_rows

    def test_merge_upsert_no_overlap(self, tmp_path):
        """Test _merge_upsert when no keys overlap."""
        existing_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]}
        )

        source_data = pa.table(
            {"id": [3, 4], "name": ["Charlie", "David"], "value": [30, 40]}
        )

        result = self.handler._merge_upsert(existing_data, source_data, ["id"])

        assert result.num_rows == 4
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(result_dict["id"], result_dict["name"], result_dict["value"])
        )
        expected_rows = sorted(
            [(1, "Alice", 10), (2, "Bob", 20), (3, "Charlie", 30), (4, "David", 40)]
        )
        assert result_rows == expected_rows

    def test_merge_upsert_all_overlap(self, tmp_path):
        """Test _merge_upsert when all keys overlap."""
        existing_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]}
        )

        source_data = pa.table(
            {"id": [1, 2], "name": ["Alice Updated", "Bob Updated"], "value": [15, 25]}
        )

        result = self.handler._merge_upsert(existing_data, source_data, ["id"])

        assert result.num_rows == 2
        result_dict = result.to_pydict()

        assert result_dict["id"] == [1, 2]
        assert result_dict["name"] == ["Alice Updated", "Bob Updated"]
        assert result_dict["value"] == [15, 25]

    def test_merge_upsert_composite_keys(self, tmp_path):
        """Test _merge_upsert with composite keys."""
        existing_data = pa.table(
            {
                "user_id": [1, 1, 2],
                "date": ["2025-01-01", "2025-01-02", "2025-01-01"],
                "value": [100, 200, 300],
            }
        )

        source_data = pa.table(
            {
                "user_id": [1, 2, 2],
                "date": ["2025-01-01", "2025-01-01", "2025-01-02"],
                "value": [150, 400, 500],
            }
        )

        result = self.handler._merge_upsert(
            existing_data, source_data, ["user_id", "date"]
        )

        assert result.num_rows == 4
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(result_dict["user_id"], result_dict["date"], result_dict["value"])
        )
        expected_rows = sorted(
            [
                (1, "2025-01-01", 150),
                (1, "2025-01-02", 200),
                (2, "2025-01-01", 400),
                (2, "2025-01-02", 500),
            ]
        )
        assert result_rows == expected_rows

    def test_merge_update_basic(self, tmp_path):
        """Test _merge_update with basic data."""
        existing_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10, 20, 30],
            }
        )

        source_data = pa.table(
            {
                "id": [2, 3, 4],
                "name": ["Bob Updated", "Charlie", "David"],
                "value": [25, 35, 40],
            }
        )

        result = self.handler._merge_update(existing_data, source_data, ["id"])

        assert result.num_rows == 3
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(result_dict["id"], result_dict["name"], result_dict["value"])
        )
        expected_rows = sorted(
            [(1, "Alice", 10), (2, "Bob Updated", 25), (3, "Charlie", 35)]
        )
        assert result_rows == expected_rows

    def test_merge_update_no_overlap(self, tmp_path):
        """Test _merge_update when no keys overlap - should return existing only."""
        existing_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]}
        )

        source_data = pa.table(
            {"id": [3, 4], "name": ["Charlie", "David"], "value": [30, 40]}
        )

        result = self.handler._merge_update(existing_data, source_data, ["id"])

        assert result.num_rows == 2
        result_dict = result.to_pydict()

        assert result_dict["id"] == [1, 2]
        assert result_dict["name"] == ["Alice", "Bob"]
        assert result_dict["value"] == [10, 20]

    def test_merge_update_all_overlap(self, tmp_path):
        """Test _merge_update when all keys overlap."""
        existing_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]}
        )

        source_data = pa.table(
            {"id": [1, 2], "name": ["Alice Updated", "Bob Updated"], "value": [15, 25]}
        )

        result = self.handler._merge_update(existing_data, source_data, ["id"])

        assert result.num_rows == 2
        result_dict = result.to_pydict()

        assert result_dict["id"] == [1, 2]
        assert result_dict["name"] == ["Alice Updated", "Bob Updated"]
        assert result_dict["value"] == [15, 25]

    def test_merge_extract_inserted_rows_basic(self, tmp_path):
        """Test _extract_inserted_rows with basic data."""
        existing_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10, 20, 30],
            }
        )

        source_data = pa.table(
            {
                "id": [2, 3, 4],
                "name": ["Bob", "Charlie", "David"],
                "value": [25, 35, 40],
            }
        )

        result = self.handler._extract_inserted_rows(existing_data, source_data, ["id"])

        assert result.num_rows == 1
        result_dict = result.to_pydict()

        assert result_dict["id"] == [4]
        assert result_dict["name"] == ["David"]
        assert result_dict["value"] == [40]

    def test_merge_extract_inserted_rows_no_new(self, tmp_path):
        """Test _extract_inserted_rows when no new rows exist."""
        existing_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10, 20, 30],
            }
        )

        source_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10, 20, 30],
            }
        )

        result = self.handler._extract_inserted_rows(existing_data, source_data, ["id"])

        assert result.num_rows == 0

    def test_merge_extract_inserted_rows_all_new(self, tmp_path):
        """Test _extract_inserted_rows when all source rows are new."""
        existing_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]}
        )

        source_data = pa.table(
            {
                "id": [3, 4, 5],
                "name": ["Charlie", "David", "Eve"],
                "value": [30, 40, 50],
            }
        )

        result = self.handler._extract_inserted_rows(existing_data, source_data, ["id"])

        assert result.num_rows == 3
        result_dict = result.to_pydict()

        assert result_dict["id"] == [3, 4, 5]
        assert result_dict["name"] == ["Charlie", "David", "Eve"]
        assert result_dict["value"] == [30, 40, 50]


class TestPyArrowMergeMethods:
    """Test PyArrow merge methods directly."""

    def setup_method(self):
        """Setup for each test method."""
        from fsspeckit.datasets.pyarrow.dataset import (
            merge_upsert_pyarrow,
            merge_update_pyarrow,
        )
        self.merge_upsert_pyarrow = merge_upsert_pyarrow
        self.merge_update_pyarrow = merge_update_pyarrow

    def test_merge_upsert_pyarrow_basic(self, tmp_path):
        """Test merge_upsert_pyarrow with basic data."""
        existing = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10, 20, 30],
            }
        )

        source = pa.table(
            {
                "id": [2, 3, 4],
                "name": ["Bob Updated", "Charlie", "David"],
                "value": [25, 35, 40],
            }
        )

        result = self.merge_upsert_pyarrow(existing, source, ["id"])

        assert result.num_rows == 4
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(result_dict["id"], result_dict["name"], result_dict["value"])
        )
        expected_rows = sorted(
            [
                (1, "Alice", 10),
                (2, "Bob Updated", 25),
                (3, "Charlie", 35),
                (4, "David", 40),
            ]
        )
        assert result_rows == expected_rows

    def test_merge_upsert_pyarrow_no_overlap(self, tmp_path):
        """Test merge_upsert_pyarrow when no keys overlap."""
        existing = pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]})

        source = pa.table(
            {"id": [3, 4], "name": ["Charlie", "David"], "value": [30, 40]}
        )

        result = self.merge_upsert_pyarrow(existing, source, ["id"])

        assert result.num_rows == 4
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(result_dict["id"], result_dict["name"], result_dict["value"])
        )
        expected_rows = sorted(
            [(1, "Alice", 10), (2, "Bob", 20), (3, "Charlie", 30), (4, "David", 40)]
        )
        assert result_rows == expected_rows

    def test_merge_upsert_pyarrow_all_overlap(self, tmp_path):
        """Test _merge_upsert_pyarrow when all keys overlap."""
        existing = pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]})

        source = pa.table(
            {"id": [1, 2], "name": ["Alice Updated", "Bob Updated"], "value": [15, 25]}
        )

        result = self.merge_upsert_pyarrow(existing, source, ["id"])

        assert result.num_rows == 2
        result_dict = result.to_pydict()

        assert result_dict["id"] == [1, 2]
        assert result_dict["name"] == ["Alice Updated", "Bob Updated"]
        assert result_dict["value"] == [15, 25]

    def test_merge_upsert_pyarrow_composite_keys(self, tmp_path):
        """Test _merge_upsert_pyarrow with composite keys."""
        existing = pa.table(
            {
                "user_id": [1, 1, 2],
                "date": ["2025-01-01", "2025-01-02", "2025-01-01"],
                "value": [100, 200, 300],
            }
        )

        source = pa.table(
            {
                "user_id": [1, 2, 2],
                "date": ["2025-01-01", "2025-01-01", "2025-01-02"],
                "value": [150, 400, 500],
            }
        )

        result = self.merge_upsert_pyarrow(
            existing, source, ["user_id", "date"]
        )

        assert result.num_rows == 4
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(result_dict["user_id"], result_dict["date"], result_dict["value"])
        )
        expected_rows = sorted(
            [
                (1, "2025-01-01", 150),
                (1, "2025-01-02", 200),
                (2, "2025-01-01", 400),
                (2, "2025-01-02", 500),
            ]
        )
        assert result_rows == expected_rows

    def test_merge_update_pyarrow_basic(self, tmp_path):
        """Test _merge_update_pyarrow with basic data."""
        existing = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10, 20, 30],
            }
        )

        source = pa.table(
            {
                "id": [2, 3, 4],
                "name": ["Bob Updated", "Charlie", "David"],
                "value": [25, 35, 40],
            }
        )

        result = self.merge_update_pyarrow(existing, source, ["id"])

        assert result.num_rows == 3
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(result_dict["id"], result_dict["name"], result_dict["value"])
        )
        expected_rows = sorted(
            [(1, "Alice", 10), (2, "Bob Updated", 25), (3, "Charlie", 35)]
        )
        assert result_rows == expected_rows

    def test_merge_update_pyarrow_no_overlap(self, tmp_path):
        """Test _merge_update_pyarrow when no keys overlap - should return existing only."""
        existing = pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]})

        source = pa.table(
            {"id": [3, 4], "name": ["Charlie", "David"], "value": [30, 40]}
        )

        result = self.merge_update_pyarrow(existing, source, ["id"])

        assert result.num_rows == 2
        result_dict = result.to_pydict()

        assert result_dict["id"] == [1, 2]
        assert result_dict["name"] == ["Alice", "Bob"]
        assert result_dict["value"] == [10, 20]

    def test_merge_update_pyarrow_all_overlap(self, tmp_path):
        """Test _merge_update_pyarrow when all keys overlap."""
        existing = pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]})

        source = pa.table(
            {"id": [1, 2], "name": ["Alice Updated", "Bob Updated"], "value": [15, 25]}
        )

        result = self.merge_update_pyarrow(existing, source, ["id"])

        assert result.num_rows == 2
        result_dict = result.to_pydict()

        assert result_dict["id"] == [1, 2]
        assert result_dict["name"] == ["Alice Updated", "Bob Updated"]
        assert result_dict["value"] == [15, 25]

    def test_merge_upsert_pyarrow_missing_columns(self, tmp_path):
        """Test _merge_upsert_pyarrow when source has missing columns."""
        existing = pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "value": [10, 20],
                "category": ["A", "B"],
            }
        )

        source = pa.table(
            {"id": [2, 3], "name": ["Bob Updated", "Charlie"], "value": [25, 30]}
        )

        result = self.merge_upsert_pyarrow(existing, source, ["id"])

        assert result.num_rows == 3
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(
                result_dict["id"],
                result_dict["name"],
                result_dict["value"],
                result_dict["category"],
            )
        )
        expected_rows = sorted(
            [
                (1, "Alice", 10, "A"),
                (2, "Bob Updated", 25, None),
                (3, "Charlie", 30, None),
            ]
        )
        assert result_rows == expected_rows

    def test_merge_update_pyarrow_missing_columns(self, tmp_path):
        """Test _merge_update_pyarrow when source has missing columns."""
        existing = pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "value": [10, 20],
                "category": ["A", "B"],
            }
        )

        source = pa.table(
            {"id": [2, 3], "name": ["Bob Updated", "Charlie"], "value": [25, 30]}
        )

        result = self.merge_update_pyarrow(existing, source, ["id"])

        assert result.num_rows == 2
        result_dict = result.to_pydict()

        result_rows = sorted(
            zip(
                result_dict["id"],
                result_dict["name"],
                result_dict["value"],
                result_dict["category"],
            )
        )
        expected_rows = sorted([(1, "Alice", 10, "A"), (2, "Bob Updated", 25, None)])
        assert result_rows == expected_rows
