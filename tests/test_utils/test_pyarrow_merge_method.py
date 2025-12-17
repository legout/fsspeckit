"""Tests for PyarrowDatasetIO.merge() method with incremental rewrite strategies."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from fsspeckit.core.incremental import MergeResult
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO


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
