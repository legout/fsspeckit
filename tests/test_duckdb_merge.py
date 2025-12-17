"""Tests for DuckDB merge functionality.

This module tests the merge() method with insert, update, and upsert strategies,
ensuring correct semantics and incremental file rewriting.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from fsspeckit.common.optional import _DUCKDB_AVAILABLE
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

pytestmark = pytest.mark.skipif(not _DUCKDB_AVAILABLE, reason="DuckDB not available")


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


@pytest.fixture
def duckdb_io():
    """Create a DuckDB dataset I/O instance."""
    conn = create_duckdb_connection()
    return DuckDBDatasetIO(conn)


@pytest.fixture
def initial_dataset(temp_dir, duckdb_io):
    """Create an initial dataset for testing merge operations."""
    data = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "value": [100, 200, 300, 400, 500],
        }
    )
    dataset_path = str(temp_dir / "dataset")
    duckdb_io.write_dataset(data, dataset_path, mode="overwrite")
    return dataset_path


class TestMergeInsert:
    """Tests for INSERT strategy."""

    def test_insert_new_keys(self, temp_dir, duckdb_io, initial_dataset):
        """Test that INSERT only adds new keys."""
        # New data with new keys
        new_data = pa.table(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "value": [600, 700],
            }
        )

        result = duckdb_io.merge(
            data=new_data,
            path=initial_dataset,
            strategy="insert",
            key_columns=["id"],
        )

        # Verify statistics
        assert result.strategy == "insert"
        assert result.source_count == 2
        assert result.target_count_before == 5
        assert result.target_count_after == 7
        assert result.inserted == 2
        assert result.updated == 0
        assert result.deleted == 0

        # Verify new records were inserted
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') ORDER BY id"
        ).fetch_arrow_table()

        assert final_data.num_rows == 7
        assert final_data.column("id").to_pylist() == [1, 2, 3, 4, 5, 6, 7]

    def test_insert_existing_keys_ignored(self, temp_dir, duckdb_io, initial_dataset):
        """Test that INSERT ignores existing keys."""
        # Data with existing keys
        existing_data = pa.table(
            {
                "id": [1, 2],
                "name": ["Alice_Updated", "Bob_Updated"],
                "value": [999, 888],
            }
        )

        result = duckdb_io.merge(
            data=existing_data,
            path=initial_dataset,
            strategy="insert",
            key_columns=["id"],
        )

        # Verify no records inserted
        assert result.inserted == 0
        assert result.target_count_after == result.target_count_before

        # Verify original data unchanged
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') WHERE id IN (1, 2) ORDER BY id"
        ).fetch_arrow_table()

        assert final_data.column("name").to_pylist() == ["Alice", "Bob"]
        assert final_data.column("value").to_pylist() == [100, 200]

    def test_insert_mixed_keys(self, temp_dir, duckdb_io, initial_dataset):
        """Test INSERT with mix of new and existing keys."""
        # Mix of new and existing keys
        mixed_data = pa.table(
            {
                "id": [2, 6, 3, 7],  # 2 and 3 exist, 6 and 7 are new
                "name": ["Bob_New", "Frank", "Charlie_New", "Grace"],
                "value": [999, 600, 888, 700],
            }
        )

        result = duckdb_io.merge(
            data=mixed_data,
            path=initial_dataset,
            strategy="insert",
            key_columns=["id"],
        )

        # Only new keys (6, 7) should be inserted
        assert result.inserted == 2
        assert result.target_count_after == 7

        # Verify existing keys unchanged
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') ORDER BY id"
        ).fetch_arrow_table()

        # IDs 2 and 3 should have original values
        id_2_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') WHERE id = 2"
        ).fetch_arrow_table()
        assert id_2_data.column("name")[0].as_py() == "Bob"
        assert id_2_data.column("value")[0].as_py() == 200

        # IDs 6 and 7 should be new
        id_6_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') WHERE id = 6"
        ).fetch_arrow_table()
        assert id_6_data.num_rows == 1


class TestMergeUpdate:
    """Tests for UPDATE strategy."""

    def test_update_existing_keys(self, temp_dir, duckdb_io, initial_dataset):
        """Test that UPDATE only modifies existing keys."""
        # Update data for existing keys
        update_data = pa.table(
            {
                "id": [1, 3],
                "name": ["Alice_Updated", "Charlie_Updated"],
                "value": [111, 333],
            }
        )

        result = duckdb_io.merge(
            data=update_data,
            path=initial_dataset,
            strategy="update",
            key_columns=["id"],
        )

        # Verify statistics
        assert result.strategy == "update"
        assert result.inserted == 0
        assert result.updated > 0
        assert result.target_count_after == result.target_count_before

        # Verify updates applied
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') WHERE id IN (1, 3) ORDER BY id"
        ).fetch_arrow_table()

        assert final_data.column("name").to_pylist() == [
            "Alice_Updated",
            "Charlie_Updated",
        ]
        assert final_data.column("value").to_pylist() == [111, 333]

    def test_update_new_keys_ignored(self, temp_dir, duckdb_io, initial_dataset):
        """Test that UPDATE ignores new keys."""
        # Data with new keys
        new_data = pa.table(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "value": [600, 700],
            }
        )

        result = duckdb_io.merge(
            data=new_data,
            path=initial_dataset,
            strategy="update",
            key_columns=["id"],
        )

        # No updates should occur (no matching keys)
        assert result.updated == 0
        assert result.target_count_after == result.target_count_before

        # Verify no new records added
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') ORDER BY id"
        ).fetch_arrow_table()

        assert final_data.num_rows == 5  # Original count

    def test_update_preserves_unaffected_rows(
        self, temp_dir, duckdb_io, initial_dataset
    ):
        """Test that UPDATE preserves rows not in source."""
        # Update only some keys
        update_data = pa.table(
            {
                "id": [1, 2],
                "name": ["Alice_Updated", "Bob_Updated"],
                "value": [111, 222],
            }
        )

        result = duckdb_io.merge(
            data=update_data,
            path=initial_dataset,
            strategy="update",
            key_columns=["id"],
        )

        # Verify all original rows still present
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') ORDER BY id"
        ).fetch_arrow_table()

        assert final_data.num_rows == 5
        # IDs 3, 4, 5 should be unchanged
        id_3_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') WHERE id = 3"
        ).fetch_arrow_table()
        assert id_3_data.column("name")[0].as_py() == "Charlie"
        assert id_3_data.column("value")[0].as_py() == 300


class TestMergeUpsert:
    """Tests for UPSERT strategy."""

    def test_upsert_inserts_and_updates(self, temp_dir, duckdb_io, initial_dataset):
        """Test that UPSERT both inserts new keys and updates existing ones."""
        # Mix of new and existing keys
        upsert_data = pa.table(
            {
                "id": [1, 3, 6, 7],  # 1,3 exist; 6,7 are new
                "name": ["Alice_Updated", "Charlie_Updated", "Frank", "Grace"],
                "value": [111, 333, 600, 700],
            }
        )

        result = duckdb_io.merge(
            data=upsert_data,
            path=initial_dataset,
            strategy="upsert",
            key_columns=["id"],
        )

        # Verify statistics
        assert result.strategy == "upsert"
        assert result.inserted == 2  # IDs 6, 7
        assert result.updated >= 1  # IDs 1, 3
        assert result.target_count_after == 7

        # Verify updates and inserts
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') ORDER BY id"
        ).fetch_arrow_table()

        assert final_data.num_rows == 7
        assert final_data.column("id").to_pylist() == [1, 2, 3, 4, 5, 6, 7]

        # Check updated rows
        id_1_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') WHERE id = 1"
        ).fetch_arrow_table()
        assert id_1_data.column("name")[0].as_py() == "Alice_Updated"
        assert id_1_data.column("value")[0].as_py() == 111

        # Check inserted rows
        id_6_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') WHERE id = 6"
        ).fetch_arrow_table()
        assert id_6_data.column("name")[0].as_py() == "Frank"
        assert id_6_data.column("value")[0].as_py() == 600

    def test_upsert_preserves_unaffected_rows(
        self, temp_dir, duckdb_io, initial_dataset
    ):
        """Test that UPSERT preserves rows not in source."""
        # Update and insert, but don't touch some rows
        upsert_data = pa.table(
            {
                "id": [1, 6],  # Update 1, insert 6, leave 2-5 unchanged
                "name": ["Alice_Updated", "Frank"],
                "value": [111, 600],
            }
        )

        result = duckdb_io.merge(
            data=upsert_data,
            path=initial_dataset,
            strategy="upsert",
            key_columns=["id"],
        )

        # Verify all rows present
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') ORDER BY id"
        ).fetch_arrow_table()

        assert final_data.num_rows == 6

        # IDs 2-5 should be unchanged
        id_2_data = conn.execute(
            f"SELECT * FROM parquet_scan('{initial_dataset}/**/*.parquet') WHERE id = 2"
        ).fetch_arrow_table()
        assert id_2_data.column("name")[0].as_py() == "Bob"
        assert id_2_data.column("value")[0].as_py() == 200


class TestMergeIncrementalRewrite:
    """Tests for incremental rewrite behavior."""

    def test_update_only_rewrites_affected_files(self, temp_dir, duckdb_io):
        """Test that UPDATE only rewrites files containing affected keys."""
        dataset_dir = temp_dir / "dataset_multi"
        dataset_dir.mkdir()

        # Create multiple files with different key ranges
        file1 = pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]})
        file2 = pa.table({"id": [10, 20], "name": ["J", "T"], "value": [10, 20]})
        file3 = pa.table(
            {"id": [100, 200], "name": ["Big1", "Big2"], "value": [100, 200]}
        )

        pq.write_table(file1, dataset_dir / "part-0.parquet")
        pq.write_table(file2, dataset_dir / "part-1.parquet")
        pq.write_table(file3, dataset_dir / "part-2.parquet")

        # Update only keys in file1
        update_data = pa.table(
            {
                "id": [1, 2],
                "name": ["Alice_Updated", "Bob_Updated"],
                "value": [111, 222],
            }
        )

        result = duckdb_io.merge(
            data=update_data,
            path=str(dataset_dir),
            strategy="update",
            key_columns=["id"],
        )

        # Should have rewritten only 1 file, preserved 2
        assert len(result.rewritten_files) == 1
        assert len(result.preserved_files) == 2

        # Verify file count unchanged
        parquet_files = list(dataset_dir.glob("**/*.parquet"))
        assert len(parquet_files) == 3

        # Verify data correctness via DuckDB
        conn = duckdb_io._connection.connection
        final_data = conn.execute(
            f"SELECT * FROM parquet_scan('{dataset_dir}/**/*.parquet') ORDER BY id"
        ).fetch_arrow_table()
        values_by_id = dict(
            zip(
                final_data.column("id").to_pylist(),
                final_data.column("value").to_pylist(),
            )
        )
        assert values_by_id[1] == 111
        assert values_by_id[2] == 222
        assert values_by_id[10] == 10
        assert values_by_id[20] == 20
        assert values_by_id[100] == 100
        assert values_by_id[200] == 200


class TestMergeFileMetadata:
    """Tests for file metadata returned by merge."""

    def test_merge_returns_file_metadata(self, temp_dir, duckdb_io, initial_dataset):
        """Test that merge returns detailed file metadata."""
        upsert_data = pa.table(
            {
                "id": [1, 6],
                "name": ["Alice_Updated", "Frank"],
                "value": [111, 600],
            }
        )

        result = duckdb_io.merge(
            data=upsert_data,
            path=initial_dataset,
            strategy="upsert",
            key_columns=["id"],
        )

        # Verify file metadata structure
        assert hasattr(result, "files")
        assert hasattr(result, "rewritten_files")
        assert hasattr(result, "inserted_files")
        assert hasattr(result, "preserved_files")

        # Should have some files in the result
        assert len(result.rewritten_files) > 0 or len(result.inserted_files) > 0
        assert isinstance(result.files, list)

        # If we both update and insert, result.files should reflect it.
        ops = {m.operation for m in result.files}
        assert "rewritten" in ops or "inserted" in ops
        if result.rewritten_files:
            assert set(result.rewritten_files) <= {m.path for m in result.files}
        if result.inserted_files:
            assert set(result.inserted_files) <= {m.path for m in result.files}

    def test_insert_files_metadata(self, temp_dir, duckdb_io, initial_dataset):
        """Test that INSERT returns metadata for inserted files."""
        new_data = pa.table(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "value": [600, 700],
            }
        )

        result = duckdb_io.merge(
            data=new_data,
            path=initial_dataset,
            strategy="insert",
            key_columns=["id"],
        )

        # Should have inserted files
        assert len(result.inserted_files) > 0
        assert len(result.rewritten_files) == 0
        assert any(m.operation == "inserted" for m in result.files)


class TestMergeEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_merge_to_nonexistent_dataset_with_update_fails(self, temp_dir, duckdb_io):
        """Test that UPDATE on non-existent dataset fails."""
        data = pa.table({"id": [1], "name": ["Alice"], "value": [100]})
        nonexistent_path = str(temp_dir / "nonexistent")

        with pytest.raises(ValueError, match="non-existent target"):
            duckdb_io.merge(
                data=data,
                path=nonexistent_path,
                strategy="update",
                key_columns=["id"],
            )

    def test_merge_to_nonexistent_dataset_with_insert(self, temp_dir, duckdb_io):
        """Test that INSERT on non-existent dataset performs initial write."""
        data = pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]})
        new_path = str(temp_dir / "new_dataset")

        result = duckdb_io.merge(
            data=data,
            path=new_path,
            strategy="insert",
            key_columns=["id"],
        )

        # Should perform initial write
        assert result.target_count_before == 0
        assert result.target_count_after == 2
        assert result.inserted == 2

    def test_merge_with_multi_column_keys(self, temp_dir, duckdb_io):
        """Test merge with multiple key columns."""
        # Create initial dataset
        initial_data = pa.table(
            {
                "id": [1, 2, 3],
                "category": ["A", "B", "A"],
                "value": [100, 200, 300],
            }
        )
        dataset_path = str(temp_dir / "dataset")
        duckdb_io._write_parquet_dataset_standard(
            data=initial_data, path=dataset_path, mode="overwrite"
        )

        # Upsert with composite key
        upsert_data = pa.table(
            {
                "id": [1, 4],
                "category": ["A", "B"],  # (1,A) exists, (4,B) is new
                "value": [111, 400],
            }
        )

        result = duckdb_io.merge(
            data=upsert_data,
            path=dataset_path,
            strategy="upsert",
            key_columns=["id", "category"],
        )

        assert result.inserted == 1  # (4,B)
        assert result.updated >= 1  # (1,A)

    def test_merge_empty_source(self, temp_dir, duckdb_io, initial_dataset):
        """Test merge with empty source data."""
        empty_data = pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "name": pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.int64()),
            }
        )

        result = duckdb_io.merge(
            data=empty_data,
            path=initial_dataset,
            strategy="upsert",
            key_columns=["id"],
        )

        # No changes should occur
        assert result.inserted == 0
        assert result.updated == 0
        assert result.target_count_after == result.target_count_before

    def test_null_keys_rejected(self, temp_dir, duckdb_io):
        """Merge should reject NULL keys in source."""
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir()

        existing = pa.table({"id": [1, 2], "name": ["A", "B"], "value": [10, 20]})
        pq.write_table(existing, dataset_dir / "part-0.parquet")

        source = pa.table(
            {"id": [2, None, 3], "name": ["B2", "NULL", "C"], "value": [22, 0, 30]}
        )

        with pytest.raises(ValueError, match="NULL"):
            duckdb_io.merge(
                data=source,
                path=str(dataset_dir),
                strategy="upsert",
                key_columns=["id"],
            )

    def test_partition_immutability_enforced(self, temp_dir, duckdb_io):
        """Merge should reject partition column changes for existing keys."""
        dataset_dir = temp_dir / "dataset_partition"
        dataset_dir.mkdir()

        existing = pa.table(
            {
                "id": [1, 2],
                "partition": ["A", "A"],
                "name": ["A1", "A2"],
                "value": [10, 20],
            }
        )
        pq.write_table(existing, dataset_dir / "part-0.parquet")

        # Attempt to change partition for existing key 2
        source = pa.table(
            {"id": [2], "partition": ["B"], "name": ["A2_updated"], "value": [222]}
        )

        with pytest.raises(ValueError, match="partition"):
            duckdb_io.merge(
                data=source,
                path=str(dataset_dir),
                strategy="upsert",
                key_columns=["id"],
                partition_columns=["partition"],
            )
