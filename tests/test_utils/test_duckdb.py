"""Tests for DuckDB parquet handler."""

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from fsspeckit.storage_options import LocalStorageOptions
from fsspeckit.datasets.duckdb import DuckDBParquetHandler
from fsspeckit.common.optional import _DUCKDB_AVAILABLE
from fsspeckit import filesystem


@pytest.fixture
def sample_table():
    """Create a sample PyArrow table for testing."""
    return pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [28, 34, 45, 29, 52],
            "city": ["New York", "London", "Paris", "Tokyo", "Sydney"],
            "amount": [150.50, 89.99, 234.75, 67.25, 412.80],
            "category": ["A", "B", "A", "C", "B"],
            "active": [True, True, False, True, False],
        }
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


class TestDuckDBParquetHandlerInit:
    """Tests for DuckDBParquetHandler initialization."""

    def test_init_default(self):
        """Test initialization with default parameters."""
        handler = DuckDBParquetHandler()
        assert handler is not None
        assert handler._filesystem is not None
        handler.close()

    def test_init_with_storage_options(self):
        """Test initialization with storage options."""
        storage_options = LocalStorageOptions()
        handler = DuckDBParquetHandler(storage_options=storage_options)
        assert handler is not None
        assert handler._filesystem is not None
        handler.close()

    def test_init_with_filesystem(self):
        """Test initialization with filesystem instance."""
        fs = filesystem("file")
        handler = DuckDBParquetHandler(filesystem=fs)
        assert handler is not None
        assert handler._filesystem is fs
        handler.close()

    def test_init_filesystem_precedence(self):
        """Test that filesystem parameter takes precedence over storage_options."""
        storage_options = LocalStorageOptions()
        fs = filesystem("file")
        handler = DuckDBParquetHandler(storage_options=storage_options, filesystem=fs)
        assert handler._filesystem is fs
        handler.close()


class TestDuckDBParquetHandlerContextManager:
    """Tests for context manager functionality."""

    def test_context_manager(self):
        """Test context manager protocol."""
        with DuckDBParquetHandler() as handler:
            assert handler is not None
            assert handler._connection is not None

    def test_connection_closed_after_context(self):
        """Test that connection is closed after context manager exits."""
        handler = DuckDBParquetHandler()
        with handler:
            assert handler._connection is not None
        # Connection should be closed after exiting context
        assert handler._connection is None

    def test_context_manager_with_exception(self, sample_table, temp_dir):
        """Test that connection is closed even when exception occurs."""
        handler = DuckDBParquetHandler()
        try:
            with handler:
                # Deliberately cause an error
                handler.read_parquet("/nonexistent/path.parquet")
        except Exception:
            pass
        # Connection should still be closed
        assert handler._connection is None


class TestDuckDBParquetHandlerReadWrite:
    """Tests for read and write operations."""

    def test_write_and_read_single_file(self, sample_table, temp_dir):
        """Test writing and reading a single parquet file."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            # Write
            handler.write_parquet(sample_table, str(parquet_file))
            assert parquet_file.exists()

            # Read
            result = handler.read_parquet(str(parquet_file))
            assert isinstance(result, pa.Table)
            assert result.num_rows == sample_table.num_rows
            assert result.num_columns == sample_table.num_columns
            assert result.column_names == sample_table.column_names

    def test_write_with_compression_snappy(self, sample_table, temp_dir):
        """Test writing with snappy compression."""
        parquet_file = temp_dir / "data_snappy.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file), compression="snappy")
            assert parquet_file.exists()

            result = handler.read_parquet(str(parquet_file))
            assert result.num_rows == sample_table.num_rows

    def test_write_with_compression_gzip(self, sample_table, temp_dir):
        """Test writing with gzip compression."""
        parquet_file = temp_dir / "data_gzip.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file), compression="gzip")
            assert parquet_file.exists()

            result = handler.read_parquet(str(parquet_file))
            assert result.num_rows == sample_table.num_rows

    def test_write_with_compression_zstd(self, sample_table, temp_dir):
        """Test writing with zstd compression."""
        parquet_file = temp_dir / "data_zstd.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file), compression="zstd")
            assert parquet_file.exists()

            result = handler.read_parquet(str(parquet_file))
            assert result.num_rows == sample_table.num_rows

    def test_write_to_nested_directory(self, sample_table, temp_dir):
        """Test writing to nested directory structure."""
        nested_file = temp_dir / "year=2024" / "month=01" / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(nested_file))
            assert nested_file.exists()

            result = handler.read_parquet(str(nested_file))
            assert result.num_rows == sample_table.num_rows

    def test_read_parquet_with_column_selection(self, sample_table, temp_dir):
        """Test reading parquet with specific columns."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            # Read only specific columns
            result = handler.read_parquet(
                str(parquet_file), columns=["id", "name", "age"]
            )
            assert result.num_rows == sample_table.num_rows
            assert result.num_columns == 3
            assert result.column_names == ["id", "name", "age"]

    def test_read_parquet_dataset_directory(self, sample_table, temp_dir):
        """Test reading parquet dataset from directory."""
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir()

        with DuckDBParquetHandler() as handler:
            # Write multiple files
            for i in range(3):
                chunk = sample_table.slice(i, min(2, sample_table.num_rows - i * 2))
                if chunk.num_rows > 0:
                    file_path = dataset_dir / f"part_{i}.parquet"
                    handler.write_parquet(chunk, str(file_path))

            # Read entire dataset
            result = handler.read_parquet(str(dataset_dir))
            assert isinstance(result, pa.Table)
            # Should have read all rows across all files
            assert result.num_rows >= sample_table.num_rows

    def test_read_nonexistent_file_raises_error(self):
        """Test that reading nonexistent file raises error."""
        with DuckDBParquetHandler() as handler:
            with pytest.raises(Exception):
                handler.read_parquet("/nonexistent/file.parquet")

    def test_write_empty_table(self, temp_dir):
        """Test writing empty table."""
        empty_table = pa.table(
            {
                "col1": pa.array([], type=pa.int64()),
                "col2": pa.array([], type=pa.string()),
            }
        )
        parquet_file = temp_dir / "empty.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(empty_table, str(parquet_file))
            assert parquet_file.exists()

            result = handler.read_parquet(str(parquet_file))
            assert result.num_rows == 0
            assert result.num_columns == 2


class TestDuckDBParquetHandlerSQL:
    """Tests for SQL query execution."""

    def test_execute_simple_query(self, sample_table, temp_dir):
        """Test executing a simple SQL query."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            query = f"SELECT * FROM parquet_scan('{parquet_file}') WHERE age > 30"
            result = handler.execute_sql(query)

            assert isinstance(result, pa.Table)
            assert result.num_rows < sample_table.num_rows
            # Verify all ages are > 30
            ages = result.column("age").to_pylist()
            assert all(age > 30 for age in ages)

    def test_execute_aggregation_query(self, sample_table, temp_dir):
        """Test executing aggregation query."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            query = f"""
            SELECT category, COUNT(*) as count, AVG(amount) as avg_amount
            FROM parquet_scan('{parquet_file}')
            GROUP BY category
            ORDER BY category
            """
            result = handler.execute_sql(query)

            assert isinstance(result, pa.Table)
            assert "category" in result.column_names
            assert "count" in result.column_names
            assert "avg_amount" in result.column_names

    def test_execute_parameterized_query(self, sample_table, temp_dir):
        """Test executing parameterized query."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            query = f"SELECT * FROM parquet_scan('{parquet_file}') WHERE age BETWEEN ? AND ?"
            result = handler.execute_sql(query, parameters=[30, 50])

            assert isinstance(result, pa.Table)
            # Verify ages are in range
            ages = result.column("age").to_pylist()
            assert all(30 <= age <= 50 for age in ages)

    def test_execute_column_selection_query(self, sample_table, temp_dir):
        """Test executing query with column selection."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            query = f"SELECT id, name, city FROM parquet_scan('{parquet_file}')"
            result = handler.execute_sql(query)

            assert result.num_columns == 3
            assert result.column_names == ["id", "name", "city"]

    def test_execute_with_order_by(self, sample_table, temp_dir):
        """Test executing query with ORDER BY."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            query = f"SELECT * FROM parquet_scan('{parquet_file}') ORDER BY age DESC"
            result = handler.execute_sql(query)

            ages = result.column("age").to_pylist()
            # Verify descending order
            assert ages == sorted(ages, reverse=True)

    def test_execute_with_limit(self, sample_table, temp_dir):
        """Test executing query with LIMIT."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            query = f"SELECT * FROM parquet_scan('{parquet_file}') LIMIT 3"
            result = handler.execute_sql(query)

            assert result.num_rows == 3

    def test_execute_invalid_query_raises_error(self, sample_table, temp_dir):
        """Test that invalid SQL raises error."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            # Invalid SQL syntax
            with pytest.raises(Exception):
                handler.execute_sql("INVALID SQL QUERY")

    def test_execute_query_on_nonexistent_column(self, sample_table, temp_dir):
        """Test query on nonexistent column raises error."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            query = f"SELECT nonexistent_column FROM parquet_scan('{parquet_file}')"
            with pytest.raises(Exception):
                handler.execute_sql(query)


class TestDuckDBParquetHandlerIntegration:
    """Integration tests for real-world scenarios."""

    def test_multiple_operations_in_session(self, sample_table, temp_dir):
        """Test multiple operations within a single session."""
        file1 = temp_dir / "data1.parquet"
        file2 = temp_dir / "data2.parquet"

        with DuckDBParquetHandler() as handler:
            # Write two files
            handler.write_parquet(sample_table, str(file1))
            handler.write_parquet(sample_table, str(file2))

            # Read both files
            result1 = handler.read_parquet(str(file1))
            result2 = handler.read_parquet(str(file2))

            assert result1.num_rows == result2.num_rows

            # Query joining both files
            query = f"""
            SELECT a.id, a.name, b.amount
            FROM parquet_scan('{file1}') a
            JOIN parquet_scan('{file2}') b ON a.id = b.id
            """
            result = handler.execute_sql(query)
            assert result.num_rows == sample_table.num_rows

    def test_large_dataset(self, temp_dir):
        """Test with larger dataset."""
        # Create larger dataset
        large_data = {
            "id": list(range(1000)),
            "value": [i * 1.5 for i in range(1000)],
            "category": [f"cat_{i % 10}" for i in range(1000)],
        }
        large_table = pa.table(large_data)
        parquet_file = temp_dir / "large.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(large_table, str(parquet_file))

            # Read with column selection for performance
            result = handler.read_parquet(str(parquet_file), columns=["id", "category"])
            assert result.num_rows == 1000
            assert result.num_columns == 2

            # Aggregation on large dataset
            query = f"""
            SELECT category, COUNT(*) as count, AVG(value) as avg_value
            FROM parquet_scan('{parquet_file}')
            GROUP BY category
            """
            result = handler.execute_sql(query)
            assert result.num_rows == 10  # 10 categories

    def test_window_function_query(self, sample_table, temp_dir):
        """Test window function in SQL query."""
        parquet_file = temp_dir / "data.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sample_table, str(parquet_file))

            query = f"""
            SELECT
                name,
                amount,
                AVG(amount) OVER () as avg_amount,
                ROW_NUMBER() OVER (ORDER BY amount DESC) as rank
            FROM parquet_scan('{parquet_file}')
            """
            result = handler.execute_sql(query)

            assert result.num_rows == sample_table.num_rows
            assert "rank" in result.column_names
            assert "avg_amount" in result.column_names

    def test_data_type_preservation(self, temp_dir):
        """Test that data types are preserved through write and read."""
        test_table = pa.table(
            {
                "int32": pa.array([1, 2, 3], type=pa.int32()),
                "int64": pa.array([1, 2, 3], type=pa.int64()),
                "float32": pa.array([1.1, 2.2, 3.3], type=pa.float32()),
                "float64": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
                "string": pa.array(["a", "b", "c"], type=pa.string()),
                "bool": pa.array([True, False, True], type=pa.bool_()),
            }
        )
        parquet_file = temp_dir / "types.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(test_table, str(parquet_file))
            result = handler.read_parquet(str(parquet_file))

            # Check that types are preserved (or compatible)
            assert result.num_columns == test_table.num_columns
            assert result.column_names == test_table.column_names


class TestDuckDBParquetHandlerDatasetWrite:
    """Tests for dataset write functionality."""

    def test_write_dataset_basic(self, sample_table, temp_dir):
        """Test basic dataset write with single file."""
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(sample_table, str(dataset_dir))

            # Check directory created
            assert dataset_dir.exists()
            assert dataset_dir.is_dir()

            # Check file created
            files = list(dataset_dir.glob("*.parquet"))
            assert len(files) == 1
            assert files[0].name.startswith("part-")

            # Verify data is readable
            result = handler.read_parquet(str(dataset_dir))
            assert result.num_rows == sample_table.num_rows
            assert result.column_names == sample_table.column_names

    def test_write_dataset_append_mode(self, sample_table, temp_dir):
        """Test append mode adds files without deleting."""
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            # First write
            handler.write_parquet_dataset(sample_table, str(dataset_dir), mode="append")
            files_after_first = list(dataset_dir.glob("*.parquet"))
            assert len(files_after_first) == 1

            # Second write (append)
            handler.write_parquet_dataset(sample_table, str(dataset_dir), mode="append")
            files_after_second = list(dataset_dir.glob("*.parquet"))
            assert len(files_after_second) == 2

            # Both files should exist
            assert files_after_first[0] in files_after_second

            # Read combined dataset
            result = handler.read_parquet(str(dataset_dir))
            assert result.num_rows == sample_table.num_rows * 2

    def test_write_dataset_overwrite_mode(self, sample_table, temp_dir):
        """Test overwrite mode replaces existing files."""
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            # First write
            handler.write_parquet_dataset(sample_table, str(dataset_dir))
            files_after_first = list(dataset_dir.glob("*.parquet"))
            assert len(files_after_first) == 1
            first_file = files_after_first[0]

            # Second write (overwrite)
            handler.write_parquet_dataset(
                sample_table, str(dataset_dir), mode="overwrite"
            )
            files_after_second = list(dataset_dir.glob("*.parquet"))
            assert len(files_after_second) == 1

            # File should be different
            assert first_file not in files_after_second

            # Read dataset - should have only new data
            result = handler.read_parquet(str(dataset_dir))
            assert result.num_rows == sample_table.num_rows

    def test_write_dataset_with_max_rows_per_file(self, temp_dir):
        """Test splitting large table into multiple files."""
        # Create table with 100 rows
        large_data = {
            "id": list(range(100)),
            "value": [i * 2 for i in range(100)],
        }
        large_table = pa.table(large_data)
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            # Split into files with max 30 rows each
            handler.write_parquet_dataset(
                large_table, str(dataset_dir), max_rows_per_file=30
            )

            # Should create 4 files (30, 30, 30, 10)
            files = list(dataset_dir.glob("*.parquet"))
            assert len(files) == 4

            # Read back and verify all data present
            result = handler.read_parquet(str(dataset_dir))
            assert result.num_rows == 100

    def test_write_dataset_custom_basename_template(self, sample_table, temp_dir):
        """Test custom filename template."""
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(
                sample_table, str(dataset_dir), basename_template="data_{}.parquet"
            )

            files = list(dataset_dir.glob("*.parquet"))
            assert len(files) == 1
            assert files[0].name.startswith("data_")

    def test_write_dataset_with_compression(self, sample_table, temp_dir):
        """Test dataset write with different compression."""
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(
                sample_table, str(dataset_dir), compression="gzip"
            )

            # Verify file exists and is readable
            result = handler.read_parquet(str(dataset_dir))
            assert result.num_rows == sample_table.num_rows

    def test_write_dataset_invalid_mode_error(self, sample_table, temp_dir):
        """Test error on invalid mode."""
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            with pytest.raises(ValueError, match="Invalid mode"):
                handler.write_parquet_dataset(
                    sample_table,
                    str(dataset_dir),
                    mode="invalid",  # type: ignore
                )

    def test_write_dataset_invalid_max_rows_error(self, sample_table, temp_dir):
        """Test error on invalid max_rows_per_file."""
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            with pytest.raises(ValueError, match="must be > 0"):
                handler.write_parquet_dataset(
                    sample_table, str(dataset_dir), max_rows_per_file=0
                )

            with pytest.raises(ValueError, match="must be > 0"):
                handler.write_parquet_dataset(
                    sample_table, str(dataset_dir), max_rows_per_file=-10
                )

    def test_write_dataset_empty_table(self, temp_dir):
        """Test writing empty table to dataset."""
        empty_table = pa.table(
            {
                "col1": pa.array([], type=pa.int64()),
                "col2": pa.array([], type=pa.string()),
            }
        )
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(empty_table, str(dataset_dir))

            # Should create file with schema but no rows
            files = list(dataset_dir.glob("*.parquet"))
            assert len(files) == 1

            result = handler.read_parquet(str(dataset_dir))
            assert result.num_rows == 0
            assert result.num_columns == 2

    def test_write_dataset_unique_filenames(self, sample_table, temp_dir):
        """Test that multiple writes create unique filenames."""
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            filenames = set()

            # Write 5 times
            for _ in range(5):
                handler.write_parquet_dataset(
                    sample_table, str(dataset_dir), mode="append"
                )

            files = list(dataset_dir.glob("*.parquet"))
            assert len(files) == 5

            # All filenames should be unique
            filenames = {f.name for f in files}
            assert len(filenames) == 5

    def test_write_dataset_overwrite_preserves_non_parquet(
        self, sample_table, temp_dir
    ):
        """Test that overwrite mode preserves non-parquet files."""
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir()

        # Create non-parquet file
        readme_file = dataset_dir / "README.txt"
        readme_file.write_text("This is a readme")

        with DuckDBParquetHandler() as handler:
            # First write
            handler.write_parquet_dataset(sample_table, str(dataset_dir))

            # Overwrite
            handler.write_parquet_dataset(
                sample_table, str(dataset_dir), mode="overwrite"
            )

            # README should still exist
            assert readme_file.exists()
            assert readme_file.read_text() == "This is a readme"

            # Should have one parquet file
            parquet_files = list(dataset_dir.glob("*.parquet"))
            assert len(parquet_files) == 1

    def test_write_dataset_multiple_splits_with_template(self, temp_dir):
        """Test splitting with custom template."""
        large_table = pa.table(
            {
                "id": list(range(100)),
                "value": list(range(100)),
            }
        )
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(
                large_table,
                str(dataset_dir),
                max_rows_per_file=25,
                basename_template="chunk_{}.parquet",
            )

            files = list(dataset_dir.glob("*.parquet"))
            assert len(files) == 4

            # All should start with "chunk_"
            for f in files:
                assert f.name.startswith("chunk_")

    def test_generate_unique_filename(self):
        """Test filename generation helper."""
        handler = DuckDBParquetHandler()

        # With placeholder
        filename1 = handler._generate_unique_filename("part-{}.parquet")
        assert filename1.startswith("part-")
        assert filename1.endswith(".parquet")

        # Without placeholder
        filename2 = handler._generate_unique_filename("data.parquet")
        assert filename2.startswith("data-")
        assert filename2.endswith(".parquet")

        # Multiple calls should produce different filenames
        filename3 = handler._generate_unique_filename("part-{}.parquet")
        assert filename3 != filename1

        handler.close()

    def test_write_dataset_path_is_file_error(self, sample_table, temp_dir):
        """Test error when dataset path exists but is a file."""
        # Create a file where we want to write dataset
        existing_file = temp_dir / "existing_file.txt"
        existing_file.write_text("This is a file, not a directory")

        with DuckDBParquetHandler() as handler:
            with pytest.raises(NotADirectoryError, match="exists but is a file"):
                handler.write_parquet_dataset(sample_table, str(existing_file))

    def test_write_parquet_parent_is_file_error(self, sample_table, temp_dir):
        """Test error when parent directory for parquet file is a file."""
        # Create a file where we want to create a parent directory
        parent_file = temp_dir / "parent_file.txt"
        parent_file.write_text("This is a file, not a directory")

        parquet_path = parent_file / "subdir" / "output.parquet"

        with DuckDBParquetHandler() as handler:
            with pytest.raises(
                (NotADirectoryError, Exception),
                match="Parent directory.*exists but is a file|Cannot open file.*Not a directory",
            ):
                handler.write_parquet(sample_table, str(parquet_path))


class TestDuckDBParquetHandlerMerge:
    """Tests for dataset merge functionality."""

    def test_merge_upsert_basic(self, temp_dir):
        """Test UPSERT strategy with basic insert and update."""
        # Create initial target data
        target_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100, 200, 300],
            }
        )

        # Create source with update to id=2 and new id=4
        source_data = pa.table(
            {"id": [2, 4], "name": ["Bob Updated", "Diana"], "value": [250, 400]}
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            # Write initial target
            handler.write_parquet_dataset(target_data, str(dataset_dir))

            # Merge with UPSERT
            stats = handler.merge_parquet_dataset(
                source=source_data,
                target_path=str(dataset_dir),
                key_columns="id",
                strategy="upsert",
            )

            # Verify statistics
            assert stats["inserted"] == 1  # id=4 inserted
            assert stats["updated"] == 1  # id=2 updated
            assert stats["deleted"] == 0
            assert stats["total"] == 4  # 1,2,3,4

            # Verify result
            result = handler.read_parquet(str(dataset_dir))
            assert result.num_rows == 4

            # Check that id=2 was updated
            result_dict = result.to_pydict()
            bob_idx = result_dict["id"].index(2)
            assert result_dict["name"][bob_idx] == "Bob Updated"
            assert result_dict["value"][bob_idx] == 250

            # Check that id=4 was inserted
            assert 4 in result_dict["id"]

    def test_merge_insert_only(self, temp_dir):
        """Test INSERT strategy adds only new records."""
        target_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]}
        )

        source_data = pa.table(
            {
                "id": [2, 3],  # id=2 exists, id=3 is new
                "name": ["Bob Updated", "Charlie"],
                "value": [250, 300],
            }
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(target_data, str(dataset_dir))

            stats = handler.merge_parquet_dataset(
                source=source_data,
                target_path=str(dataset_dir),
                key_columns="id",
                strategy="insert",
            )

            # Only id=3 should be inserted
            assert stats["inserted"] == 1
            assert stats["updated"] == 0
            assert stats["deleted"] == 0
            assert stats["total"] == 3

            # Verify id=2 was NOT updated
            result = handler.read_parquet(str(dataset_dir))
            result_dict = result.to_pydict()
            bob_idx = result_dict["id"].index(2)
            assert result_dict["name"][bob_idx] == "Bob"  # Original name
            assert result_dict["value"][bob_idx] == 200  # Original value

    def test_merge_update_only(self, temp_dir):
        """Test UPDATE strategy updates only existing records."""
        target_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]}
        )

        source_data = pa.table(
            {
                "id": [2, 3],  # id=2 exists, id=3 is new
                "name": ["Bob Updated", "Charlie"],
                "value": [250, 300],
            }
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(target_data, str(dataset_dir))

            stats = handler.merge_parquet_dataset(
                source=source_data,
                target_path=str(dataset_dir),
                key_columns="id",
                strategy="update",
            )

            # Only id=2 should be updated
            assert stats["inserted"] == 0
            assert stats["updated"] == 2  # All existing potentially updated
            assert stats["deleted"] == 0
            assert stats["total"] == 2  # Still just 1, 2

            # Verify id=2 was updated
            result = handler.read_parquet(str(dataset_dir))
            result_dict = result.to_pydict()
            bob_idx = result_dict["id"].index(2)
            assert result_dict["name"][bob_idx] == "Bob Updated"

            # Verify id=3 was NOT inserted
            assert 3 not in result_dict["id"]

    def test_merge_full_merge(self, temp_dir):
        """Test FULL_MERGE strategy with deletes."""
        target_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100, 200, 300],
            }
        )

        source_data = pa.table(
            {
                "id": [2, 4],  # id=1,3 will be deleted, id=4 inserted
                "name": ["Bob Updated", "Diana"],
                "value": [250, 400],
            }
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(target_data, str(dataset_dir))

            stats = handler.merge_parquet_dataset(
                source=source_data,
                target_path=str(dataset_dir),
                key_columns="id",
                strategy="full_merge",
            )

            # Full replacement
            assert stats["inserted"] == 2  # Source count
            assert stats["updated"] == 0
            assert stats["deleted"] == 3  # Target count
            assert stats["total"] == 2

            # Verify only source records remain
            result = handler.read_parquet(str(dataset_dir))
            result_dict = result.to_pydict()
            assert sorted(result_dict["id"]) == [2, 4]
            assert 1 not in result_dict["id"]
            assert 3 not in result_dict["id"]

    def test_merge_deduplicate(self, temp_dir):
        """Test DEDUPLICATE strategy with QUALIFY."""
        target_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "timestamp": [100, 200]}
        )

        # Source has duplicates
        source_data = pa.table(
            {
                "id": [2, 2, 3],  # id=2 appears twice
                "name": ["Bob V1", "Bob V2", "Charlie"],
                "timestamp": [250, 300, 350],  # Higher timestamp wins
            }
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(target_data, str(dataset_dir))

            stats = handler.merge_parquet_dataset(
                source=source_data,
                target_path=str(dataset_dir),
                key_columns="id",
                strategy="deduplicate",
                dedup_order_by=["timestamp"],  # Keep highest timestamp
            )

            assert stats["total"] == 3  # 1, 2, 3

            # Verify only one id=2 remains (with highest timestamp)
            result = handler.read_parquet(str(dataset_dir))
            result_dict = result.to_pydict()
            bob_indices = [i for i, x in enumerate(result_dict["id"]) if x == 2]
            assert len(bob_indices) == 1  # Only one id=2
            bob_idx = bob_indices[0]
            assert result_dict["name"][bob_idx] == "Bob V2"  # Highest timestamp
            assert result_dict["timestamp"][bob_idx] == 300

    def test_merge_composite_key(self, temp_dir):
        """Test merge with composite key columns."""
        target_data = pa.table(
            {
                "user_id": [1, 1, 2],
                "date": ["2024-01-01", "2024-01-02", "2024-01-01"],
                "value": [100, 200, 300],
            }
        )

        source_data = pa.table(
            {
                "user_id": [1, 2],
                "date": [
                    "2024-01-02",
                    "2024-01-03",
                ],  # Update (1, 2024-01-02), Insert (2, 2024-01-03)
                "value": [250, 400],
            }
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(target_data, str(dataset_dir))

            stats = handler.merge_parquet_dataset(
                source=source_data,
                target_path=str(dataset_dir),
                key_columns=["user_id", "date"],
                strategy="upsert",
            )

            assert stats["inserted"] == 1  # (2, 2024-01-03)
            assert stats["updated"] == 1  # (1, 2024-01-02)
            assert stats["total"] == 4

    def test_merge_from_path_source(self, temp_dir):
        """Test merge with source as path to dataset."""
        target_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]}
        )

        source_data = pa.table(
            {"id": [2, 3], "name": ["Bob Updated", "Charlie"], "value": [250, 300]}
        )

        target_dir = temp_dir / "target"
        source_dir = temp_dir / "source"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(target_data, str(target_dir))
            handler.write_parquet_dataset(source_data, str(source_dir))

            # Merge using source path
            stats = handler.merge_parquet_dataset(
                source=str(source_dir),
                target_path=str(target_dir),
                key_columns="id",
                strategy="upsert",
            )

            assert stats["total"] == 3
            result = handler.read_parquet(str(target_dir))
            assert result.num_rows == 3

    def test_merge_empty_target(self, temp_dir):
        """Test merge to non-existent target (initial load)."""
        source_data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100, 200, 300],
            }
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            # Target doesn't exist yet
            stats = handler.merge_parquet_dataset(
                source=source_data,
                target_path=str(dataset_dir),
                key_columns="id",
                strategy="upsert",
            )

            # Should insert all records
            assert stats["inserted"] == 3
            assert stats["total"] == 3

            result = handler.read_parquet(str(dataset_dir))
            assert result.num_rows == 3

    def test_merge_invalid_strategy(self, temp_dir):
        """Test error on invalid strategy."""
        source_data = pa.table({"id": [1], "name": ["Alice"]})
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            with pytest.raises(ValueError, match="Invalid strategy"):
                handler.merge_parquet_dataset(
                    source=source_data,
                    target_path=str(dataset_dir),
                    key_columns="id",
                    strategy="invalid",  # type: ignore
                )

    def test_merge_missing_key_column(self, temp_dir):
        """Test error when key column missing from source."""
        source_data = pa.table({"id": [1], "name": ["Alice"]})
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            with pytest.raises(ValueError, match="Key column 'missing_key' not found"):
                handler.merge_parquet_dataset(
                    source=source_data,
                    target_path=str(dataset_dir),
                    key_columns="missing_key",
                    strategy="upsert",
                )

    def test_merge_null_in_key_column(self, temp_dir):
        """Test error when key column contains NULLs."""
        source_data = pa.table(
            {"id": [1, None, 3], "name": ["Alice", "Bob", "Charlie"]}
        )
        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            with pytest.raises(ValueError, match="contains .* NULL values"):
                handler.merge_parquet_dataset(
                    source=source_data,
                    target_path=str(dataset_dir),
                    key_columns="id",
                    strategy="upsert",
                )

    def test_merge_schema_mismatch(self, temp_dir):
        """Test error on schema mismatch."""
        target_data = pa.table(
            {"id": [1, 2], "name": ["Alice", "Bob"], "value": [100, 200]}
        )

        # Source has different schema
        source_data = pa.table(
            {
                "id": [2, 3],
                "name": ["Bob", "Charlie"],
                "amount": [250, 300],  # Different column name
            }
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(target_data, str(dataset_dir))

            with pytest.raises(ValueError, match="Schema mismatch"):
                handler.merge_parquet_dataset(
                    source=source_data,
                    target_path=str(dataset_dir),
                    key_columns="id",
                    strategy="upsert",
                )

    def test_merge_type_mismatch(self, temp_dir):
        """Test error on column type mismatch."""
        target_data = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "value": pa.array([100, 200], type=pa.int64()),
            }
        )

        source_data = pa.table(
            {
                "id": pa.array([2, 3], type=pa.int64()),
                "value": pa.array(["250", "300"], type=pa.string()),  # Wrong type
            }
        )

        dataset_dir = temp_dir / "dataset"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(target_data, str(dataset_dir))

            with pytest.raises(TypeError, match="type mismatch"):
                handler.merge_parquet_dataset(
                    source=source_data,
                    target_path=str(dataset_dir),
                    key_columns="id",
                    strategy="upsert",
                )


class TestDuckDBParquetHandlerEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_special_characters_in_data(self, temp_dir):
        """Test handling of special characters in data."""
        special_table = pa.table(
            {
                "text": ["hello", "world", "café", "naïve", "日本語"],
                "symbols": ["@", "#", "$", "%", "&"],
            }
        )
        parquet_file = temp_dir / "special.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(special_table, str(parquet_file))
            result = handler.read_parquet(str(parquet_file))

            assert result.num_rows == special_table.num_rows
            # Verify special characters preserved
            text_values = result.column("text").to_pylist()
            assert "café" in text_values
            assert "日本語" in text_values

    def test_null_values_handling(self, temp_dir):
        """Test handling of null values."""
        null_table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", None, "Charlie", None, "Eve"],
                "age": [28, 34, None, 29, None],
            }
        )
        parquet_file = temp_dir / "nulls.parquet"

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(null_table, str(parquet_file))
            result = handler.read_parquet(str(parquet_file))

            assert result.num_rows == null_table.num_rows
            # Verify nulls are preserved
            name_values = result.column("name").to_pylist()
            assert name_values[1] is None
            assert name_values[3] is None

    def test_reuse_handler_without_context_manager(self, sample_table, temp_dir):
        """Test reusing handler instance without context manager."""
        handler = DuckDBParquetHandler()
        file1 = temp_dir / "data1.parquet"
        file2 = temp_dir / "data2.parquet"

        try:
            handler.write_parquet(sample_table, str(file1))
            handler.write_parquet(sample_table, str(file2))

            result1 = handler.read_parquet(str(file1))
            result2 = handler.read_parquet(str(file2))

            assert result1.num_rows == sample_table.num_rows
            assert result2.num_rows == sample_table.num_rows
        finally:
            handler.close()


@pytest.fixture
def fragmented_dataset(temp_dir):
    """Create a fragmented parquet dataset with many small files for maintenance tests."""
    dataset_dir = temp_dir / "fragmented"
    dataset_dir.mkdir()
    with DuckDBParquetHandler() as handler:
        # Create 10 small files each with 50 rows
        for i in range(10):
            table = pa.table(
                {
                    "id": list(range(i * 50, i * 50 + 50)),
                    "group": [i] * 50,
                    "value": [float(x) for x in range(50)],
                }
            )
            handler.write_parquet(table, str(dataset_dir / f"part_{i}.parquet"))
    return dataset_dir


class TestDuckDBParquetHandlerMaintenance:
    """Tests for compaction and optimization maintenance operations."""

    def test_compact_by_size(self, fragmented_dataset):
        """Verify compaction reduces file count and preserves row count (size threshold)."""
        with DuckDBParquetHandler() as handler:
            before_table = handler.read_parquet(str(fragmented_dataset))
            before_files = list(Path(fragmented_dataset).glob("*.parquet"))
            # Use 1MB target; small files should group
            result = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,
            )
            after_files = list(Path(fragmented_dataset).glob("*.parquet"))
            after_table = handler.read_parquet(str(fragmented_dataset))
            assert result["before_file_count"] == len(before_files)
            assert len(after_files) < len(before_files)
            assert after_table.num_rows == before_table.num_rows

    def test_compact_by_rows(self, fragmented_dataset):
        """Verify compaction using row threshold produces files within limit."""
        with DuckDBParquetHandler() as handler:
            total_rows = handler.read_parquet(str(fragmented_dataset)).num_rows
            result = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_rows_per_file=120,
            )
            assert result["dry_run"] is False
            # Inspect each file row count
            for f in Path(fragmented_dataset).glob("*.parquet"):
                table = handler.read_parquet(str(f))
                assert table.num_rows <= 120
            # Total rows preserved
            assert handler.read_parquet(str(fragmented_dataset)).num_rows == total_rows

    def test_compact_dry_run(self, fragmented_dataset):
        """Verify dry-run returns plan and does not modify files."""
        before_files = list(Path(fragmented_dataset).glob("*.parquet"))
        with DuckDBParquetHandler() as handler:
            result = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,
                dry_run=True,
            )
        after_files = list(Path(fragmented_dataset).glob("*.parquet"))
        assert before_files == after_files
        assert result["dry_run"] is True
        assert "planned_groups" in result and result["planned_groups"]

    def test_optimize_with_zorder(self, fragmented_dataset):
        """Verify optimize rewrites dataset ordering by given columns."""
        with DuckDBParquetHandler() as handler:
            result = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
            )
            assert result["dry_run"] is False
            assert result["after_file_count"] >= 1
            # Verify ordering on primary column 'group'
            combined = handler.read_parquet(str(fragmented_dataset))
            groups = combined.column("group").to_pylist()
            assert groups == sorted(groups)

    def test_optimize_with_chunking(self, fragmented_dataset):
        """Verify optimize can chunk output using row threshold."""
        with DuckDBParquetHandler() as handler:
            result = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
                target_rows_per_file=200,
            )
            assert result["dry_run"] is False
            files = list(Path(fragmented_dataset).glob("optimized-*.parquet"))
            assert len(files) >= 2 or result["after_file_count"] >= 2

    def test_optimize_dry_run(self, fragmented_dataset):
        """Verify optimize dry-run returns planned groups without rewriting."""
        before_files = list(Path(fragmented_dataset).glob("*.parquet"))
        with DuckDBParquetHandler() as handler:
            result = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
                dry_run=True,
            )
        after_files = list(Path(fragmented_dataset).glob("*.parquet"))
        assert before_files == after_files
        assert result["dry_run"] is True
        assert "planned_groups" in result

    def test_optimize_invalid_column(self, fragmented_dataset):
        """Verify error raised for missing z-order column."""
        with DuckDBParquetHandler() as handler:
            with pytest.raises(ValueError, match="Missing z-order columns"):
                handler.optimize_parquet_dataset(
                    path=str(fragmented_dataset),
                    zorder_columns=["nonexistent"],
                )

    def test_compact_invalid_threshold(self, fragmented_dataset):
        """Verify invalid threshold raises error."""
        with DuckDBParquetHandler() as handler:
            with pytest.raises(ValueError):
                handler.compact_parquet_dataset(
                    path=str(fragmented_dataset),
                    target_mb_per_file=0,
                )

    def test_compact_stats_keys(self, fragmented_dataset):
        """Verify statistics contain canonical MaintenanceStats structure."""
        with DuckDBParquetHandler() as handler:
            result = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,
            )
            # Assert canonical stats structure from shared core
            canonical_keys = [
                "before_file_count",
                "after_file_count",
                "before_total_bytes",
                "after_total_bytes",
                "compacted_file_count",
                "rewritten_bytes",
                "compression_codec",
                "dry_run",
            ]
            for key in canonical_keys:
                assert key in result, f"Missing canonical key: {key}"

            # Validate types and basic constraints
            assert isinstance(result["before_file_count"], int)
            assert isinstance(result["after_file_count"], int)
            assert isinstance(result["before_total_bytes"], int)
            assert isinstance(result["after_total_bytes"], int)
            assert isinstance(result["compacted_file_count"], int)
            assert isinstance(result["rewritten_bytes"], int)
            assert isinstance(result["dry_run"], bool)

            # Verify logical consistency
            assert result["before_file_count"] >= 0
            assert result["after_file_count"] >= 0
            assert result["before_total_bytes"] >= 0
            assert result["after_total_bytes"] >= 0
            assert result["compacted_file_count"] >= 0
            assert result["rewritten_bytes"] >= 0

    def test_optimize_stats_keys(self, fragmented_dataset):
        """Verify optimize statistics contain canonical MaintenanceStats structure."""
        with DuckDBParquetHandler() as handler:
            result = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
            )
            # Assert canonical stats structure from shared core
            canonical_keys = [
                "before_file_count",
                "after_file_count",
                "before_total_bytes",
                "after_total_bytes",
                "compacted_file_count",
                "rewritten_bytes",
                "compression_codec",
                "dry_run",
                "zorder_columns",
            ]
            for key in canonical_keys:
                assert key in result, f"Missing canonical key: {key}"

            # Validate optimization-specific fields
            assert isinstance(result["zorder_columns"], list)
            assert result["zorder_columns"] == ["group", "id"]
            assert isinstance(result["compression_codec"], str)

            # Verify logical consistency
            assert result["before_file_count"] >= 0
            assert result["after_file_count"] >= 0
            assert result["before_total_bytes"] >= 0
            assert result["after_total_bytes"] >= 0
            assert result["compacted_file_count"] >= 0
            assert result["rewritten_bytes"] >= 0
            assert isinstance(result["dry_run"], bool)

    def test_compact_dry_run_stats_structure(self, fragmented_dataset):
        """Verify dry run includes planned_groups in canonical structure."""
        with DuckDBParquetHandler() as handler:
            result = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,
                dry_run=True,
            )
            # Dry run should include planning metadata
            assert result["dry_run"] is True
            assert "planned_groups" in result
            assert isinstance(result["planned_groups"], list)

            # Should have canonical structure
            canonical_keys = [
                "before_file_count",
                "after_file_count",
                "before_total_bytes",
                "after_total_bytes",
                "compacted_file_count",
                "rewritten_bytes",
                "compression_codec",
                "dry_run",
                "planned_groups",
            ]
            for key in canonical_keys:
                assert key in result, f"Missing canonical key in dry run: {key}"

    def test_optimize_dry_run_stats_structure(self, fragmented_dataset):
        """Verify optimization dry run includes all required fields."""
        with DuckDBParquetHandler() as handler:
            result = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
                dry_run=True,
            )
            # Dry run should include planning metadata
            assert result["dry_run"] is True
            assert "planned_groups" in result
            assert isinstance(result["planned_groups"], list)
            assert result["zorder_columns"] == ["group", "id"]

            # Should have complete canonical structure
            canonical_keys = [
                "before_file_count",
                "after_file_count",
                "before_total_bytes",
                "after_total_bytes",
                "compacted_file_count",
                "rewritten_bytes",
                "compression_codec",
                "dry_run",
                "zorder_columns",
                "planned_groups",
            ]
            for key in canonical_keys:
                assert key in result, f"Missing canonical key in optimization dry run: {key}"

    def test_compact_empty_dataset_error(self, temp_dir):
        """Verify error on empty dataset path (no parquet files)."""
        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()
        with DuckDBParquetHandler() as handler:
            with pytest.raises(FileNotFoundError):
                handler.compact_parquet_dataset(
                    path=str(empty_dir),
                    target_mb_per_file=1,
                )

    def test_optimize_recompression(self, fragmented_dataset):
        """Verify optimization with custom compression codec succeeds."""
        with DuckDBParquetHandler() as handler:
            result = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
                compression="zstd",
            )
            assert result["compression_codec"] == "zstd"

    def test_compact_nonexistent_dataset_error(self, temp_dir):
        """Test compaction error on non-existent dataset path."""
        nonexistent_dir = temp_dir / "nonexistent"

        with DuckDBParquetHandler() as handler:
            with pytest.raises(FileNotFoundError, match="Dataset path.*does not exist"):
                handler.compact_parquet_dataset(
                    path=str(nonexistent_dir),
                    target_mb_per_file=1,
                )

    def test_optimize_nonexistent_dataset_error(self, temp_dir):
        """Test optimization error on non-existent dataset path."""
        nonexistent_dir = temp_dir / "nonexistent"

        with DuckDBParquetHandler() as handler:
            with pytest.raises(FileNotFoundError, match="Dataset path.*does not exist"):
                handler.optimize_parquet_dataset(
                    path=str(nonexistent_dir),
                    zorder_columns=["id"],
                )

    def test_compact_partition_filter_no_files_error(
        self, fragmented_dataset, temp_dir
    ):
        """Test compaction error when partition filter excludes all files."""
        with DuckDBParquetHandler() as handler:
            with pytest.raises(
                FileNotFoundError, match="No parquet files found.*matching filter"
            ):
                handler.compact_parquet_dataset(
                    path=str(fragmented_dataset),
                    target_mb_per_file=1,  # Add required parameter
                    partition_filter=["nonexistent_partition"],
                )

    def test_optimize_partition_filter_no_files_error(
        self, fragmented_dataset, temp_dir
    ):
        """Test optimization error when partition filter excludes all files."""
        with DuckDBParquetHandler() as handler:
            with pytest.raises(
                FileNotFoundError, match="No parquet files found.*matching filter"
            ):
                handler.optimize_parquet_dataset(
                    path=str(fragmented_dataset),
                    zorder_columns=["id"],
                    partition_filter=["nonexistent_partition"],
                )

    def test_compact_recompression(self, fragmented_dataset):
        """Verify compaction with custom compression codec succeeds."""
        with DuckDBParquetHandler() as handler:
            result = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,
                compression="zstd",
            )
            assert result["compression_codec"] == "zstd"

    def test_optimize_compaction_combined(self, fragmented_dataset):
        """Verify optimize with row chunking acts like compaction + clustering."""
        with DuckDBParquetHandler() as handler:
            stats_before = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
                dry_run=True,
                target_rows_per_file=150,
            )
            after = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
                target_rows_per_file=150,
            )
            assert after["dry_run"] is False
            assert after["after_file_count"] == stats_before["after_file_count"]
            # Verify ordering by both columns (group then id non-decreasing within group)
            table = handler.read_parquet(str(fragmented_dataset))
            groups = table.column("group").to_pylist()
            ids = table.column("id").to_pylist()
            assert groups == sorted(groups)
            last_group = None
            last_id = -1
            for g, i in zip(groups, ids):
                if last_group is None or g != last_group:
                    last_group = g
                    last_id = -1
                assert i >= last_id
                last_id = i

    def test_partition_filter_limits_scope(self, fragmented_dataset):
        """Verify partition_filter restricts optimize scope to matching files."""
        # Create simple partitions by moving some files under prefixes
        # We'll simulate partitions like group=0/, group=1/
        for p in Path(fragmented_dataset).glob("part_0.parquet"):
            new_dir = Path(fragmented_dataset) / "group=0"
            new_dir.mkdir(exist_ok=True)
            p.rename(new_dir / p.name)
        for p in Path(fragmented_dataset).glob("part_1.parquet"):
            new_dir = Path(fragmented_dataset) / "group=1"
            new_dir.mkdir(exist_ok=True)
            p.rename(new_dir / p.name)
        with DuckDBParquetHandler() as handler:
            stats_all = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
                dry_run=True,
            )
            stats_subset = handler.optimize_parquet_dataset(
                path=str(fragmented_dataset),
                zorder_columns=["group", "id"],
                dry_run=True,
                partition_filter=["group=0/"],
            )
            assert stats_subset["before_file_count"] < stats_all["before_file_count"]
            assert (
                stats_subset["compacted_file_count"]
                <= stats_subset["before_file_count"]
            )

    def test_compact_noop_behavior(self, fragmented_dataset):
        """Verify compaction returns unchanged stats when no groups form."""
        with DuckDBParquetHandler() as handler:
            # Set very small thresholds so each file exceeds size threshold (simulate no grouping)
            result = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,  # minimal 1MB threshold; expect limited grouping
            )
            # With extremely low threshold groups may still form; ensure file count not increased
            assert result["after_file_count"] <= result["before_file_count"]

    def test_stats_integrity(self, fragmented_dataset):
        """Verify stats correctness for compaction operation."""
        with DuckDBParquetHandler() as handler:
            dry = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,
                dry_run=True,
            )
            live = handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,
            )
            assert live["before_file_count"] == dry["before_file_count"]
            assert live["before_total_bytes"] == dry["before_total_bytes"]
            assert (
                live["after_total_bytes"] <= live["before_total_bytes"]
            )  # can shrink slightly
            assert live["rewritten_bytes"] <= live["before_total_bytes"]

    def test_query_performance_proxy(self, fragmented_dataset):
        """Approximate performance improvement: fewer files scanned after compaction."""
        with DuckDBParquetHandler() as handler:
            before_files = list(Path(fragmented_dataset).glob("*.parquet"))
            handler.compact_parquet_dataset(
                path=str(fragmented_dataset),
                target_mb_per_file=1,
            )
            after_files = list(Path(fragmented_dataset).glob("*.parquet"))
            # Simple proxy: fewer files implies fewer metadata reads
            assert len(after_files) <= len(before_files)


class TestUnregisterDuckDBTableSafely:
    """Tests for _unregister_duckdb_table_safely helper function."""

    @pytest.mark.skipif(
        not _DUCKDB_AVAILABLE,
        reason="duckdb not installed"
    )
    def test_successful_unregistration(self, temp_dir):
        """Test successful table unregistration."""
        from fsspeckit.datasets.duckdb.helpers import _unregister_duckdb_table_safely

        with DuckDBParquetHandler() as handler:
            # Create a test table
            test_table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            handler.write_parquet(test_table, str(temp_dir / "test.parquet"))

            # Register it
            handler._connection.register("test_table", str(temp_dir / "test.parquet"))

            # Verify it exists
            result = handler._connection.execute("SELECT * FROM test_table").fetchall()
            assert len(result) == 3

            # Unregister successfully
            _unregister_duckdb_table_safely(handler._connection, "test_table")

            # Verify it's gone (should raise if accessed)
            with pytest.raises(Exception):
                handler._connection.execute("SELECT * FROM test_table").fetchall()

    @pytest.mark.skipif(
        not _DUCKDB_AVAILABLE,
        reason="duckdb not installed"
    )
    def test_catalog_exception_logging(self, temp_dir, caplog):
        """Test that CatalogException is logged as warning and doesn't raise."""
        from fsspeckit.datasets.duckdb.helpers import _unregister_duckdb_table_safely

        with DuckDBParquetHandler() as handler:
            # Try to unregister a non-existent table (should raise CatalogException)
            _unregister_duckdb_table_safely(handler._connection, "nonexistent_table")

            # Verify warning was logged
            assert len(caplog.records) == 1
            assert caplog.records[0].levelname == "WARNING"
            assert "Failed to unregister DuckDB table" in caplog.records[0].message
            assert "nonexistent_table" in caplog.records[0].message

    @pytest.mark.skipif(
        not _DUCKDB_AVAILABLE,
        reason="duckdb not installed"
    )
    def test_connection_exception_logging(self, temp_dir, caplog):
        """Test that ConnectionException is logged as warning and doesn't raise."""
        from fsspeckit.datasets.duckdb.helpers import _unregister_duckdb_table_safely

        with DuckDBParquetHandler() as handler:
            # Try to unregister with invalid connection state
            # We'll close the connection first to trigger ConnectionException
            handler.close()

            # Try to unregister a table (should raise ConnectionException)
            _unregister_duckdb_table_safely(handler._connection, "any_table")

            # Verify warning was logged
            assert len(caplog.records) == 1
            assert caplog.records[0].levelname == "WARNING"
            assert "Failed to unregister DuckDB table" in caplog.records[0].message

    @pytest.mark.skipif(
        not _DUCKDB_AVAILABLE,
        reason="duckdb not installed"
    )
    def test_cleanup_continues_despite_errors(self, temp_dir, caplog):
        """Test that cleanup continues even when unregistration fails."""
        from fsspeckit.datasets.duckdb.helpers import _unregister_duckdb_table_safely

        with DuckDBParquetHandler() as handler:
            # Try to unregister multiple non-existent tables
            _unregister_duckdb_table_safely(handler._connection, "table1")
            _unregister_duckdb_table_safely(handler._connection, "table2")
            _unregister_duckdb_table_safely(handler._connection, "table3")

            # Verify all attempts were logged as warnings
            assert len(caplog.records) == 3
            for record in caplog.records:
                assert record.levelname == "WARNING"
                assert "Failed to unregister DuckDB table" in record.message

            # Verify no exceptions were raised
            # (If exceptions were raised, this test would fail before reaching this point)

    def test_all_duckdb_modules_use_canonical_helper(self):
        """Verify that DuckDB modules import and use the canonical helper."""
        from fsspeckit.datasets.duckdb import connection, dataset

        # Verify both modules import the canonical helper
        assert hasattr(connection, '_unregister_duckdb_table_safely')
        assert hasattr(dataset, '_unregister_duckdb_table_safely')

        # Verify it's the same function (not duplicates)
        import fsspeckit.datasets.duckdb.helpers as helpers
        assert connection._unregister_duckdb_table_safely is helpers._unregister_duckdb_table_safely
        assert dataset._unregister_duckdb_table_safely is helpers._unregister_duckdb_table_safely


class TestDuckDBMergeAwareWrite:
    """Tests for merge-aware write functionality in DuckDB dataset handler."""

    @pytest.fixture
    def handler(self):
        """Create a DuckDB handler for testing."""
        handler = DuckDBParquetHandler()
        yield handler
        handler.close()

    @pytest.fixture
    def base_table(self):
        """Create a base table for merge tests."""
        return pa.table({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })

    @pytest.fixture
    def update_table(self):
        """Create an update table for merge tests."""
        return pa.table({
            "id": [2, 3, 4],
            "value": ["b_updated", "c_updated", "d_new"],
        })

    def test_write_without_strategy_backward_compatible(self, handler, base_table, temp_dir):
        """Test that write without strategy works as before (backward compatible)."""
        output_path = str(temp_dir / "dataset")

        # Write without strategy - should work as before
        result = handler.write_parquet_dataset(base_table, output_path)

        # Should return None for standard write
        assert result is None

        # Verify data was written
        read_back = handler.read_parquet(f"{output_path}/*.parquet")
        assert read_back.num_rows == 3

    def test_write_with_upsert_strategy(self, handler, base_table, update_table, temp_dir):
        """Test upsert strategy via write_parquet_dataset."""
        output_path = str(temp_dir / "dataset")

        # Write initial data
        handler.write_parquet_dataset(base_table, output_path)

        # Upsert with new and updated records
        stats = handler.write_parquet_dataset(
            update_table,
            output_path,
            strategy="upsert",
            key_columns=["id"],
        )

        # Should return MergeStats
        assert stats is not None

        # Verify merged result
        read_back = handler.read_parquet(f"{output_path}/*.parquet")
        # Should have: id=1 (original), id=2,3 (updated), id=4 (new)
        assert read_back.num_rows >= 3  # At least the source rows

    def test_write_with_insert_strategy(self, handler, base_table, update_table, temp_dir):
        """Test insert-only strategy via write_parquet_dataset."""
        output_path = str(temp_dir / "dataset")

        # Write initial data
        handler.write_parquet_dataset(base_table, output_path)

        # Insert only - should only add id=4
        stats = handler.write_parquet_dataset(
            update_table,
            output_path,
            strategy="insert",
            key_columns=["id"],
        )

        assert stats is not None

    def test_write_with_update_strategy(self, handler, base_table, update_table, temp_dir):
        """Test update-only strategy via write_parquet_dataset."""
        output_path = str(temp_dir / "dataset")

        # Write initial data
        handler.write_parquet_dataset(base_table, output_path)

        # Update only - should update id=2,3, ignore id=4
        stats = handler.write_parquet_dataset(
            update_table,
            output_path,
            strategy="update",
            key_columns=["id"],
        )

        assert stats is not None

    def test_write_with_deduplicate_strategy(self, handler, temp_dir):
        """Test deduplication strategy via write_parquet_dataset."""
        output_path = str(temp_dir / "dataset")

        # Data with duplicates
        data_with_dupes = pa.table({
            "id": [1, 1, 2, 2, 3],
            "value": ["a1", "a2", "b1", "b2", "c"],
        })

        # Deduplicate on write
        stats = handler.write_parquet_dataset(
            data_with_dupes,
            output_path,
            strategy="deduplicate",
            key_columns=["id"],
        )

        assert stats is not None

    def test_write_with_full_merge_strategy(self, handler, base_table, update_table, temp_dir):
        """Test full_merge strategy via write_parquet_dataset."""
        output_path = str(temp_dir / "dataset")

        # Write initial data
        handler.write_parquet_dataset(base_table, output_path)

        # Full merge - replaces with source
        stats = handler.write_parquet_dataset(
            update_table,
            output_path,
            strategy="full_merge",
        )

        assert stats is not None

    def test_write_invalid_strategy_raises_error(self, handler, base_table, temp_dir):
        """Test that invalid strategy raises ValueError."""
        output_path = str(temp_dir / "dataset")

        with pytest.raises(ValueError, match="Invalid strategy"):
            handler.write_parquet_dataset(
                base_table,
                output_path,
                strategy="invalid_strategy",
            )

    def test_insert_dataset_helper(self, handler, base_table, update_table, temp_dir):
        """Test insert_dataset convenience helper."""
        output_path = str(temp_dir / "dataset")

        # Write initial data
        handler.write_parquet_dataset(base_table, output_path)

        # Use helper
        stats = handler.insert_dataset(
            update_table,
            output_path,
            key_columns=["id"],
        )

        assert stats is not None

    def test_upsert_dataset_helper(self, handler, base_table, update_table, temp_dir):
        """Test upsert_dataset convenience helper."""
        output_path = str(temp_dir / "dataset")

        # Write initial data
        handler.write_parquet_dataset(base_table, output_path)

        # Use helper
        stats = handler.upsert_dataset(
            update_table,
            output_path,
            key_columns=["id"],
        )

        assert stats is not None

    def test_update_dataset_helper(self, handler, base_table, update_table, temp_dir):
        """Test update_dataset convenience helper."""
        output_path = str(temp_dir / "dataset")

        # Write initial data
        handler.write_parquet_dataset(base_table, output_path)

        # Use helper
        stats = handler.update_dataset(
            update_table,
            output_path,
            key_columns=["id"],
        )

        assert stats is not None

    def test_deduplicate_dataset_helper(self, handler, temp_dir):
        """Test deduplicate_dataset convenience helper."""
        output_path = str(temp_dir / "dataset")

        data_with_dupes = pa.table({
            "id": [1, 1, 2],
            "value": ["a", "b", "c"],
        })

        # Use helper
        stats = handler.deduplicate_dataset(
            data_with_dupes,
            output_path,
            key_columns=["id"],
        )

        assert stats is not None

    def test_insert_dataset_requires_key_columns(self, handler, base_table, temp_dir):
        """Test that insert_dataset requires key_columns."""
        output_path = str(temp_dir / "dataset")

        with pytest.raises(ValueError, match="key_columns is required"):
            handler.insert_dataset(base_table, output_path, key_columns=None)

    def test_upsert_dataset_requires_key_columns(self, handler, base_table, temp_dir):
        """Test that upsert_dataset requires key_columns."""
        output_path = str(temp_dir / "dataset")

        with pytest.raises(ValueError, match="key_columns is required"):
            handler.upsert_dataset(base_table, output_path, key_columns=None)

    def test_update_dataset_requires_key_columns(self, handler, base_table, temp_dir):
        """Test that update_dataset requires key_columns."""
        output_path = str(temp_dir / "dataset")

        with pytest.raises(ValueError, match="key_columns is required"):
            handler.update_dataset(base_table, output_path, key_columns=None)

    def test_deduplicate_dataset_optional_key_columns(self, handler, temp_dir):
        """Test that deduplicate_dataset works without key_columns (exact dedup)."""
        output_path = str(temp_dir / "dataset")

        data_with_dupes = pa.table({
            "id": [1, 1, 2],
            "value": ["a", "a", "b"],  # First two rows are exact duplicates
        })

        # Should work without key_columns
        stats = handler.deduplicate_dataset(
            data_with_dupes,
            output_path,
            key_columns=None,
        )

        assert stats is not None

    def test_upsert_creates_new_dataset_if_target_missing(self, handler, base_table, temp_dir):
        """Test that upsert/insert creates new dataset if target doesn't exist."""
        output_path = str(temp_dir / "new_dataset")

        # No existing dataset - should create one
        result = handler.upsert_dataset(
            base_table,
            output_path,
            key_columns=["id"],
        )

        # Verify data was written
        read_back = handler.read_parquet(f"{output_path}/*.parquet")
        assert read_back.num_rows == 3
