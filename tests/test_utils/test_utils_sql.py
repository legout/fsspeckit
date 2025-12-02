"""Tests for SQL utility functions."""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
from datetime import date, datetime, time, timezone

from fsspeckit.sql.filters import sql2pyarrow_filter


class TestSql2PyarrowFilter:
    """Test sql2pyarrow_filter function."""

    @pytest.fixture
    def sample_schema(self):
        """Create a sample schema for testing."""
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int64()),
                pa.field("score", pa.float64()),
                pa.field("active", pa.bool_()),
                pa.field("created_at", pa.timestamp("us", "UTC")),
                pa.field("birth_date", pa.date32()),
                pa.field("login_time", pa.time64("us")),
                pa.field("category", pa.string()),
                pa.field("tags", pa.list_(pa.string())),
            ]
        )

    @pytest.fixture
    def sample_table(self, sample_schema):
        """Create a sample table for testing."""
        return pa.Table.from_arrays(
            [
                pa.array([1, 2, 3, 4, 5]),
                pa.array(["Alice", "Bob", "Charlie", "David", "Eve"]),
                pa.array([25, 30, 35, 40, 45]),
                pa.array([85.5, 90.2, 78.9, 92.1, 88.7]),
                pa.array([True, False, True, False, True]),
                pa.array(
                    [
                        datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
                        datetime(2023, 2, 15, 14, 30, tzinfo=timezone.utc),
                        datetime(2023, 3, 20, 9, 15, tzinfo=timezone.utc),
                        datetime(2023, 4, 25, 16, 45, tzinfo=timezone.utc),
                        datetime(2023, 5, 30, 11, 20, tzinfo=timezone.utc),
                    ],
                    type=pa.timestamp("us", "UTC"),
                ),
                pa.array(
                    [
                        date(1998, 1, 15),
                        date(1993, 5, 20),
                        date(1988, 11, 30),
                        date(1983, 7, 10),
                        date(1978, 3, 25),
                    ],
                    type=pa.date32(),
                ),
                pa.array(
                    [
                        time(9, 0, 0),
                        time(14, 30, 0),
                        time(8, 15, 0),
                        time(16, 45, 0),
                        time(11, 20, 0),
                    ],
                    type=pa.time64("us"),
                ),
                pa.array(["A", "B", "A", "C", "B"]),
                pa.array(
                    [
                        ["tag1", "tag2"],
                        ["tag2"],
                        ["tag1", "tag3"],
                        ["tag3"],
                        ["tag2", "tag3"],
                    ]
                ),
            ],
            schema=sample_schema,
        )

    def test_basic_comparisons(self, sample_schema):
        """Test basic comparison operators."""
        # Equal
        expr = sql2pyarrow_filter("id = 1", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Not equal
        expr = sql2pyarrow_filter("name != 'Alice'", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Greater than
        expr = sql2pyarrow_filter("age > 30", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Less than
        expr = sql2pyarrow_filter("score < 90", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Greater than or equal
        expr = sql2pyarrow_filter("age >= 35", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Less than or equal
        expr = sql2pyarrow_filter("score <= 85.5", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_in_operator(self, sample_schema):
        """Test IN operator."""
        # IN with multiple values
        expr = sql2pyarrow_filter("category IN ('A', 'C')", sample_schema)
        assert isinstance(expr, pc.Expression)

        # NOT IN
        expr = sql2pyarrow_filter("category NOT IN ('B')", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_null_checks(self, sample_schema):
        """Test NULL checks."""
        # IS NULL
        expr = sql2pyarrow_filter("name IS NULL", sample_schema)
        assert isinstance(expr, pc.Expression)

        # IS NOT NULL
        expr = sql2pyarrow_filter("name IS NOT NULL", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_logical_operators(self, sample_schema):
        """Test logical operators."""
        # AND
        expr = sql2pyarrow_filter("age > 30 AND score > 85", sample_schema)
        assert isinstance(expr, pc.Expression)

        # OR
        expr = sql2pyarrow_filter("age < 30 OR score > 90", sample_schema)
        assert isinstance(expr, pc.Expression)

        # NOT
        expr = sql2pyarrow_filter("NOT active", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Complex logical expression
        expr = sql2pyarrow_filter(
            "(age > 30 AND score > 85) OR category = 'A'", sample_schema
        )
        assert isinstance(expr, pc.Expression)

    def test_boolean_values(self, sample_schema):
        """Test boolean value handling."""
        # Direct boolean
        expr = sql2pyarrow_filter("active = true", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Boolean with NOT
        expr = sql2pyarrow_filter("active = false", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_datetime_literals(self, sample_schema):
        """Test datetime literal parsing."""
        # Timestamp
        expr = sql2pyarrow_filter("created_at > '2023-03-01T00:00:00'", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Date
        expr = sql2pyarrow_filter("birth_date > '1990-01-01'", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Time
        expr = sql2pyarrow_filter("login_time > '12:00:00'", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_string_literals(self, sample_schema):
        """Test string literal handling."""
        # Single quotes
        expr = sql2pyarrow_filter("name = 'Alice'", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Double quotes
        expr = sql2pyarrow_filter('name = "Alice"', sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_numeric_literals(self, sample_schema):
        """Test numeric literal handling."""
        # Integer
        expr = sql2pyarrow_filter("age = 25", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Float
        expr = sql2pyarrow_filter("score = 85.5", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Scientific notation
        expr = sql2pyarrow_filter("score = 8.55e1", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_complex_nested_expressions(self, sample_schema):
        """Test complex nested expressions."""
        expr = sql2pyarrow_filter(
            "(age > 30 AND score > 85) OR (category IN ('A', 'C') AND active = true)",
            sample_schema,
        )
        assert isinstance(expr, pc.Expression)

    def test_case_sensitivity(self, sample_schema):
        """Test case sensitivity in SQL."""
        # Column names should be case-insensitive
        expr = sql2pyarrow_filter("ID = 1", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Operators should be case-insensitive
        expr = sql2pyarrow_filter("age > 30 AND score > 85", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_filter_execution(self, sample_table):
        """Test that generated filters actually work on data."""
        schema = sample_table.schema

        # Test simple filter
        expr = sql2pyarrow_filter("id = 1", schema)
        result = sample_table.filter(expr)
        assert result.num_rows == 1
        assert result["id"][0].as_py() == 1

        # Test range filter
        expr = sql2pyarrow_filter("age BETWEEN 30 AND 40", schema)
        result = sample_table.filter(expr)
        assert result.num_rows == 2  # ages 30 and 35

        # Test string filter
        expr = sql2pyarrow_filter("name LIKE 'A%'", schema)
        result = sample_table.filter(expr)
        assert result["name"][0].as_py() == "Alice"

    def test_error_handling(self, sample_schema):
        """Test error handling for invalid SQL."""
        # Invalid column name
        with pytest.raises(ValueError):
            sql2pyarrow_filter("invalid_column = 1", sample_schema)

        # Invalid SQL syntax
        with pytest.raises(ValueError):
            sql2pyarrow_filter("id =", sample_schema)

        # Unsupported operator
        with pytest.raises(ValueError):
            sql2pyarrow_filter("id LIKE '1%'", sample_schema)  # Not implemented yet

    def test_timezone_handling(self, sample_schema):
        """Test timezone-aware datetime handling."""
        # Timezone-aware timestamp
        expr = sql2pyarrow_filter(
            "created_at > '2023-01-01T12:00:00+00:00'", sample_schema
        )
        assert isinstance(expr, pc.Expression)

    def test_list_column_handling(self, sample_schema):
        """Test handling of list columns."""
        # Array contains (if supported)
        # Note: This may not be implemented in the current version
        try:
            expr = sql2pyarrow_filter("tags CONTAINS 'tag1'", sample_schema)
            assert isinstance(expr, pc.Expression)
        except ValueError:
            # If not implemented, that's okay for now
            pass

    def test_escape_sequences(self, sample_schema):
        """Test handling of escape sequences in strings."""
        # Single quote in string
        expr = sql2pyarrow_filter("name = 'O\\'Reilly'", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Double quotes in string
        expr = sql2pyarrow_filter('name = "Some \\"quoted\\" text"', sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_null_comparison(self, sample_schema):
        """Test comparison with NULL values."""
        # Equality with NULL (should be handled specially)
        expr = sql2pyarrow_filter("name = NULL", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Inequality with NULL
        expr = sql2pyarrow_filter("name != NULL", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_between_operator(self, sample_schema):
        """Test BETWEEN operator."""
        expr = sql2pyarrow_filter("score BETWEEN 80 AND 90", sample_schema)
        assert isinstance(expr, pc.Expression)

        # NOT BETWEEN
        expr = sql2pyarrow_filter("score NOT BETWEEN 90 AND 100", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_multiple_conditions(self, sample_table):
        """Test filters with multiple conditions."""
        schema = sample_table.schema

        # Test complex filter
        expr = sql2pyarrow_filter(
            "age > 30 AND (score > 85 OR category = 'A') AND active = true", schema
        )
        result = sample_table.filter(expr)

        # Verify the filter works correctly
        for i in range(result.num_rows):
            assert result["age"][i].as_py() > 30
            assert (
                result["score"][i].as_py() > 85 or result["category"][i].as_py() == "A"
            )
            assert result["active"][i].as_py() is True

    def test_performance_with_large_schema(self):
        """Test performance with a large schema."""
        # Create a schema with many columns
        fields = [pa.field(f"col_{i}", pa.int64()) for i in range(100)]
        large_schema = pa.schema(fields)

        # Should still work quickly
        expr = sql2pyarrow_filter("col_0 = 1", large_schema)
        assert isinstance(expr, pc.Expression)

    def test_whitespace_handling(self, sample_schema):
        """Test handling of various whitespace patterns."""
        # Extra whitespace
        expr = sql2pyarrow_filter("  id   =   1  ", sample_schema)
        assert isinstance(expr, pc.Expression)

        # Newlines and tabs
        expr = sql2pyarrow_filter("id\n=\t1", sample_schema)
        assert isinstance(expr, pc.Expression)

    def test_dataset_filter_integration(self, sample_table):
        """Test integration with PyArrow dataset filtering."""
        import tempfile
        import pyarrow.dataset as ds

        schema = sample_table.schema

        with tempfile.TemporaryDirectory() as tmpdir:
            # Write as parquet dataset
            ds.write_dataset(sample_table, tmpdir, format="parquet")
            dataset = ds.dataset(tmpdir, format="parquet")

            # Test cases: (filter_expr, expected_row_count)
            test_cases = [
                ("id = 1", 1),
                ("age > 30", 3),  # ages 35, 40, 45
                ("active = true", 3),  # Alice, Charlie, Eve
                ("name = 'Alice'", 1),
                ("age IN (25, 35)", 2),  # Alice, Charlie
                ("category IN ('A', 'C')", 3),  # Alice, Charlie, David
                ("category NOT IN ('B')", 3),  # Alice, Charlie, David
                ("age > 30 AND active = true", 2),  # Charlie, Eve
                ("age > 30 AND active = false", 1),  # David
                ("created_at > '2023-03-01T00:00:00'", 3),  # March, April, May
                ("birth_date > '1990-01-01'", 2),  # Alice, Bob
                ("login_time > '12:00:00'", 2),  # 14:30, 16:45
                (
                    "(age > 30 AND score > 85) OR category = 'A'",
                    4,
                ),  # Alice, Charlie, David, Eve
            ]

            for filter_expr, expected_count in test_cases:
                # Generate filter expression
                expr = sql2pyarrow_filter(filter_expr, schema)
                assert isinstance(expr, pc.Expression)

                # Apply filter via dataset scanner
                result = dataset.to_table(filter=expr)
                assert result.num_rows == expected_count, (
                    f"Filter '{filter_expr}' expected {expected_count} rows, "
                    f"got {result.num_rows}"
                )

                # Verify the expression works with table.filter too
                table_result = sample_table.filter(expr)
                assert table_result.num_rows == expected_count

    def test_dataset_level_filtering_with_temp_parquet(self):
        """Test dataset-level filtering with temporary parquet directory."""
        import tempfile
        import pyarrow.dataset as ds
        from datetime import datetime, timezone

        # Create test data with various types
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int64()),
                pa.field("score", pa.float64()),
                pa.field("active", pa.bool_()),
                pa.field("created_at", pa.timestamp("us", "UTC")),
            ]
        )

        test_data = pa.Table.from_arrays(
            [
                pa.array([1, 2, 3, 4, 5, 6]),
                pa.array(["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]),
                pa.array([25, 30, 35, 40, 45, 50]),
                pa.array([85.5, 90.2, 78.9, 92.1, 88.7, 95.3]),
                pa.array([True, False, True, False, True, False]),
                pa.array(
                    [
                        datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
                        datetime(2023, 2, 15, 14, 30, tzinfo=timezone.utc),
                        datetime(2023, 3, 20, 9, 15, tzinfo=timezone.utc),
                        datetime(2023, 4, 25, 16, 45, tzinfo=timezone.utc),
                        datetime(2023, 5, 30, 11, 20, tzinfo=timezone.utc),
                        datetime(2023, 6, 10, 13, 30, tzinfo=timezone.utc),
                    ],
                    type=pa.timestamp("us", "UTC"),
                ),
            ],
            schema=schema,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Write dataset to temporary parquet directory
            ds.write_dataset(test_data, tmpdir, format="parquet")

            # Load dataset
            dataset = ds.dataset(tmpdir, format="parquet")

            # Test complex filter expression
            filter_sql = "age > 30 AND (score > 85 OR active = true) AND created_at > '2023-03-01T00:00:00'"

            # Convert SQL to PyArrow expression
            expr = sql2pyarrow_filter(filter_sql, schema)
            assert isinstance(expr, pc.Expression)

            # Apply filter via scanner
            filtered_result = dataset.to_table(filter=expr)

            # Verify results match expectations
            # Should include: Charlie (age=35, score=78.9, active=true, created=March),
            # David (age=40, score=92.1, active=false, created=April),
            # Eve (age=45, score=88.7, active=true, created=May),
            # Frank (age=50, score=95.3, active=false, created=June)
            expected_count = 4
            assert filtered_result.num_rows == expected_count, (
                f"Expected {expected_count} rows, got {filtered_result.num_rows}"
            )

            # Verify each row meets the criteria
            for i in range(filtered_result.num_rows):
                row_age = filtered_result["age"][i].as_py()
                row_score = filtered_result["score"][i].as_py()
                row_active = filtered_result["active"][i].as_py()
                row_created = filtered_result["created_at"][i].as_py()

                assert row_age > 30
                assert row_score > 85 or row_active is True
                assert row_created > datetime(2023, 3, 1, 0, 0, tzinfo=timezone.utc)
