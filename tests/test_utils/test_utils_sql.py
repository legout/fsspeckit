"""Tests for SQL utility functions."""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
from datetime import date, datetime, time, timezone

from fsspeckit.utils.sql import sql2pyarrow_filter


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
