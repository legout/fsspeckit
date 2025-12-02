"""Tests for datetime utility functions."""

import pytest
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import polars as pl
import pyarrow as pa
import pandas as pd

from fsspeckit.common.datetime import (
    get_timestamp_column,
    get_timedelta_str,
    timestamp_from_string,
)


class TestGetTimestampColumn:
    """Test get_timestamp_column function."""

    def test_polars_dataframe(self):
        """Test with Polars DataFrame."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "created_at": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
                "updated_at": [
                    datetime(2023, 1, 4, 12, 0),
                    datetime(2023, 1, 5, 12, 0),
                    datetime(2023, 1, 6, 12, 0),
                ],
                "name": ["a", "b", "c"],
            }
        )

        result = get_timestamp_column(df)
        assert sorted(result) == ["created_at", "updated_at"]

    def test_polars_lazyframe(self):
        """Test with Polars LazyFrame."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "created_at": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        ).lazy()

        result = get_timestamp_column(df)
        assert result == ["created_at"]

    def test_pyarrow_table(self):
        """Test with PyArrow Table."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "timestamp_col": pa.array(
                    [
                        datetime(2023, 1, 1, 12, 0),
                        datetime(2023, 1, 2, 12, 0),
                        datetime(2023, 1, 3, 12, 0),
                    ],
                    type=pa.timestamp("ns"),
                ),
            }
        )

        result = get_timestamp_column(table)
        assert result == ["timestamp_col"]

    def test_pandas_dataframe(self):
        """Test with Pandas DataFrame."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "date": pd.date_range("2023-01-01", periods=3),
            }
        )

        result = get_timestamp_column(df)
        assert len(result) >= 1
        assert "date" in result

    def test_no_timestamp_columns(self):
        """Test with no timestamp columns."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
            }
        )

        result = get_timestamp_column(df)
        assert result == []

    def test_case_sensitivity(self):
        """Test case sensitivity in column names."""
        df = pl.DataFrame(
            {
                "ID": [1, 2, 3],
                "TIMESTAMP": [
                    datetime(2023, 1, 1, 12, 0),
                    datetime(2023, 1, 2, 12, 0),
                    datetime(2023, 1, 3, 12, 0),
                ],
            }
        )

        result = get_timestamp_column(df)
        assert "TIMESTAMP" in result


class TestGetTimedeltaStr:
    """Test get_timedelta_str function."""

    def test_polars_durations(self):
        """Test with Polars duration strings."""
        assert get_timedelta_str("1h", to="polars") == "1h"
        assert get_timedelta_str("1d", to="polars") == "1d"
        assert get_timedelta_str("1w", to="polars") == "1w"
        assert get_timedelta_str("1mo", to="polars") == "1mo"
        assert get_timedelta_str("1y", to="polars") == "1y"

    def test_duckdb_durations(self):
        """Test with DuckDB duration strings."""
        assert "second" in get_timedelta_str("1s", to="duckdb")
        assert "minute" in get_timedelta_str("1m", to="duckdb")
        assert "hour" in get_timedelta_str("1h", to="duckdb")

    def test_invalid_unit(self):
        """Test with invalid unit."""
        # Should return the value + unit without 's'
        result = get_timedelta_str("1invalid", to="polars")
        assert result == "1 invalid"


class TestTimestampFromString:
    """Test timestamp_from_string function."""

    def test_iso_format(self):
        """Test ISO format timestamp."""
        result = timestamp_from_string("2023-12-31T23:59:59")
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 31
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59

    def test_with_timezone(self):
        """Test timestamp with timezone."""
        result = timestamp_from_string("2023-12-31T23:59:59+01:00")
        assert result.tzinfo is not None
        assert result.tzinfo.utcoffset(result) == timedelta(hours=1)

    def test_various_formats(self):
        """Test various date formats."""
        formats = [
            "2023-12-31",
            "2023-12-31T23:59:59",
            "20231231",
            "2023-12-31 23:59:59",
        ]

        for fmt in formats:
            result = timestamp_from_string(fmt)
            assert isinstance(result, datetime)

    def test_invalid_format(self):
        """Test invalid format handling."""
        with pytest.raises(ValueError):
            timestamp_from_string("invalid_date")
