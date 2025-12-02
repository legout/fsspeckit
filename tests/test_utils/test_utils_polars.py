"""Tests for polars utility functions."""

import pytest
import polars as pl
import pyarrow as pa
from datetime import datetime, timezone

from fsspeckit.common.polars import (
    opt_dtype,
    unnest_all,
    explode_all,
    with_row_count,
    with_datepart_columns,
    with_truncated_columns,
    with_strftime_columns,
    cast_relaxed,
    delta,
    partition_by,
    drop_null_columns,
    unify_schemas,
)


class TestOptDtype:
    """Test opt_dtype function for Polars DataFrames."""

    def test_basic_type_inference(self):
        """Test basic data type inference."""
        df = pl.DataFrame(
            {
                "int_col": ["1", "2", "3", "4"],
                "float_col": ["1.5", "2.5", "3.5", "4.5"],
                "bool_col": ["true", "false", "yes", "no"],
                "str_col": ["a", "b", "c", "d"],
            }
        )

        result = opt_dtype(df)

        assert result.schema["int_col"] == pl.Int64
        assert result.schema["float_col"] == pl.Float64
        assert result.schema["bool_col"] == pl.Boolean
        assert result.schema["str_col"] == pl.Utf8

    def test_datetime_parsing(self):
        """Test datetime parsing with various formats."""
        df = pl.DataFrame(
            {
                "iso_datetime": ["2023-12-31T23:59:59", "2024-01-01T00:00:00"],
                "us_date": ["12/31/2023", "01/01/2024"],
                "german_date": ["31.12.2023", "01.01.2024"],
                "compact": ["20231231", "20240101"],
                "with_tz": ["2023-12-31T23:59:59+01:00", "2024-01-01T00:00:00Z"],
            }
        )

        result = opt_dtype(df)

        assert result.schema["iso_datetime"] == pl.Datetime(
            time_unit="us", time_zone=None
        )
        assert result.schema["us_date"] == pl.Datetime(time_unit="us", time_zone=None)
        assert result.schema["german_date"] == pl.Datetime(
            time_unit="us", time_zone=None
        )
        assert result.schema["compact"] == pl.Datetime(time_unit="us", time_zone=None)
        assert result.schema["with_tz"] == pl.Datetime(time_unit="us", time_zone="UTC")

    def test_timezone_handling(self):
        """Test timezone parameter handling."""
        df = pl.DataFrame(
            {
                "datetime": ["2023-12-31T23:59:59", "2024-01-01T00:00:00"],
                "datetime_tz": ["2023-12-31T23:59:59+01:00", "2024-01-01T00:00:00Z"],
            }
        )

        # Test time_zone hint
        result = opt_dtype(df, time_zone="UTC")

        # Test force_timezone
        result_forced = opt_dtype(df, force_timezone="America/New_York")
        assert result_forced.schema["datetime"].time_zone == "America/New_York"
        assert result_forced.schema["datetime_tz"].time_zone == "America/New_York"

    def test_include_exclude_columns(self):
        """Test include and exclude parameters."""
        df = pl.DataFrame(
            {
                "col1": ["1", "2", "3"],
                "col2": ["1.5", "2.5", "3.5"],
                "col3": ["a", "b", "c"],
            }
        )

        # Test include
        result = opt_dtype(df, include=["col1", "col2"])
        assert result.schema["col1"] == pl.Int64
        assert result.schema["col2"] == pl.Float64
        assert result.schema["col3"] == pl.Utf8  # Unchanged

        # Test exclude
        result = opt_dtype(df, exclude=["col3"])
        assert result.schema["col1"] == pl.Int64
        assert result.schema["col2"] == pl.Float64
        assert result.schema["col3"] == pl.Utf8  # Unchanged

    def test_shrink_numerics(self):
        """Test numeric shrinking functionality."""
        df = pl.DataFrame(
            {
                "small_int": ["1", "2", "3"],
                "large_int": ["100000", "200000", "300000"],
                "small_float": ["1.1", "2.2", "3.3"],
            }
        )

        # With shrinking
        result = opt_dtype(df, shrink_numerics=True)
        assert result.schema["small_int"] == pl.UInt8
        assert result.schema["large_int"] == pl.UInt32
        assert result.schema["small_float"] == pl.Float32

        # Without shrinking
        result = opt_dtype(df, shrink_numerics=False)
        assert result.schema["small_int"] == pl.Int64
        assert result.schema["large_int"] == pl.Int64
        assert result.schema["small_float"] == pl.Float64

    def test_allow_unsigned(self):
        """Test unsigned integer type allowance."""
        df = pl.DataFrame(
            {
                "positive": ["1", "2", "3"],
                "mixed": ["-1", "0", "1"],
            }
        )

        # Allow unsigned with shrinking
        result = opt_dtype(df, allow_unsigned=True, shrink_numerics=True)
        assert result.schema["positive"] == pl.UInt8

        # Don't allow unsigned
        result = opt_dtype(df, allow_unsigned=False, shrink_numerics=True)
        assert result.schema["positive"] == pl.Int8
        assert result.schema["mixed"] == pl.Int8

    def test_null_handling(self):
        """Test null-like value handling."""
        df = pl.DataFrame(
            {
                "all_null": ["", "None", "null", "NaN"],
                "mixed_null": ["1", "", "2", "null"],
                "no_null": ["1", "2", "3", "4"],
            }
        )

        result = opt_dtype(df, allow_null=True)
        assert result.schema["all_null"] == pl.Null
        assert result.schema["mixed_null"] == pl.Int64
        assert result.schema["no_null"] == pl.Int64

        # Test with allow_null=False
        result = opt_dtype(df, allow_null=False)
        assert result.schema["all_null"] == pl.Utf8

    def test_strict_mode(self):
        """Test strict error handling."""
        df = pl.DataFrame(
            {
                "valid": ["1", "2", "3"],
                "invalid": ["1", "2", "invalid"],
            }
        )

        # Non-strict mode (default)
        result = opt_dtype(df, strict=False)
        assert result.schema["valid"] == pl.Int64
        assert result.schema["invalid"] == pl.Utf8  # Falls back to string

        # Strict mode
        with pytest.raises(Exception):
            opt_dtype(df, strict=True)

    def test_lazy_frame(self):
        """Test opt_dtype with LazyFrame."""
        df = pl.DataFrame(
            {
                "int_col": ["1", "2", "3"],
                "float_col": ["1.5", "2.5", "3.5"],
            }
        )

        lazy_df = df.lazy()
        result = opt_dtype(lazy_df)

        assert isinstance(result, pl.LazyFrame)
        result_collected = result.collect()
        assert result_collected.schema["int_col"] == pl.Int64
        assert result_collected.schema["float_col"] == pl.Float64

    def test_sample_inference_applied_to_full_column(self):
        """Sample-driven schema should dictate casting for entire column."""
        df = pl.DataFrame({"value": ["1", "2", "foo", "bar"]})
        result = opt_dtype(df, sample_size=2, sample_method="first")

        assert result.schema["value"] == pl.Int64
        assert result["value"].to_list() == [1, 2, None, None]

    def test_sampling_controls(self):
        """Ensure sampling parameters are accepted while keeping default inference."""
        df = pl.DataFrame({"value": ["1", "2", "3"]})
        first_sample = opt_dtype(df, sample_size=2, sample_method="first")
        assert first_sample.schema["value"] == pl.Int64

        random_sample = opt_dtype(df, sample_size=2, sample_method="random")
        assert random_sample.schema["value"] == pl.Int64

        no_sample = opt_dtype(df, sample_size=None)
        assert no_sample.schema["value"] == pl.Int64

    def test_sampling_invalid_method(self):
        """Invalid sampling strategy should raise early."""
        df = pl.DataFrame({"value": ["1"]})
        with pytest.raises(ValueError):
            opt_dtype(df, sample_method="invalid")


class TestExtensionMethods:
    """Test Polars DataFrame extension methods."""

    def test_unnest_all(self):
        """Test unnest_all method."""
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "data": [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            }
        )

        result = df.unnest_all()
        assert "data_a" in result.columns
        assert "data_b" in result.columns

    def test_explode_all(self):
        """Test explode_all method."""
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "items": [[1, 2], [3, 4, 5]],
            }
        )

        result = df.explode_all()
        assert result.shape[0] == 5  # 2 + 3 exploded rows

    def test_with_row_count_ext(self):
        """Test with_row_count_ext method."""
        df = pl.DataFrame(
            {
                "group": ["a", "a", "b", "b"],
                "value": [1, 2, 3, 4],
            }
        )

        # Without over
        result = df.with_row_count_ext()
        assert "row_nr" in result.columns
        assert result["row_nr"].to_list() == [1, 2, 3, 4]

        # With over
        result = df.with_row_count_ext(over="group")
        assert result["row_nr"].to_list() == [1, 2, 1, 2]

    def test_with_datepart_columns(self):
        """Test with_datepart_columns method."""
        df = pl.DataFrame(
            {
                "timestamp": ["2023-12-31T23:59:59", "2024-01-01T00:00:00"],
            }
        ).with_columns(pl.col("timestamp").str.to_datetime())

        result = df.with_datepart_columns(
            timestamp_column="timestamp",
            year=True,
            month=True,
            day=True,
            hour=True,
        )

        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" in result.columns
        assert "hour" in result.columns

    def test_with_strftime_columns(self):
        """Test with_strftime_columns method."""
        df = pl.DataFrame(
            {
                "timestamp": ["2023-12-31T23:59:59", "2024-01-01T00:00:00"],
            }
        ).with_columns(pl.col("timestamp").str.to_datetime())

        result = df.with_strftime_columns(
            timestamp_column="timestamp",
            strftime="%Y-%m-%d",
            column_names="date_str",
        )

        assert "date_str" in result.columns
        assert result["date_str"][0].strftime("%Y-%m-%d") == "2023-12-31"

    def test_cast_relaxed(self):
        """Test cast_relaxed method."""
        df1 = pl.DataFrame(
            {
                "a": [1, 2],
                "b": ["x", "y"],
            }
        )

        schema = pl.Schema(
            {
                "a": pl.Int64,
                "b": pl.Utf8,
                "c": pl.Float64,
            }
        )

        result = df1.cast_relaxed(schema)
        assert "c" in result.columns
        assert result["c"].null_count() == 2

    def test_delta(self):
        """Test delta method."""
        df1 = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": ["a", "b", "c"],
            }
        )

        df2 = pl.DataFrame(
            {
                "id": [2, 3, 4],
                "value": ["b", "c", "d"],
            }
        )

        result = df1.delta(df2)
        # Should contain only id=1 (not in df2)
        assert result.shape[0] == 1
        assert result["id"][0] == 1

    def test_drop_null_columns(self):
        """Test drop_null_columns method."""
        df = pl.DataFrame(
            {
                "keep": [1, 2, 3],
                "drop": [None, None, None],
            }
        )

        result = df.drop_null_columns()
        assert "keep" in result.columns
        assert "drop" not in result.columns

    def test_unify_schemas(self):
        """Test unify_schemas function."""
        df1 = pl.DataFrame(
            {
                "a": [1, 2],
                "b": ["x", "y"],
            }
        )

        df2 = pl.DataFrame(
            {
                "a": [3, 4],
                "c": [True, False],
            }
        )

        schema = unify_schemas([df1, df2])
        assert "a" in schema.names()
        assert "b" in schema.names()
        assert "c" in schema.names()


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_dataframe(self):
        """Test opt_dtype with empty DataFrame."""
        df = pl.DataFrame()
        result = opt_dtype(df)
        assert result.shape == df.shape

    def test_all_null_columns(self):
        """Test DataFrame with all null columns."""
        df = pl.DataFrame(
            {
                "all_null": [None, None, None],
                "mixed": [1, None, 3],
            }
        )

        result = opt_dtype(df)
        assert result.schema["all_null"] == pl.Null
        assert result.schema["mixed"] == pl.UInt64

    def test_mixed_datetime_formats(self):
        """Test mixed datetime formats in same column."""
        df = pl.DataFrame(
            {
                "mixed_dates": [
                    "2023-12-31",
                    "12/31/2023",
                    "31.12.2023",
                    "20231231",
                ],
            }
        )

        result = opt_dtype(df)
        assert result.schema["mixed_dates"] == pl.Datetime

    def test_special_float_values(self):
        """Test special float values (inf, nan)."""
        df = pl.DataFrame(
            {
                "floats": ["1.5", "inf", "-inf", "nan"],
            }
        )

        result = opt_dtype(df)
        assert result.schema["floats"] == pl.Float64

    def test_unicode_strings(self):
        """Test unicode string handling."""
        df = pl.DataFrame(
            {
                "unicode": ["café", "naïve", "résumé"],
            }
        )

        result = opt_dtype(df)
        assert result.schema["unicode"] == pl.Utf8
        assert result["unicode"].to_list() == ["café", "naïve", "résumé"]
