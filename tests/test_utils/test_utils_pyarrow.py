"""Tests for pyarrow utility functions."""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
import polars as pl
from datetime import datetime

from fsspeckit.utils.pyarrow import (
    opt_dtype,
    unify_schemas,
    cast_schema,
    convert_large_types_to_normal,
    standardize_schema_timezones,
    standardize_schema_timezones_by_majority,
    dominant_timezone_per_column,
)


class TestOptDtype:
    """Test opt_dtype function for PyArrow Tables."""

    def test_basic_type_inference(self):
        """Test basic data type inference."""
        data = {
            "int_col": ["1", "2", "3", "4"],
            "float_col": ["1.5", "2.5", "3.5", "4.5"],
            "bool_col": ["true", "false", "yes", "no"],
            "str_col": ["a", "b", "c", "d"],
        }
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table)

        assert result.schema.field("int_col").type == pa.int64()
        assert result.schema.field("float_col").type == pa.float64()
        assert result.schema.field("bool_col").type == pa.bool_()
        assert result.schema.field("str_col").type == pa.string()

    def test_datetime_parsing(self):
        """Test datetime parsing with various formats."""
        data = {
            "iso_datetime": ["2023-12-31T23:59:59", "2024-01-01T00:00:00"],
            "us_date": ["12/31/2023", "01/01/2024"],
            "german_date": ["31.12.2023", "01.01.2024"],
            "compact": ["20231231", "20240101"],
            "with_tz": ["2023-12-31T23:59:59+01:00", "2024-01-01T00:00:00Z"],
        }
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table)

        assert pa.types.is_timestamp(result.schema.field("iso_datetime").type)
        assert pa.types.is_timestamp(result.schema.field("us_date").type)
        assert pa.types.is_timestamp(result.schema.field("german_date").type)
        assert pa.types.is_timestamp(result.schema.field("compact").type)
        assert pa.types.is_timestamp(result.schema.field("with_tz").type)

    def test_timezone_handling(self):
        """Test timezone parameter handling."""
        data = {
            "datetime": ["2023-12-31T23:59:59", "2024-01-01T00:00:00"],
            "datetime_tz": ["2023-12-31T23:59:59+01:00", "2024-01-01T00:00:00Z"],
        }
        table = pa.Table.from_pydict(data)

        # Test time_zone hint
        result = opt_dtype(table, time_zone="UTC")

        # Test force_timezone
        result_forced = opt_dtype(table, force_timezone="America/New_York")
        # Check that timezones are applied
        for field_name in result_forced.schema.names:
            if pa.types.is_timestamp(result_forced.schema.field(field_name).type):
                tz = result_forced.schema.field(field_name).type.tz
                assert tz == "America/New_York"

    def test_include_exclude_columns(self):
        """Test include and exclude parameters."""
        data = {
            "col1": ["1", "2", "3"],
            "col2": ["1.5", "2.5", "3.5"],
            "col3": ["a", "b", "c"],
        }
        table = pa.Table.from_pydict(data)

        # Test include
        result = opt_dtype(table, include=["col1", "col2"])
        assert result.schema.field("col1").type == pa.int64()
        assert result.schema.field("col2").type == pa.float64()
        assert result.schema.field("col3").type == pa.string()  # Unchanged

        # Test exclude
        result = opt_dtype(table, exclude=["col3"])
        assert result.schema.field("col1").type == pa.int64()
        assert result.schema.field("col2").type == pa.float64()
        assert result.schema.field("col3").type == pa.string()  # Unchanged

    def test_shrink_numerics(self):
        """Test numeric shrinking functionality."""
        data = {
            "small_int": ["1", "2", "3"],
            "large_int": ["100000", "200000", "300000"],
            "small_float": ["1.1", "2.2", "3.3"],
        }
        table = pa.Table.from_pydict(data)

        # With shrinking
        result = opt_dtype(table, shrink_numerics=True)
        assert result.schema.field("small_int").type == pa.uint8()
        assert result.schema.field("large_int").type == pa.uint32()
        assert result.schema.field("small_float").type == pa.float32()

        # Without shrinking
        result = opt_dtype(table, shrink_numerics=False)
        assert result.schema.field("small_int").type == pa.int64()
        assert result.schema.field("large_int").type == pa.int64()
        assert result.schema.field("small_float").type == pa.float64()

    def test_allow_unsigned(self):
        """Test unsigned integer type allowance."""
        data = {
            "positive": ["1", "2", "3"],
            "mixed": ["-1", "0", "1"],
        }
        table = pa.Table.from_pydict(data)

        # Allow unsigned (with shrinking)
        result = opt_dtype(table, allow_unsigned=True, shrink_numerics=True)
        assert result.schema.field("positive").type == pa.uint8()

        # Don't allow unsigned (with shrinking)
        result = opt_dtype(table, allow_unsigned=False, shrink_numerics=True)
        assert result.schema.field("positive").type == pa.int8()
        assert result.schema.field("mixed").type == pa.int8()

    def test_null_handling(self):
        """Test null-like value handling."""
        data = {
            "all_null": ["", "None", "null", "NaN"],
            "mixed_null": ["1", "", "2", "null"],
            "no_null": ["1", "2", "3", "4"],
        }
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table, allow_null=True)
        assert result.schema.field("all_null").type == pa.null()
        assert result.schema.field("mixed_null").type == pa.int64()
        assert result.schema.field("no_null").type == pa.int64()

        # Test with allow_null=False
        result = opt_dtype(table, allow_null=False)
        assert result.schema.field("all_null").type == pa.string()

    def test_use_large_dtypes(self):
        """Test large dtypes handling."""
        data = {
            "str_col": ["a", "b", "c"],
        }
        table = pa.Table.from_pydict(data)

        # Convert to large string first
        large_table = table.cast(pa.schema([pa.field("str_col", pa.large_string())]))

        # Without use_large_dtypes (default)
        result = opt_dtype(large_table, use_large_dtypes=False)
        assert result.schema.field("str_col").type == pa.string()

        # With use_large_dtypes
        result = opt_dtype(large_table, use_large_dtypes=True)
        assert result.schema.field("str_col").type == pa.large_string()

    def test_strict_mode(self):
        """Test strict error handling."""
        data = {
            "valid": ["1", "2", "3"],
            "invalid": ["1", "2", "invalid"],
        }
        table = pa.Table.from_pydict(data)

        # Non-strict mode (default)
        result = opt_dtype(table, strict=False)
        assert result.schema.field("valid").type == pa.int64()
        assert (
            result.schema.field("invalid").type == pa.string()
        )  # Falls back to string

        # Strict mode
        with pytest.raises(Exception):
            opt_dtype(table, strict=True)

    def test_sample_inference_applies_schema(self):
        """Sample should dictate schema and casting for remainder of column."""
        table = pa.Table.from_pydict({"value": ["1", "2", "foo", "bar"]})
        result = opt_dtype(table, sample_size=2, sample_method="first")

        assert result.schema.field("value").type == pa.int64()
        assert result.column("value").to_pylist() == [1, 2, None, None]

    def test_sampling_controls(self):
        """Custom sampling parameters should still accept the defaults."""
        table = pa.Table.from_pydict({"value": ["1", "2", "3"]})
        first_sample = opt_dtype(table, sample_size=2, sample_method="first")
        assert first_sample.schema.field("value").type == pa.int64()

        random_sample = opt_dtype(table, sample_size=2, sample_method="random")
        assert random_sample.schema.field("value").type == pa.int64()

        no_sample = opt_dtype(table, sample_size=None)
        assert no_sample.schema.field("value").type == pa.int64()

    def test_sampling_invalid_method(self):
        """Invalid sampling strategies raise immediately."""
        table = pa.Table.from_pydict({"value": ["1"]})
        with pytest.raises(ValueError):
            opt_dtype(table, sample_method="bad")


class TestSchemaFunctions:
    """Test schema manipulation functions."""

    def test_unify_schemas(self):
        """Test schema unification."""
        schema1 = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
            ]
        )
        schema2 = pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("c", pa.float64()),
            ]
        )

        unified = unify_schemas([schema1, schema2])
        assert "a" in unified.names
        assert "b" in unified.names
        assert "c" in unified.names

    def test_cast_schema(self):
        """Test schema casting."""
        data = {"a": [1, 2, 3], "b": ["x", "y", "z"]}
        table = pa.Table.from_pydict(data)

        target_schema = pa.schema(
            [
                pa.field("a", pa.float64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.int32()),
            ]
        )

        result = cast_schema(table, target_schema)
        assert result.schema.field("a").type == pa.float64()
        assert "c" in result.schema.names

    def test_convert_large_types_to_normal(self):
        """Test large type conversion."""
        schema = pa.schema(
            [
                pa.field("str_col", pa.large_string()),
                pa.field("bin_col", pa.large_binary()),
                pa.field("list_col", pa.large_list(pa.int64())),
            ]
        )

        converted = convert_large_types_to_normal(schema)

        assert converted.field("str_col").type == pa.string()
        assert converted.field("bin_col").type == pa.binary()
        assert converted.field("list_col").type == pa.list_(pa.int64())

    def test_dominant_timezone_per_column(self):
        """Test dominant timezone detection."""
        schema1 = pa.schema(
            [
                pa.field("ts", pa.timestamp("us", "UTC")),
            ]
        )
        schema2 = pa.schema(
            [
                pa.field("ts", pa.timestamp("us", "America/New_York")),
            ]
        )
        schema3 = pa.schema(
            [
                pa.field("ts", pa.timestamp("us", "UTC")),
            ]
        )

        dominant = dominant_timezone_per_column([schema1, schema2, schema3])
        assert dominant["ts"] == ("us", "UTC")

    def test_standardize_schema_timezones(self):
        """Test timezone standardization."""
        schema = pa.schema(
            [
                pa.field("ts1", pa.timestamp("us", "UTC")),
                pa.field("ts2", pa.timestamp("us", "America/New_York")),
                pa.field("ts3", pa.timestamp("us", None)),
            ]
        )

        # Standardize to UTC
        standardized = standardize_schema_timezones(schema, "UTC")
        for field in standardized:
            if pa.types.is_timestamp(field.type):
                assert field.type.tz == "UTC"

        # Remove timezones
        standardized = standardize_schema_timezones(schema, None)
        for field in standardized:
            if pa.types.is_timestamp(field.type):
                assert field.type.tz is None

    def test_standardize_schema_timezones_by_majority(self):
        """Test timezone standardization by majority."""
        schema1 = pa.schema(
            [
                pa.field("ts", pa.timestamp("us", "UTC")),
            ]
        )
        schema2 = pa.schema(
            [
                pa.field("ts", pa.timestamp("us", "UTC")),
            ]
        )
        schema3 = pa.schema(
            [
                pa.field("ts", pa.timestamp("us", "America/New_York")),
            ]
        )

        standardized = standardize_schema_timezones_by_majority(
            [schema1, schema2, schema3]
        )
        for field in standardized:
            if pa.types.is_timestamp(field.type):
                assert field.type.tz == "UTC"


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_table(self):
        """Test opt_dtype with empty table."""
        table = pa.Table.from_pydict({})
        result = opt_dtype(table)
        assert result.num_rows == 0
        assert result.num_columns == 0

    def test_all_null_columns(self):
        """Test table with all null columns."""
        data = {
            "all_null": [None, None, None],
            "mixed": [1, None, 3],
        }
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table)
        assert result.schema.field("all_null").type == pa.null()
        assert result.schema.field("mixed").type == pa.int64()

    def test_mixed_datetime_formats(self):
        """Test mixed datetime formats in same column."""
        data = {
            "mixed_dates": [
                "2023-12-31",
                "12/31/2023",
                "31.12.2023",
                "20231231",
            ],
        }
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table)
        assert pa.types.is_timestamp(result.schema.field("mixed_dates").type)

    def test_special_float_values(self):
        """Test special float values (inf, nan)."""
        data = {
            "floats": ["1.5", "inf", "-inf", "nan"],
        }
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table)
        assert result.schema.field("floats").type == pa.float64()

    def test_unicode_strings(self):
        """Test unicode string handling."""
        data = {
            "unicode": ["café", "naïve", "résumé"],
        }
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table)
        assert result.schema.field("unicode").type == pa.string()
        assert result["unicode"].to_pylist() == ["café", "naïve", "résumé"]

    def test_parallel_processing(self):
        """Test that parallel processing works correctly."""
        # Create a table with many columns to trigger parallel processing
        data = {f"col_{i}": ["1", "2", "3"] for i in range(20)}
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table)
        assert result.num_columns == 20
        for i in range(20):
            assert result.schema.field(f"col_{i}").type == pa.int64()

    def test_boolean_patterns(self):
        """Test various boolean pattern recognition."""
        data = {
            "bool_standard": ["true", "false", "TRUE", "FALSE"],
            "bool_numeric": ["1", "0", "1", "0"],
            "bool_words": ["yes", "no", "YES", "NO"],
            "bool_mixed": ["true", "0", "yes", "false"],
        }
        table = pa.Table.from_pydict(data)

        result = opt_dtype(table)
        assert result.schema.field("bool_standard").type == pa.bool_()
        assert result.schema.field("bool_numeric").type == pa.bool_()
        assert result.schema.field("bool_words").type == pa.bool_()
        assert result.schema.field("bool_mixed").type == pa.bool_()

    def test_integer_range_optimization(self):
        """Test integer type selection based on value range."""
        test_cases = [
            (["0", "1"], pa.uint8()),  # Small unsigned
            (["-1", "0", "1"], pa.int8()),  # Small signed
            (["0", "255"], pa.uint8()),  # Max uint8
            (["-128", "127"], pa.int8()),  # Max int8
            (["0", "256"], pa.uint16()),  # Exceeds uint8
            (["-129", "128"], pa.int16()),  # Exceeds int8
            (["-32768", "32767"], pa.int16()),  # Max int16
            (["0", "65535"], pa.uint16()),  # Max uint16
            (["-2147483648", "2147483647"], pa.int32()),  # Max int32
            (["0", "4294967295"], pa.uint32()),  # Max uint32
        ]

        for values, expected_type in test_cases:
            data = {"col": values}
            table = pa.Table.from_pydict(data)
            result = opt_dtype(table, shrink_numerics=True)
            assert result.schema.field("col").type == expected_type, (
                f"Failed for {values}"
            )
