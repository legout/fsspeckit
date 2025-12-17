"""Tests for shared schema utilities."""

import pytest
import pyarrow as pa
from fsspeckit.common.schema import (
    convert_large_types_to_normal,
    dominant_timezone_per_column,
    standardize_schema_timezones_by_majority,
    standardize_schema_timezones,
    cast_schema,
    remove_empty_columns,
    unify_schemas,
)


class TestConvertLargeTypesToNormal:
    """Test conversion of large types to standard types."""

    def test_large_string_to_string(self):
        """Test large_string conversion to string."""
        schema = pa.schema(
            [
                pa.field("large_str", pa.large_string()),
                pa.field("normal_str", pa.string()),
            ]
        )

        result = convert_large_types_to_normal(schema)

        assert result.field("large_str").type == pa.string()
        assert result.field("normal_str").type == pa.string()
        assert len(result) == 2

    def test_large_binary_to_binary(self):
        """Test large_binary conversion to binary."""
        schema = pa.schema(
            [
                pa.field("large_binary", pa.large_binary()),
                pa.field("normal_binary", pa.binary()),
            ]
        )

        result = convert_large_types_to_normal(schema)

        assert result.field("large_binary").type == pa.binary()
        assert result.field("normal_binary").type == pa.binary()

    def test_large_utf8_to_utf8(self):
        """Test large_utf8 conversion to utf8."""
        schema = pa.schema(
            [
                pa.field("large_utf8", pa.large_utf8()),
                pa.field("normal_utf8", pa.utf8()),
            ]
        )

        result = convert_large_types_to_normal(schema)

        assert result.field("large_utf8").type == pa.utf8()
        assert result.field("normal_utf8").type == pa.utf8()

    def test_large_list_to_list(self):
        """Test large_list conversion to list."""
        schema = pa.schema(
            [
                pa.field("large_list", pa.large_list(pa.int32())),
                pa.field("normal_list", pa.list_(pa.int32())),
            ]
        )

        result = convert_large_types_to_normal(schema)

        assert result.field("large_list").type == pa.list_(pa.int32())
        assert result.field("normal_list").type == pa.list_(pa.int32())

    def test_dictionary_with_large_value_type(self):
        """Test dictionary with large value type."""
        schema = pa.schema(
            [
                pa.field("dict_large", pa.dictionary(pa.int8(), pa.large_string())),
                pa.field("dict_normal", pa.dictionary(pa.int8(), pa.string())),
            ]
        )

        result = convert_large_types_to_normal(schema)

        assert result.field("dict_large").type.value_type == pa.string()
        assert result.field("dict_normal").type.value_type == pa.string()

    def test_preserve_metadata(self):
        """Test that field metadata is preserved."""
        metadata = {"key": "value"}
        schema = pa.schema(
            [
                pa.field("large_str", pa.large_string(), metadata=metadata),
            ]
        )

        result = convert_large_types_to_normal(schema)

        assert result.field("large_str").metadata == {b"key": b"value"}


class TestDominantTimezonePerColumn:
    """Test timezone detection across schemas."""

    def test_single_timezone(self):
        """Test with single timezone across all schemas."""
        schemas = [
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "UTC")),
                    pa.field("value", pa.int32()),
                ]
            ),
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "UTC")),
                    pa.field("value", pa.int32()),
                ]
            ),
        ]

        result = dominant_timezone_per_column(schemas)

        assert result["ts"] == ("us", "UTC")

    def test_mixed_timezones_with_majority(self):
        """Test mixed timezones with clear majority."""
        schemas = [
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "UTC")),
                ]
            ),
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "America/New_York")),
                ]
            ),
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "America/New_York")),
                ]
            ),
        ]

        result = dominant_timezone_per_column(schemas)

        assert result["ts"] == ("us", "America/New_York")

    def test_tie_between_none_and_timezone(self):
        """Test tie between None and timezone - should prefer timezone."""
        schemas = [
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", None)),  # No timezone
                ]
            ),
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "UTC")),  # Has timezone
                ]
            ),
        ]

        result = dominant_timezone_per_column(schemas)

        assert result["ts"] == ("us", "UTC")

    def test_no_timestamp_columns(self):
        """Test with no timestamp columns."""
        schemas = [
            pa.schema(
                [
                    pa.field("value", pa.int32()),
                    pa.field("name", pa.string()),
                ]
            ),
            pa.schema(
                [
                    pa.field("value", pa.int64()),
                    pa.field("name", pa.string()),
                ]
            ),
        ]

        result = dominant_timezone_per_column(schemas)

        assert result == {}


class TestStandardizeSchemaTimezones:
    """Test timezone standardization."""

    def test_remove_timezone(self):
        """Test removing timezone from all timestamp columns."""
        schema = pa.schema(
            [
                pa.field("ts_utc", pa.timestamp("us", "UTC")),
                pa.field("ts_ny", pa.timestamp("us", "America/New_York")),
                pa.field("value", pa.int32()),
            ]
        )

        result = standardize_schema_timezones(schema, timezone=None)

        assert result.field("ts_utc").type.tz is None
        assert result.field("ts_ny").type.tz is None
        assert result.field("value").type == pa.int32()

    def test_set_specific_timezone(self):
        """Test setting specific timezone."""
        schema = pa.schema(
            [
                pa.field("ts_none", pa.timestamp("us", None)),
                pa.field("ts_utc", pa.timestamp("us", "UTC")),
                pa.field("value", pa.int32()),
            ]
        )

        result = standardize_schema_timezones(schema, timezone="Europe/London")

        assert result.field("ts_none").type.tz == "Europe/London"
        assert result.field("ts_utc").type.tz == "Europe/London"
        assert result.field("value").type == pa.int32()

    def test_auto_timezone_with_list(self):
        """Test auto timezone detection with list of schemas."""
        schemas = [
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "UTC")),
                ]
            ),
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "UTC")),
                ]
            ),
            pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "America/New_York")),
                ]
            ),
        ]

        result = standardize_schema_timezones(schemas, timezone="auto")

        # Should have majority UTC (2 out of 3)
        assert result[0].field("ts").type.tz == "UTC"
        assert result[1].field("ts").type.tz == "UTC"
        assert result[2].field("ts").type.tz == "UTC"


class TestCastSchema:
    """Test schema casting."""

    def test_add_missing_column(self):
        """Test adding missing column to table."""
        schema = pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.string()),
            ]
        )
        table = pa.table(
            {
                "a": [1, 2, 3],
                "c": ["x", "y", "z"],  # Missing 'b', has 'c'
            }
        )

        result = cast_schema(table, schema)

        assert result.schema.names == ["a", "b", "c"]
        assert result.column("b").null_count == 3  # All null since missing

    def test_existing_columns_preserved(self):
        """Test that existing columns are preserved."""
        schema = pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.string()),
            ]
        )
        table = pa.table(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
            }
        )

        result = cast_schema(table, schema)

        assert result.schema.names == ["a", "b"]
        assert result.column("a").to_pylist() == [1, 2, 3]
        assert result.column("b").to_pylist() == ["x", "y", "z"]


class TestRemoveEmptyColumns:
    """Test empty column removal."""

    def test_remove_all_null_column(self):
        """Test removing column with all null values."""
        table = pa.table(
            {
                "a": [1, 2, 3],
                "b": [None, None, None],  # All null
                "c": ["x", "y", "z"],
            }
        )

        result = remove_empty_columns(table)

        assert result.column_names == ["a", "c"]
        assert result.num_columns == 2

    def test_preserve_non_empty_columns(self):
        """Test that non-empty columns are preserved."""
        table = pa.table(
            {
                "a": [1, 2, 3],
                "b": [None, "x", None],  # Not all null
                "c": ["x", "y", "z"],
            }
        )

        result = remove_empty_columns(table)

        assert result.column_names == ["a", "b", "c"]
        assert result.num_columns == 3

    def test_empty_table(self):
        """Test with empty table."""
        table = pa.table(
            {
                "a": [],
                "b": [],
            }
        )

        result = remove_empty_columns(table)

        # Empty table should be returned unchanged
        assert result.column_names == ["a", "b"]
        assert result.num_rows == 0


class TestUnifySchemas:
    """Test schema unification."""

    def test_single_schema(self):
        """Test unification of single schema."""
        schemas = [
            pa.schema(
                [
                    pa.field("a", pa.int32()),
                    pa.field("b", pa.string()),
                ]
            )
        ]

        result = unify_schemas(schemas)

        assert result.field("a").type == pa.int32()
        assert result.field("b").type == pa.string()

    def test_compatible_numeric_types(self):
        """Test unification of compatible numeric types."""
        schemas = [
            pa.schema([pa.field("value", pa.int8())]),
            pa.schema([pa.field("value", pa.int16())]),
            pa.schema([pa.field("value", pa.int32())]),
        ]

        result = unify_schemas(schemas)

        # Should promote to largest type (int32)
        assert result.field("value").type == pa.int32()

    def test_mixed_signed_unsigned_integers(self):
        """Test mixed signed/unsigned integers."""
        schemas = [
            pa.schema([pa.field("value", pa.int8())]),
            pa.schema([pa.field("value", pa.uint8())]),
        ]

        result = unify_schemas(schemas)

        # Should promote to float due to signed/unsigned mix
        assert pa.types.is_floating(result.field("value").type)

    def test_string_vs_numeric_conflict(self):
        """Test string vs numeric type conflict."""
        schemas = [
            pa.schema([pa.field("value", pa.int32())]),
            pa.schema([pa.field("value", pa.string())]),
        ]

        result = unify_schemas(schemas)

        # Should default to string for safety
        assert result.field("value").type == pa.string()

    def test_temporal_type_unification(self):
        """Test temporal type unification."""
        schemas = [
            pa.schema([pa.field("date_val", pa.date32())]),
            pa.schema([pa.field("date_val", pa.date64())]),
        ]

        result = unify_schemas(schemas)

        # Should promote to higher precision (date64)
        assert result.field("date_val").type == pa.date64()

    def test_with_timezone_standardization(self):
        """Test unification with timezone standardization."""
        schemas = [
            pa.schema([pa.field("ts", pa.timestamp("us", "UTC"))]),
            pa.schema([pa.field("ts", pa.timestamp("us", "America/New_York"))]),
            pa.schema([pa.field("ts", pa.timestamp("us", None))]),
        ]

        result = unify_schemas(schemas, standardize_timezones=True)

        # Should standardize to majority timezone (UTC)
        assert result.field("ts").type.tz == "UTC"

    def test_large_types_option(self):
        """Test preserve large types option."""
        schemas = [
            pa.schema([pa.field("text", pa.large_string())]),
            pa.schema([pa.field("text", pa.string())]),
        ]

        # With large types preserved
        result_large = unify_schemas(schemas, use_large_dtypes=True)
        assert result_large.field("text").type == pa.large_string()

        # With large types converted
        result_normal = unify_schemas(schemas, use_large_dtypes=False)
        assert result_normal.field("text").type == pa.string()

    def test_verbose_logging(self, capsys):
        """Test verbose conflict logging."""
        schemas = [
            pa.schema([pa.field("value", pa.int32())]),
            pa.schema([pa.field("value", pa.string())]),
        ]

        result = unify_schemas(schemas, verbose=True)

        # Should still produce a result
        assert result.field("value").type == pa.string()

    def test_remove_conflicting_columns_option(self):
        """Test remove conflicting columns option."""
        schemas = [
            pa.schema([pa.field("keep", pa.int32())]),
            pa.schema([pa.field("keep", pa.int64())]),  # Compatible
            pa.schema([pa.field("conflict", pa.int32())]),
            pa.schema([pa.field("conflict", pa.string())]),  # Incompatible
        ]

        result = unify_schemas(schemas, remove_conflicting_columns=True)

        # Should keep 'keep' (compatible) and remove 'conflict' (incompatible)
        assert "keep" in result.names
        assert "conflict" not in result.names
