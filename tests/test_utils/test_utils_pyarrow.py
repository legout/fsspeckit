"""Tests for pyarrow utility functions."""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime

from fsspeckit.datasets.pyarrow import (
    opt_dtype,
    unify_schemas,
    cast_schema,
    convert_large_types_to_normal,
    standardize_schema_timezones,
    standardize_schema_timezones_by_majority,
    dominant_timezone_per_column,
    collect_dataset_stats_pyarrow,
    compact_parquet_dataset_pyarrow,
    optimize_parquet_dataset_pyarrow,
    merge_parquet_dataset_pyarrow,
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


class TestParquetDatasetMaintenance:
    """Tests for PyArrow-based parquet dataset maintenance helpers."""

    def test_collect_dataset_stats_pyarrow_local(self, tmp_path):
        """collect_dataset_stats_pyarrow should see files and basic stats."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table({"id": [1, 2, 3]})
        file1 = path / "part-0.parquet"
        file2 = path / "nested" / "part-1.parquet"
        file2.parent.mkdir()
        pq.write_table(table, file1)
        pq.write_table(table, file2)

        from fsspeckit.datasets.pyarrow import collect_dataset_stats_pyarrow

        stats = collect_dataset_stats_pyarrow(str(path))
        assert stats["total_rows"] == 6
        assert stats["total_bytes"] > 0
        assert len(stats["files"]) == 2

    def test_compact_parquet_dataset_pyarrow_dry_run_and_live(self, tmp_path):
        """compact_parquet_dataset_pyarrow should reduce file count while preserving rows."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table({"id": list(range(10))})
        # Create four small files
        for i in range(4):
            pq.write_table(table, path / f"part-{i}.parquet")

        from fsspeckit.datasets.pyarrow import compact_parquet_dataset_pyarrow

        files_before = sorted(p.name for p in path.glob("*.parquet"))
        dry = compact_parquet_dataset_pyarrow(
            str(path), target_rows_per_file=25, dry_run=True
        )
        assert dry["before_file_count"] == 4
        assert dry["after_file_count"] <= 4
        assert dry["dry_run"] is True
        assert dry["planned_groups"]
        assert sorted(p.name for p in path.glob("*.parquet")) == files_before

        live = compact_parquet_dataset_pyarrow(
            str(path), target_rows_per_file=25, dry_run=False
        )
        assert live["dry_run"] is False
        # Still 40 rows total
        stats_after = collect_dataset_stats_pyarrow(str(path))
        assert stats_after["total_rows"] == 40
        assert live["after_file_count"] <= 4

    def test_optimize_parquet_dataset_pyarrow_dry_run(self, tmp_path):
        """optimize_parquet_dataset_pyarrow dry-run should return a plan."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table(
            {
                "group": [1, 2, 1, 2],
                "id": [3, 4, 1, 2],
            }
        )
        pq.write_table(table, path / "part-0.parquet")

        from fsspeckit.datasets.pyarrow import optimize_parquet_dataset_pyarrow

        files_before = sorted(p.name for p in path.glob("*.parquet"))
        dry = optimize_parquet_dataset_pyarrow(
            str(path),
            zorder_columns=["group", "id"],
            target_rows_per_file=2,
            dry_run=True,
        )
        assert dry["before_file_count"] == 1
        assert dry["after_file_count"] >= 1
        assert dry["dry_run"] is True
        assert dry["zorder_columns"] == ["group", "id"]
        assert dry["planned_groups"]
        assert sorted(p.name for p in path.glob("*.parquet")) == files_before

    def test_optimize_parquet_dataset_pyarrow_live(self, tmp_path):
        """optimize_parquet_dataset_pyarrow should rewrite clustered files."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table(
            {
                "group": [2, 1, 2, 1],
                "id": [4, 1, 3, 2],
            }
        )
        pq.write_table(table, path / "part-0.parquet")

        from fsspeckit.datasets.pyarrow import (
            optimize_parquet_dataset_pyarrow,
            collect_dataset_stats_pyarrow,
        )

        result = optimize_parquet_dataset_pyarrow(
            str(path),
            zorder_columns=["group", "id"],
            target_rows_per_file=2,
            dry_run=False,
        )
        assert result["dry_run"] is False
        stats = collect_dataset_stats_pyarrow(str(path))
        assert stats["total_rows"] == 4

    def test_compact_parquet_dataset_pyarrow_with_dual_thresholds(self, tmp_path):
        """Compaction should respect both target MB and row thresholds simultaneously."""
        path = tmp_path / "dataset"
        path.mkdir()
        payload = "x" * 2048  # ~2 KB per value keeps files small but non-trivial
        rows_per_file = 40
        for idx in range(5):
            table = pa.table(
                {
                    "id": list(range(idx * rows_per_file, (idx + 1) * rows_per_file)),
                    "payload": [payload] * rows_per_file,
                }
            )
            pq.write_table(table, path / f"chunk-{idx}.parquet")

        stats = collect_dataset_stats_pyarrow(str(path))
        info_by_path = {str(info["path"]): info for info in stats["files"]}

        threshold_mb = 2
        dry = compact_parquet_dataset_pyarrow(
            str(path),
            target_mb_per_file=threshold_mb,
            target_rows_per_file=80,
            dry_run=True,
        )

        assert dry["planned_groups"], "Expected planned compaction groups"
        bytes_threshold = threshold_mb * 1024 * 1024
        for group in dry["planned_groups"]:
            group_rows = sum(int(info_by_path[file]["num_rows"]) for file in group)
            group_bytes = sum(int(info_by_path[file]["size_bytes"]) for file in group)
            assert group_rows <= 80
            assert group_bytes <= bytes_threshold

    def test_compact_parquet_dataset_pyarrow_partition_filter(self, tmp_path):
        """Partition filters must restrict compaction scope to matching prefixes."""
        base = tmp_path / "dataset"
        for partition in ("date=2025-11-14", "date=2025-11-15"):
            part_dir = base / partition
            part_dir.mkdir(parents=True, exist_ok=True)
            for idx in range(3):
                table = pa.table(
                    {
                        "date": [partition.split("=")[1]] * 5,
                        "value": list(range(idx * 5, idx * 5 + 5)),
                    }
                )
                pq.write_table(table, part_dir / f"part-{idx}.parquet")

        untouched_partition = base / "date=2025-11-15"
        before_other = sorted(p.name for p in untouched_partition.glob("*.parquet"))

        live = compact_parquet_dataset_pyarrow(
            str(base),
            target_rows_per_file=10,
            partition_filter=["date=2025-11-14"],
            dry_run=False,
        )

        assert live["dry_run"] is False
        assert (
            sorted(p.name for p in untouched_partition.glob("*.parquet"))
            == before_other
        )
        compacted = list(base.glob("compact-*.parquet"))
        assert compacted, "Filtered partition should receive rewritten files"

        stats = collect_dataset_stats_pyarrow(str(base))
        assert stats["total_rows"] == 2 * 3 * 5

    def test_compact_parquet_dataset_pyarrow_many_small_files(self, tmp_path):
        """Compaction should handle many tiny files without exhausting memory."""
        path = tmp_path / "dataset"
        path.mkdir()
        total_rows = 0
        for idx in range(24):
            table = pa.table({"id": list(range(idx * 4, idx * 4 + 4))})
            pq.write_table(table, path / f"part-{idx:03d}.parquet")
            total_rows += table.num_rows

        result = compact_parquet_dataset_pyarrow(
            str(path), target_rows_per_file=20, dry_run=False
        )

        assert result["after_file_count"] < result["before_file_count"]
        stats = collect_dataset_stats_pyarrow(str(path))
        assert stats["total_rows"] == total_rows

    def test_optimize_parquet_dataset_pyarrow_enforces_order(self, tmp_path):
        """Each optimized file should be ordered by the provided z-order columns."""
        path = tmp_path / "dataset"
        path.mkdir()
        tables = [
            pa.table({"group": [2, 1], "value": [4, 3]}),
            pa.table({"group": [1, 2], "value": [2, 5]}),
            pa.table({"group": [3, 1], "value": [8, 1]}),
            pa.table({"group": [2, 3], "value": [7, 6]}),
        ]
        for idx, table in enumerate(tables):
            pq.write_table(table, path / f"part-{idx}.parquet")

        result = optimize_parquet_dataset_pyarrow(
            str(path),
            zorder_columns=["group", "value"],
            target_rows_per_file=2,
        )

        assert result["dry_run"] is False
        optimized_files = sorted(path.glob("optimized-*.parquet"))
        assert optimized_files, "Optimizer should rewrite dataset"
        for file_path in optimized_files:
            table = pq.read_table(file_path)
            pairs = list(
                zip(
                    table.column("group").to_pylist(), table.column("value").to_pylist()
                )
            )
            assert pairs == sorted(pairs)

    def test_merge_parquet_dataset_pyarrow_simple(self, tmp_path):
        """merge_parquet_dataset_pyarrow should upsert rows with minimal input."""
        path = tmp_path / "merge-target"
        path.mkdir()
        pq.write_table(
            pa.table({"id": [1], "value": ["base"]}), path / "part-0.parquet"
        )
        source = pa.table({"id": [1, 2], "value": ["new", "insert"]})

        stats = merge_parquet_dataset_pyarrow(
            source,
            str(path),
            key_columns="id",
            strategy="upsert",
        )

        assert stats["inserted"] == 1
        assert stats["updated"] == 1
        table = ds.dataset(str(path)).to_table()
        values = dict(
            zip(table.column("id").to_pylist(), table.column("value").to_pylist())
        )
        assert values == {1: "new", 2: "insert"}

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


class TestPyArrowCanonicalStatsStructure:
    """Tests for canonical MaintenanceStats structure in PyArrow maintenance operations."""

    def test_compact_canonical_stats_structure(self, tmp_path):
        """Verify compact_parquet_dataset_pyarrow returns canonical stats structure."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table({"id": list(range(20))})

        # Create 3 small files to trigger compaction
        for i in range(3):
            pq.write_table(table, path / f"part-{i}.parquet")

        result = compact_parquet_dataset_pyarrow(
            str(path), target_rows_per_file=15, dry_run=False
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
        assert result["dry_run"] is False

    def test_optimize_canonical_stats_structure(self, tmp_path):
        """Verify optimize_parquet_dataset_pyarrow returns canonical stats structure."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table({"group": [1, 2, 1, 2], "id": [3, 4, 1, 2]})
        pq.write_table(table, path / "part-0.parquet")

        result = optimize_parquet_dataset_pyarrow(
            str(path),
            zorder_columns=["group", "id"],
            target_rows_per_file=2,
            dry_run=False,
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

    def test_compact_dry_run_canonical_structure(self, tmp_path):
        """Verify compact dry run includes planned_groups in canonical structure."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table({"id": list(range(10))})

        # Create 4 small files to trigger grouping
        for i in range(4):
            pq.write_table(table, path / f"part-{i}.parquet")

        result = compact_parquet_dataset_pyarrow(
            str(path), target_rows_per_file=15, dry_run=True
        )

        # Dry run should include planning metadata
        assert result["dry_run"] is True
        assert "planned_groups" in result
        assert isinstance(result["planned_groups"], list)

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
            "planned_groups",
        ]
        for key in canonical_keys:
            assert key in result, f"Missing canonical key in dry run: {key}"

    def test_optimize_dry_run_canonical_structure(self, tmp_path):
        """Verify optimization dry run includes all required fields."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table({"group": [1, 2], "id": [1, 2]})
        pq.write_table(table, path / "part-0.parquet")

        result = optimize_parquet_dataset_pyarrow(
            str(path),
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
            assert key in result, (
                f"Missing canonical key in optimization dry run: {key}"
            )

    def test_canonical_stats_logical_consistency(self, tmp_path):
        """Test logical consistency of canonical stats across operations."""
        path = tmp_path / "dataset"
        path.mkdir()
        table = pa.table({"id": list(range(5)), "value": [float(x) for x in range(5)]})

        # Create initial files
        for i in range(2):
            pq.write_table(table, path / f"part-{i}.parquet")

        # Test compaction logical consistency
        result = compact_parquet_dataset_pyarrow(
            str(path), target_rows_per_file=20, dry_run=False
        )

        # After compaction: should have fewer files but same total content
        assert result["after_file_count"] <= result["before_file_count"]
        assert result["compacted_file_count"] > 0
        assert result["rewritten_bytes"] > 0

        # Re-read to verify actual filesystem state
        stats_after = collect_dataset_stats_pyarrow(str(path))
        assert result["after_file_count"] == len(stats_after["files"])
        assert result["after_total_bytes"] == stats_after["total_bytes"]
