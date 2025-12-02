"""Tests for shared partition utilities."""

import pytest
from fsspeckit.common.partitions import (
    get_partitions_from_path,
    normalize_partition_value,
    validate_partition_columns,
    build_partition_path,
    extract_partition_filters,
    filter_paths_by_partitions,
    infer_partitioning_scheme,
    get_partition_columns_from_paths,
    create_partition_expression,
    apply_partition_pruning,
)


class TestGetPartitionsFromPath:
    """Test partition extraction from file paths."""

    def test_hive_style_partitioning(self):
        """Test Hive-style partitioning."""
        path = "data/year=2023/month=01/day=15/file.parquet"
        result = get_partitions_from_path(path, "hive")

        expected = [("year", "2023"), ("month", "01"), ("day", "15")]
        assert result == expected

    def test_single_partition_column(self):
        """Test single partition column."""
        path = "data/2023/file.parquet"
        result = get_partitions_from_path(path, "year")

        expected = [("year", "2023")]
        assert result == expected

    def test_multiple_partition_columns(self):
        """Test multiple partition columns."""
        path = "data/2023/01/15/file.parquet"
        result = get_partitions_from_path(path, ["year", "month", "day"])

        expected = [("year", "2023"), ("month", "01"), ("day", "15")]
        assert result == expected

    def test_no_partitioning(self):
        """Test with no partitioning."""
        path = "data/file.parquet"
        result = get_partitions_from_path(path, None)

        assert result == []

    def test_with_filename_stripping(self):
        """Test that filename is stripped from path."""
        path = "data/year=2023/file.parquet"
        result = get_partitions_from_path(path, "hive")

        # Should not include filename in partitions
        assert all("=" in part for part, _ in result)

    def test_empty_path(self):
        """Test with empty path."""
        result = get_partitions_from_path("", "hive")

        assert result == []

    def test_path_without_directories(self):
        """Test path without directories."""
        path = "file.parquet"
        result = get_partitions_from_path(path, "year")

        assert result == []


class TestNormalizePartitionValue:
    """Test partition value normalization."""

    def test_basic_string(self):
        """Test basic string normalization."""
        result = normalize_partition_value('  "test"  ')

        assert result == "test"

    def test_with_quotes(self):
        """Test normalization with quotes."""
        result = normalize_partition_value("'quoted_value'")

        assert result == "quoted_value"

    def test_with_backslashes(self):
        """Test normalization with backslashes."""
        result = normalize_partition_value(r"test\value")

        assert result == "testvalue"

    def test_empty_string(self):
        """Test empty string normalization."""
        result = normalize_partition_value("")

        assert result == ""

    def test_none_value(self):
        """Test None value handling."""
        result = normalize_partition_value(None)

        # Should handle None gracefully
        assert result is None or result == ""


class TestValidatePartitionColumns:
    """Test partition column validation."""

    def test_valid_partitions(self):
        """Test with valid partitions."""
        partitions = [("year", "2023"), ("month", "01")]
        expected_columns = ["year", "month"]

        assert validate_partition_columns(partitions, expected_columns) is True

    def test_missing_expected_column(self):
        """Test with missing expected column."""
        partitions = [("year", "2023")]
        expected_columns = ["year", "month"]

        assert validate_partition_columns(partitions, expected_columns) is False

    def test_empty_column_names(self):
        """Test with empty column names."""
        partitions = [("", "2023"), ("month", "01")]

        assert validate_partition_columns(partitions, None) is False

    def test_no_expected_columns(self):
        """Test without expected columns (always valid)."""
        partitions = [("year", "2023"), ("month", "01")]

        assert validate_partition_columns(partitions, None) is True

    def test_empty_partitions(self):
        """Test with empty partitions."""
        partitions = []

        assert validate_partition_columns(partitions, None) is True


class TestBuildPartitionPath:
    """Test building partitioned paths."""

    def test_hive_style_path(self):
        """Test building Hive-style path."""
        base_path = "data"
        partitions = [("year", "2023"), ("month", "01")]

        result = build_partition_path(base_path, partitions, "hive")

        expected = "data/year=2023/month=01"
        assert result == expected

    def test_directory_style_path(self):
        """Test building directory-style path."""
        base_path = "data"
        partitions = [("year", "2023"), ("month", "01")]

        result = build_partition_path(base_path, partitions, "directory")

        expected = "data/2023/01"
        assert result == expected

    def test_empty_partitions(self):
        """Test with no partitions."""
        base_path = "data"
        partitions = []

        result = build_partition_path(base_path, partitions, "hive")

        assert result == "data"

    def test_base_path_trailing_slash(self):
        """Test base path with trailing slash."""
        base_path = "data/"
        partitions = [("year", "2023")]

        result = build_partition_path(base_path, partitions, "hive")

        assert result == "data/year=2023"


class TestExtractPartitionFilters:
    """Test extraction of partition filters from paths."""

    def test_extract_hive_partitions(self):
        """Test extracting Hive-style partitions."""
        paths = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2023/month=02/file2.parquet",
            "data/year=2024/month=01/file3.parquet",
        ]

        result = extract_partition_filters(paths, "hive")

        expected = {"year": {"2023", "2024"}, "month": {"01", "02"}}
        assert result == expected

    def test_extract_directory_partitions(self):
        """Test extracting directory-style partitions."""
        paths = [
            "data/2023/01/file1.parquet",
            "data/2023/02/file2.parquet",
            "data/2024/01/file3.parquet",
        ]

        result = extract_partition_filters(paths, ["year", "month"])

        expected = {"year": {"2023", "2024"}, "month": {"01", "02"}}
        assert result == expected

    def test_empty_paths(self):
        """Test with empty paths list."""
        result = extract_partition_filters([], "hive")

        assert result == {}


class TestFilterPathsByPartitions:
    """Test filtering paths by partitions."""

    def test_filter_single_value(self):
        """Test filtering by single partition value."""
        paths = [
            "data/year=2023/file1.parquet",
            "data/year=2024/file2.parquet",
            "data/year=2023/file3.parquet",
        ]
        filters = {"year": "2023"}

        result = filter_paths_by_partitions(paths, filters, "hive")

        expected = ["data/year=2023/file1.parquet", "data/year=2023/file3.parquet"]
        assert result == expected

    def test_filter_multiple_values(self):
        """Test filtering by multiple partition values."""
        paths = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2023/month=02/file2.parquet",
            "data/year=2024/month=01/file3.parquet",
        ]
        filters = {"year": "2023", "month": ["01", "02"]}

        result = filter_paths_by_partitions(paths, filters, "hive")

        expected = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2023/month=02/file2.parquet",
        ]
        assert result == expected

    def test_no_matching_paths(self):
        """Test with no matching paths."""
        paths = [
            "data/year=2023/file1.parquet",
            "data/year=2024/file2.parquet",
        ]
        filters = {"year": "2025"}

        result = filter_paths_by_partitions(paths, filters, "hive")

        assert result == []

    def test_missing_partition_in_path(self):
        """Test path missing the partition."""
        paths = [
            "data/file1.parquet",  # No year partition
            "data/year=2023/file2.parquet",
        ]
        filters = {"year": "2023"}

        result = filter_paths_by_partitions(paths, filters, "hive")

        expected = ["data/year=2023/file2.parquet"]
        assert result == expected


class TestInferPartitioningScheme:
    """Test partitioning scheme inference."""

    def test_infer_hive_scheme(self):
        """Test inferring Hive-style scheme."""
        paths = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2023/month=02/file2.parquet",
            "data/year=2024/month=01/file3.parquet",
        ]

        result = infer_partitioning_scheme(paths)

        assert result["scheme"] == "hive"
        assert result["confidence"] > 0.5
        assert "avg_partitions" in result

    def test_infer_directory_scheme(self):
        """Test inferring directory-style scheme."""
        paths = [
            "data/2023/01/file1.parquet",
            "data/2023/02/file2.parquet",
            "data/2024/01/file3.parquet",
        ]

        result = infer_partitioning_scheme(paths, ["year", "month"])

        assert result["scheme"] == "directory"
        assert result["confidence"] > 0.5
        assert "avg_partitions" in result

    def test_no_clear_scheme(self):
        """Test with no clear partitioning scheme."""
        paths = [
            "data/file1.parquet",
            "data/file2.parquet",
            "data/random/file3.parquet",
        ]

        result = infer_partitioning_scheme(paths)

        assert result["scheme"] is None
        assert result["confidence"] == 0.0

    def test_empty_paths(self):
        """Test with empty paths list."""
        result = infer_partitioning_scheme(paths)

        assert result["scheme"] is None
        assert result["confidence"] == 0.0


class TestGetPartitionColumnsFromPaths:
    """Test extraction of partition column names."""

    def test_extract_hive_columns(self):
        """Test extracting Hive-style column names."""
        paths = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2023/day=15/file2.parquet",
        ]

        result = get_partition_columns_from_paths(paths, "hive")

        expected = ["day", "month", "year"]  # Sorted
        assert result == expected

    def test_extract_directory_columns(self):
        """Test extracting directory-style column names."""
        paths = [
            "data/2023/01/file1.parquet",
            "data/2023/02/file2.parquet",
        ]

        result = get_partition_columns_from_paths(paths, ["year", "month"])

        expected = ["month", "year"]  # Sorted
        assert result == expected

    def test_no_partitions(self):
        """Test with no partitions."""
        paths = [
            "data/file1.parquet",
            "data/file2.parquet",
        ]

        result = get_partition_columns_from_paths(paths, "hive")

        assert result == []


class TestCreatePartitionExpression:
    """Test creation of partition filter expressions."""

    def test_pyarrow_expression(self):
        """Test PyArrow expression creation."""
        partitions = [("year", "2023"), ("month", "01")]

        result = create_partition_expression(partitions, "pyarrow")

        # Should return a PyArrow dataset expression
        assert result is not None
        # Note: Can't easily test the exact expression without importing pyarrow.dataset

    def test_duckdb_expression(self):
        """Test DuckDB expression creation."""
        partitions = [("year", "2023"), ("month", "01")]

        result = create_partition_expression(partitions, "duckdb")

        expected = "\"year\" = '2023' AND \"month\" = '01'"
        assert result == expected

    def test_empty_partitions(self):
        """Test with empty partitions."""
        result = create_partition_expression([], "pyarrow")

        assert result is None

    def test_unsupported_backend(self):
        """Test with unsupported backend."""
        partitions = [("year", "2023")]

        with pytest.raises(ValueError, match="Unsupported backend"):
            create_partition_expression(partitions, "unsupported")


class TestApplyPartitionPruning:
    """Test partition pruning functionality."""

    def test_pruning_with_filters(self):
        """Test pruning with active filters."""
        paths = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2023/month=02/file2.parquet",
            "data/year=2024/month=01/file3.parquet",
        ]
        filters = {"year": "2023"}

        result = apply_partition_pruning(paths, filters, "hive")

        expected = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2023/month=02/file2.parquet",
        ]
        assert result == expected

    def test_pruning_no_filters(self):
        """Test pruning with no filters."""
        paths = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2024/month=01/file2.parquet",
        ]

        result = apply_partition_pruning(paths, {}, "hive")

        # Should return all paths unchanged
        assert result == paths

    def test_pruning_no_matches(self):
        """Test pruning with no matching filters."""
        paths = [
            "data/year=2023/month=01/file1.parquet",
            "data/year=2024/month=01/file2.parquet",
        ]
        filters = {"year": "2025"}

        result = apply_partition_pruning(paths, filters, "hive")

        assert result == []
