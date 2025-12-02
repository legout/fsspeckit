"""Integration tests to verify consistent behavior across backends."""

import pytest
import tempfile
from pathlib import Path

# These tests will verify that DuckDB and PyArrow backends
# produce consistent results when using shared schema and partition logic


class TestBackendConsistency:
    """Test consistency between DuckDB and PyArrow backends."""

    def test_schema_unification_consistency(self):
        """Test that both backends handle schema unification consistently."""
        # This test will verify that when both backends are given
        # the same schemas with conflicts, they produce compatible results

        # For now, we'll test that the shared schema utilities work
        # The actual backend consistency testing will require more setup

        # Test that shared schema utilities can be imported and used
        try:
            from fsspeckit.common.schema import unify_schemas
            import pyarrow as pa

            schema1 = pa.schema([pa.field("value", pa.int32())])
            schema2 = pa.schema([pa.field("value", pa.string())])

            result = unify_schemas([schema1, schema2])

            # Should produce a valid unified schema
            assert result is not None
            assert "value" in result.names

        except ImportError:
            pytest.skip("Shared schema utilities not available during transition")

    def test_partition_parsing_consistency(self):
        """Test that partition parsing is consistent across backends."""
        try:
            from fsspeckit.common.partitions import get_partitions_from_path

            # Test Hive-style partitioning
            path = "data/year=2023/month=01/file.parquet"
            result = get_partitions_from_path(path, "hive")

            expected = [("year", "2023"), ("month", "01")]
            assert result == expected

        except ImportError:
            pytest.skip("Shared partition utilities not available during transition")

    def test_maintenance_stats_consistency(self):
        """Test that maintenance stats are consistent."""
        try:
            from fsspeckit.core.maintenance import MaintenanceStats

            # Test that MaintenanceStats can be created and validated
            stats = MaintenanceStats(
                before_file_count=5,
                after_file_count=3,
                before_total_bytes=1000,
                after_total_bytes=800,
                compacted_file_count=2,
                rewritten_bytes=500,
            )

            assert stats.before_file_count == 5
            assert stats.after_file_count == 3
            assert stats.before_total_bytes == 1000
            assert stats.after_total_bytes == 800
            assert stats.compacted_file_count == 2
            assert stats.rewritten_bytes == 500

            # Test to_dict conversion
            stats_dict = stats.to_dict()
            assert stats_dict["before_file_count"] == 5
            assert stats_dict["after_file_count"] == 3

        except ImportError:
            pytest.skip("Core maintenance utilities not available during transition")


class TestSharedUtilitiesIntegration:
    """Test integration of shared utilities with existing code."""

    def test_schema_and_partition_integration(self):
        """Test that schema and partition utilities work together."""
        try:
            from fsspeckit.common.schema import (
                unify_schemas,
                standardize_schema_timezones,
            )
            from fsspeckit.common.partitions import (
                get_partitions_from_path,
                infer_partitioning_scheme,
            )
            import pyarrow as pa

            # Create test schemas with timestamp columns
            schema1 = pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "UTC")),
                    pa.field("value", pa.int32()),
                ]
            )
            schema2 = pa.schema(
                [
                    pa.field("ts", pa.timestamp("us", "America/New_York")),
                    pa.field("value", pa.int32()),
                ]
            )

            # Test schema unification with timezone standardization
            unified = unify_schemas([schema1, schema2], standardize_timezones=True)
            assert unified is not None

            # Test partition inference on sample paths
            paths = [
                "data/year=2023/month=01/file1.parquet",
                "data/year=2023/month=02/file2.parquet",
            ]
            scheme = infer_partitioning_scheme(paths)
            assert scheme["scheme"] == "hive"

        except ImportError:
            pytest.skip("Shared utilities not available during transition")

    def test_error_handling_consistency(self):
        """Test that error handling is consistent."""
        try:
            from fsspeckit.common.schema import unify_schemas
            from fsspeckit.common.partitions import get_partitions_from_path
            import pyarrow as pa

            # Test schema unification error handling
            with pytest.raises(
                ValueError, match="At least one schema must be provided"
            ):
                unify_schemas([])

            # Test partition parsing with invalid input
            result = get_partitions_from_path("invalid/path", "hive")
            assert isinstance(result, list)

        except ImportError:
            pytest.skip("Shared utilities not available during transition")
