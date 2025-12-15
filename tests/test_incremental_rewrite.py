"""Tests for incremental rewrite functionality."""

import pytest
import tempfile
import pyarrow as pa
from pathlib import Path

from fsspeckit.core.merge import MergeStrategy, validate_rewrite_mode_compatibility
from fsspeckit.datasets.duckdb import DuckDBParquetHandler
from fsspeckit.datasets.pyarrow import PyarrowDatasetHandler


class TestIncrementalRewriteValidation:
    """Test rewrite_mode validation logic."""

    def test_validate_rewrite_mode_compatibility_upsert_incremental(self):
        """Test that UPSERT with incremental rewrite is valid."""
        # Should not raise any exception
        validate_rewrite_mode_compatibility(MergeStrategy.UPSERT, "incremental")

    def test_validate_rewrite_mode_compatibility_update_incremental(self):
        """Test that UPDATE with incremental rewrite is valid."""
        # Should not raise any exception
        validate_rewrite_mode_compatibility(MergeStrategy.UPDATE, "incremental")

    def test_validate_rewrite_mode_compatibility_insert_incremental_invalid(self):
        """Test that INSERT with incremental rewrite raises ValueError."""
        with pytest.raises(
            ValueError, match="rewrite_mode='incremental' is not supported"
        ):
            validate_rewrite_mode_compatibility(MergeStrategy.INSERT, "incremental")

    def test_validate_rewrite_mode_compatibility_full_merge_incremental_invalid(self):
        """Test that FULL_MERGE with incremental rewrite raises ValueError."""
        with pytest.raises(
            ValueError, match="rewrite_mode='incremental' is not supported"
        ):
            validate_rewrite_mode_compatibility(MergeStrategy.FULL_MERGE, "incremental")

    def test_validate_rewrite_mode_compatibility_deduplicate_incremental_invalid(self):
        """Test that DEDUPLICATE with incremental rewrite raises ValueError."""
        with pytest.raises(
            ValueError, match="rewrite_mode='incremental' is not supported"
        ):
            validate_rewrite_mode_compatibility(
                MergeStrategy.DEDUPLICATE, "incremental"
            )

    def test_validate_rewrite_mode_compatibility_full_mode_valid(self):
        """Test that any strategy with rewrite_mode='full' is valid."""
        for strategy in MergeStrategy:
            # Should not raise any exception
            validate_rewrite_mode_compatibility(strategy, "full")


class TestIncrementalRewriteIntegration:
    """Test incremental rewrite integration with handlers."""

    def test_duckdb_handler_accepts_rewrite_mode_parameter(self):
        """Test that DuckDB handler accepts rewrite_mode parameter."""
        handler = DuckDBParquetHandler()

        # Should not raise exception for method signature
        try:
            # This will fail due to missing connection, but should accept the parameter
            handler.write_parquet_dataset(
                pa.table({"id": [1], "value": ["test"]}),
                "/tmp/test",
                strategy="upsert",
                key_columns=["id"],
                rewrite_mode="incremental",
            )
        except Exception as e:
            # We expect this to fail due to missing connection, but not due to parameter issues
            assert "rewrite_mode" not in str(e).lower()

    def test_pyarrow_handler_accepts_rewrite_mode_parameter(self):
        """Test that PyArrow handler accepts rewrite_mode parameter."""
        handler = PyarrowDatasetHandler()

        # Should not raise exception for method signature
        try:
            # This will fail due to filesystem issues, but should accept the parameter
            handler.write_parquet_dataset(
                pa.table({"id": [1], "value": ["test"]}),
                "/tmp/test",
                strategy="upsert",
                key_columns=["id"],
                rewrite_mode="incremental",
            )
        except Exception as e:
            # We expect this to fail due to filesystem issues, but not due to parameter issues
            assert "rewrite_mode" not in str(e).lower()

    def test_incremental_rewrite_with_invalid_strategy_duckdb(self):
        """Test that DuckDB handler rejects invalid strategy combinations."""
        handler = DuckDBParquetHandler()

        with tempfile.TemporaryDirectory() as temp_dir:
            test_data = pa.table({"id": [1, 2], "value": ["a", "b"]})

            # Should raise ValueError for unsupported strategy
            with pytest.raises(
                ValueError, match="rewrite_mode='incremental' is not supported"
            ):
                handler.write_parquet_dataset(
                    test_data,
                    temp_dir,
                    strategy="insert",  # Not supported with incremental
                    key_columns=["id"],
                    rewrite_mode="incremental",
                )

    def test_incremental_rewrite_with_invalid_strategy_pyarrow(self):
        """Test that PyArrow handler rejects invalid strategy combinations."""
        handler = PyarrowDatasetHandler()

        with tempfile.TemporaryDirectory() as temp_dir:
            test_data = pa.table({"id": [1, 2], "value": ["a", "b"]})

            # Should raise ValueError for unsupported strategy
            with pytest.raises(
                ValueError, match="rewrite_mode='incremental' is not supported"
            ):
                handler.write_parquet_dataset(
                    test_data,
                    temp_dir,
                    strategy="insert",  # Not supported with incremental
                    key_columns=["id"],
                    rewrite_mode="incremental",
                )


class TestIncrementalRewriteMetadata:
    """Test incremental rewrite metadata planning functionality."""

    def test_incremental_module_imports(self):
        """Test that the incremental module can be imported."""
        try:
            from fsspeckit.core.incremental import (
                ParquetMetadataAnalyzer,
                PartitionPruner,
                ConservativeMembershipChecker,
                IncrementalFileManager,
                plan_incremental_rewrite,
            )

            # All classes and functions should be importable
            assert ParquetMetadataAnalyzer is not None
            assert PartitionPruner is not None
            assert ConservativeMembershipChecker is not None
            assert IncrementalFileManager is not None
            assert plan_incremental_rewrite is not None
        except ImportError:
            pytest.skip("Incremental module not available")

    def test_metadata_analyzer_basic_functionality(self):
        """Test basic functionality of ParquetMetadataAnalyzer."""
        try:
            from fsspeckit.core.incremental import ParquetMetadataAnalyzer

            analyzer = ParquetMetadataAnalyzer()
            assert analyzer is not None

            # Test with empty directory
            with tempfile.TemporaryDirectory() as temp_dir:
                metadata_list = analyzer.analyze_dataset_files(temp_dir)
                assert metadata_list == []  # No parquet files

        except ImportError:
            pytest.skip("Incremental module not available")

    def test_partition_pruner_basic_functionality(self):
        """Test basic functionality of PartitionPruner."""
        try:
            from fsspeckit.core.incremental import PartitionPruner, ParquetFileMetadata

            pruner = PartitionPruner()
            assert pruner is not None

            # Test with no files
            candidate_files = pruner.identify_candidate_files([], ["id"], [1, 2, 3])
            assert candidate_files == []

        except ImportError:
            pytest.skip("Incremental module not available")


if __name__ == "__main__":
    pytest.main([__file__])
