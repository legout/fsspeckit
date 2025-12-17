"""Tests for incremental parquet dataset core utilities (non-legacy)."""

import tempfile

import pytest


class TestIncrementalCoreImports:
    def test_incremental_module_imports(self):
        """Incremental core module remains importable."""
        from fsspeckit.core.incremental import (
            ConservativeMembershipChecker,
            IncrementalFileManager,
            ParquetMetadataAnalyzer,
            PartitionPruner,
            confirm_affected_files,
            list_dataset_files,
            plan_incremental_rewrite,
        )

        assert ParquetMetadataAnalyzer is not None
        assert PartitionPruner is not None
        assert ConservativeMembershipChecker is not None
        assert IncrementalFileManager is not None
        assert plan_incremental_rewrite is not None
        assert confirm_affected_files is not None
        assert list_dataset_files is not None


class TestParquetMetadataAnalyzer:
    def test_analyze_empty_directory(self):
        """Analyzer returns empty for directories without parquet files."""
        from fsspeckit.core.incremental import ParquetMetadataAnalyzer

        analyzer = ParquetMetadataAnalyzer()
        with tempfile.TemporaryDirectory() as temp_dir:
            metadata_list = analyzer.analyze_dataset_files(temp_dir)
            assert metadata_list == []


class TestPartitionPruner:
    def test_identify_candidate_files_no_files(self):
        from fsspeckit.core.incremental import PartitionPruner

        pruner = PartitionPruner()
        assert pruner.identify_candidate_files([], ["id"], [1, 2, 3]) == []


if __name__ == "__main__":
    pytest.main([__file__])
