"""Tests for mode/strategy compatibility validation in dataset handlers."""

import pytest
import pyarrow as pa
import tempfile
from pathlib import Path

from fsspeckit.datasets.duckdb import DuckDBParquetHandler
from fsspeckit.datasets.pyarrow import PyarrowDatasetHandler


class TestModeStrategyCompatibility:
    """Test mode and strategy compatibility validation."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table for testing."""
        return pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})

    def test_duckdb_append_with_insert_strategy_allowed(self, sample_table):
        """Test that mode='append' with strategy='insert' is allowed for DuckDB."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with DuckDBParquetHandler() as handler:
                # Should not raise any exception
                handler.write_parquet_dataset(
                    sample_table,
                    dataset_dir,
                    mode="append",
                    strategy="insert",
                    key_columns=["id"],
                )

                # Verify file was created
                files = list(Path(dataset_dir).glob("*.parquet"))
                assert len(files) > 0

    def test_duckdb_append_with_rewrite_strategies_rejected(self, sample_table):
        """Test that mode='append' with rewrite strategies is rejected for DuckDB."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            rewrite_strategies = ["upsert", "update", "full_merge", "deduplicate"]

            with DuckDBParquetHandler() as handler:
                for strategy in rewrite_strategies:
                    with pytest.raises(
                        ValueError, match="mode='append' is not compatible"
                    ):
                        handler.write_parquet_dataset(
                            sample_table,
                            dataset_dir,
                            mode="append",
                            strategy=strategy,
                            key_columns=["id"],
                        )

    def test_duckdb_overwrite_with_all_strategies_allowed(self, sample_table):
        """Test that mode='overwrite' with any strategy is allowed for DuckDB."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            all_strategies = ["insert", "upsert", "update", "full_merge", "deduplicate"]

            with DuckDBParquetHandler() as handler:
                for strategy in all_strategies:
                    # Should not raise any exception
                    handler.write_parquet_dataset(
                        sample_table,
                        dataset_dir,
                        mode="overwrite",
                        strategy=strategy,
                        key_columns=["id"],
                    )

    def test_pyarrow_append_with_insert_strategy_allowed(self, sample_table):
        """Test that mode='append' with strategy='insert' is allowed for PyArrow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with PyarrowDatasetHandler() as handler:
                # Should not raise any exception
                handler.write_parquet_dataset(
                    sample_table,
                    dataset_dir,
                    mode="append",
                    strategy="insert",
                    key_columns=["id"],
                )

                # Verify file was created
                files = list(Path(dataset_dir).glob("*.parquet"))
                assert len(files) > 0

    def test_pyarrow_append_with_rewrite_strategies_rejected(self, sample_table):
        """Test that mode='append' with rewrite strategies is rejected for PyArrow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            rewrite_strategies = ["upsert", "update", "full_merge", "deduplicate"]

            with PyarrowDatasetHandler() as handler:
                for strategy in rewrite_strategies:
                    with pytest.raises(
                        ValueError, match="mode='append' is not compatible"
                    ):
                        handler.write_parquet_dataset(
                            sample_table,
                            dataset_dir,
                            mode="append",
                            strategy=strategy,
                            key_columns=["id"],
                        )

    def test_pyarrow_overwrite_with_all_strategies_allowed(self, sample_table):
        """Test that mode='overwrite' with any strategy is allowed for PyArrow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            all_strategies = ["insert", "upsert", "update", "full_merge", "deduplicate"]

            with PyarrowDatasetHandler() as handler:
                for strategy in all_strategies:
                    # Should not raise any exception
                    handler.write_parquet_dataset(
                        sample_table,
                        dataset_dir,
                        mode="overwrite",
                        strategy=strategy,
                        key_columns=["id"],
                    )

    def test_error_messages_are_clear_and_actionable(self, sample_table):
        """Test that error messages provide clear guidance to users."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_dir = f"{temp_dir}/dataset"

            with DuckDBParquetHandler() as handler:
                with pytest.raises(ValueError) as exc_info:
                    handler.write_parquet_dataset(
                        sample_table,
                        dataset_dir,
                        mode="append",
                        strategy="upsert",
                        key_columns=["id"],
                    )

                error_message = str(exc_info.value)

                # Check that error message contains key information
                assert "mode='append' is not compatible" in error_message
                assert "strategy='upsert'" in error_message
                assert "Use mode='overwrite'" in error_message
                assert "strategy='insert'" in error_message
