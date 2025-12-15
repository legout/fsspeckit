"""Tests for mode/strategy precedence behavior in dataset handlers."""

import pytest
import pyarrow as pa
import tempfile
from pathlib import Path
import warnings

from fsspeckit.datasets.duckdb import DuckDBParquetHandler
from fsspeckit.datasets.pyarrow import PyarrowDatasetHandler


class TestModeStrategyPrecedence:
    """Test mode/strategy precedence where strategy takes precedence over mode."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table for testing."""
        return pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})

    @pytest.fixture
    def existing_dataset(self, tmp_path):
        """Create an existing dataset for merge testing."""
        dataset_dir = tmp_path / "existing_dataset"
        dataset_dir.mkdir()

        # Create initial data
        existing_data = pa.table({"id": [1, 2], "value": ["x", "y"]})

        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(existing_data, str(dataset_dir))

        return str(dataset_dir)

    def test_duckdb_upsert_with_append_mode_no_error(
        self, sample_table, existing_dataset
    ):
        """Test that DuckDB upsert with mode='append' doesn't raise error (strategy takes precedence)."""
        with DuckDBParquetHandler() as handler:
            # This should not raise an error - strategy takes precedence
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                handler.write_parquet_dataset(
                    sample_table,
                    existing_dataset,
                    strategy="upsert",
                    key_columns=["id"],
                    mode="append",  # Previously would cause error
                )

                # Should emit warning about precedence
                assert len(w) > 0
                assert "takes precedence" in str(w[0].message)
                assert "mode" in str(w[0].message).lower()

    def test_duckdb_update_with_overwrite_mode_no_error(
        self, sample_table, existing_dataset
    ):
        """Test that DuckDB update with mode='overwrite' doesn't raise error (strategy takes precedence)."""
        with DuckDBParquetHandler() as handler:
            # This should not raise an error - strategy takes precedence
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                handler.write_parquet_dataset(
                    sample_table,
                    existing_dataset,
                    strategy="update",
                    key_columns=["id"],
                    mode="overwrite",  # Explicit mode with strategy
                )

                # Should emit warning about precedence
                assert len(w) > 0
                assert "takes precedence" in str(w[0].message)

    def test_duckdb_insert_with_append_mode_allowed(self, sample_table, tmp_path):
        """Test that DuckDB insert with mode='append' still works as before."""
        dataset_dir = tmp_path / "dataset"

        with DuckDBParquetHandler() as handler:
            # This should work as before - insert + append is valid combination
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                handler.write_parquet_dataset(
                    sample_table,
                    str(dataset_dir),
                    strategy="insert",
                    key_columns=["id"],
                    mode="append",
                )

                # Should still emit warning about precedence even for valid combination
                assert len(w) > 0
                assert "takes precedence" in str(w[0].message)

    def test_pyarrow_upsert_with_append_mode_no_error(
        self, sample_table, existing_dataset
    ):
        """Test that PyArrow upsert with mode='append' doesn't raise error (strategy takes precedence)."""
        with PyarrowDatasetHandler() as handler:
            # This should not raise an error - strategy takes precedence
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                handler.write_parquet_dataset(
                    sample_table,
                    existing_dataset,
                    strategy="upsert",
                    key_columns=["id"],
                    mode="append",  # Previously would cause error
                )

                # Should emit warning about precedence
                assert len(w) > 0
                assert "takes precedence" in str(w[0].message)

    def test_pyarrow_update_with_overwrite_mode_no_error(
        self, sample_table, existing_dataset
    ):
        """Test that PyArrow update with mode='overwrite' doesn't raise error (strategy takes precedence)."""
        with PyarrowDatasetHandler() as handler:
            # This should not raise an error - strategy takes precedence
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                handler.write_parquet_dataset(
                    sample_table,
                    existing_dataset,
                    strategy="update",
                    key_columns=["id"],
                    mode="overwrite",  # Explicit mode with strategy
                )

                # Should emit warning about precedence
                assert len(w) > 0
                assert "takes precedence" in str(w[0].message)

    def test_mode_only_behavior_unchanged(self, sample_table, tmp_path):
        """Test that mode-only behavior (no strategy) remains unchanged."""
        dataset_dir = tmp_path / "dataset"

        # Test DuckDB
        with DuckDBParquetHandler() as duckdb_handler:
            # Mode-only should work as before
            duckdb_handler.write_parquet_dataset(
                sample_table, str(dataset_dir), mode="append"
            )

            # Should create parquet file
            files = list(Path(dataset_dir).glob("*.parquet"))
            assert len(files) > 0

        # Test PyArrow
        with PyarrowDatasetHandler() as pyarrow_handler:
            # Mode-only should work as before
            pyarrow_handler.write_parquet_dataset(
                sample_table, str(dataset_dir), mode="overwrite"
            )

            # Should work without warnings (no strategy provided)
            files = list(Path(dataset_dir).glob("*.parquet"))
            assert len(files) > 0

    def test_no_mode_no_strategy_behavior_unchanged(self, sample_table, tmp_path):
        """Test that default behavior (no mode, no strategy) remains unchanged."""
        dataset_dir = tmp_path / "dataset"

        # Test DuckDB - should default to append mode
        with DuckDBParquetHandler() as duckdb_handler:
            duckdb_handler.write_parquet_dataset(sample_table, str(dataset_dir))

            # Should create parquet file with append behavior
            files = list(Path(dataset_dir).glob("*.parquet"))
            assert len(files) > 0

        # Test PyArrow - should default to append mode
        with PyarrowDatasetHandler() as pyarrow_handler:
            pyarrow_handler.write_parquet_dataset(sample_table, str(dataset_dir))

            # Should create parquet file
            files = list(Path(dataset_dir).glob("*.parquet"))
            assert len(files) > 0

    def test_strategy_without_mode_no_warning(self, sample_table, existing_dataset):
        """Test that strategy without explicit mode doesn't emit warning."""
        with DuckDBParquetHandler() as handler:
            # Strategy without explicit mode should not emit warning
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                handler.write_parquet_dataset(
                    sample_table,
                    existing_dataset,
                    strategy="upsert",
                    key_columns=["id"],
                    # No mode specified
                )

                # Should not emit warning about mode/strategy precedence
                precedence_warnings = [
                    warning
                    for warning in w
                    if "takes precedence" in str(warning.message)
                ]
                assert len(precedence_warnings) == 0

    def test_duckdb_full_merge_with_append_mode(self, sample_table, existing_dataset):
        """Test DuckDB full_merge with mode='append' works without error."""
        with DuckDBParquetHandler() as handler:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                handler.write_parquet_dataset(
                    sample_table,
                    existing_dataset,
                    strategy="full_merge",
                    key_columns=["id"],
                    mode="append",  # Previously incompatible
                )

                # Should emit warning but not error
                assert len(w) > 0
                assert "takes precedence" in str(w[0].message)

    def test_pyarrow_deduplicate_with_append_mode(self, sample_table, existing_dataset):
        """Test PyArrow deduplicate with mode='append' works without error."""
        with PyarrowDatasetHandler() as handler:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                handler.write_parquet_dataset(
                    sample_table,
                    existing_dataset,
                    strategy="deduplicate",
                    key_columns=["id"],
                    mode="append",  # Previously incompatible
                )

                # Should emit warning but not error
                assert len(w) > 0
                assert "takes precedence" in str(w[0].message)
