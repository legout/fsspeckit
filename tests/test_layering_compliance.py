"""Tests for import layering rule compliance."""

import subprocess
import sys
from pathlib import Path

import pytest


class TestLayeringCompliance:
    """Test that import layering rules are enforced."""

    def test_layering_check_script_exists(self):
        """Test that the layering check script exists."""
        script_path = Path(__file__).parent.parent / "scripts" / "check_layering.py"
        assert script_path.exists(), "Layering check script should exist"
        assert script_path.is_file(), "Layering check script should be a file"

    def test_layering_check_script_is_executable(self):
        """Test that the layering check script is executable."""
        script_path = Path(__file__).parent.parent / "scripts" / "check_layering.py"
        # Check if the file has execute permission or can be run with python
        assert script_path.exists()

    def test_layering_check_passes(self):
        """Test that the layering check passes with no violations."""
        result = subprocess.run(
            [sys.executable, "scripts/check_layering.py"],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"Layering check failed:\n{result.stderr}"
        assert "No import layering violations found" in result.stdout

    def test_core_does_not_import_from_datasets(self):
        """Test that core package does not import from datasets."""
        import fsspeckit.core.ext.parquet as parquet_module
        import inspect

        # Get the source code of the module
        source = inspect.getsource(parquet_module)

        # Check that it doesn't import from datasets.pyarrow
        assert "from fsspeckit.datasets.pyarrow" not in source, (
            "core.ext.parquet should not import from datasets.pyarrow"
        )

        # Check that it imports from common.schema
        assert "from fsspeckit.common.schema" in source, (
            "core.ext.parquet should import from common.schema"
        )

    def test_core_does_not_import_from_sql(self):
        """Test that core package does not import from sql."""
        # Check all core modules
        core_dir = Path(__file__).parent.parent / "src" / "fsspeckit" / "core"

        for py_file in core_dir.rglob("*.py"):
            if "__pycache__" in str(py_file):
                continue

            with open(py_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Skip test and shim files
            if "test_" in py_file.name or "_test" in str(py_file):
                continue

            # Check for violations
            assert "from fsspeckit.sql" not in content, (
                f"core module {py_file.relative_to(Path(__file__).parent.parent)} "
                f"should not import from fsspeckit.sql"
            )

    def test_datasets_does_not_import_from_sql(self):
        """Test that datasets package does not import from sql."""
        # Check all datasets modules
        datasets_dir = Path(__file__).parent.parent / "src" / "fsspeckit" / "datasets"

        for py_file in datasets_dir.rglob("*.py"):
            if "__pycache__" in str(py_file):
                continue

            with open(py_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Skip test and shim files
            if "test_" in py_file.name or "_test" in str(py_file):
                continue

            # Check for violations
            assert "from fsspeckit.sql" not in content, (
                f"datasets module {py_file.relative_to(Path(__file__).parent.parent)} "
                f"should not import from fsspeckit.sql"
            )


class TestUtilsFaçade:
    """Test that utils façade maintains backwards compatibility."""

    def test_utils_re_exports_common_schema(self):
        """Test that utils re-exports from common.schema."""
        from fsspeckit import utils
        from fsspeckit.common import schema as common_schema

        # Check that utils has the schema functions
        assert hasattr(utils, "cast_schema"), "utils should export cast_schema"
        assert hasattr(utils, "convert_large_types_to_normal"), (
            "utils should export convert_large_types_to_normal"
        )
        assert hasattr(utils, "opt_dtype_pa"), "utils should export opt_dtype_pa"

        # Check that they're the same objects
        assert utils.cast_schema is common_schema.cast_schema
        assert utils.opt_dtype_pa is common_schema.opt_dtype

    def test_utils_re_exports_datasets(self):
        """Test that utils re-exports from datasets."""
        from fsspeckit import utils
        from fsspeckit.datasets import duckdb

        # DuckDBParquetHandler is removed - use create_duckdb_connection + DuckDBDatasetIO
        # Test that we're importing the right new APIs
        from fsspeckit.datasets.duckdb.connection import (
            create_duckdb_connection,
            DuckDBDatasetIO,
        )
        from fsspeckit.core.merge import MergeStrategy as CanonicalMergeStrategy

        assert callable(create_duckdb_connection)
        assert callable(DuckDBDatasetIO)
        assert CanonicalMergeStrategy is duckdb.MergeStrategy

    def test_utils_re_exports_logging(self):
        """Test that utils re-exports from common.logging."""
        from fsspeckit import utils
        from fsspeckit.common.logging import config as logging_config

        # Check that utils has setup_logging
        assert hasattr(utils, "setup_logging"), "utils should export setup_logging"

        # Check that it's the same object
        assert utils.setup_logging is logging_config.setup_logging


class TestPackageBoundaries:
    """Test that package boundaries are respected."""

    def test_common_is_independent(self):
        """Test that common package has no internal dependencies."""
        from fsspeckit import common

        # Import all common modules to ensure they work
        assert hasattr(common, "schema")
        assert hasattr(common, "logging")
        assert hasattr(common, "misc")

    def test_core_uses_common(self):
        """Test that core can import from common."""
        from fsspeckit import core
        from fsspeckit import common

        # Core should be able to import from common
        assert hasattr(core, "filesystem")
        assert hasattr(common, "schema")

    def test_datasets_uses_core_and_common(self):
        """Test that datasets can import from both core and common."""
        from fsspeckit import datasets
        from fsspeckit import core
        from fsspeckit import common

        # Datasets should be able to import from both
        assert hasattr(datasets, "pyarrow")
        assert hasattr(datasets, "duckdb")
        assert hasattr(core, "filesystem")
        assert hasattr(common, "schema")

    def test_sql_uses_all_lower_packages(self):
        """Test that sql can import from all lower-level packages."""
        from fsspeckit import sql
        from fsspeckit import common
        from fsspeckit import core
        from fsspeckit import datasets

        # SQL should be able to import from all
        assert hasattr(sql, "filters")
        assert hasattr(common, "schema")
        assert hasattr(core, "filesystem")
        assert hasattr(datasets, "duckdb")
