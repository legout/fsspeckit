"""Tests for fsspeckit.utils backwards compatibility.

This module tests that fsspeckit.utils correctly re-exports symbols
from canonical domain packages and maintains backwards compatibility.
"""

import pytest


class TestUtilsBackwardsCompatibility:
    """Test that fsspeckit.utils re-exports match canonical implementations."""

    def test_utils_re_exports_match_canonical(self):
        """Test that utils re-exports refer to same objects as canonical modules."""
        # DuckDBParquetHandler is removed - users should use create_duckdb_connection + DuckDBDatasetIO
        # Test that we're importing the right new APIs
        from fsspeckit.datasets.duckdb import create_duckdb_connection, DuckDBDatasetIO
        from fsspeckit.datasets.duckdb import MergeStrategy as CanonicalMergeStrategy

        assert callable(create_duckdb_connection)
        assert callable(DuckDBDatasetIO)
        assert MergeStrategy is CanonicalMergeStrategy

        # Test common utilities
        from fsspeckit.utils import setup_logging, run_parallel
        from fsspeckit.common.logging import setup_logging as CanonicalSetupLogging
        from fsspeckit.common.misc import run_parallel as CanonicalRunParallel

        assert setup_logging is CanonicalSetupLogging
        assert run_parallel is CanonicalRunParallel

        # Test polars utilities
        from fsspeckit.utils import opt_dtype_pl, pl
        from fsspeckit.common.polars import (
            opt_dtype as CanonicalOptDtypePl,
            pl as CanonicalPl,
        )

        assert opt_dtype_pl is CanonicalOptDtypePl
        assert pl is CanonicalPl

        # Test type utilities
        from fsspeckit.utils import to_pyarrow_table, dict_to_dataframe
        from fsspeckit.common.types import (
            to_pyarrow_table as CanonicalToPyArrowTable,
            dict_to_dataframe as CanonicalDictToDataFrame,
        )

        assert to_pyarrow_table is CanonicalToPyArrowTable
        assert dict_to_dataframe is CanonicalDictToDataFrame

        # Test Progress from utils.misc shim
        from fsspeckit.utils.misc import Progress
        from rich.progress import Progress as CanonicalProgress

        assert Progress is CanonicalProgress

    def test_utils_all_exports(self):
        """Test that all expected symbols are in utils.__all__."""
        from fsspeckit.utils import __all__ as utils_all

        expected_exports = {
            "setup_logging",
            "run_parallel",
            "get_partitions_from_path",
            "to_pyarrow_table",
            "dict_to_dataframe",
            "opt_dtype_pl",
            "opt_dtype_pa",
            "cast_schema",
            "convert_large_types_to_normal",
            "pl",
            "sync_dir",
            "sync_files",
            "DuckDBParquetHandler",
            "Progress",
        }

        assert set(utils_all) == expected_exports

    def test_utils_misc_shim_exports(self):
        """Test that utils.misc shim exports expected symbols."""
        from fsspeckit.utils.misc import __all__ as misc_all

        expected_misc_exports = {
            "get_partitions_from_path",
            "run_parallel",
            "sync_dir",
            "sync_files",
            "Progress",
        }

        assert set(misc_all) == expected_misc_exports
