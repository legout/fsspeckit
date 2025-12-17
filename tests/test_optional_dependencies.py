"""Tests for optional dependency management.

This module tests the lazy loading and error handling for optional dependencies
in fsspeckit, ensuring that:
1. Core functionality works without optional dependencies
2. Clear error messages when optional features are used without dependencies
3. Full functionality when all dependencies are available
"""

import importlib
import sys
from unittest.mock import patch

import pytest


class TestOptionalModule:
    """Test the optional dependency utilities module."""

    def test_optional_module_imports(self):
        """Test that the optional module imports successfully."""
        from fsspeckit.common import optional

        assert hasattr(optional, "_POLARS_AVAILABLE")
        assert hasattr(optional, "_PANDAS_AVAILABLE")
        assert hasattr(optional, "_PYARROW_AVAILABLE")
        assert hasattr(optional, "_DUCKDB_AVAILABLE")
        assert hasattr(optional, "_SQLGLOT_AVAILABLE")
        assert hasattr(optional, "_ORJSON_AVAILABLE")

    def test_availability_flags_are_boolean(self):
        """Test that all availability flags are boolean."""
        from fsspeckit.common.optional import (
            _DUCKDB_AVAILABLE,
            _ORJSON_AVAILABLE,
            _PANDAS_AVAILABLE,
            _POLARS_AVAILABLE,
            _PYARROW_AVAILABLE,
            _SQLGLOT_AVAILABLE,
        )

        assert isinstance(_POLARS_AVAILABLE, bool)
        assert isinstance(_PANDAS_AVAILABLE, bool)
        assert isinstance(_PYARROW_AVAILABLE, bool)
        assert isinstance(_DUCKDB_AVAILABLE, bool)
        assert isinstance(_SQLGLOT_AVAILABLE, bool)
        assert isinstance(_ORJSON_AVAILABLE, bool)

    def test_import_functions_exist(self):
        """Test that all import helper functions exist."""
        from fsspeckit.common import optional

        assert hasattr(optional, "_import_polars")
        assert hasattr(optional, "_import_pandas")
        assert hasattr(optional, "_import_pyarrow")
        assert hasattr(optional, "_import_duckdb")
        assert hasattr(optional, "_import_sqlglot")
        assert hasattr(optional, "_import_orjson")
        assert hasattr(optional, "check_optional_dependency")


class TestLazyImports:
    """Test lazy import functionality."""

    def test_import_polars_when_available(self):
        """Test importing polars when it's available."""
        from fsspeckit.common.optional import _POLARS_AVAILABLE, _import_polars

        if _POLARS_AVAILABLE:
            pl = _import_polars()
            assert pl is not None
            assert hasattr(pl, "DataFrame")
        else:
            pytest.skip("polars not installed")

    def test_import_pandas_when_available(self):
        """Test importing pandas when it's available."""
        from fsspeckit.common.optional import _PANDAS_AVAILABLE, _import_pandas

        if _PANDAS_AVAILABLE:
            pd = _import_pandas()
            assert pd is not None
            assert hasattr(pd, "DataFrame")
        else:
            pytest.skip("pandas not installed")

    def test_import_pyarrow_when_available(self):
        """Test importing pyarrow when it's available."""
        from fsspeckit.common.optional import _PYARROW_AVAILABLE, _import_pyarrow

        if _PYARROW_AVAILABLE:
            pa = _import_pyarrow()
            assert pa is not None
            assert hasattr(pa, "Table")
            assert hasattr(pa, "Schema")
        else:
            pytest.skip("pyarrow not installed")

    def test_import_duckdb_when_available(self):
        """Test importing duckdb when it's available."""
        from fsspeckit.common.optional import _DUCKDB_AVAILABLE, _import_duckdb

        if _DUCKDB_AVAILABLE:
            duckdb = _import_duckdb()
            assert duckdb is not None
            assert hasattr(duckdb, "connect")
        else:
            pytest.skip("duckdb not installed")

    def test_import_sqlglot_when_available(self):
        """Test importing sqlglot when it's available."""
        from fsspeckit.common.optional import _SQLGLOT_AVAILABLE, _import_sqlglot

        if _SQLGLOT_AVAILABLE:
            sqlglot = _import_sqlglot()
            assert sqlglot is not None
            assert hasattr(sqlglot, "parse_one")
        else:
            pytest.skip("sqlglot not installed")

    def test_import_orjson_when_available(self):
        """Test importing orjson when it's available."""
        from fsspeckit.common.optional import _ORJSON_AVAILABLE, _import_orjson

        if _ORJSON_AVAILABLE:
            orjson = _import_orjson()
            assert orjson is not None
            assert hasattr(orjson, "loads")
            assert hasattr(orjson, "dumps")
        else:
            pytest.skip("orjson not installed")

    def test_import_caching(self):
        """Test that imports are cached on subsequent calls."""
        from fsspeckit.common.optional import _PYARROW_AVAILABLE, _import_pyarrow

        if _PYARROW_AVAILABLE:
            pa1 = _import_pyarrow()
            pa2 = _import_pyarrow()
            # Should return the same cached module
            assert pa1 is pa2
        else:
            pytest.skip("pyarrow not installed")


class TestErrorMessages:
    """Test error messages when optional dependencies are missing."""

    def test_polars_error_message(self):
        """Test error message when polars is not available."""
        from fsspeckit.common import optional

        # Temporarily mark polars as unavailable
        original_available = optional._POLARS_AVAILABLE
        optional._POLARS_AVAILABLE = False

        try:
            with pytest.raises(ImportError, match="polars is required"):
                optional._import_polars()

            with pytest.raises(
                ImportError, match="pip install fsspeckit\\[datasets\\]"
            ):
                optional._import_polars()
        finally:
            optional._POLARS_AVAILABLE = original_available

    def test_pyarrow_error_message(self):
        """Test error message when pyarrow is not available."""
        from fsspeckit.common import optional

        original_available = optional._PYARROW_AVAILABLE
        optional._PYARROW_AVAILABLE = False

        try:
            with pytest.raises(ImportError, match="pyarrow is required"):
                optional._import_pyarrow()

            with pytest.raises(
                ImportError, match="pip install fsspeckit\\[datasets\\]"
            ):
                optional._import_pyarrow()
        finally:
            optional._PYARROW_AVAILABLE = original_available

    def test_duckdb_error_message(self):
        """Test error message when duckdb is not available."""
        from fsspeckit.common import optional

        original_available = optional._DUCKDB_AVAILABLE
        optional._DUCKDB_AVAILABLE = False

        try:
            with pytest.raises(ImportError, match="duckdb is required"):
                optional._import_duckdb()

            with pytest.raises(ImportError, match="pip install fsspeckit\\[sql\\]"):
                optional._import_duckdb()
        finally:
            optional._DUCKDB_AVAILABLE = original_available

    def test_check_optional_dependency(self):
        """Test the check_optional_dependency helper function."""
        from fsspeckit.common.optional import check_optional_dependency

        # This should not raise for installed packages
        # Test with a package that definitely exists in the test environment
        try:
            check_optional_dependency("pytest")
        except ImportError:
            pytest.fail("check_optional_dependency raised for installed package")

        # This should raise for non-existent package
        with pytest.raises(ImportError, match="nonexistent_package_xyz is required"):
            check_optional_dependency("nonexistent_package_xyz")

    def test_check_optional_dependency_extras_messages(self):
        """Test that check_optional_dependency provides correct extras guidance."""
        from fsspeckit.common.optional import check_optional_dependency

        from unittest.mock import patch
        
        # Test specific packages and their expected extras
        test_cases = [
            ("polars", "datasets"),
            ("pyarrow", "datasets"),
            ("duckdb", "sql"),
            ("sqlglot", "sql"),
            ("orjson", "sql"),
            ("joblib", "datasets"),
            ("fsspeckit_definitely_non_existent_package_12345", "full"),  # fallback case
        ]

        # Mock find_spec to return None, forcing check_optional_dependency to raise ImportError
        with patch("importlib.util.find_spec", return_value=None):
            for package_name, expected_extra in test_cases:
                with pytest.raises(ImportError) as exc_info:
                    check_optional_dependency(package_name)

                error_message = str(exc_info.value)
                assert (
                    f"Install with: pip install fsspeckit[{expected_extra}]"
                    in error_message
                ), (
                    f"Expected extras '{expected_extra}' not found in error message for {package_name}: {error_message}"
                )


class TestModuleImportsWithoutDependencies:
    """Test that core modules can be imported without optional dependencies."""

    def test_common_optional_imports(self):
        """Test that common.optional module imports."""
        from fsspeckit.common import optional

        assert optional is not None

    def test_common_types_imports(self):
        """Test that common.types module imports."""
        from fsspeckit.common import types

        assert types is not None
        assert hasattr(types, "dict_to_dataframe")
        assert hasattr(types, "to_pyarrow_table")

    def test_common_datetime_imports(self):
        """Test that common.datetime module imports."""
        from fsspeckit.common import datetime

        assert datetime is not None
        assert hasattr(datetime, "get_timestamp_column")
        assert hasattr(datetime, "get_timedelta_str")

    def test_datasets_pyarrow_imports(self):
        """Test that datasets.pyarrow module imports."""
        from fsspeckit.datasets import pyarrow

        assert pyarrow is not None

    def test_datasets_duckdb_imports(self):
        """Test that datasets.duckdb module imports."""
        from fsspeckit.datasets import duckdb

        assert duckdb is not None

    def test_core_merge_imports(self):
        """Test that core.merge module imports."""
        from fsspeckit.core import merge

        assert merge is not None
        assert hasattr(merge, "MergeStrategy")
        assert hasattr(merge, "MergePlan")
        assert hasattr(merge, "MergeStats")

    def test_sql_filters_imports(self):
        """Test that sql.filters module imports."""
        from fsspeckit.sql import filters

        assert filters is not None
        assert hasattr(filters, "sql2pyarrow_filter")
        assert hasattr(filters, "sql2polars_filter")


class TestFunctionalityWithDependencies:
    """Test that functionality works correctly when dependencies are available."""

    def test_dict_to_dataframe_with_polars(self):
        """Test dict_to_dataframe when polars is available."""
        from fsspeckit.common.optional import _POLARS_AVAILABLE
        from fsspeckit.common.types import dict_to_dataframe

        if not _POLARS_AVAILABLE:
            pytest.skip("polars not installed")

        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        result = dict_to_dataframe(data)

        assert result is not None
        assert len(result) == 2

    def test_to_pyarrow_table_conversion(self):
        """Test to_pyarrow_table when pyarrow is available."""
        from fsspeckit.common.optional import _PYARROW_AVAILABLE
        from fsspeckit.common.types import to_pyarrow_table

        if not _PYARROW_AVAILABLE:
            pytest.skip("pyarrow not installed")

        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        result = to_pyarrow_table(data, concat=True)

        assert result is not None
        assert result.num_rows == 2
        assert "a" in result.column_names
        assert "b" in result.column_names

    def test_merge_validation_with_pyarrow(self):
        """Test merge validation when pyarrow is available."""
        from fsspeckit.common.optional import _PYARROW_AVAILABLE

        if not _PYARROW_AVAILABLE:
            pytest.skip("pyarrow not installed")

        import pyarrow as pa

        from fsspeckit.core.merge import MergeStrategy, validate_merge_inputs

        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])

        plan = validate_merge_inputs(schema, None, ["id"], MergeStrategy.UPSERT)

        assert plan is not None
        assert plan.key_columns == ["id"]
        assert plan.strategy == MergeStrategy.UPSERT

    def test_sql_filter_with_pyarrow(self):
        """Test SQL filter conversion when pyarrow is available."""
        from fsspeckit.common.optional import _PYARROW_AVAILABLE, _SQLGLOT_AVAILABLE

        if not _PYARROW_AVAILABLE or not _SQLGLOT_AVAILABLE:
            pytest.skip("pyarrow or sqlglot not installed")

        import pyarrow as pa

        from fsspeckit.sql.filters import sql2pyarrow_filter

        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])

        filter_expr = sql2pyarrow_filter("id > 10", schema)

        assert filter_expr is not None


class TestBackwardCompatibility:
    """Test that existing code patterns still work."""

    def test_common_init_exports(self):
        """Test that common.__init__ exports work correctly."""
        from fsspeckit.common.optional import _POLARS_AVAILABLE

        if _POLARS_AVAILABLE:
            from fsspeckit.common import opt_dtype_pl, pl

            assert opt_dtype_pl is not None
            assert pl is not None
        else:
            # Should still import but be None
            from fsspeckit.common import opt_dtype_pl, pl

            assert opt_dtype_pl is None
            assert pl is None

    def test_datasets_init_exports(self):
        """Test that datasets.__init__ exports work correctly."""
        from fsspeckit import datasets

        assert datasets is not None

    def test_existing_import_patterns(self):
        """Test that existing import patterns still work."""
        # These should all work without errors
        from fsspeckit.common.datetime import get_timedelta_str
        from fsspeckit.common.misc import run_parallel
        from fsspeckit.core.merge import MergeStrategy

        assert get_timedelta_str is not None
        assert run_parallel is not None
        assert MergeStrategy is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
